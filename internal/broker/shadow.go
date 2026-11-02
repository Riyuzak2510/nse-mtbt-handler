// Package broker provides shadow consumers for independent replay of broker streams.
package broker

import (
	"context"
	"crypto/rand"
	"fmt"
	"github.com/nats-io/nats.go"
	"nse-mtbt-handler/internal/runconfig"
	"strconv"
	"sync"
	"time"
)

// Shadow is a new independent ephemeral consumer for each process/rebuild.
// It embeds the shared Client connection and maintains its own isolated subscription state.
type Shadow struct {
	*Client
	Subscription  *nats.Subscription
	Name          string
	ReplayThrough uint64
	once          sync.Once
	closeErr      error
}

// OpenShadow creates a new shadow consumer for independent stream replay.
// The stream must already exist (matching the broker's configuration).
// No stream or book consumer is modified; one isolated ephemeral consumer is created.
// Returns an error if:
//   - The stream doesn't exist
//   - FirstSeq > 1 (expired stream prefix)
//   - Messages == 0 && LastSeq > 0 (expired stream)
//   - Stream configuration doesn't match broker expectations
func OpenShadow(ctx context.Context, url string, m runconfig.Manifest) (*Shadow, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}

	d, err := m.Digest()
	if err != nil {
		return nil, err
	}

	streamID := strconv.Itoa(int(m.StreamID))
	nc, err := nats.Connect(url, nats.Name("shadow-"+streamID), nats.Timeout(3*time.Second), nats.NoReconnect())
	if err != nil {
		return nil, fmt.Errorf("connect NATS: %w", err)
	}

	js, err := nc.JetStream(nats.MaxWait(3 * time.Second))
	if err != nil {
		nc.Close()
		return nil, err
	}

	c := &Client{nc, js, "MTBT_" + strconv.Itoa(int(m.StreamID)), "mtbt." + strconv.Itoa(int(m.StreamID)), m, d}

	// Verify stream exists and has expected configuration.
	// Do not create or modify; only validate existing state.
	info, err := c.JS.StreamInfo(c.Stream, nats.Context(ctx))
	if err != nil {
		nc.Close()
		if err == nats.ErrStreamNotFound {
			return nil, fmt.Errorf("stream does not exist: %s", c.Stream)
		}
		return nil, fmt.Errorf("stream info: %w", err)
	}

	// Reject if stream has expired prefix (FirstSeq > 1)
	if info.State.FirstSeq > 1 {
		nc.Close()
		return nil, fmt.Errorf("stream has expired prefix: FirstSeq=%d", info.State.FirstSeq)
	}

	// Reject if stream is empty and has seen messages (Msgs == 0 && LastSeq > 0)
	if info.State.Msgs == 0 && info.State.LastSeq > 0 {
		nc.Close()
		return nil, fmt.Errorf("stream is expired (no messages, LastSeq=%d)", info.State.LastSeq)
	}

	// Verify stream configuration matches broker expectations
	maxBytes, err := StreamMaxBytes()
	if err != nil {
		nc.Close()
		return nil, err
	}
	v := info.Config
	if v.Description != "mtbt-manifest-v2:"+c.digest || len(v.Subjects) != 1 || v.Subjects[0] != c.Subject || v.Retention != nats.LimitsPolicy || v.Storage != nats.FileStorage || v.Discard != nats.DiscardNew || v.MaxAge != 24*time.Hour || v.MaxBytes != maxBytes || v.Replicas != 1 || v.MaxMsgSize != 65535 {
		nc.Close()
		return nil, fmt.Errorf("existing stream has incompatible configuration")
	}

	// Generate unique ephemeral consumer name with crypto/rand hex.
	consumerName, err := generateShadowName()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("generate consumer name: %w", err)
	}

	// Create shadow consumer with explicit ACK policy for isolated replay.
	// MaxAckPending=1 ensures bounded in-flight messages and prevents unbounded queuing.
	// DeliverPolicy=DeliverAllPolicy starts from the beginning of the stream.
	// ReplayPolicy=ReplayInstantPolicy replays at maximum speed without rate limiting.
	consumerConfig := &nats.ConsumerConfig{
		Name:              consumerName,
		Durable:           "",
		AckPolicy:         nats.AckExplicitPolicy,
		AckWait:           30 * time.Second,
		MaxAckPending:     1,
		DeliverPolicy:     nats.DeliverAllPolicy,
		ReplayPolicy:      nats.ReplayInstantPolicy,
		FilterSubject:     c.Subject,
		InactiveThreshold: 30 * time.Second,
		MaxWaiting:        1,
		Replicas:          1,
	}

	ci, err := c.JS.AddConsumer(c.Stream, consumerConfig, nats.Context(ctx))
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("create shadow consumer: %w", err)
	}

	// Pull subscribe using the ephemeral consumer name.
	// Bind ensures NATS API support and that the name is ephemeral (not a durable).
	sub, err := c.JS.PullSubscribe(
		c.Subject,
		"",
		nats.Bind(c.Stream, ci.Name),
	)
	if err != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = c.JS.DeleteConsumer(c.Stream, ci.Name, nats.Context(cleanup))
		cancel()
		nc.Close()
		return nil, fmt.Errorf("pull subscribe: %w", err)
	}

	// ReplayThrough tracks the last sequence seen at startup for continuity hints.
	replayThrough := info.State.LastSeq

	shadow := &Shadow{
		Client:        c,
		Subscription:  sub,
		Name:          ci.Name,
		ReplayThrough: replayThrough,
	}

	return shadow, nil
}

// generateShadowName creates a unique "shadow-..." consumer name using crypto/rand hex.
func generateShadowName() (string, error) {
	b := make([]byte, 8)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("shadow-%x", b), nil
}

// Close idempotently cleans up the shadow consumer and connection.
// It unsubscribes the subscription, deletes the consumer (with 3s timeout),
// then closes the connection. Idempotent via sync.Once.
func (s *Shadow) Close() error {
	s.once.Do(func() {
		if s.Subscription != nil {
			s.Subscription.Unsubscribe()
		}

		// Delete the ephemeral consumer with 3s timeout.
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		if err := s.JS.DeleteConsumer(s.Stream, s.Name, nats.Context(ctx)); err != nil && err != nats.ErrConsumerNotFound {
			s.closeErr = err
		}

		s.Conn.Close()
	})
	return s.closeErr
}
