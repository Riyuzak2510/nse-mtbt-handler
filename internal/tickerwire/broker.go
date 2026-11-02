package tickerwire

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	"nse-mtbt-handler/internal/runconfig"
	"strconv"
	"time"
)

const (
	DurablePrefix   = "ticker-"
	MaxAckPending   = 1
	AckWait         = 30 * time.Second
	DuplicateWindow = 10 * time.Minute
	MaxMessageBytes = 16 * 1024
	Replicas        = 1
)

// Bus wraps a JetStream connection for ticker-scoped command routing.
// All streams/subjects follow the per-token naming convention.
type Bus struct {
	conn     *nats.Conn
	js       nats.JetStreamContext
	stream   string
	manifest runconfig.Manifest
	digest   string
}

// Open creates a new Bus, ensuring stream and consumer configuration matches manifest constraints.
// Refuses incompatible existing state rather than altering it.
// Stream: file storage, 24h retention, 64MiB limit, DiscardNew, 16KiB max message.
// The manifest must be passed explicitly since broker.Client does not expose it.
func Open(ctx context.Context, conn *nats.Conn, js nats.JetStreamContext, m runconfig.Manifest) (*Bus, error) {
	if conn == nil || js == nil {
		return nil, fmt.Errorf("invalid connection/jetstream")
	}

	// Validate manifest
	if err := m.Validate(); err != nil {
		return nil, fmt.Errorf("invalid manifest: %w", err)
	}

	manifestDigest, err := m.Digest()
	if err != nil {
		return nil, fmt.Errorf("manifest digest: %w", err)
	}

	streamID := m.StreamID
	stream := StreamName(streamID)

	// Ensure stream exists with correct config
	streamConfig := &nats.StreamConfig{
		Name:        stream,
		Subjects:    []string{fmt.Sprintf("mtbtmatch.%d.*", streamID)}, // mtbtmatch.<stream>.*
		Storage:     nats.FileStorage,
		Retention:   nats.LimitsPolicy,
		Discard:     nats.DiscardNew,
		MaxAge:      24 * time.Hour,
		MaxBytes:    64 << 20, // 64 MiB
		MaxMsgSize:  MaxMessageBytes,
		Replicas:    Replicas,
		Duplicates:  DuplicateWindow,
		Description: "tickerwire-manifest-v1:" + manifestDigest,
	}

	info, err := js.StreamInfo(stream, nats.Context(ctx))
	if err == nats.ErrStreamNotFound {
		info, err = js.AddStream(streamConfig, nats.Context(ctx))
		if err != nil {
			// Attempt to retrieve again; if it exists, proceed with validation
			info, err = js.StreamInfo(stream, nats.Context(ctx))
		}
	}

	if err != nil {
		return nil, fmt.Errorf("stream info: %w", err)
	}

	// Validate stream config is compatible
	if err := validateStreamConfig(&info.Config, streamConfig); err != nil {
		return nil, fmt.Errorf("stream incompatible: %w", err)
	}

	return &Bus{
		conn:     conn,
		js:       js,
		stream:   stream,
		manifest: m,
		digest:   manifestDigest,
	}, nil
}

// validateStreamConfig ensures the existing stream matches desired config.
func validateStreamConfig(actual, desired *nats.StreamConfig) error {
	if actual.Description != desired.Description || actual.NoAck || actual.MaxMsgsPerSubject > 0 || actual.MaxMsgs > 0 || len(actual.Subjects) != 1 || len(desired.Subjects) != 1 || actual.Subjects[0] != desired.Subjects[0] || actual.Name != desired.Name ||
		len(actual.Subjects) != len(desired.Subjects) ||
		actual.Storage != desired.Storage ||
		actual.Retention != desired.Retention ||
		actual.Discard != desired.Discard ||
		actual.MaxAge != desired.MaxAge ||
		actual.MaxBytes != desired.MaxBytes ||
		actual.MaxMsgSize != desired.MaxMsgSize ||
		actual.Replicas != desired.Replicas ||
		actual.Duplicates != desired.Duplicates {
		return fmt.Errorf("stream config mismatch")
	}
	if actual.Mirror != nil || len(actual.Sources) != 0 || actual.Sealed {
		return fmt.Errorf("stream has incompatible features")
	}
	return nil
}

// Publish validates the command, encodes it, and publishes with Nats-Msg-Id.
// Expected stream and nonzero sequence validation performed.
func (b *Bus) Publish(ctx context.Context, c Command) error {
	if b == nil || b.conn == nil {
		return fmt.Errorf("bus not connected")
	}

	if err := Validate(b.manifest, c); err != nil {
		return fmt.Errorf("validate: %w", err)
	}

	data, err := Encode(c)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}

	subject := Subject(c.StreamID, c.Token)
	msg := nats.NewMsg(subject)
	msg.Data = data
	msg.Header.Set(nats.MsgIdHdr, ID(c))

	ack, err := b.js.PublishMsg(msg, nats.Context(ctx), nats.ExpectStream(b.stream))
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	if ack == nil || ack.Stream != b.stream || ack.Sequence == 0 {
		return fmt.Errorf("invalid publish ack")
	}

	return nil
}

// Subscribe creates an explicit pull subscription for a specific token.
// Durable name: ticker-<token>
// Explicit ACK, MaxAckPending=1, AckWait=30s, DeliverAll, ReplayInstant.
// Validates existing consumer settings; never DeliverNew.
func (b *Bus) Subscribe(ctx context.Context, token int32) (*nats.Subscription, error) {
	if b == nil || b.js == nil {
		return nil, fmt.Errorf("bus not connected")
	}

	if token <= 0 {
		return nil, fmt.Errorf("token must be positive")
	}

	if !b.manifest.TokenAllowed(token) {
		return nil, fmt.Errorf("token %d not allowed", token)
	}

	streamID := b.manifest.StreamID
	subject := Subject(streamID, token)
	durable := DurablePrefix + strconv.Itoa(int(token))

	// Define desired consumer config
	consumerConfig := &nats.ConsumerConfig{
		Durable:       durable,
		FilterSubject: subject,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       AckWait,
		MaxAckPending: MaxAckPending,
		DeliverPolicy: nats.DeliverAllPolicy,
		ReplayPolicy:  nats.ReplayInstantPolicy,
	}

	// Check if consumer exists
	ci, err := b.js.ConsumerInfo(b.stream, durable, nats.Context(ctx))
	if err != nil && err != nats.ErrConsumerNotFound {
		return nil, fmt.Errorf("consumer info: %w", err)
	}

	if err == nats.ErrConsumerNotFound {
		// Create consumer
		ci, err = b.js.AddConsumer(b.stream, consumerConfig, nats.Context(ctx))
		if err != nil {
			// Retry fetch
			ci, err = b.js.ConsumerInfo(b.stream, durable, nats.Context(ctx))
		}
		if err != nil {
			return nil, fmt.Errorf("add consumer: %w", err)
		}
	} else {
		// Validate existing consumer settings
		if err := validateConsumerConfig(&ci.Config, consumerConfig); err != nil {
			return nil, fmt.Errorf("consumer incompatible: %w", err)
		}
	}

	// Create subscription
	sub, err := b.js.PullSubscribe(subject, durable, nats.Bind(b.stream, durable), nats.Context(ctx))
	if err != nil {
		return nil, fmt.Errorf("subscribe: %w", err)
	}

	return sub, nil
}

// validateConsumerConfig ensures existing consumer matches desired settings.
func validateConsumerConfig(actual, desired *nats.ConsumerConfig) error {
	if actual.Durable != desired.Durable ||
		actual.FilterSubject != desired.FilterSubject ||
		actual.AckPolicy != desired.AckPolicy ||
		actual.AckWait != desired.AckWait ||
		actual.MaxAckPending != desired.MaxAckPending ||
		actual.DeliverPolicy != desired.DeliverPolicy ||
		actual.ReplayPolicy != desired.ReplayPolicy || actual.DeliverSubject != "" || len(actual.FilterSubjects) > 0 || actual.MaxDeliver > 0 || actual.HeadersOnly || len(actual.BackOff) > 0 || actual.InactiveThreshold > 0 {
		return fmt.Errorf("consumer config mismatch")
	}
	return nil
}

// Validate decodes and validates a NATS message, checking subject match, Nats-Msg-Id,
// and manifest alignment. Returns the decoded Command.
func (b *Bus) Validate(msg *nats.Msg) (Command, error) {
	if msg == nil || msg.Data == nil {
		return Command{}, fmt.Errorf("nil message")
	}

	// Strict decode
	c, err := Decode(msg.Data)
	if err != nil {
		return Command{}, fmt.Errorf("decode: %w", err)
	}

	// Manifest validation
	if err := Validate(b.manifest, c); err != nil {
		return Command{}, fmt.Errorf("validate: %w", err)
	}

	// Subject validation
	expectedSubject := Subject(c.StreamID, c.Token)
	if msg.Subject != expectedSubject {
		return Command{}, fmt.Errorf("subject mismatch: %s vs %s", msg.Subject, expectedSubject)
	}

	// Nats-Msg-Id validation
	expectedID := ID(c)
	actualID := msg.Header.Get(nats.MsgIdHdr)
	if actualID != expectedID {
		return Command{}, fmt.Errorf("Nats-Msg-Id mismatch: %s vs %s", actualID, expectedID)
	}

	meta, e := msg.Metadata()
	if e != nil || meta.Stream != b.stream || meta.Sequence.Stream == 0 {
		return Command{}, fmt.Errorf("routed broker metadata mismatch")
	}
	return c, nil
}

// Close closes the underlying NATS connection.
func (b *Bus) Close() {
	if b != nil && b.conn != nil {
		b.conn.Close()
	}
}

// Accessor methods for tests and integration
func (b *Bus) Stream() string {
	return b.stream
}

func (b *Bus) GetManifest() runconfig.Manifest {
	return b.manifest
}

func (b *Bus) ManifestDigest() string {
	return b.digest
}
