// Package broker preserves unchanged packets in a durable log scoped by the native stream ID.
package broker

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/runconfig"
	"strconv"
	"strings"
	"time"
)

const ProfileHeader = "Mtbt-Profile"
const ManifestHeader = "Mtbt-Manifest"
const ReceivedHeader = "Mtbt-Received"
const VersionHeader = "Mtbt-Envelope-Version"
const Consumer = "book"

type Client struct {
	Conn            *nats.Conn
	JS              nats.JetStreamContext
	Stream, Subject string
	manifest        runconfig.Manifest
	digest          string
}

func Connect(ctx context.Context, url string, m runconfig.Manifest) (*Client, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	d, err := m.Digest()
	if err != nil {
		return nil, err
	}
	streamID := strconv.Itoa(int(m.StreamID))
	nc, err := nats.Connect(url, nats.Name("mtbt-"+streamID), nats.Timeout(3*time.Second), nats.NoReconnect())
	if err != nil {
		return nil, fmt.Errorf("connect NATS: %w", err)
	}
	js, err := nc.JetStream(nats.MaxWait(3 * time.Second))
	if err != nil {
		nc.Close()
		return nil, err
	}
	c := &Client{nc, js, "MTBT_" + streamID, "mtbt." + streamID, m, d}
	if err = c.ensure(ctx); err != nil {
		nc.Close()
		return nil, err
	}
	return c, nil
}
func (c *Client) Close() { c.Conn.Close() }
func (c *Client) ensure(ctx context.Context) error {
	maxBytes, err := StreamMaxBytes()
	if err != nil {
		return err
	}
	desired := &nats.StreamConfig{Name: c.Stream, Description: "mtbt-manifest-v2:" + c.digest, Subjects: []string{c.Subject}, Retention: nats.LimitsPolicy, Storage: nats.FileStorage, Discard: nats.DiscardNew, MaxAge: 24 * time.Hour, MaxBytes: maxBytes, Replicas: 1, MaxMsgSize: 65535}
	info, err := c.JS.StreamInfo(c.Stream, nats.Context(ctx))
	if err == nats.ErrStreamNotFound {
		info, err = c.JS.AddStream(desired, nats.Context(ctx))
		if err != nil {
			info, err = c.JS.StreamInfo(c.Stream, nats.Context(ctx))
		}
	}
	if err != nil {
		return err
	}
	v := info.Config
	if v.Description != desired.Description || len(v.Subjects) != 1 || v.Subjects[0] != c.Subject || v.Retention != desired.Retention || v.Storage != desired.Storage || v.Discard != desired.Discard || v.MaxAge != desired.MaxAge || v.MaxBytes != desired.MaxBytes || v.Replicas != 1 || v.MaxMsgSize != desired.MaxMsgSize {
		return fmt.Errorf("existing JetStream stream has incompatible configuration")
	}
	ci, err := c.JS.ConsumerInfo(c.Stream, Consumer, nats.Context(ctx))
	if err == nats.ErrConsumerNotFound {
		ci, err = c.JS.AddConsumer(c.Stream, &nats.ConsumerConfig{Durable: Consumer, AckPolicy: nats.AckExplicitPolicy, AckWait: 30 * time.Second, MaxAckPending: 1, DeliverPolicy: nats.DeliverAllPolicy, FilterSubject: c.Subject, ReplayPolicy: nats.ReplayInstantPolicy}, nats.Context(ctx))
		if err != nil {
			ci, err = c.JS.ConsumerInfo(c.Stream, Consumer, nats.Context(ctx))
		}
	}
	if err != nil {
		return err
	}
	v2 := ci.Config
	if v2.Durable != Consumer || v2.DeliverSubject != "" || v2.FilterSubject != c.Subject || v2.AckPolicy != nats.AckExplicitPolicy || v2.MaxAckPending != 1 || v2.AckWait != 30*time.Second || v2.DeliverPolicy != nats.DeliverAllPolicy {
		return fmt.Errorf("existing durable consumer has incompatible configuration")
	}
	return nil
}
func (c *Client) Publish(ctx context.Context, raw []byte, received time.Time) error {
	msg := nats.NewMsg(c.Subject)
	msg.Data = raw
	msg.Header.Set(ProfileHeader, feed.Profile)
	msg.Header.Set(ManifestHeader, c.digest)
	msg.Header.Set(VersionHeader, "2")
	msg.Header.Set(ReceivedHeader, strconv.FormatInt(received.UTC().UnixNano(), 10))
	ack, err := c.JS.PublishMsg(msg, nats.Context(ctx))
	if err != nil {
		return err
	}
	if ack.Stream != c.Stream {
		return fmt.Errorf("unexpected publication stream")
	}
	return nil
}
func (c *Client) Subscribe() (*nats.Subscription, error) {
	return c.JS.PullSubscribe(c.Subject, Consumer, nats.Bind(c.Stream, Consumer))
}
func (c *Client) Validate(msg *nats.Msg) (time.Time, error) {
	// Strict v2: reject legacy Mtbt-Run header if present
	if hasLegacyRunHeader(msg.Header) {
		return time.Time{}, fmt.Errorf("legacy Mtbt-Run header not supported, envelope v1 rejected")
	}

	if msg.Subject != c.Subject || msg.Header.Get(ProfileHeader) != feed.Profile || msg.Header.Get(ManifestHeader) != c.digest || msg.Header.Get(VersionHeader) != "2" {
		return time.Time{}, fmt.Errorf("broker envelope/profile/manifest/version mismatch")
	}
	ns, err := strconv.ParseInt(msg.Header.Get(ReceivedHeader), 10, 64)
	if err != nil || ns <= 0 {
		return time.Time{}, fmt.Errorf("invalid receive timestamp")
	}
	received := time.Unix(0, ns).UTC()
	if received.After(time.Now().Add(time.Minute)) {
		return time.Time{}, fmt.Errorf("receive timestamp is in the future")
	}
	return received, nil
}

// A v2 envelope must not carry the removed field, even empty or differently cased.
func hasLegacyRunHeader(headers nats.Header) bool {
	for name := range headers {
		if strings.EqualFold(name, "Mtbt-Run") {
			return true
		}
	}
	return false
}
