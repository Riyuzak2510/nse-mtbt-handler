package outcomes

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"time"
)

type Config struct {
	MaxBytes                int64
	MaxAge, DuplicateWindow time.Duration
	Replicas                int
}

func DefaultConfig() Config { return Config{64 << 20, 24 * time.Hour, 10 * time.Minute, 1} }
func (c Config) Validate() error {
	if c.MaxBytes < 8192 || c.MaxBytes > 1<<40 || c.MaxAge <= 0 || c.MaxAge > 30*24*time.Hour || c.DuplicateWindow <= 0 || c.DuplicateWindow > c.MaxAge || c.Replicas < 1 || c.Replicas > 5 {
		return fmt.Errorf("invalid outcome stream bounds")
	}
	return nil
}

type ConfigError struct{ Reason string }

func (e *ConfigError) Error() string { return e.Reason }

type Publisher struct {
	conn                *nats.Conn
	js                  nats.JetStreamContext
	streamID            int16
	streamName, subject string
}

func Names(stream int16) (string, string) {
	return fmt.Sprintf("MTBT_OUTCOMES_%d", stream), fmt.Sprintf("mtbt.outcomes.%d", stream)
}
func Connect(url string, stream int16, cfg Config) (*Publisher, error) {
	if stream <= 0 {
		return nil, &ConfigError{"invalid native stream"}
	}
	if e := cfg.Validate(); e != nil {
		return nil, &ConfigError{e.Error()}
	}
	nc, e := nats.Connect(url, nats.NoReconnect(), nats.Timeout(3*time.Second))
	if e != nil {
		return nil, e
	}
	ok := false
	defer func() {
		if !ok {
			nc.Close()
		}
	}()
	js, e := nc.JetStream(nats.MaxWait(3 * time.Second))
	if e != nil {
		return nil, e
	}
	name, subject := Names(stream)
	desired := &nats.StreamConfig{Name: name, Subjects: []string{subject}, Storage: nats.FileStorage, Retention: nats.LimitsPolicy, Discard: nats.DiscardNew, MaxBytes: cfg.MaxBytes, MaxAge: cfg.MaxAge, MaxMsgSize: 8192, Replicas: cfg.Replicas, Duplicates: cfg.DuplicateWindow}
	info, e := js.StreamInfo(name)
	if errors.Is(e, nats.ErrStreamNotFound) {
		info, e = js.AddStream(desired)
	}
	if e != nil {
		return nil, e
	}
	if e = validateStreamConfig(&info.Config, desired); e != nil {
		return nil, &ConfigError{e.Error()}
	}
	ok = true
	return &Publisher{nc, js, stream, name, subject}, nil
}
func validateStreamConfig(a, b *nats.StreamConfig) error {
	if a.Name != b.Name || len(a.Subjects) != 1 || a.Subjects[0] != b.Subjects[0] || a.Storage != b.Storage || a.Retention != b.Retention || a.Discard != b.Discard || a.MaxBytes != b.MaxBytes || a.MaxAge != b.MaxAge || a.MaxMsgSize != b.MaxMsgSize || a.Replicas != b.Replicas || a.Duplicates != b.Duplicates || a.Mirror != nil || len(a.Sources) != 0 || a.Sealed || a.NoAck || a.MaxMsgs > 0 || a.MaxMsgsPerSubject > 0 || a.AllowRollup || a.DiscardNewPerSubject {
		return fmt.Errorf("incompatible existing outcome stream; refusing update")
	}
	return nil
}
func (p *Publisher) Publish(ctx context.Context, event Event, raw []byte) (*nats.PubAck, error) {
	if p == nil || p.conn == nil || !p.conn.IsConnected() {
		return nil, nats.ErrDisconnected
	}
	key, e := event.GetKey()
	if e != nil {
		return nil, e
	}
	decoded, e := ValidateKey(raw, p.streamID, key.SourceSequence, key.OrderIDBytes[:])
	if e != nil {
		return nil, e
	}
	if decoded.EventID != event.EventID {
		return nil, fmt.Errorf("event mismatch")
	}
	msg := nats.NewMsg(p.subject)
	msg.Data = raw
	msg.Header.Set(nats.MsgIdHdr, event.EventID)
	ack, e := p.js.PublishMsg(msg, nats.Context(ctx), nats.ExpectStream(p.streamName), nats.RetryAttempts(0))
	if e != nil {
		return nil, e
	}
	if ack == nil || ack.Stream != p.streamName || ack.Sequence == 0 {
		return nil, fmt.Errorf("invalid outcome publish acknowledgement")
	}
	return ack, nil
}
func (p *Publisher) Close() {
	if p != nil && p.conn != nil {
		p.conn.Close()
	}
}
