package broker

import (
	"fmt"
	"github.com/nats-io/nats.go"
	"nse-mtbt-handler/internal/feed"
	"strconv"
	"time"
)

// AsyncPublisher belongs to one ordered submitter on the existing NoReconnect
// connection. Caller owns immutable raw bytes until the future resolves.
type AsyncPublisher struct {
	js       nats.JetStreamContext
	client   *Client
	baseline uint64
}

func (c *Client) AsyncPublisher(maxPending int, ackTimeout time.Duration) (*AsyncPublisher, error) {
	if maxPending < 1 || maxPending > 4096 || ackTimeout <= 0 {
		return nil, fmt.Errorf("invalid async pending bound or acknowledgement timeout")
	}
	info, e := c.JS.StreamInfo(c.Stream)
	if e != nil {
		return nil, e
	}
	js, e := c.Conn.JetStream(nats.PublishAsyncMaxPending(maxPending), nats.PublishAsyncTimeout(ackTimeout))
	if e != nil {
		return nil, e
	}
	return &AsyncPublisher{js: js, client: c, baseline: info.State.LastSeq}, nil
}
func (a *AsyncPublisher) Baseline() uint64       { return a.baseline }
func (a *AsyncPublisher) ExpectedStream() string { return a.client.Stream }
func (c *Client) publication(raw []byte, received time.Time) *nats.Msg {
	m := nats.NewMsg(c.Subject)
	m.Data = raw
	m.Header.Set(ProfileHeader, feed.Profile)
	m.Header.Set(ManifestHeader, c.digest)
	m.Header.Set(VersionHeader, "2")
	m.Header.Set(ReceivedHeader, strconv.FormatInt(received.UTC().UnixNano(), 10))
	return m
}
func (a *AsyncPublisher) Submit(raw []byte, received time.Time) (nats.PubAckFuture, error) {
	// Disable no-responder retries: replaying an older request after newer ones
	// would break native order. No deduplication hides malformed/conflicting input.
	return a.js.PublishMsgAsync(a.client.publication(raw, received), nats.RetryAttempts(0), nats.ExpectStream(a.client.Stream), nats.StallWait(100*time.Millisecond))
}

// Close discards pending futures; the caller must drain/validate them first on
// graceful shutdown. Failure is ambiguous, not an automatic retry/replay.
func (a *AsyncPublisher) Close() {
	if a != nil {
		a.js.CleanupPublisher()
	}
}
func ValidateAck(ack *nats.PubAck, stream string, sequence uint64) error {
	if ack == nil || ack.Stream != stream || ack.Sequence == 0 || ack.Sequence != sequence || ack.Duplicate {
		return fmt.Errorf("unexpected publish acknowledgement: expected stream=%s sequence=%d, got %+v", stream, sequence, ack)
	}
	return nil
}
