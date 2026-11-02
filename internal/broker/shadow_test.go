package broker

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/runconfig"
)

func shadowManifest() runconfig.Manifest {
	return runconfig.Manifest{
		Version:   2,
		Profile:   feed.Profile,
		StreamID:  1,
		Seed:      42,
		Baseline:  "empty",
		CreatedAt: time.Now().UTC(),
		Instruments: []runconfig.Instrument{
			{Token: 1, Symbol: "A", TickSize: 5, BasePrice: 100},
			{Token: 2, Symbol: "B", TickSize: 5, BasePrice: 200},
		},
	}
}

func createExistingStream(t *testing.T, ctx context.Context, c *Client) {
	// Publish a test message to ensure stream exists with data.
	id, _ := feed.IDFromFloat64(1)
	raw, err := feed.Encode(feed.Packet{
		Stream:    1,
		Sequence:  1,
		Type:      feed.New,
		Timestamp: 1,
		OrderID:   id,
		Token:     1,
		Side:      feed.Buy,
		Price:     100,
		Quantity:  10,
	})
	if err != nil {
		t.Fatal(err)
	}

	msg := nats.NewMsg(c.Subject)
	msg.Data = raw
	msg.Header.Set(ProfileHeader, feed.Profile)
	msg.Header.Set(ManifestHeader, c.digest)
	msg.Header.Set(VersionHeader, "2")
	msg.Header.Set(ReceivedHeader, strconv.FormatInt(time.Now().UnixNano(), 10))

	ack, err := c.JS.PublishMsg(msg, nats.Context(ctx))
	if err != nil {
		t.Fatal(err)
	}
	if ack.Stream != c.Stream {
		t.Fatal("unexpected publication stream")
	}
}

func TestShadowRejectMissingStream(t *testing.T) {
	url := os.Getenv("MTBT_TEST_NATS_URL")
	if url == "" {
		t.Skip("set MTBT_TEST_NATS_URL")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m := shadowManifest()
	// Use a unique stream name that doesn't exist.
	m.StreamID = 2

	_, err := OpenShadow(ctx, url, m)
	if err == nil {
		t.Fatal("OpenShadow should reject nonexistent stream")
	}
	if fmt.Sprint(err) != fmt.Sprintf("stream does not exist: MTBT_%d", m.StreamID) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestShadowRejectExpiredPrefixFirstSeq(t *testing.T) {
	url := os.Getenv("MTBT_TEST_NATS_URL")
	if url == "" {
		t.Skip("set MTBT_TEST_NATS_URL")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m := shadowManifest()
	pubC, err := Connect(ctx, url, m)
	if err != nil {
		t.Fatal(err)
	}
	defer pubC.Close()
	defer pubC.JS.DeleteStream(pubC.Stream)

	// Publish messages to establish stream with content.
	for i := 0; i < 5; i++ {
		id, _ := feed.IDFromFloat64(1)
		raw, err := feed.Encode(feed.Packet{
			Stream: 1, Sequence: int32(i + 1), Type: feed.New,
			Timestamp: int64(i + 1), OrderID: id, Token: 1, Side: feed.Buy,
			Price: 100, Quantity: 10,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := pubC.Publish(ctx, raw, time.Now()); err != nil {
			t.Fatal(err)
		}
	}

	// Purge stream to create expired prefix (FirstSeq > 1).
	if err := pubC.JS.PurgeStream(pubC.Stream, &nats.StreamPurgeRequest{
		Sequence: 3,
	}, nats.Context(ctx)); err != nil {
		t.Fatal(err)
	}

	// OpenShadow should reject stream with FirstSeq > 1.
	_, err = OpenShadow(ctx, url, m)
	if err == nil {
		t.Fatal("OpenShadow should reject stream with expired prefix")
	}
	if fmt.Sprint(err) != "stream has expired prefix: FirstSeq=3" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestShadowRejectExpiredStreamEmptyWithLastSeq(t *testing.T) {
	url := os.Getenv("MTBT_TEST_NATS_URL")
	if url == "" {
		t.Skip("set MTBT_TEST_NATS_URL")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m := shadowManifest()
	pubC, err := Connect(ctx, url, m)
	if err != nil {
		t.Fatal(err)
	}
	defer pubC.Close()
	defer pubC.JS.DeleteStream(pubC.Stream)

	// Publish messages to establish stream.
	for i := 0; i < 3; i++ {
		id, _ := feed.IDFromFloat64(1)
		raw, err := feed.Encode(feed.Packet{
			Stream: 1, Sequence: int32(i + 1), Type: feed.New,
			Timestamp: int64(i + 1), OrderID: id, Token: 1, Side: feed.Buy,
			Price: 100, Quantity: 10,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := pubC.Publish(ctx, raw, time.Now()); err != nil {
			t.Fatal(err)
		}
	}

	// Purge entire stream (empty all messages).
	if err := pubC.JS.PurgeStream(pubC.Stream, nats.Context(ctx)); err != nil {
		t.Fatal(err)
	}

	// OpenShadow should reject empty stream with LastSeq > 0.
	_, err = OpenShadow(ctx, url, m)
	if err == nil {
		t.Fatal("OpenShadow should reject empty expired stream")
	}
	if fmt.Sprint(err) != "stream has expired prefix: FirstSeq=4" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestShadowAcceptEmptyFreshStream(t *testing.T) {
	url := os.Getenv("MTBT_TEST_NATS_URL")
	if url == "" {
		t.Skip("set MTBT_TEST_NATS_URL")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m := shadowManifest()
	pubC, err := Connect(ctx, url, m)
	if err != nil {
		t.Fatal(err)
	}
	defer pubC.Close()
	defer pubC.JS.DeleteStream(pubC.Stream)

	// OpenShadow should accept fresh empty stream.
	shadow, err := OpenShadow(ctx, url, m)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()

	if shadow.ReplayThrough != 0 {
		t.Fatalf("expected ReplayThrough=0 for fresh stream, got %d", shadow.ReplayThrough)
	}
}

func TestShadowIndependentFanout(t *testing.T) {
	url := os.Getenv("MTBT_TEST_NATS_URL")
	if url == "" {
		t.Skip("set MTBT_TEST_NATS_URL")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	m := shadowManifest()
	pubC, err := Connect(ctx, url, m)
	if err != nil {
		t.Fatal(err)
	}
	defer pubC.Close()
	defer pubC.JS.DeleteStream(pubC.Stream)

	// Create existing stream with book consumer subscription.
	createExistingStream(t, ctx, pubC)

	// Open book subscription (regular consumer).
	bookSub, err := pubC.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer bookSub.Unsubscribe()

	// Open shadow consumer.
	shadowC, err := OpenShadow(ctx, url, m)
	if err != nil {
		t.Fatal(err)
	}
	defer shadowC.Close()

	// Publish N messages via publisher (uses existing stream).
	for i := 0; i < 3; i++ {
		id, _ := feed.IDFromFloat64(1)
		raw, err := feed.Encode(feed.Packet{
			Stream: 1, Sequence: int32(i + 2), Type: feed.New,
			Timestamp: int64(i + 2), OrderID: id, Token: 1 + int32(i%2), Side: feed.Buy,
			Price: 100 + int32(i)*5, Quantity: 10,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := pubC.Publish(ctx, raw, time.Now()); err != nil {
			t.Fatal(err)
		}
	}

	// Book consumer fetches from position 1 (first message from stream creation).
	msgs, err := bookSub.Fetch(1, nats.MaxWait(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := msgs[0].AckSync(); err != nil {
		t.Fatal(err)
	}

	// Shadow consumer replays from position 1 independently.
	msgs, err = shadowC.Subscription.Fetch(1, nats.MaxWait(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	_, err = pubC.Validate(msgs[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := msgs[0].AckSync(); err != nil {
		t.Fatal(err)
	}

	// Book consumer ack does not affect shadow consumer.
	// Shadow consumer should still replay from its own position.
	msgs, err = shadowC.Subscription.Fetch(1, nats.MaxWait(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := msgs[0].AckSync(); err != nil {
		t.Fatal(err)
	}
}

func TestShadowConsumerDeletedOnClose(t *testing.T) {
	url := os.Getenv("MTBT_TEST_NATS_URL")
	if url == "" {
		t.Skip("set MTBT_TEST_NATS_URL")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m := shadowManifest()
	pubC, err := Connect(ctx, url, m)
	if err != nil {
		t.Fatal(err)
	}
	defer pubC.Close()
	defer pubC.JS.DeleteStream(pubC.Stream)

	createExistingStream(t, ctx, pubC)

	shadow, err := OpenShadow(ctx, url, m)
	if err != nil {
		t.Fatal(err)
	}

	consumerName := shadow.Name
	streamName := shadow.Stream

	// Verify consumer exists.
	ci, err := pubC.JS.ConsumerInfo(streamName, consumerName, nats.Context(ctx))
	if err != nil {
		t.Fatal(err)
	}
	if ci.Name != consumerName {
		t.Fatalf("consumer name mismatch: expected %s, got %s", consumerName, ci.Name)
	}

	// Close shadow.
	if err := shadow.Close(); err != nil {
		t.Fatal(err)
	}

	// Consumer should be deleted.
	_, err = pubC.JS.ConsumerInfo(streamName, consumerName, nats.Context(ctx))
	if err != nats.ErrConsumerNotFound {
		t.Fatalf("expected consumer to be deleted, got err=%v", err)
	}
}

func TestShadowReplayFullStream(t *testing.T) {
	url := os.Getenv("MTBT_TEST_NATS_URL")
	if url == "" {
		t.Skip("set MTBT_TEST_NATS_URL")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	m := shadowManifest()
	pubC, err := Connect(ctx, url, m)
	if err != nil {
		t.Fatal(err)
	}
	defer pubC.Close()
	defer pubC.JS.DeleteStream(pubC.Stream)

	// Publish 5 messages to the stream.
	for i := 0; i < 5; i++ {
		id, _ := feed.IDFromFloat64(1)
		raw, err := feed.Encode(feed.Packet{
			Stream: 1, Sequence: int32(i + 1), Type: feed.New,
			Timestamp: int64(i + 1), OrderID: id, Token: 1 + int32(i%2), Side: feed.Buy,
			Price: 100 + int32(i)*5, Quantity: 10,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := pubC.Publish(ctx, raw, time.Now()); err != nil {
			t.Fatal(err)
		}
	}

	// Open shadow consumer.
	shadow, err := OpenShadow(ctx, url, m)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()

	// Shadow should replay all 5 messages from the beginning.
	for i := 0; i < 5; i++ {
		msgs, err := shadow.Subscription.Fetch(1, nats.MaxWait(2*time.Second))
		if err != nil {
			t.Fatalf("fetch message %d: %v", i, err)
		}
		if _, err := pubC.Validate(msgs[0]); err != nil {
			t.Fatalf("validate message %d: %v", i, err)
		}
		if err := msgs[0].AckSync(); err != nil {
			t.Fatalf("ack message %d: %v", i, err)
		}
	}

	// No more messages available.
	_, err = shadow.Subscription.Fetch(1, nats.MaxWait(500*time.Millisecond))
	if err == nil {
		t.Fatal("expected no more messages")
	}
}

func TestShadowNewConsumerRestarts(t *testing.T) {
	url := os.Getenv("MTBT_TEST_NATS_URL")
	if url == "" {
		t.Skip("set MTBT_TEST_NATS_URL")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	m := shadowManifest()
	pubC, err := Connect(ctx, url, m)
	if err != nil {
		t.Fatal(err)
	}
	defer pubC.Close()
	defer pubC.JS.DeleteStream(pubC.Stream)

	// Publish 3 messages.
	for i := 0; i < 3; i++ {
		id, _ := feed.IDFromFloat64(1)
		raw, err := feed.Encode(feed.Packet{
			Stream: 1, Sequence: int32(i + 1), Type: feed.New,
			Timestamp: int64(i + 1), OrderID: id, Token: 1, Side: feed.Buy,
			Price: 100, Quantity: 10,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := pubC.Publish(ctx, raw, time.Now()); err != nil {
			t.Fatal(err)
		}
	}

	// Open first shadow consumer and consume one message.
	shadow1, err := OpenShadow(ctx, url, m)
	if err != nil {
		t.Fatal(err)
	}

	msgs, err := shadow1.Subscription.Fetch(1, nats.MaxWait(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := msgs[0].AckSync(); err != nil {
		t.Fatal(err)
	}

	// Close first shadow consumer.
	if err := shadow1.Close(); err != nil {
		t.Fatal(err)
	}

	// Open second shadow consumer (new consumer, no durable state).
	shadow2, err := OpenShadow(ctx, url, m)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow2.Close()

	// Second shadow should replay from position 1, not from where first stopped.
	msgs, err = shadow2.Subscription.Fetch(1, nats.MaxWait(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := msgs[0].AckSync(); err != nil {
		t.Fatal(err)
	}

	// Continue consuming all messages.
	for i := 1; i < 3; i++ {
		msgs, err = shadow2.Subscription.Fetch(1, nats.MaxWait(2*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if err := msgs[0].AckSync(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestShadowEphemeralConsumerUnaffectedByBookConfig(t *testing.T) {
	url := os.Getenv("MTBT_TEST_NATS_URL")
	if url == "" {
		t.Skip("set MTBT_TEST_NATS_URL")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m := shadowManifest()
	pubC, err := Connect(ctx, url, m)
	if err != nil {
		t.Fatal(err)
	}
	defer pubC.Close()
	defer pubC.JS.DeleteStream(pubC.Stream)

	createExistingStream(t, ctx, pubC)

	// Verify that book consumer has Durable="book".
	ci, err := pubC.JS.ConsumerInfo(pubC.Stream, Consumer, nats.Context(ctx))
	if err != nil {
		t.Fatal(err)
	}
	if ci.Config.Durable != Consumer {
		t.Fatalf("expected durable=%s, got %s", Consumer, ci.Config.Durable)
	}

	// Open shadow consumer.
	shadow, err := OpenShadow(ctx, url, m)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()

	// Shadow consumer should have empty Durable (ephemeral).
	ci, err = pubC.JS.ConsumerInfo(shadow.Stream, shadow.Name, nats.Context(ctx))
	if err != nil {
		t.Fatal(err)
	}
	if ci.Config.Durable != "" {
		t.Fatalf("expected shadow consumer to be ephemeral (Durable=''), got %s", ci.Config.Durable)
	}

	// Book consumer configuration should be unchanged.
	ci, err = pubC.JS.ConsumerInfo(pubC.Stream, Consumer, nats.Context(ctx))
	if err != nil {
		t.Fatal(err)
	}
	if ci.Config.Durable != Consumer {
		t.Fatalf("book consumer durable changed: expected %s, got %s", Consumer, ci.Config.Durable)
	}
}
