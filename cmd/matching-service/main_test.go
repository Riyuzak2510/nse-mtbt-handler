package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/nats-io/nats.go"
	"net/http/httptest"
	"nse-mtbt-handler/internal/broker"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/matching"
	"nse-mtbt-handler/internal/runconfig"
	"os"
	"reflect"
	"testing"
	"time"
)

func testManifest() runconfig.Manifest {
	return runconfig.Manifest{Version: 2, Profile: feed.Profile, StreamID: 1, Seed: 42, Baseline: "empty", CreatedAt: time.Now().UTC(), Instruments: []runconfig.Instrument{{Token: 1, Symbol: "ONE", TickSize: 5, BasePrice: 100}}}
}
func testPacket(kind feed.Kind, seq int32, id float64, side feed.Side, price, qty int32) feed.Packet {
	oid, _ := feed.IDFromFloat64(id)
	return feed.Packet{Stream: 1, Sequence: seq, Type: kind, OrderID: oid, Token: 1, Side: side, Price: price, Quantity: qty}
}
func raw(t *testing.T, p feed.Packet) []byte {
	t.Helper()
	b, err := feed.Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func TestHTTPShadowHealthReadinessAndBounds(t *testing.T) {
	m := testManifest()
	c, _ := matching.NewController(m, 10, 0)
	h := routes(c, m, time.Second)
	check := func(path string, want int) {
		t.Helper()
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != want {
			t.Fatalf("%s got%d want%d: %s", path, w.Code, want, w.Body)
		}
	}
	check("/healthz", 200)
	check("/readyz", 503)
	check("/shadow/book?token=1", 503)
	check("/shadow/book?token=9", 400)
	check("/shadow/book?token=1&depth=101", 400)
	if _, err := c.Process(raw(t, testPacket(feed.New, 1, 1, feed.Buy, 100, 5)), 1, time.Now()); err != nil {
		t.Fatal(err)
	}
	check("/readyz", 200)
	check("/shadow/book?token=1", 200)
	c.Invalidate("testgap")
	check("/readyz", 503)
	check("/healthz", 200)
}
func TestJetStreamServiceFanoutMatchingAndRestart(t *testing.T) {
	url := os.Getenv("MTBT_TEST_NATS_URL")
	if url == "" {
		t.Skip("set MTBT_TEST_NATS_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	m := testManifest()
	pub, err := broker.Connect(ctx, url, m)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	defer pub.JS.DeleteStream(pub.Stream)
	shadow, err := broker.OpenShadow(ctx, url, m)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	c, _ := matching.NewController(m, 10, shadow.ReplayThrough)
	packets := []feed.Packet{
		testPacket(feed.New, 1, 1, feed.Sell, 100, 5), testPacket(feed.New, 2, 2, feed.Sell, 100, 7), testPacket(feed.New, 3, 3, feed.Buy, 105, 8),
		{Stream: 1, Sequence: 4, Type: feed.Trade, Token: 1, Price: 100, Quantity: 2},
		testPacket(feed.Modify, 5, 3, feed.Buy, 100, 2), testPacket(feed.Cancel, 6, 2, feed.Sell, 0, 0),
		{Stream: 1, Type: feed.Heartbeat, LastSequence: 6}}
	child, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consume(child, shadow, c, nil, make(chan error)) }()
	for _, p := range packets {
		if err := pub.Publish(ctx, raw(t, p), time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	var first matching.Snapshot
	for time.Now().Before(deadline) {
		first, _ = c.Snapshot(1, 5, time.Now(), time.Minute)
		if first.BrokerSequence == 7 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if first.BrokerSequence != 7 || first.Stats.Executions != 3 || first.Stats.ExecutedQuantity != 10 || first.ActiveOrders != 0 || first.Stats.ObservedTrades != 1 {
		b, _ := json.Marshal(first)
		t.Fatal(string(b))
	}
	// Durable book still has its own complete copy, regardless of shadow acknowledgements.
	book, err := pub.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer book.Unsubscribe()
	for i := range packets {
		msgs, err := book.Fetch(1, nats.MaxWait(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		meta, _ := msgs[0].Metadata()
		if meta.Sequence.Stream != uint64(i+1) {
			t.Fatal(meta)
		}
		if err := msgs[0].AckSync(); err != nil {
			t.Fatal(err)
		}
	}
	stop()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := shadow.Close(); err != nil {
		t.Fatal(err)
	}
	replay, err := broker.OpenShadow(ctx, url, m)
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	rebuilt, _ := matching.NewController(m, 10, replay.ReplayThrough)
	for range packets {
		msgs, err := replay.Subscription.Fetch(1, nats.MaxWait(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		msg := msgs[0]
		received, err := replay.Validate(msg)
		if err != nil {
			t.Fatal(err)
		}
		meta, _ := msg.Metadata()
		if _, err := rebuilt.Process(msg.Data, meta.Sequence.Stream, received); err != nil {
			t.Fatal(err)
		}
		if err := msg.AckSync(); err != nil {
			t.Fatal(err)
		}
	}
	second, _ := rebuilt.Snapshot(1, 5, time.Now(), time.Minute)
	if first.Stats != second.Stats || !reflect.DeepEqual(first.Depth, second.Depth) || second.BrokerSequence != 7 {
		t.Fatal(first, second)
	}
}
