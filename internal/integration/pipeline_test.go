package integration

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"net"
	"nse-mtbt-handler/internal/broker"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/runconfig"
	"nse-mtbt-handler/internal/simulator"
	"nse-mtbt-handler/internal/store"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestUDPJetStreamPostgres(t *testing.T) {
	dsn, url := os.Getenv("MTBT_TEST_DATABASE_URL"), os.Getenv("MTBT_TEST_NATS_URL")
	if dsn == "" || url == "" {
		t.Skip("requires isolated PostgreSQL and NATS integration environment")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	m := runconfig.Manifest{Version: 2, StreamID: 1, Profile: feed.Profile, Seed: 42, Baseline: "empty", CreatedAt: time.Now().UTC(), Instruments: []runconfig.Instrument{{Token: 1, Symbol: "SIM", TickSize: 5, BasePrice: 10000}}}
	db, e := store.Open(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	if e = db.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	if e = db.Init(ctx, m); e != nil {
		t.Fatal(e)
	}
	release, e := db.AcquireLease(ctx, m)
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	cleanupPool, e := pgxpool.New(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer cleanupPool.Close()
	defer func() {
		_, _ = cleanupPool.Exec(context.Background(), `DELETE FROM mtbt.feed_state WHERE stream_id=$1`, m.StreamID)
	}()
	c, e := broker.Connect(ctx, url, m)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	defer c.JS.DeleteStream(c.Stream)
	sub, e := c.Subscribe()
	if e != nil {
		t.Fatal(e)
	}
	defer sub.Unsubscribe()
	listener, e := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if e != nil {
		t.Fatal(e)
	}
	defer listener.Close()
	sender, e := net.DialUDP("udp4", nil, listener.LocalAddr().(*net.UDPAddr))
	if e != nil {
		t.Fatal(e)
	}
	defer sender.Close()
	g, e := simulator.New(m)
	if e != nil {
		t.Fatal(e)
	}
	const count = 200
	apply := func(p feed.Packet, ack bool) *nats.Msg {
		t.Helper()
		raw, e := feed.Encode(p)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = sender.Write(raw); e != nil {
			t.Fatal(e)
		}
		buf := make([]byte, 65535)
		_ = listener.SetReadDeadline(time.Now().Add(3 * time.Second))
		n, _, e := listener.ReadFromUDP(buf)
		if e != nil {
			t.Fatal(e)
		}
		if !reflect.DeepEqual(raw, buf[:n]) {
			t.Fatal("UDP payload changed")
		}
		if e = c.Publish(ctx, buf[:n], time.Now()); e != nil {
			t.Fatal(e)
		}
		msgs, e := sub.Fetch(1, nats.MaxWait(3*time.Second))
		if e != nil {
			t.Fatal(e)
		}
		msg := msgs[0]
		received, e := c.Validate(msg)
		if e != nil {
			t.Fatal(e)
		}
		meta, e := msg.Metadata()
		if e != nil {
			t.Fatal(e)
		}
		if e = db.Process(ctx, m, msg.Data, meta.Sequence.Stream, received); e != nil {
			t.Fatal(e)
		}
		// Simulates lost ack / ambiguous retry against the same persisted broker position.
		if e = db.Process(ctx, m, msg.Data, meta.Sequence.Stream, received); e != nil {
			t.Fatal(e)
		}
		if ack {
			if e = msg.AckSync(); e != nil {
				t.Fatal(e)
			}
		}
		return msg
	}
	for i := 0; i < count; i++ {
		p, e := g.Next(int64(i + 1))
		if e != nil {
			t.Fatal(e)
		}
		apply(p, true)
		if i == 50 {
			apply(p, true)
		}
	}
	apply(g.Heartbeat(), true)
	v, e := db.Snapshot(ctx, m, 1, 100)
	if e != nil {
		t.Fatal(e)
	}
	if v.Sequence != count || v.Status != "VALID" {
		t.Fatal(v)
	}
	expected := func(side feed.Side) []store.Level {
		levels := map[int32]store.Level{}
		for _, o := range g.Orders() {
			if o.Side == side {
				l := levels[o.Price]
				l.Price = o.Price
				l.Quantity += int64(o.Quantity)
				l.Orders++
				levels[o.Price] = l
			}
		}
		result := []store.Level{}
		for _, l := range levels {
			result = append(result, l)
		}
		sort.Slice(result, func(i, j int) bool {
			if side == feed.Buy {
				return result[i].Price > result[j].Price
			}
			return result[i].Price < result[j].Price
		})
		return result
	}
	if !reflect.DeepEqual(v.Bids, expected(feed.Buy)) || !reflect.DeepEqual(v.Asks, expected(feed.Sell)) {
		t.Fatalf("depth differs from independent generator: %+v", v)
	}
	// Required tail data never published: heartbeat detects incompleteness and persists INVALID.
	hb := g.Heartbeat()
	hb.LastSequence++
	raw, _ := feed.Encode(hb)
	if e = c.Publish(ctx, raw, time.Now()); e != nil {
		t.Fatal(e)
	}
	msgs, e := sub.Fetch(1, nats.MaxWait(time.Second))
	if e != nil {
		t.Fatal(e)
	}
	meta, _ := msgs[0].Metadata()
	received, _ := c.Validate(msgs[0])
	if e = db.Process(ctx, m, msgs[0].Data, meta.Sequence.Stream, received); !store.IsFatal(e) {
		t.Fatal("tail gap not detected", e)
	}
	v, e = db.Snapshot(ctx, m, 1, 100)
	if e != nil || v.Status != "INVALID" || v.Sequence != count {
		t.Fatal(v, e)
	}
}
