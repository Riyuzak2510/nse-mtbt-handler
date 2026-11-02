package store

import (
	"context"
	"encoding/hex"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/runconfig"
	"os"
	"testing"
	"time"
)

func setup(t *testing.T) (*Store, runconfig.Manifest) {
	t.Helper()
	dsn := os.Getenv("MTBT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set MTBT_TEST_DATABASE_URL for PostgreSQL integration")
	}
	ctx := context.Background()
	s, e := Open(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(s.Close)
	if e = s.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	m := runconfig.Manifest{Version: 2, StreamID: 1, Profile: feed.Profile, Seed: 42, Baseline: "empty", CreatedAt: time.Now().UTC(), Instruments: []runconfig.Instrument{{Token: 1, Symbol: "SIM1", TickSize: 5, BasePrice: 100}}}
	if e = s.Init(ctx, m); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		_, _ = s.pool.Exec(context.Background(), `DELETE FROM mtbt.feed_state WHERE stream_id=$1`, m.StreamID)
	})
	return s, m
}
func id(n float64) feed.OrderID { x, _ := feed.IDFromFloat64(n); return x }
func order(seq int32, n float64, side feed.Side) feed.Packet {
	return feed.Packet{Stream: 1, Sequence: seq, Type: feed.New, Timestamp: 1, OrderID: id(n), Token: 1, Side: side, Price: 100, Quantity: 10}
}
func process(t *testing.T, s *Store, m runconfig.Manifest, p feed.Packet, b uint64) error {
	t.Helper()
	raw, e := feed.Encode(p)
	if e != nil {
		t.Fatal(e)
	}
	return s.Process(context.Background(), m, raw, b, time.Now())
}
func snap(t *testing.T, s *Store, m runconfig.Manifest) Snapshot {
	t.Helper()
	v, e := s.Snapshot(context.Background(), m, 1, 5)
	if e != nil {
		t.Fatal(e)
	}
	return v
}
func TestCommitAndRedelivery(t *testing.T) {
	s, m := setup(t)
	ctx := context.Background()
	p := order(1, 1, feed.Buy)
	if e := process(t, s, m, p, 1); e != nil {
		t.Fatal(e)
	}
	// Lost ack: same broker position; source duplicate from a new publication is also safe.
	for _, b := range []uint64{1, 2} {
		if e := process(t, s, m, p, b); e != nil {
			t.Fatal(e)
		}
	}
	v := snap(t, s, m)
	if v.Sequence != 1 || v.Status != "VALID" || len(v.Bids) != 1 || v.Bids[0].Quantity != 10 {
		t.Fatal(v)
	}
	if e := process(t, s, m, feed.Packet{Stream: 1, Type: feed.Heartbeat, LastSequence: 1}, 3); e != nil {
		t.Fatal(e)
	}
	if v = snap(t, s, m); v.Sequence != 1 {
		t.Fatal("heartbeat changed checkpoint")
	}
	if e := s.Init(ctx, m); e != nil {
		t.Fatal(e)
	}
	changed := m
	changed.Seed++
	if e := s.Init(ctx, changed); !IsFatal(e) {
		t.Fatal("manifest mutation allowed", e)
	}
}
func TestGapAndHeartbeatTail(t *testing.T) {
	for _, kind := range []string{"source", "tail", "broker", "backward", "conflict"} {
		t.Run(kind, func(t *testing.T) {
			s, m := setup(t)
			if e := process(t, s, m, order(1, 1, feed.Buy), 1); e != nil {
				t.Fatal(e)
			}
			p := order(3, 2, feed.Sell)
			b := uint64(2)
			switch kind {
			case "tail":
				p = feed.Packet{Stream: 1, Type: feed.Heartbeat, LastSequence: 2}
			case "broker":
				p = order(2, 2, feed.Sell)
				b = 3
			case "backward":
				if e := process(t, s, m, order(2, 2, feed.Sell), 2); e != nil {
					t.Fatal(e)
				}
				p = order(1, 1, feed.Buy)
				b = 3
			case "conflict":
				p = order(1, 1, feed.Buy)
				p.Quantity = 99
			}
			e := process(t, s, m, p, b)
			if !IsFatal(e) {
				t.Fatal("did not fail closed", e)
			}
			v := snap(t, s, m)
			if v.Status != "INVALID" || len(v.Bids) != 1 || v.Bids[0].Quantity != 10 {
				t.Fatal(v)
			}
		})
	}
}
func TestTradeRollback(t *testing.T) {
	s, m := setup(t)
	for i, p := range []feed.Packet{order(1, 1, feed.Buy), order(2, 2, feed.Sell)} {
		if e := process(t, s, m, p, uint64(i+1)); e != nil {
			t.Fatal(e)
		}
	}
	// First side could be reduced, but second reference is the wrong side. Neither may commit.
	p := feed.Packet{Stream: 1, Sequence: 3, Type: feed.Trade, Timestamp: 2, BuyID: id(2), SellID: id(1), Token: 1, Price: 100, Quantity: 5}
	if e := process(t, s, m, p, 3); !IsFatal(e) {
		t.Fatal(e)
	}
	v := snap(t, s, m)
	if v.Sequence != 2 || v.Bids[0].Quantity != 10 || v.Asks[0].Quantity != 10 {
		t.Fatal(v)
	}
}
func TestLease(t *testing.T) {
	s, m := setup(t)
	release, e := s.AcquireLease(context.Background(), m)
	if e != nil {
		t.Fatal(e)
	}
	if r, e := s.AcquireLease(context.Background(), m); e == nil {
		r()
		t.Fatal("second writer allowed")
	}
	release()
	release()
	r, e := s.AcquireLease(context.Background(), m)
	if e != nil {
		t.Fatal(e)
	}
	r()
}
func TestMalformedPersistsInvalid(t *testing.T) {
	s, m := setup(t)
	if e := s.Process(context.Background(), m, []byte{1}, 1, time.Now()); !IsFatal(e) {
		t.Fatal(e)
	}
	if snap(t, s, m).Status != "INVALID" {
		t.Fatal("not invalid")
	}
}
func TestOrdersOracle(t *testing.T) {
	s, m := setup(t)
	if e := process(t, s, m, order(1, 12345, feed.Buy), 1); e != nil {
		t.Fatal(e)
	}
	var raw []byte
	if e := s.pool.QueryRow(context.Background(), `SELECT order_id FROM mtbt.active_orders WHERE stream_id=$1`, m.StreamID).Scan(&raw); e != nil {
		t.Fatal(e)
	}
	if hex.EncodeToString(raw) != hex.EncodeToString(id(12345).Bytes()) {
		t.Fatal("ID lost precision")
	}
}
