package durable

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"nse-mtbt-handler/internal/book"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/matching"
	"nse-mtbt-handler/internal/runconfig"
	"nse-mtbt-handler/internal/store"
)

func manifest() runconfig.Manifest {
	return runconfig.Manifest{Version: 2, StreamID: 17, Profile: feed.Profile, Seed: 42, Baseline: "empty", CreatedAt: time.Now().UTC(), Instruments: []runconfig.Instrument{{Token: 1, Symbol: "SIM1", TickSize: 5, BasePrice: 100}}}
}
func setup(t *testing.T) (*Service, runconfig.Manifest, string) {
	t.Helper()
	dsn := os.Getenv("MTBT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set MTBT_TEST_DATABASE_URL for real PostgreSQL tests")
	}
	m := manifest()
	s, e := Open(context.Background(), dsn, m, 100)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		s.Close()
		db, e := store.Open(context.Background(), dsn)
		if e == nil {
			defer db.Close()
			_, _ = db.Pool().Exec(context.Background(), "DELETE FROM mtbt.feed_state WHERE stream_id=$1", m.StreamID)
		}
	})
	s.Connected(true)
	return s, m, dsn
}
func id(n float64) feed.OrderID { x, _ := feed.IDFromFloat64(n); return x }
func packet(seq int32, n float64, side feed.Side, qty int32) feed.Packet {
	return feed.Packet{Stream: 17, Sequence: seq, Type: feed.New, OrderID: id(n), Token: 1, Side: side, Price: 100, Quantity: qty}
}
func send(t *testing.T, s *Service, p feed.Packet, pos uint64) error {
	t.Helper()
	raw, e := feed.Encode(p)
	if e != nil {
		t.Fatal(e)
	}
	return s.Process(context.Background(), raw, pos, time.Now())
}
func want(t *testing.T, s *Service, n float64, remaining int32, filled int64, state string) {
	t.Helper()
	o, ok, e := s.Order(context.Background(), id(n))
	if e != nil || !ok || o.Remaining != remaining || o.Filled != filled || o.Status != state {
		t.Fatalf("order%v=%+v found%v err%v", n, o, ok, e)
	}
	if o.Accepted != int64(o.Remaining)+o.Filled+o.Cancelled {
		t.Fatal("quantity conservation", o)
	}
}
func must(t *testing.T, e error) {
	t.Helper()
	if e != nil {
		t.Fatal(e)
	}
}

func TestAtomicPartialCompleteAndDatabaseRebuild(t *testing.T) {
	s, m, dsn := setup(t)
	must(t, send(t, s, packet(1, 1, feed.Buy, 100), 1))
	must(t, send(t, s, packet(2, 2, feed.Sell, 30), 2))
	want(t, s, 1, 70, 30, "PARTIALLY_FILLED")
	want(t, s, 2, 0, 30, "FILLED")
	// Lost acknowledgement, same native event republished, no repeated subtraction.
	must(t, send(t, s, packet(2, 2, feed.Sell, 30), 2))
	must(t, send(t, s, packet(2, 2, feed.Sell, 30), 3))
	want(t, s, 1, 70, 30, "PARTIALLY_FILLED")
	s.Close()
	rebuilt, e := Open(context.Background(), dsn, m, 100)
	must(t, e)
	defer rebuilt.Close()
	rebuilt.Connected(true)
	if rebuilt.engine.ActiveOrders() != 1 || rebuilt.Position() != 3 {
		t.Fatal("restore wrong")
	}
	must(t, send(t, rebuilt, packet(3, 3, feed.Sell, 70), 4))
	want(t, rebuilt, 1, 0, 100, "FILLED")
	want(t, rebuilt, 3, 0, 70, "FILLED")
	if rebuilt.engine.ActiveOrders() != 0 {
		t.Fatal("completed orders remained in RAM")
	}
}
func TestCommitFailureRollsBackBothSidesAndRetriesAfterRestart(t *testing.T) {
	s, m, dsn := setup(t)
	must(t, send(t, s, packet(1, 1, feed.Buy, 100), 1))
	s.beforeCommit = func(pgx.Tx) error { return errors.New("injected commit failure") }
	if e := send(t, s, packet(2, 2, feed.Sell, 30), 2); e == nil {
		t.Fatal("injection not executed")
	}
	want(t, s, 1, 100, 0, "OPEN")
	if _, found, e := s.Order(context.Background(), id(2)); e != nil || found {
		t.Fatal("uncommitted incoming persisted", e)
	}
	snap, e := s.Snapshot(context.Background(), 1, 5, time.Now(), time.Minute)
	must(t, e)
	if snap.Status != "INVALID" || len(snap.Bids) != 0 || len(snap.Asks) != 0 {
		t.Fatal("tentative RAM depth was exposed", snap)
	}
	if e := send(t, s, packet(2, 2, feed.Sell, 30), 2); e == nil {
		t.Fatal("failed instance reused tentative RAM")
	}
	s.Close()
	rebuilt, e := Open(context.Background(), dsn, m, 100)
	must(t, e)
	defer rebuilt.Close()
	must(t, send(t, rebuilt, packet(2, 2, feed.Sell, 30), 2))
	want(t, rebuilt, 1, 70, 30, "PARTIALLY_FILLED")
}
func TestBackendDeathRollsBackAndReleasesOwnership(t *testing.T) {
	s, m, dsn := setup(t)
	must(t, send(t, s, packet(1, 1, feed.Buy, 100), 1))
	admin, e := store.Open(context.Background(), dsn)
	must(t, e)
	defer admin.Close()
	s.beforeCommit = func(tx pgx.Tx) error {
		var pid int32
		if e := tx.QueryRow(context.Background(), "SELECT pg_backend_pid()").Scan(&pid); e != nil {
			return e
		}
		_, e := admin.Pool().Exec(context.Background(), "SELECT pg_terminate_backend($1)", pid)
		return e
	}
	if send(t, s, packet(2, 2, feed.Sell, 30), 2) == nil {
		t.Fatal("dead backend committed")
	}
	s.Close()
	rebuilt, e := Open(context.Background(), dsn, m, 100)
	must(t, e)
	defer rebuilt.Close()
	want(t, rebuilt, 1, 100, 0, "OPEN")
	must(t, send(t, rebuilt, packet(2, 2, feed.Sell, 30), 2))
	want(t, rebuilt, 1, 70, 30, "PARTIALLY_FILLED")
}
func TestModificationCancellationConservationAndObserveOnlyTrade(t *testing.T) {
	s, _, _ := setup(t)
	must(t, send(t, s, packet(1, 1, feed.Buy, 100), 1))
	must(t, send(t, s, packet(2, 2, feed.Sell, 30), 2))
	p := packet(3, 1, feed.Buy, 50)
	p.Type = feed.Modify
	must(t, send(t, s, p, 3))
	want(t, s, 1, 50, 30, "PARTIALLY_FILLED")
	o, _, _ := s.Order(context.Background(), id(1))
	if o.Accepted != 150 || o.Cancelled != 70 {
		t.Fatal("replacement accounting", o)
	}
	p = feed.Packet{Stream: 17, Sequence: 4, Type: feed.Trade, Token: 1, BuyID: id(1), SellID: id(2), Price: 100, Quantity: 30}
	must(t, send(t, s, p, 4))
	want(t, s, 1, 50, 30, "PARTIALLY_FILLED")
	p = packet(5, 1, feed.Buy, 0)
	p.Type = feed.Cancel
	p.Price = 0
	must(t, send(t, s, p, 5))
	want(t, s, 1, 0, 30, "CANCELLED")
	p.Sequence = 6
	must(t, send(t, s, p, 6))
	want(t, s, 1, 0, 30, "CANCELLED")
}
func TestExclusiveOwnershipModeAndReadConcurrency(t *testing.T) {
	s, m, dsn := setup(t)
	if second, e := Open(context.Background(), dsn, m, 100); e == nil {
		second.Close()
		t.Fatal("second matching writer acquired lease")
	}
	reconstruction, e := store.Open(context.Background(), dsn)
	must(t, e)
	defer reconstruction.Close()
	if e = reconstruction.Init(context.Background(), m); e == nil {
		t.Fatal("reconstruction adopted matching state")
	}
	if release, e := reconstruction.AcquireLease(context.Background(), m); e == nil {
		release()
		t.Fatal("reconstruction acquired matching lease")
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				_, e := s.Snapshot(context.Background(), 1, 5, time.Now(), time.Minute)
				if e != nil {
					t.Error(e)
				}
			}
		}()
	}
	for i := int32(1); i <= 20; i++ {
		must(t, send(t, s, packet(i, float64(i), feed.Buy, 10), uint64(i)))
	}
	wg.Wait()
	s.CatchUpThrough(21)
	snap, e := s.Snapshot(context.Background(), 1, 5, time.Now(), time.Minute)
	must(t, e)
	if snap.Status != "REBUILDING" {
		t.Fatal(snap.Status)
	}
	must(t, send(t, s, packet(21, 21, feed.Buy, 10), 21))
	snap, e = s.Snapshot(context.Background(), 1, 5, time.Now(), time.Minute)
	must(t, e)
	if snap.Status != "READY" {
		t.Fatal(snap.Status)
	}
}
func TestGapConflictAndCapacityFailClosed(t *testing.T) {
	for _, kind := range []string{"gap", "conflict", "capacity"} {
		t.Run(kind, func(t *testing.T) {
			s, m, dsn := setup(t)
			must(t, send(t, s, packet(1, 1, feed.Buy, 100), 1))
			p := packet(2, 2, feed.Buy, 10)
			pos := uint64(2)
			switch kind {
			case "gap":
				p.Sequence = 3
			case "conflict":
				p.Sequence = 1
				pos = 1
			case "capacity":
				small, _ := matching.New(1)
				must(t, s.engine.Walk(func(o book.Order) error { return small.Restore(o) }))
				s.engine = small
			}
			if send(t, s, p, pos) == nil {
				t.Fatal("expected rejection")
			}
			want(t, s, 1, 100, 0, "OPEN")
			if kind != "capacity" {
				s.Close()
				if rebuilt, e := Open(context.Background(), dsn, m, 100); e == nil {
					rebuilt.Close()
					t.Fatal("invalid source reopened")
				}
			}
		})
	}
}
