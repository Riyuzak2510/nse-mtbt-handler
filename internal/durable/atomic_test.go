package durable

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/store"
)

func TestMultiCounterpartyAtomicityAndRestoreBudget(t *testing.T) {
	s, m, dsn := setup(t)
	for i := int32(1); i <= 3; i++ {
		must(t, send(t, s, packet(i, float64(i), feed.Buy, 20), uint64(i)))
	}
	s.beforeCommit = func(pgx.Tx) error { return errors.New("fail multi-order transaction") }
	if send(t, s, packet(4, 4, feed.Sell, 50), 4) == nil {
		t.Fatal("expected rollback")
	}
	for i := 1; i <= 3; i++ {
		want(t, s, float64(i), 20, 0, "OPEN")
	}
	s.Close()
	// Capacity failure must not publish an incomplete book or clear persisted rows.
	if tooSmall, e := Open(context.Background(), dsn, m, 2); e == nil {
		tooSmall.Close()
		t.Fatal("restored incomplete book")
	}
	restored, e := Open(context.Background(), dsn, m, 100)
	must(t, e)
	defer restored.Close()
	must(t, send(t, restored, packet(4, 4, feed.Sell, 50), 4))
	var totalFilled int64
	var totalRemaining int32
	var completed int
	for i := 1; i <= 3; i++ {
		o, ok, e := restored.Order(context.Background(), id(float64(i)))
		must(t, e)
		if !ok {
			t.Fatal("missing order")
		}
		totalFilled += o.Filled
		totalRemaining += o.Remaining
		if o.Status == "FILLED" {
			completed++
		}
	}
	if totalFilled != 50 || totalRemaining != 10 || completed != 2 {
		t.Fatal("multi-order quantity conservation", totalFilled, totalRemaining, completed)
	}
	want(t, restored, 4, 0, 50, "FILLED")
}

func TestReconstructionHistoryCannotBecomeMatchingHistory(t *testing.T) {
	dsn := os.Getenv("MTBT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires PostgreSQL")
	}
	m := manifest()
	m.StreamID = 19
	db, e := store.Open(context.Background(), dsn)
	must(t, e)
	defer db.Close()
	must(t, db.Migrate(context.Background()))
	must(t, db.Init(context.Background(), m))
	defer db.Pool().Exec(context.Background(), "DELETE FROM mtbt.feed_state WHERE stream_id=$1", m.StreamID)
	if s, e := Open(context.Background(), dsn, m, 100); e == nil {
		s.Close()
		t.Fatal("matching adopted exchange reconstruction stream")
	}
}
