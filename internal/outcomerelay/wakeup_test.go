package outcomerelay

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/outcomes"
)

func waitOK(t *testing.T, r *Relay, limit time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	defer cancel()
	if e := r.Wait(ctx, time.Minute); e != nil {
		t.Fatal("expected commit wake-up before deadline:", e)
	}
}

func TestCommitBetweenEmptyScanAndWaitIsNotLost(t *testing.T) {
	r, s, _, _ := setup(t)
	if ok, e := r.Step(context.Background(), &fake{}); e != nil || ok {
		t.Fatal(ok, e)
	}
	send(t, s, 1, feed.Buy, 100)
	waitOK(t, r, 2*time.Second)
	if ok, e := r.Step(context.Background(), &fake{}); e != nil || !ok {
		t.Fatal(ok, e)
	}
}

func TestWaitWakesWhileBlockedAndDrainsAllChangedOrders(t *testing.T) {
	r, s, _, _ := setup(t)
	// Only this goroutine operates the relay connection; the producer uses a
	// different connection. This also exercises a server push to a blocked wait.
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		done <- r.Wait(ctx, time.Minute)
	}()
	send(t, s, 1, feed.Buy, 100)
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	f := &fake{}
	if _, e := r.Step(context.Background(), f); e != nil {
		t.Fatal(e)
	}
	send(t, s, 2, feed.Sell, 30)
	waitOK(t, r, 2*time.Second)
	for n := 0; n < 2; n++ {
		if ok, e := r.Step(context.Background(), f); e != nil || !ok {
			t.Fatal(ok, e)
		}
	}
	if ok, e := r.Step(context.Background(), f); e != nil || ok || len(f.ids) != 3 {
		t.Fatal(ok, e, len(f.ids))
	}
}

func TestListenBeforeInitialScanAndReconnectRecoversMissedHints(t *testing.T) {
	r, s, dsn, hash := setup(t)
	if e := r.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
	// No listener: this notification is deliberately lost. The row is durable.
	send(t, s, 1, feed.Buy, 100)
	restored, e := Open(context.Background(), dsn, 24, hash)
	if e != nil {
		t.Fatal(e)
	}
	defer restored.Close(context.Background())
	if ok, e := restored.Step(context.Background(), &fake{}); e != nil || !ok {
		t.Fatal("startup scan did not recover pending row", ok, e)
	}
	// A commit after LISTEN but before any further wait is also delivered.
	send(t, s, 2, feed.Sell, 30)
	waitOK(t, restored, 2*time.Second)
}

func TestReconciliationFindsUnnotifiedRowsAndTimeoutKeepsLease(t *testing.T) {
	r, s, _, _ := setup(t)
	if _, e := r.conn.Exec(context.Background(), "UNLISTEN *"); e != nil {
		t.Fatal(e)
	}
	send(t, s, 1, feed.Buy, 100)
	if e := r.Wait(context.Background(), 20*time.Millisecond); e != nil {
		t.Fatal(e)
	}
	if ok, e := r.Step(context.Background(), &fake{}); e != nil || !ok {
		t.Fatal("reconciliation missed row or lost connection", ok, e)
	}
}

func TestWaitCancellationAndBackendLoss(t *testing.T) {
	r, _, dsn, _ := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if e := r.Wait(ctx, time.Minute); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	c, e := pgx.Connect(context.Background(), dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close(context.Background())
	if _, e = c.Exec(context.Background(), "SELECT pg_terminate_backend($1)", r.conn.PgConn().PID()); e != nil {
		t.Fatal(e)
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if e = r.Wait(ctx, time.Minute); e == nil {
		t.Fatal("backend loss treated as successful notification")
	}
}

func TestSpuriousAndCoalescedHintsDoNotCreateEvents(t *testing.T) {
	r, _, dsn, _ := setup(t)
	c, e := pgx.Connect(context.Background(), dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close(context.Background())
	for i := 0; i < 100; i++ {
		if _, e = c.Exec(context.Background(), "SELECT pg_notify($1, '')", outcomes.WakeChannel(24)); e != nil {
			t.Fatal(e)
		}
	}
	waitOK(t, r, 2*time.Second)
	f := &fake{}
	if ok, e := r.Step(context.Background(), f); e != nil || ok || len(f.ids) != 0 {
		t.Fatal(ok, e, len(f.ids))
	}
}

func TestWaitConfiguration(t *testing.T) {
	var r *Relay
	if e := r.Wait(context.Background(), time.Second); e == nil {
		t.Fatal("nil relay accepted")
	}
}
