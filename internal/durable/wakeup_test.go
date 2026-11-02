package durable

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/outcomes"
)

func listenOutcomes(t *testing.T, dsn string, stream int16) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, e := pgx.Connect(ctx, dsn)
	must(t, e)
	_, e = c.Exec(ctx, "LISTEN "+pgx.Identifier{outcomes.WakeChannel(stream)}.Sanitize())
	must(t, e)
	t.Cleanup(func() { c.Close(context.Background()) })
	return c
}

func noHint(t *testing.T, c *pgx.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	n, e := c.WaitForNotification(ctx)
	if n != nil || !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal("unexpected wake-up", n, e)
	}
}

func TestOutcomeHintOnlyAfterCommitAndOnePerTransaction(t *testing.T) {
	s, dsn := outcomeSetup(t, DefaultOutcomeOptions())
	c := listenOutcomes(t, dsn, s.manifest.StreamID)
	s.beforeCommit = func(tx pgx.Tx) error {
		noHint(t, c) // pg_notify has executed, but its transaction is uncommitted
		return nil
	}
	must(t, send(t, s, outcomePacket(s, 1, 1, feed.Buy, 100), 1))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	n, e := c.WaitForNotification(ctx)
	must(t, e)
	if n.Channel != outcomes.WakeChannel(s.manifest.StreamID) || n.Payload != "" {
		t.Fatal(n)
	}
	must(t, send(t, s, outcomePacket(s, 2, 2, feed.Sell, 30), 2))
	_, e = c.WaitForNotification(ctx)
	must(t, e)
	noHint(t, c) // two changed orders, only one hint for the transaction
	// Reprocessing committed input must not notify or create extra outcomes.
	must(t, send(t, s, outcomePacket(s, 2, 2, feed.Sell, 30), 2))
	noHint(t, c)
}

func TestRollbackDoesNotDeliverHint(t *testing.T) {
	s, dsn := outcomeSetup(t, DefaultOutcomeOptions())
	c := listenOutcomes(t, dsn, s.manifest.StreamID)
	s.beforeCommit = func(tx pgx.Tx) error { return errors.New("rollback after queued hint") }
	if send(t, s, outcomePacket(s, 1, 1, feed.Buy, 100), 1) == nil {
		t.Fatal("expected rollback")
	}
	noHint(t, c)
	if len(pending(t, s)) != 0 {
		t.Fatal("rolled back pending event retained")
	}
}
