package durable

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/runconfig"
	"nse-mtbt-handler/internal/tickerwire"
	"os"
	"sync"
	"testing"
	"time"
)

func intakeSetup(t *testing.T) (*Intake, runconfig.Manifest, string) {
	t.Helper()
	dsn := os.Getenv("MTBT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set MTBT_TEST_DATABASE_URL for actual PostgreSQL")
	}
	m := manifest()
	m.StreamID = 81
	m.Instruments = append(m.Instruments, runconfig.Instrument{Token: 2, Symbol: "SIM2", TickSize: 5, BasePrice: 100})
	s, e := OpenIntake(context.Background(), dsn, m)
	must(t, e)
	t.Cleanup(func() {
		s.Close()
		c, e := pgx.Connect(context.Background(), dsn)
		if e != nil {
			t.Error(e)
			return
		}
		defer c.Close(context.Background())
		_, e = c.Exec(context.Background(), `DELETE FROM mtbt.match_work WHERE stream_id=81;DELETE FROM mtbt.ticker_applied WHERE stream_id=81;DELETE FROM mtbt.ticker_progress WHERE stream_id=81;DELETE FROM mtbt.ticker_topology WHERE stream_id=81;SELECT set_config('mtbt.outcome_writer','v1',false);DELETE FROM mtbt.order_outbox WHERE stream_id=81;DELETE FROM mtbt.orders WHERE stream_id=81;DELETE FROM mtbt.feed_state WHERE stream_id=81`)
		if e != nil {
			t.Error(e)
		}
	})
	return s, m, dsn
}
func admit(t *testing.T, s *Intake, seq int32, token int32, n float64, side feed.Side, quantity int32) []byte {
	t.Helper()
	p := packet(seq, n, side, quantity)
	p.Stream = s.m.StreamID
	p.Token = token
	raw, e := feed.Encode(p)
	must(t, e)
	must(t, s.Process(context.Background(), raw, uint64(seq), time.Now().UTC()))
	return raw
}
func commands(t *testing.T, s *Intake, token int32) []tickerwire.Command {
	t.Helper()
	rows, e := s.db.Pool().Query(context.Background(), "SELECT payload FROM mtbt.match_work WHERE stream_id=$1 AND token=$2 ORDER BY sequence", s.m.StreamID, token)
	must(t, e)
	defer rows.Close()
	var out []tickerwire.Command
	for rows.Next() {
		var raw []byte
		must(t, rows.Scan(&raw))
		c, e := tickerwire.Decode(raw)
		must(t, e)
		out = append(out, c)
	}
	must(t, rows.Err())
	return out
}
func ticker(t *testing.T, dsn string, m runconfig.Manifest, token int32) *Service {
	t.Helper()
	s, e := OpenTicker(context.Background(), dsn, m, token, 100)
	must(t, e)
	s.Connected(true)
	t.Cleanup(func() { s.Close() })
	return s
}
func TestPendingAdmissionMatchingAndRestart(t *testing.T) {
	s, m, dsn := intakeSetup(t)
	raw := admit(t, s, 1, 1, 1, feed.Buy, 100)
	admit(t, s, 2, 2, 2, feed.Buy, 100)
	admit(t, s, 3, 1, 3, feed.Sell, 30)
	a := ticker(t, dsn, m, 1)
	b := ticker(t, dsn, m, 2)
	if a.engine.ActiveOrders() != 0 || b.engine.ActiveOrders() != 0 {
		t.Fatal("PENDING restored as liquidity")
	}
	want(t, a, 1, 100, 0, "PENDING")
	cs := commands(t, s, 1)
	if len(cs) != 2 || cs[0].Sequence != 1 || cs[1].Sequence != 2 || cs[1].SourceBroker != 3 {
		t.Fatal(cs)
	}
	must(t, a.ProcessCommand(context.Background(), cs[0]))
	must(t, a.ProcessCommand(context.Background(), cs[1]))
	want(t, a, 1, 70, 30, "PARTIALLY_FILLED")
	want(t, a, 3, 0, 30, "FILLED")
	want(t, b, 2, 100, 0, "PENDING")
	must(t, b.ProcessCommand(context.Background(), commands(t, s, 2)[0]))
	want(t, b, 2, 100, 0, "OPEN")
	must(t, a.ProcessCommand(context.Background(), cs[1]))
	want(t, a, 1, 70, 30, "PARTIALLY_FILLED")
	// Last raw admission redelivery has identical identity; use source3.
	r := cs[1].Raw
	must(t, s.Process(context.Background(), r, 3, cs[1].Received))
	if _, e := Open(context.Background(), dsn, m, 100); e == nil {
		t.Fatal("combined owner accepted split topology")
	}
	if _, e := OpenTicker(context.Background(), dsn, m, 1, 100); e == nil {
		t.Fatal("duplicate owner accepted")
	}
	a.Close()
	a = ticker(t, dsn, m, 1)
	if a.engine.ActiveOrders() != 1 {
		t.Fatal("restore omitted partial fill")
	}
	must(t, a.ProcessCommand(context.Background(), cs[1]))
	want(t, a, 1, 70, 30, "PARTIALLY_FILLED")
	_ = raw
}
func TestPendingAdmissionRollbackAndConflictingCommand(t *testing.T) {
	s, m, dsn := intakeSetup(t)
	s.beforeCommit = func(pgx.Tx) error { return errors.New("admission crash") }
	p := packet(1, 1, feed.Buy, 100)
	p.Stream = m.StreamID
	raw, e := feed.Encode(p)
	must(t, e)
	if s.Process(context.Background(), raw, 1, time.Now()) == nil {
		t.Fatal("expected rollback")
	}
	var n int
	must(t, s.db.Pool().QueryRow(context.Background(), "SELECT count(*) FROM mtbt.orders WHERE stream_id=$1", m.StreamID).Scan(&n))
	if n != 0 || len(commands(t, s, 1)) != 0 {
		t.Fatal("partial admission committed")
	}
	s.Close()
	s, e = OpenIntake(context.Background(), dsn, m)
	must(t, e)
	defer s.Close()
	admit(t, s, 1, 1, 1, feed.Buy, 100)
	a := ticker(t, dsn, m, 1)
	c := commands(t, s, 1)[0]
	// Fully self-consistent forged envelope must still match durable admission.
	p.Quantity = 99
	raw, e = feed.Encode(p)
	must(t, e)
	bad, e := tickerwire.New(m, 1, 1, 1, c.Received, raw, "")
	must(t, e)
	if a.ProcessCommand(context.Background(), bad) == nil {
		t.Fatal("forged admitted command accepted")
	}
	want(t, a, 1, 100, 0, "PENDING")
	a.Close()
	a = ticker(t, dsn, m, 1)
	must(t, a.ProcessCommand(context.Background(), c))
	want(t, a, 1, 100, 0, "OPEN")
}
func TestTickerTransactionsActuallyOverlap(t *testing.T) {
	s, m, dsn := intakeSetup(t)
	admit(t, s, 1, 1, 1, feed.Buy, 100)
	admit(t, s, 2, 2, 2, feed.Buy, 100)
	a := ticker(t, dsn, m, 1)
	b := ticker(t, dsn, m, 2)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	a.beforeCommit = func(pgx.Tx) error { once.Do(func() { close(entered) }); <-release; return nil }
	done := make(chan error, 1)
	go func() { done <- a.ProcessCommand(context.Background(), commands(t, s, 1)[0]) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("tickerA never reached transaction gate")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	e := b.ProcessCommand(ctx, commands(t, s, 2)[0])
	// Intake can admit additional work while A's matching transaction is open.
	p := packet(3, 3, feed.Buy, 20)
	p.Stream = m.StreamID
	p.Token = 2
	raw, x := feed.Encode(p)
	must(t, x)
	admissionError := s.Process(ctx, raw, 3, time.Now().UTC())
	close(release)
	must(t, admissionError)
	must(t, e)
	must(t, <-done)
	want(t, b, 2, 100, 0, "OPEN")
	// Admission for B must not wait for A's matching transaction either.
}

type capturePublisher struct {
	commands []tickerwire.Command
	fail     bool
}

func (p *capturePublisher) Publish(_ context.Context, c tickerwire.Command) error {
	p.commands = append(p.commands, c)
	if p.fail {
		return fmt.Errorf("broker unavailable")
	}
	return nil
}
func TestRoutingCrashAndWitnessLifetime(t *testing.T) {
	s, m, dsn := intakeSetup(t)
	admit(t, s, 1, 1, 1, feed.Buy, 100)
	d, e := OpenDispatcher(context.Background(), dsn, m, 1)
	must(t, e)
	defer d.Close()
	p := &capturePublisher{}
	d.afterPublish = func() error { return fmt.Errorf("crash after broker ack") }
	if _, e = d.Step(context.Background(), p); e == nil {
		t.Fatal("expected dispatch crash")
	}
	if len(commands(t, s, 1)) != 1 {
		t.Fatal("witness removed before recovery")
	}
	d.Close()
	d, e = OpenDispatcher(context.Background(), dsn, m, 1)
	must(t, e)
	defer d.Close()
	_, e = d.Step(context.Background(), p)
	must(t, e)
	if len(p.commands) != 2 || p.commands[0].Digest != p.commands[1].Digest {
		t.Fatal("unstable retry")
	}
	if len(commands(t, s, 1)) != 1 {
		t.Fatal("witness removed before matching")
	}
	a := ticker(t, dsn, m, 1)
	a.beforeCommit = func(pgx.Tx) error { return errors.New("matching crash") }
	if a.ProcessCommand(context.Background(), p.commands[0]) == nil {
		t.Fatal("expected rollback")
	}
	want(t, a, 1, 100, 0, "PENDING")
	a.Close()
	a = ticker(t, dsn, m, 1)
	must(t, a.ProcessCommand(context.Background(), p.commands[0]))
	_, e = d.Step(context.Background(), p)
	must(t, e)
	if len(commands(t, s, 1)) != 0 {
		t.Fatal("completed pending work leaked")
	}
	want(t, a, 1, 100, 0, "OPEN")
}
func TestTickerSourceGapAndFailClosedReads(t *testing.T) {
	s, m, dsn := intakeSetup(t)
	admit(t, s, 1, 1, 1, feed.Buy, 100)
	a := ticker(t, dsn, m, 1)
	p := packet(3, 3, feed.Buy, 100)
	p.Stream = m.StreamID
	raw, e := feed.Encode(p)
	must(t, e)
	if s.Process(context.Background(), raw, 2, time.Now()) == nil {
		t.Fatal("source gap accepted")
	}
	v, e := a.TickerStatus(context.Background())
	must(t, e)
	if v["status"] != "INVALID" {
		t.Fatal(v)
	}
	if a.ProcessCommand(context.Background(), commands(t, s, 1)[0]) == nil {
		t.Fatal("matching continued on invalid feed")
	}
}
