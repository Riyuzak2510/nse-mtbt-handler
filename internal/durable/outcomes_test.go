package durable

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/outcomes"
	"nse-mtbt-handler/internal/store"
	"os"
	"testing"
	"time"
)

func outcomeSetup(t *testing.T, options OutcomeOptions) (*Service, string) {
	t.Helper()
	dsn := os.Getenv("MTBT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("setMTBT_TEST_DATABASE_URL for realoutcomeatomicity")
	}
	m := manifest()
	m.StreamID = 23
	s, e := OpenWithOutcomes(context.Background(), dsn, m, 100, options)
	must(t, e)
	t.Cleanup(func() {
		s.Close()
		db, e := store.Open(context.Background(), dsn)
		if e == nil {
			defer db.Close()
			tx, e := db.Pool().Begin(context.Background())
			if e == nil {
				defer tx.Rollback(context.Background())
				tx.Exec(context.Background(), "SELECT set_config('mtbt.outcome_writer','v1',true)")
				tx.Exec(context.Background(), "DELETE FROM mtbt.order_outbox WHERE stream_id=23")
				tx.Exec(context.Background(), "DELETE FROM mtbt.feed_state WHERE stream_id=23")
				tx.Commit(context.Background())
			}
		}
	})
	return s, dsn
}
func outcomePacket(s *Service, n int32, oid float64, side feed.Side, q int32) feed.Packet {
	p := packet(n, oid, side, q)
	p.Stream = s.manifest.StreamID
	return p
}
func pending(t *testing.T, s *Service) []outcomes.Event {
	t.Helper()
	rows, e := s.db.Pool().Query(context.Background(), "SELECT payload FROM mtbt.order_outbox WHERE stream_id=$1 ORDER BY source_sequence,order_id", s.manifest.StreamID)
	must(t, e)
	defer rows.Close()
	var result []outcomes.Event
	for rows.Next() {
		var raw []byte
		must(t, rows.Scan(&raw))
		ev, e := outcomes.Decode(raw)
		must(t, e)
		result = append(result, ev)
	}
	must(t, rows.Err())
	return result
}
func TestOutcomesBothSidesDeduplicatedAndCoherentRead(t *testing.T) {
	s, dsn := outcomeSetup(t, DefaultOutcomeOptions())
	buy := outcomePacket(s, 1, 1, feed.Buy, 100)
	must(t, send(t, s, buy, 1))
	must(t, send(t, s, outcomePacket(s, 2, 2, feed.Sell, 30), 2))
	events := pending(t, s)
	if len(events) != 3 {
		t.Fatal(len(events))
	}
	if events[0].Order.Status != "OPEN" {
		t.Fatal(events)
	}
	var incoming, resting bool
	for _, e := range events[1:] {
		if e.Cause == "NEW" && e.Order.Status == "FILLED" {
			incoming = true
		}
		if e.Cause == "MATCH" && e.Order.Status == "PARTIALLY_FILLED" && e.Order.RemainingQty == 70 {
			resting = true
		}
	}
	if !incoming || !resting {
		t.Fatal(events)
	}
	// A broker redelivery and native byte-identical source duplicate create no extra rows.
	p := outcomePacket(s, 2, 2, feed.Sell, 30)
	must(t, send(t, s, p, 2))
	must(t, send(t, s, p, 3))
	if len(pending(t, s)) != 3 {
		t.Fatal("duplicate events")
	}
	v, ok, e := s.OrderView(context.Background(), id(1))
	must(t, e)
	if !ok || v.AsOfSourceSequence != 2 || v.Remaining != 70 || v.OutcomesStartSequence == nil {
		t.Fatal(v)
	}
	s.Close()
	if disabled, e := Open(context.Background(), dsn, s.manifest, 100); e == nil {
		disabled.Close()
		t.Fatal("outcome-enabled reopened without flag")
	}
	restored, e := OpenWithOutcomes(context.Background(), dsn, s.manifest, 100, DefaultOutcomeOptions())
	must(t, e)
	defer restored.Close()
	if len(pending(t, restored)) != 3 {
		t.Fatal("pending lost acrossrestart")
	}
}
func TestOutcomesRollbackOrdersCheckpointAndPending(t *testing.T) {
	s, _ := outcomeSetup(t, DefaultOutcomeOptions())
	must(t, send(t, s, outcomePacket(s, 1, 1, feed.Buy, 100), 1))
	s.beforeCommit = func(pgx.Tx) error { return errors.New("crash beforecommit") }
	if send(t, s, outcomePacket(s, 2, 2, feed.Sell, 30), 2) == nil {
		t.Fatal("rollback notinjected")
	}
	if len(pending(t, s)) != 1 {
		t.Fatal("uncommitted events visible")
	}
	want(t, s, 1, 100, 0, "OPEN")
	v, _, e := s.OrderView(context.Background(), id(1))
	must(t, e)
	if v.AsOfSourceSequence != 1 {
		t.Fatal(v)
	}
}
func TestOutcomesPendingBudgetFailsWholeTransaction(t *testing.T) {
	options := DefaultOutcomeOptions()
	options.MaxPendingRows = 2
	s, _ := outcomeSetup(t, options)
	must(t, send(t, s, outcomePacket(s, 1, 1, feed.Buy, 100), 1))
	if send(t, s, outcomePacket(s, 2, 2, feed.Sell, 30), 2) == nil {
		t.Fatal("budget ignored")
	}
	if len(pending(t, s)) != 1 {
		t.Fatal("partial outbox transaction")
	}
	want(t, s, 1, 100, 0, "OPEN")
	var status string
	must(t, s.db.Pool().QueryRow(context.Background(), "SELECT status FROM mtbt.feed_state WHERE stream_id=23").Scan(&status))
	if status != "VALID" {
		t.Fatal("capacity falselyinvalidatedfeed", status)
	}
}
func TestOutcomesNoopsModifyCancelAndExchangeObservation(t *testing.T) {
	s, _ := outcomeSetup(t, DefaultOutcomeOptions())
	must(t, send(t, s, outcomePacket(s, 1, 1, feed.Buy, 100), 1))
	p := outcomePacket(s, 2, 1, feed.Buy, 60)
	p.Type = feed.Modify
	must(t, send(t, s, p, 2))
	p = outcomePacket(s, 3, 1, feed.Buy, 0)
	p.Type = feed.Cancel
	must(t, send(t, s, p, 3))
	for seq := int32(4); seq <= 5; seq++ {
		p.Sequence = seq
		if seq == 5 {
			p.OrderID = id(99)
		}
		must(t, send(t, s, p, uint64(seq)))
	}
	must(t, send(t, s, feed.Packet{Stream: 23, Sequence: 6, Type: feed.Trade, Token: 1, Price: 100, Quantity: 1, BuyID: id(1), SellID: id(99)}, 6))
	must(t, send(t, s, feed.Packet{Stream: 23, Type: feed.Heartbeat, LastSequence: 6}, 7))
	e := pending(t, s)
	if len(e) != 3 || e[1].Cause != "MODIFY" || e[2].Cause != "CANCEL" || e[2].Order.Status != "CANCELLED" {
		t.Fatal(e)
	}
}
func TestEnabledDatabaseRejectsOldWriterSQL(t *testing.T) {
	s, _ := outcomeSetup(t, DefaultOutcomeOptions())
	must(t, send(t, s, outcomePacket(s, 1, 1, feed.Buy, 100), 1))
	_, e := s.db.Pool().Exec(context.Background(), "UPDATE mtbt.orders SET price=105 WHERE stream_id=23")
	if e == nil {
		t.Fatal("oldSQLwriter bypassedguard")
	}
	_, e = s.db.Pool().Exec(context.Background(), "UPDATE mtbt.feed_state SET source_sequence=999 WHERE stream_id=23")
	if e == nil {
		t.Fatal("oldcheckpointwriter bypassedguard")
	}
}
func TestEnableOutcomesRefusesExistingHistory(t *testing.T) {
	s, dsn := outcomeSetup(t, OutcomeOptions{})
	must(t, send(t, s, outcomePacket(s, 1, 1, feed.Buy, 100), 1))
	s.Close()
	if newer, e := OpenWithOutcomes(context.Background(), dsn, s.manifest, 100, DefaultOutcomeOptions()); e == nil {
		newer.Close()
		t.Fatal("silently backfilledexistinghistory")
	}
}
func TestMultiOrderFinalSnapshotsAndTimestamp(t *testing.T) {
	s, _ := outcomeSetup(t, DefaultOutcomeOptions())
	for n := int32(1); n <= 3; n++ {
		must(t, send(t, s, outcomePacket(s, n, float64(n), feed.Buy, 20), uint64(n)))
	}
	must(t, send(t, s, outcomePacket(s, 4, 4, feed.Sell, 50), 4))
	ev := pending(t, s)
	if len(ev) != 7 {
		t.Fatal(len(ev))
	}
	stamp := ev[3].RecordedAt
	for _, e := range ev[3:] {
		if e.RecordedAt != stamp || e.SourceSequence != 4 {
			t.Fatal(e)
		}
		if _, err := time.Parse(time.RFC3339Nano, e.RecordedAt); err != nil {
			t.Fatal(err)
		}
	}
}
