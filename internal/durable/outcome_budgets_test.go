package durable

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"nse-mtbt-handler/internal/feed"
	"testing"
)

func TestOutcomeByteBudgetRollsBackMultiOrderUpdate(t *testing.T) {
	opts := DefaultOutcomeOptions()
	opts.MaxPendingBytes = 8192
	s, _ := outcomeSetup(t, opts)
	var bytes int64
	var n int32
	for n = 1; n <= 100; n++ {
		e := send(t, s, outcomePacket(s, n, float64(n), feed.Buy, 1), uint64(n))
		if e != nil {
			break
		}
		if e = s.db.Pool().QueryRow(context.Background(), "SELECT COALESCE(sum(octet_length(payload)),0)::bigint FROM mtbt.order_outbox WHERE stream_id=23").Scan(&bytes); e != nil {
			t.Fatal(e)
		}
	}
	if n > 100 || bytes > 8192 || s.Position() != uint64(n-1) {
		t.Fatal("bytebudgetdidnotrollback", n, bytes, s.Position())
	}
	v := pending(t, s)
	if len(v) != int(n-1) {
		t.Fatal(len(v), n)
	}
	var seq int32
	s.db.Pool().QueryRow(context.Background(), "SELECT source_sequence FROM mtbt.feed_state WHERE stream_id=23").Scan(&seq)
	if seq != n-1 {
		t.Fatal(seq, n)
	}
}
func TestCommitAmbiguityRebuildSkipsAlreadyCommittedOutcome(t *testing.T) {
	s, dsn := outcomeSetup(t, DefaultOutcomeOptions())
	s.beforeCommit = func(tx pgx.Tx) error {
		if e := tx.Commit(context.Background()); e != nil {
			return e
		}
		return errors.New("commit succeeded but response lost")
	}
	if send(t, s, outcomePacket(s, 1, 1, feed.Buy, 100), 1) == nil {
		t.Fatal("ambiguitynotreported")
	}
	if len(pending(t, s)) != 1 {
		t.Fatal("committedjournalmissing")
	}
	s.Close()
	restored, e := OpenWithOutcomes(context.Background(), dsn, s.manifest, 100, DefaultOutcomeOptions())
	must(t, e)
	defer restored.Close()
	must(t, send(t, restored, outcomePacket(restored, 1, 1, feed.Buy, 100), 1))
	if len(pending(t, restored)) != 1 {
		t.Fatal("ambiguouscommitduplicatedoutcome")
	}
}
func TestMidOutboxInsertFailureRollsBackAllRows(t *testing.T) {
	s, _ := outcomeSetup(t, DefaultOutcomeOptions())
	must(t, send(t, s, outcomePacket(s, 1, 1, feed.Buy, 100), 1))
	_, e := s.db.Pool().Exec(context.Background(), `CREATE OR REPLACE FUNCTION mtbt.test_outcome_failure() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.stream_id=23 AND NEW.source_sequence=2 AND NEW.order_id=decode('0000000000000040','hex') THEN RAISE EXCEPTION 'injectedoutboxinsert'; END IF; RETURN NEW; END$$; CREATE TRIGGER test_outcome_failure BEFORE INSERT ON mtbt.order_outbox FOR EACH ROW EXECUTE FUNCTION mtbt.test_outcome_failure()`)
	must(t, e)
	t.Cleanup(func() {
		s.db.Pool().Exec(context.Background(), "DROP TRIGGER IF EXISTS test_outcome_failure ON mtbt.order_outbox; DROP FUNCTION IF EXISTS mtbt.test_outcome_failure()")
	})
	if send(t, s, outcomePacket(s, 2, 2, feed.Sell, 30), 2) == nil {
		t.Fatal("failure notinjected")
	}
	if len(pending(t, s)) != 1 {
		t.Fatal("partialjournal")
	}
	want(t, s, 1, 100, 0, "OPEN")
}
