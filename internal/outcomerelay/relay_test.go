package outcomerelay

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"nse-mtbt-handler/internal/durable"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/outcomes"
	"nse-mtbt-handler/internal/runconfig"
	"os"
	"testing"
	"time"
)

func setup(t *testing.T) (*Relay, *durable.Service, string, string) {
	t.Helper()
	dsn := os.Getenv("MTBT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("realPGrelaytestrequiresMTBT_TEST_DATABASE_URL")
	}
	m := runconfig.Manifest{Version: 2, Profile: feed.Profile, StreamID: 24, Seed: 42, Baseline: "empty", CreatedAt: time.Now(), Instruments: []runconfig.Instrument{{Token: 1, Symbol: "SIM", TickSize: 5, BasePrice: 100}}}
	s, e := durable.OpenWithOutcomes(context.Background(), dsn, m, 100, durable.DefaultOutcomeOptions())
	if e != nil {
		t.Fatal(e)
	}
	hash, _ := m.Digest()
	r, e := Open(context.Background(), dsn, 24, hash)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		r.Close(context.Background())
		s.Close()
		c, e := pgx.Connect(context.Background(), dsn)
		if e == nil {
			defer c.Close(context.Background())
			c.Exec(context.Background(), "BEGIN; SELECT set_config('mtbt.outcome_writer','v1',true); DELETE FROM mtbt.order_outbox WHERE stream_id=24; DELETE FROM mtbt.feed_state WHERE stream_id=24; COMMIT")
		}
	})
	return r, s, dsn, hash
}
func send(t *testing.T, s *durable.Service, seq int32, side feed.Side, q int32) {
	t.Helper()
	id, _ := feed.IDFromFloat64(float64(seq))
	raw, e := feed.Encode(feed.Packet{Stream: 24, Sequence: seq, Type: feed.New, OrderID: id, Token: 1, Side: side, Price: 100, Quantity: q})
	if e == nil {
		e = s.Process(context.Background(), raw, uint64(seq), time.Now())
	}
	if e != nil {
		t.Fatal(e)
	}
}

type fake struct {
	err       error
	ids       []string
	payloads  [][]byte
	duplicate bool
	wrong     bool
}

func (f *fake) Publish(c context.Context, e outcomes.Event, b []byte) (*nats.PubAck, error) {
	f.ids = append(f.ids, e.EventID)
	f.payloads = append(f.payloads, append([]byte(nil), b...))
	if f.err != nil {
		return nil, f.err
	}
	name, _ := outcomes.Names(e.StreamID)
	if f.wrong {
		name = "WRONG"
	}
	return &nats.PubAck{Stream: name, Sequence: 1, Duplicate: f.duplicate}, nil
}
func TestRelayFailureAndCrashAfterPublishRetryIdentical(t *testing.T) {
	r, s, dsn, hash := setup(t)
	send(t, s, 1, feed.Buy, 100)
	f := &fake{err: errors.New("lost puback")}
	if ok, e := r.Step(context.Background(), f); e == nil || ok {
		t.Fatal("lostfailure")
	}
	st, _ := r.Status(context.Background())
	if st.PendingCount != 1 {
		t.Fatal(st)
	}
	f.err = nil
	r.afterPublish = func() error { return errors.New("crash beforedelete") }
	if _, e := r.Step(context.Background(), f); e == nil {
		t.Fatal("hookfailed")
	}
	r.Close(context.Background())
	restored, e := Open(context.Background(), dsn, 24, hash)
	if e != nil {
		t.Fatal(e)
	}
	defer restored.Close(context.Background())
	f.duplicate = true
	if ok, e := restored.Step(context.Background(), f); e != nil || !ok {
		t.Fatal(ok, e)
	}
	if len(f.ids) != 3 || f.ids[0] != f.ids[2] || string(f.payloads[0]) != string(f.payloads[2]) {
		t.Fatal("unstableretry")
	}
	st, _ = restored.Status(context.Background())
	if st.PendingCount != 0 {
		t.Fatal(st)
	}
}
func TestRelayLeaseAndAckPoisonValidation(t *testing.T) {
	r, s, dsn, hash := setup(t)
	if other, e := Open(context.Background(), dsn, 24, hash); e == nil {
		other.Close(context.Background())
		t.Fatal("duplicatelease")
	}
	send(t, s, 1, feed.Buy, 100)
	if _, e := r.Step(context.Background(), &fake{wrong: true}); !IsPermanent(e) {
		t.Fatal("invalidackaccepted", e)
	}
	st, _ := r.Status(context.Background())
	if st.PendingCount != 1 {
		t.Fatal(st)
	}
	r.conn.Exec(context.Background(), "UPDATE mtbt.order_outbox SET payload=convert_to('{}','UTF8') WHERE stream_id=24")
	if _, e := r.Step(context.Background(), &fake{}); !IsPermanent(e) {
		t.Fatal("poisonnotstopped", e)
	}
}
func TestRelaySameSessionLostAfterPublishCannotDelete(t *testing.T) {
	r, s, dsn, _ := setup(t)
	send(t, s, 1, feed.Buy, 100)
	r.afterPublish = func() error { return r.conn.Close(context.Background()) }
	if _, e := r.Step(context.Background(), &fake{}); e == nil {
		t.Fatal("lostleasecontinued")
	}
	c, e := pgx.Connect(context.Background(), dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close(context.Background())
	var n int
	c.QueryRow(context.Background(), "SELECT count(*) FROM mtbt.order_outbox WHERE stream_id=24").Scan(&n)
	if n != 1 {
		t.Fatal(n)
	}
}
func TestRelayRealJetStreamDedupAfterAckBeforeDeleteCrash(t *testing.T) {
	url := os.Getenv("MTBT_TEST_NATS_URL")
	if url == "" {
		t.Skip("setMTBT_TEST_NATS_URL")
	}
	r, s, _, _ := setup(t)
	pub, e := outcomes.Connect(url, 24, outcomes.DefaultConfig())
	if e != nil {
		t.Fatal(e)
	}
	defer pub.Close()
	send(t, s, 1, feed.Buy, 100)
	r.afterPublish = func() error { return errors.New("afterbrokeraccept") }
	if _, e = r.Step(context.Background(), pub); e == nil {
		t.Fatal("crashnotinjected")
	}
	r.afterPublish = nil
	if _, e = r.Step(context.Background(), pub); e != nil {
		t.Fatal(e)
	}
	nc, e := nats.Connect(url)
	if e != nil {
		t.Fatal(e)
	}
	defer nc.Close()
	js, _ := nc.JetStream()
	name, _ := outcomes.Names(24)
	info, e := js.StreamInfo(name)
	if e != nil || info.State.Msgs != 1 {
		t.Fatal(info, e)
	}
	st, _ := r.Status(context.Background())
	if st.PendingCount != 0 {
		t.Fatal(st)
	}
	js.DeleteStream(name)
}
