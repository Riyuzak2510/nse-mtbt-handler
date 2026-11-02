package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"nse-mtbt-handler/internal/book"
	"nse-mtbt-handler/internal/broker"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/runconfig"
	"nse-mtbt-handler/internal/simulator"
	"nse-mtbt-handler/internal/store"
)

func TestParallelRuntimeOracleAndRestart(t *testing.T) {
	dsn, url := os.Getenv("MTBT_TEST_DATABASE_URL"), os.Getenv("MTBT_TEST_NATS_URL")
	if dsn == "" || url == "" {
		t.Skip("set PostgreSQL and NATS integration variables")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	m := runconfig.Manifest{Version: 2, StreamID: 1, Profile: feed.Profile, Seed: 42, Baseline: "empty", CreatedAt: time.Now().UTC(), Instruments: []runconfig.Instrument{{Token: 1, Symbol: "A", TickSize: 5, BasePrice: 10000}, {Token: 2, Symbol: "B", TickSize: 5, BasePrice: 20000}, {Token: 3, Symbol: "C", TickSize: 5, BasePrice: 30000}}}
	db, err := store.OpenWithPool(ctx, dsn, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err = db.Init(ctx, m); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if _, e := pool.Exec(cleanup, `DELETE FROM mtbt.feed_state WHERE stream_id=$1`, m.StreamID); e != nil {
			t.Error(e)
		}
	}()
	release, err := db.AcquireLease(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if err = db.InitParallel(ctx, m, 3); err != nil {
		t.Fatal(err)
	}
	c, err := broker.Connect(ctx, url, m)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	defer c.JS.DeleteStream(c.Stream)
	defer c.JS.DeleteStream("SHARDS_1")
	g, err := simulator.New(m)
	if err != nil {
		t.Fatal(err)
	}
	rawSub, err := c.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	start := func() func() {
		runCtx, stop := context.WithCancel(ctx)
		result := make(chan error, 1)
		serverErr := make(chan error)
		go func() { result <- runParallel(runCtx, db, c, rawSub, m, 3, serverErr, "test") }()
		return func() {
			stop()
			select {
			case e := <-result:
				if e != nil && ctx.Err() == nil {
					t.Errorf("runtime: %v", e)
				}
			case <-time.After(7 * time.Second):
				t.Error("runtime did not stop")
			}
		}
	}
	stop := start()
	stopped := false
	defer func() {
		if !stopped {
			stop()
		}
	}()
	publish := func(p feed.Packet) {
		t.Helper()
		raw, e := feed.Encode(p)
		if e != nil {
			t.Fatal(e)
		}
		if e = c.Publish(ctx, raw, time.Now()); e != nil {
			t.Fatal(e)
		}
	}
	const count = 300
	for i := 0; i < count; i++ {
		p, e := g.Next(int64(i + 1))
		if e != nil {
			t.Fatal(e)
		}
		publish(p)
	}
	publish(g.Heartbeat())
	waitDrained := func(brokerSeq int64) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			var last int64
			if err = pool.QueryRow(ctx, `SELECT broker_sequence FROM mtbt.feed_state WHERE stream_id=$1`, m.StreamID).Scan(&last); err != nil {
				t.Fatal(err)
			}
			valid := last == brokerSeq
			for _, inst := range m.Instruments {
				snap, e := db.Snapshot(ctx, m, inst.Token, 100)
				if e != nil {
					t.Fatal(e)
				}
				if snap.Status == "INVALID" {
					t.Fatalf("invalid: %+v", snap)
				}
				valid = valid && snap.Status == "VALID" && snap.Sequence == count && snap.PendingEvents == 0 && snap.Shards == 3
			}
			if valid {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("parallel runtime did not reach exact drained checkpoint")
	}
	actualOrders := func() map[feed.OrderID]book.Order {
		t.Helper()
		rows, e := pool.Query(ctx, `SELECT order_id,token,side,price,quantity FROM mtbt.active_orders WHERE stream_id=$1`, m.StreamID)
		if e != nil {
			t.Fatal(e)
		}
		defer rows.Close()
		out := map[feed.OrderID]book.Order{}
		for rows.Next() {
			var id []byte
			var side string
			var o book.Order
			if e = rows.Scan(&id, &o.Token, &side, &o.Price, &o.Quantity); e != nil {
				t.Fatal(e)
			}
			o.ID, e = feed.IDFromBytes(id)
			if e != nil {
				t.Fatal(e)
			}
			if len(side) > 0 {
				o.Side = feed.Side(side[0])
			}
			out[o.ID] = o
		}
		if e = rows.Err(); e != nil {
			t.Fatal(e)
		}
		return out
	}
	waitDrained(count + 1)
	expected := map[feed.OrderID]book.Order{}
	for _, o := range g.Orders() {
		expected[o.ID] = o
	}
	before := actualOrders()
	if !reflect.DeepEqual(before, expected) {
		t.Fatalf("full active orders differ: actual=%d expected=%d", len(before), len(expected))
	}
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	routes(db, m, time.Minute).ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatal("drained book not ready", w.Code)
	}
	stop()
	stopped = true
	_ = rawSub.Unsubscribe()
	rawSub, err = c.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	stop = start()
	stopped = false
	publish(g.Heartbeat())
	waitDrained(count + 2)
	if !reflect.DeepEqual(before, actualOrders()) {
		t.Fatal("restart/heartbeat mutated book")
	}
	stop()
	stopped = true
	// All raw and shard messages must eventually have confirmed acknowledgements.
	for _, stream := range []string{c.Stream, "SHARDS_1"} {
		names := []string{"book"}
		if stream != c.Stream {
			names = []string{"shard-0", "shard-1", "shard-2"}
		}
		for _, name := range names {
			ci, e := c.JS.ConsumerInfo(stream, name)
			if e != nil || ci.NumAckPending != 0 || ci.NumPending != 0 {
				t.Fatalf("undrained consumer %s/%s: %+v %v", stream, name, ci, e)
			}
		}
	}
}

func TestParallelReadFailsClosedWhilePending(t *testing.T) {
	dsn := os.Getenv("MTBT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set PostgreSQL integration variable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	m := runconfig.Manifest{Version: 2, StreamID: 1, Profile: feed.Profile, Seed: 42, Baseline: "empty", CreatedAt: time.Now().UTC(), Instruments: []runconfig.Instrument{{Token: 1, Symbol: "A", TickSize: 5, BasePrice: 100}}}
	db, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err = db.Init(ctx, m); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	defer pool.Exec(context.Background(), `DELETE FROM mtbt.feed_state WHERE stream_id=$1`, m.StreamID)
	if err = db.InitParallel(ctx, m, 2); err != nil {
		t.Fatal(err)
	}
	id, _ := feed.IDFromFloat64(1)
	raw, _ := feed.Encode(feed.Packet{Stream: 1, Sequence: 1, Type: feed.New, Timestamp: 1, OrderID: id, Token: 1, Side: feed.Buy, Price: 100, Quantity: 10})
	if err = db.Dispatch(ctx, m, 2, raw, 1, time.Now()); err != nil {
		t.Fatal(err)
	}
	handler := routes(db, m, time.Minute)
	for _, path := range []string{"/readyz", "/book?token=1"} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != 503 {
			t.Fatalf("pending %s returned %d", path, w.Code)
		}
	}
	events, err := db.PendingDispatch(ctx, m)
	if err != nil || len(events) != 1 {
		t.Fatal(events, err)
	}
	if err = db.ApplyShard(ctx, m, 2, events[0]); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/book?token=1", nil))
	if w.Code != 200 {
		t.Fatal("drained HTTP", w.Code, w.Body.String())
	}
}
