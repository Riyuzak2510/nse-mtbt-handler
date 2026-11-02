package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"nse-mtbt-handler/internal/book"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/store"
)

// Opt-in, isolated test database ONLY. Streams seed rows rather than materializing an input array.
func TestDatabaseRestoreCapacity(t *testing.T) {
	raw := os.Getenv("MTBT_TEST_RESTORE_COUNT")
	if raw == "" {
		t.Skip("set MTBT_TEST_RESTORE_COUNT in dedicated disposable database")
	}
	count, e := strconv.Atoi(raw)
	if e != nil || count < 1 || count > 1000000 {
		t.Fatal("invalid restore count")
	}
	dsn := os.Getenv("MTBT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("database required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	m := manifest()
	m.StreamID = 18
	seed, e := store.Open(ctx, dsn)
	must(t, e)
	must(t, seed.Migrate(ctx))
	must(t, seed.InitMode(ctx, m, "matching"))
	defer func() {
		cleanup, e := store.Open(context.Background(), dsn)
		if e == nil {
			defer cleanup.Close()
			_, _ = cleanup.Pool().Exec(context.Background(), "DELETE FROM mtbt.feed_state WHERE stream_id=$1", m.StreamID)
		}
	}()
	var existing int
	must(t, seed.Pool().QueryRow(ctx, "SELECT count(*) FROM mtbt.orders WHERE stream_id=$1", m.StreamID).Scan(&existing))
	if existing != 0 {
		t.Fatal("requires fresh test stream")
	}
	i := 0
	copied, e := seed.Pool().CopyFrom(ctx, pgx.Identifier{"mtbt", "orders"}, []string{"stream_id", "order_id", "token", "side", "price", "accepted_quantity", "remaining_quantity", "filled_quantity", "cancelled_quantity", "status"}, pgx.CopyFromFunc(func() ([]any, error) {
		if i >= count {
			return nil, nil
		}
		i++
		orderID, _ := feed.IDFromFloat64(float64(i))
		return []any{m.StreamID, orderID.Bytes(), int32(1), "B", int32(100 + (i%20)*5), int64(100), int32(100), int64(0), int64(0), "OPEN"}, nil
	}))
	must(t, e)
	if copied != int64(count) {
		t.Fatal("seed count", copied)
	}
	_, e = seed.Pool().Exec(ctx, "UPDATE mtbt.feed_state SET status='VALID',source_sequence=1,broker_sequence=1,last_received=now() WHERE stream_id=$1", m.StreamID)
	must(t, e)
	seed.Close()
	runtime.GC()
	debug.FreeOSMemory()
	read := func() map[string]uint64 {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		out := map[string]uint64{"heap_alloc": mem.HeapAlloc, "go_managed": mem.Sys - mem.HeapReleased}
		raw, _ := os.ReadFile("/proc/self/status")
		for _, line := range strings.Split(string(raw), "\n") {
			parts := strings.Fields(line)
			if len(parts) >= 2 && (parts[0] == "VmRSS:" || parts[0] == "VmHWM:") {
				n, _ := strconv.ParseUint(parts[1], 10, 64)
				out[parts[0]] = n * 1024
			}
		}
		for _, name := range []string{"memory.current", "memory.peak"} {
			raw, _ := os.ReadFile("/sys/fs/cgroup/" + name)
			n, _ := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
			out[name] = n
		}
		return out
	}
	baseline := read()
	start := time.Now()
	s, e := Open(ctx, dsn, m, count)
	must(t, e)
	defer s.Close()
	elapsed := time.Since(start)
	natural := read()
	runtime.GC()
	loaded := read()
	if s.engine.ActiveOrders() != count {
		t.Fatal("restore count mismatch")
	}
	var seen int
	must(t, s.engine.Walk(func(o book.Order) error {
		n := int(o.ID.Float64())
		if n < 1 || n > count || o.Quantity != 100 || o.Price != int32(100+(n%20)*5) || o.Side != feed.Buy {
			return fmt.Errorf("restore oracle mismatch %+v", o)
		}
		seen++
		return nil
	}))
	if seen != count {
		t.Fatal("oracle count mismatch")
	}
	result := map[string]any{"count": count, "restore_ns": elapsed.Nanoseconds(), "baseline": baseline, "loaded_natural": natural, "loaded_gc": loaded, "oracle": true, "note": "database hydration of100percentbid dense20levels; full service object, no HTTP/NATS clients/traffic, real PostgreSQL streaming copy+scan"}
	encoded, e := json.Marshal(result)
	must(t, e)
	t.Log("RESTORE_CAPACITY " + string(encoded))
	runtime.KeepAlive(s)
}
