// matching-capacity measures the real engine, not a packet array or database cache.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"nse-mtbt-handler/internal/book"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/matching"
)

type sample struct {
	Phase         string `json:"phase"`
	Active        int    `json:"active_orders"`
	HeapAlloc     uint64 `json:"heap_alloc_bytes"`
	HeapInuse     uint64 `json:"heap_inuse_bytes"`
	HeapReleased  uint64 `json:"heap_released_bytes"`
	Managed       uint64 `json:"go_managed_bytes"`
	RSS           uint64 `json:"rss_bytes"`
	PeakRSS       uint64 `json:"peak_rss_bytes"`
	CgroupCurrent uint64 `json:"cgroup_current_bytes"`
	CgroupPeak    uint64 `json:"cgroup_peak_bytes"`
	NumGC         uint32 `json:"num_gc"`
	GCPauseNS     uint64 `json:"gc_pause_total_ns"`
	Goroutines    int    `json:"goroutines"`
}
type result struct {
	Version    int      `json:"version"`
	Count      int      `json:"count"`
	Layout     string   `json:"layout"`
	Rounds     int      `json:"churn_rounds"`
	Go         string   `json:"go_version"`
	OS         string   `json:"os"`
	Arch       string   `json:"arch"`
	MemLimit   string   `json:"gomemlimit"`
	Duration   int64    `json:"duration_ns"`
	LoadNS     int64    `json:"load_ns"`
	ChurnNS    int64    `json:"churn_ns"`
	Samples    []sample `json:"samples"`
	Verified   bool     `json:"correctness_verified"`
	Restored   bool     `json:"database_style_restore_verified"`
	BurstFills int      `json:"burst_fills"`
}

func readNumber(path string) uint64 {
	raw, e := os.ReadFile(path)
	if e != nil {
		return 0
	}
	n, _ := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
	return n
}
func memory(phase string, active int) sample {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	s := sample{Phase: phase, Active: active, HeapAlloc: m.HeapAlloc, HeapInuse: m.HeapInuse, HeapReleased: m.HeapReleased, Managed: m.Sys - m.HeapReleased, NumGC: m.NumGC, GCPauseNS: m.PauseTotalNs, Goroutines: runtime.NumGoroutine()}
	f, e := os.Open("/proc/self/status")
	if e == nil {
		defer f.Close()
		scan := bufio.NewScanner(f)
		for scan.Scan() {
			parts := strings.Fields(scan.Text())
			if len(parts) >= 2 {
				n, _ := strconv.ParseUint(parts[1], 10, 64)
				if parts[0] == "VmRSS:" {
					s.RSS = n * 1024
				}
				if parts[0] == "VmHWM:" {
					s.PeakRSS = n * 1024
				}
			}
		}
	}
	s.CgroupCurrent = readNumber("/sys/fs/cgroup/memory.current")
	s.CgroupPeak = readNumber("/sys/fs/cgroup/memory.peak")
	return s
}
func identity(n int) feed.OrderID { id, _ := feed.IDFromFloat64(float64(n)); return id }
func order(i, n int, layout string) feed.Packet {
	side := feed.Buy
	if (i/100)%2 == 1 {
		side = feed.Sell
	}
	tier := i / 200
	if layout == "dense" {
		tier %= 20
	}
	price := int32(100000 - tier*5)
	if side == feed.Sell {
		price = int32(200000 + tier*5)
	}
	return feed.Packet{Stream: 1, Sequence: 1, Type: feed.New, OrderID: identity(n), Token: int32(i%100 + 1), Side: side, Price: price, Quantity: 100}
}
func run(count int, layout string, rounds int) (result, error) {
	out := result{Version: 1, Count: count, Layout: layout, Rounds: rounds, Go: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH, MemLimit: os.Getenv("GOMEMLIMIT"), Samples: []sample{}}
	if count < 1 || count > 1000000 || (layout != "dense" && layout != "sparse") || rounds < 1 || rounds > 10 {
		return out, fmt.Errorf("count1..1000000, layout dense|sparse, rounds1..10 required")
	}
	start := time.Now()
	runtime.GC()
	debug.FreeOSMemory()
	out.Samples = append(out.Samples, memory("baseline", 0))
	engine, e := matching.New(count)
	if e != nil {
		return out, e
	}
	take := func(phase string, gc bool) {
		if gc {
			runtime.GC()
		}
		out.Samples = append(out.Samples, memory(phase, engine.ActiveOrders()))
		runtime.KeepAlive(engine)
	}
	begin := time.Now()
	for i := 0; i < count; i++ {
		if _, e = engine.Apply(order(i, i+1, layout)); e != nil {
			return out, e
		}
	}
	out.LoadNS = time.Since(begin).Nanoseconds()
	take("loaded_natural", false)
	take("loaded_gc", true)
	verify := func(offset int) error {
		if engine.ActiveOrders() != count {
			return fmt.Errorf("active count%d want%d", engine.ActiveOrders(), count)
		}
		for i := 0; i < count; i++ {
			p := order(i, offset+i+1, layout)
			o, ok := engine.Get(p.OrderID)
			if !ok || o.Token != p.Token || o.Price != p.Price || o.Side != p.Side || o.Quantity != 100 {
				return fmt.Errorf("oracle mismatch index%d", i)
			}
		}
		return nil
	}
	if e = verify(0); e != nil {
		return out, e
	}
	begin = time.Now()
	for round := 0; round < rounds; round++ {
		for i := 0; i < count; i++ {
			p := order(i, round*count+i+1, layout)
			p.Type = feed.Cancel
			if _, e = engine.Apply(p); e != nil {
				return out, e
			}
			p = order(i, (round+1)*count+i+1, layout)
			if _, e = engine.Apply(p); e != nil {
				return out, e
			}
			if i%10 == 0 {
				p.Type = feed.Modify
				if _, e = engine.Apply(p); e != nil {
					return out, e
				}
			}
		}
		if e = verify((round + 1) * count); e != nil {
			return out, e
		}
	}
	out.ChurnNS = time.Since(begin).Nanoseconds()
	take("churn_natural", false)
	take("churn_gc", true)
	// Sequential walk tests the exact restoration path without keeping a second full engine.
	// Drain and restore generation is deterministic; no O(N) input/oracle slice is retained.
	for i := 0; i < count; i++ {
		p := order(i, rounds*count+i+1, layout)
		p.Type = feed.Cancel
		if _, e = engine.Apply(p); e != nil {
			return out, e
		}
	}
	if engine.ActiveOrders() != 0 {
		return out, fmt.Errorf("drain left active orders")
	}
	take("drained_gc", true)
	engine, e = matching.New(count)
	if e != nil {
		return out, e
	}
	for i := 0; i < count; i++ {
		p := order(i, rounds*count+i+1, layout)
		if e = engine.Restore(book.Order{ID: p.OrderID, Token: p.Token, Side: p.Side, Price: p.Price, Quantity: p.Quantity}); e != nil {
			return out, e
		}
	}
	if e = verify(rounds * count); e != nil {
		return out, e
	}
	out.Restored = true
	take("restored_gc", true)
	// Sweep one instrument's bids; transient fill allocations are included in high-water RSS.
	p := feed.Packet{Stream: 1, Sequence: 1, Type: feed.New, OrderID: identity((rounds+2)*count + 1), Token: 1, Side: feed.Sell, Price: 1, Quantity: int32(count * 100)}
	applied, e := engine.Apply(p)
	if e != nil {
		return out, e
	}
	out.BurstFills = len(applied.Fills)
	take("burst_natural", false)
	var total int64
	for _, f := range applied.Fills {
		total += int64(f.Quantity)
		if f.Token != 1 || f.Quantity <= 0 {
			return out, fmt.Errorf("bad burst fill")
		}
	}
	if total > int64(p.Quantity) {
		return out, fmt.Errorf("burst overfill")
	}
	// Empty final state via bounded Walk (one cancellation at a time through generated IDs).
	for i := 0; i < count; i++ {
		p := order(i, rounds*count+i+1, layout)
		p.Type = feed.Cancel
		if _, e = engine.Apply(p); e != nil {
			return out, e
		}
	}
	p.Type = feed.Cancel
	if _, e = engine.Apply(p); e != nil {
		return out, e
	}
	if engine.ActiveOrders() != 0 {
		return out, fmt.Errorf("final drain left active orders")
	}
	applied.Fills = nil
	take("final_drained_gc", true)
	debug.FreeOSMemory()
	take("released", false)
	out.Verified = true
	out.Duration = time.Since(start).Nanoseconds()
	runtime.KeepAlive(engine)
	return out, nil
}
func main() {
	count := flag.Int("count", 100000, "simultaneously active orders1..1000000")
	layout := flag.String("layout", "dense", "dense or sparse price levels")
	rounds := flag.Int("rounds", 2, "full cancel/readd churn rounds1..10")
	flag.Parse()
	out, e := run(*count, *layout, *rounds)
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	if e = json.NewEncoder(os.Stdout).Encode(out); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
