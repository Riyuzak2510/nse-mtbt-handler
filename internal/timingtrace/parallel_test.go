package timingtrace

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestParallelIndependentRecorders(t *testing.T) {
	path := filepath.Join(t.TempDir(), "parallel")
	p, err := NewParallel(path, 3, 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = NewParallel(path, 3, 100); err == nil {
		t.Fatal("existing directory overwritten")
	}
	var wg sync.WaitGroup
	for i, r := range append([]*Recorder{p.Dispatcher}, p.Workers...) {
		wg.Add(1)
		go func(i int, r *Recorder) {
			defer wg.Done()
			for n := 1; n <= 100; n++ {
				r.Record(Sample{Sequence: int32(n), Type: "N", Shard: i, ShardSequence: uint64(n), SourceBroker: uint64(n), RouteDurationNS: 123})
			}
		}(i, r)
	}
	wg.Wait()
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"dispatcher.csv", "shard-0.csv", "shard-1.csv", "shard-2.csv"} {
		f, e := os.Open(filepath.Join(path, name))
		if e != nil {
			t.Fatal(e)
		}
		rows, e := csv.NewReader(f).ReadAll()
		f.Close()
		if e != nil || len(rows) != 101 || len(rows[0]) != 17 || rows[1][16] != "123" {
			t.Fatal(name, len(rows), e)
		}
	}
}
func TestParallelBoundsAndOverflow(t *testing.T) {
	if p, e := NewParallel("", 0, 0); p != nil || e != nil {
		t.Fatal(p, e)
	}
	for _, count := range []int{0, 17} {
		if _, e := NewParallel(filepath.Join(t.TempDir(), "bad"), count, 10); e == nil {
			t.Fatal("bad count accepted")
		}
	}
	if _, e := NewParallel(filepath.Join(t.TempDir(), "big"), 16, 1000000); e == nil {
		t.Fatal("aggregate unbounded")
	}
	p, e := NewParallel(filepath.Join(t.TempDir(), "small"), 1, 1)
	if e != nil {
		t.Fatal(e)
	}
	p.Worker(0).Record(Sample{})
	p.Worker(0).Record(Sample{})
	if e = p.Close(); e == nil {
		t.Fatal("silent parallel overflow")
	}
}
