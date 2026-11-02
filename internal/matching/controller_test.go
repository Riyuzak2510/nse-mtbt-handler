package matching

import (
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/runconfig"
	"reflect"
	"sync"
	"testing"
	"time"
)

func manifest() runconfig.Manifest {
	return runconfig.Manifest{Version: 2, Profile: feed.Profile, StreamID: 1, Seed: 42, Baseline: "empty", CreatedAt: time.Unix(1, 0).UTC(), Instruments: []runconfig.Instrument{{Token: 1, Symbol: "ONE", TickSize: 5, BasePrice: 100}, {Token: 2, Symbol: "TWO", TickSize: 5, BasePrice: 100}}}
}
func encode(t *testing.T, p feed.Packet) []byte {
	t.Helper()
	r, err := feed.Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func TestControllerReplayDuplicatesAndFreshness(t *testing.T) {
	m := manifest()
	c, err := NewController(m, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for i := int32(1); i <= 2; i++ {
		p := packet(feed.New, int(i), feed.Buy, 100, 3)
		p.Sequence = i
		raw := encode(t, p)
		if _, err := c.Process(raw, uint64(i), now); err != nil {
			t.Fatal(err)
		}
	}
	s, _ := c.Snapshot(1, 5, now, time.Second)
	if s.Status != "REBUILDING" {
		t.Fatal(s)
	}
	hb := encode(t, feed.Packet{Stream: 1, Type: feed.Heartbeat, LastSequence: 2})
	if _, err := c.Process(hb, 3, now); err != nil {
		t.Fatal(err)
	}
	s, _ = c.Snapshot(1, 5, now, time.Second)
	if s.Status != "READY" || s.ActiveOrders != 2 {
		t.Fatal(s)
	}
	r, err := c.Process(hb, 3, now)
	if err != nil || !r.Duplicate {
		t.Fatal(r, err)
	}
	s, _ = c.Snapshot(1, 5, now.Add(2*time.Second), time.Second)
	if s.Status != "STALE" || s.Stats.NewOrders != 2 {
		t.Fatal(s)
	}
	c.SetConnected(false)
	s, _ = c.Snapshot(1, 5, now, time.Second)
	if s.Status != "DISCONNECTED" {
		t.Fatal(s)
	}
}
func TestControllerRejectsGapsConflictsCatalogAndHeartbeat(t *testing.T) {
	for _, kind := range []string{"broker-gap", "source-gap", "conflict", "unknown", "tick", "heartbeat", "stream", "malformed", "capacity"} {
		t.Run(kind, func(t *testing.T) {
			c, _ := NewController(manifest(), 1, 0)
			now := time.Now()
			p := packet(feed.New, 1, feed.Buy, 100, 3)
			raw := encode(t, p)
			if _, err := c.Process(raw, 1, now); err != nil {
				t.Fatal(err)
			}
			before, _ := c.Snapshot(1, 5, now, time.Second)
			p = packet(feed.New, 2, feed.Buy, 105, 3)
			p.Sequence = 2
			pos := uint64(2)
			switch kind {
			case "broker-gap":
				pos = 3
			case "source-gap":
				p.Sequence = 4
			case "conflict":
				pos = 1
			case "unknown":
				p.Token = 3
			case "tick":
				p.Price = 101
			case "heartbeat":
				p = feed.Packet{Stream: 1, Type: feed.Heartbeat, LastSequence: 99}
			case "stream":
				p.Stream = 2
			}
			raw = encode(t, p)
			if kind == "malformed" {
				raw = raw[:4]
			}
			if _, err := c.Process(raw, pos, now); err == nil {
				t.Fatal("accepted invalid input")
			}
			after, _ := c.Snapshot(1, 5, now, time.Second)
			if after.Status != "INVALID" || !reflect.DeepEqual(before.Depth, after.Depth) || before.Stats != after.Stats || after.SourceSequence != 1 {
				t.Fatal(after)
			}
			if _, err := c.Process(encode(t, feed.Packet{Stream: 1, Type: feed.Heartbeat, LastSequence: 1}), 2, now); err == nil {
				t.Fatal("invalid controller resumed")
			}
		})
	}
}
func TestControllerSourceDuplicateDoesNotDoubleFill(t *testing.T) {
	c, _ := NewController(manifest(), 10, 0)
	now := time.Now()
	p := packet(feed.New, 1, feed.Sell, 100, 5)
	_, _ = c.Process(encode(t, p), 1, now)
	p = packet(feed.New, 2, feed.Buy, 100, 3)
	p.Sequence = 2
	raw := encode(t, p)
	a, err := c.Process(raw, 2, now)
	if err != nil || len(a.Fills) != 1 {
		t.Fatal(a, err)
	}
	b, err := c.Process(raw, 3, now)
	if err != nil || !b.Duplicate || len(b.Fills) != 0 {
		t.Fatal(b, err)
	}
	s, _ := c.Snapshot(1, 5, now, time.Second)
	if s.Stats.Executions != 1 || s.Asks[0].Quantity != 2 {
		t.Fatal(s)
	}
}
func TestControllerConcurrentReadAndApply(t *testing.T) {
	c, _ := NewController(manifest(), 2000, 0)
	var wg sync.WaitGroup
	for n := 0; n < 4; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				if _, err := c.Snapshot(1, 5, time.Now(), time.Second); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	for i := 1; i <= 1000; i++ {
		p := packet(feed.New, i, feed.Buy, 100, 1)
		p.Sequence = int32(i)
		if _, err := c.Process(encode(t, p), uint64(i), time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
}
