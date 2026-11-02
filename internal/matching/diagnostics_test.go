package matching

import (
	"nse-mtbt-handler/internal/feed"
	"testing"
	"time"
)

func TestRecentExecutionsBoundedCopiedAndOrdered(t *testing.T) {
	c, _ := NewController(manifest(), 10, 0)
	sequence := int32(0)
	for i := 1; i <= 150; i++ {
		for _, side := range []feed.Side{feed.Sell, feed.Buy} {
			sequence++
			p := packet(feed.New, int(sequence), side, 100, 1)
			p.Sequence = sequence
			if _, err := c.Process(encode(t, p), uint64(sequence), time.Now()); err != nil {
				t.Fatal(err)
			}
		}
	}
	s, _ := c.Snapshot(1, 5, time.Now(), time.Minute)
	if s.Stats.Executions != 150 || len(s.RecentExecutions) != 100 || s.RecentExecutions[0].SourceSequence != 102 || s.RecentExecutions[99].SourceSequence != 300 {
		t.Fatal(s)
	}
	s.RecentExecutions[0].Quantity = 99
	again, _ := c.Snapshot(1, 5, time.Now(), time.Minute)
	if again.RecentExecutions[0].Quantity != 1 {
		t.Fatal("snapshot exposes mutable ring")
	}
}
func TestDepthBoundedSelectionAndLevelAggregates(t *testing.T) {
	e := newEngine(t, 1000)
	for i := 1; i <= 150; i++ {
		apply(t, e, packet(feed.New, i, feed.Buy, int32(i), 2))
		apply(t, e, packet(feed.New, 150+i, feed.Sell, int32(1000+i), 3))
	}
	d, err := e.Snapshot(1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Bids) != 3 || d.Bids[0].Price != 150 || d.Bids[2].Price != 148 || d.Asks[0].Price != 1001 || d.Asks[2].Price != 1003 {
		t.Fatal(d)
	}
	apply(t, e, packet(feed.New, 400, feed.Buy, 150, 4))
	d, _ = e.Snapshot(1, 3)
	if d.Bids[0].Quantity != 6 || d.Bids[0].Orders != 2 {
		t.Fatal(d)
	}
}
