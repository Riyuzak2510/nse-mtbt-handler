package main

import (
	"context"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/runconfig"
	"testing"
	"time"
)

func TestFixture(t *testing.T) {
	m := runconfig.Manifest{Version: 2, StreamID: 1, Profile: feed.Profile, Baseline: "empty", CreatedAt: time.Now(), Instruments: []runconfig.Instrument{{Token: 1, Symbol: "A", TickSize: 5, BasePrice: 100}, {Token: 2, Symbol: "B", TickSize: 5, BasePrice: 200}}}
	for n := 1; n <= 12; n++ {
		p, e := fixture(m, n)
		if e != nil {
			t.Fatal(e)
		}
		if p.Token != int32((n-1)%2+1) || p.Sequence != int32(n) {
			t.Fatal(p)
		}
		raw, e := feed.Encode(p)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = feed.Decode(raw); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := fixture(m, 0); e == nil {
		t.Fatal("bad fixture")
	}
}

func TestWaitCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if waitUntil(ctx, time.Now().Add(time.Hour)) == nil {
		t.Fatal("wait did not cancel")
	}
}
func TestWaitDeadline(t *testing.T) {
	start := time.Now()
	if e := waitUntil(context.Background(), start.Add(time.Millisecond)); e != nil || time.Since(start) < time.Millisecond {
		t.Fatal(e)
	}
}
