package main

import (
	"encoding/json"
	"github.com/nats-io/nats.go"
	"nse-mtbt-handler/internal/broker"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/outcomes"
	"nse-mtbt-handler/internal/runconfig"
	"strconv"
	"testing"
	"time"
)

func manifest() runconfig.Manifest {
	return runconfig.Manifest{Version: 2, StreamID: 1, Profile: feed.Profile, Baseline: "empty", CreatedAt: time.Now(), Instruments: []runconfig.Instrument{{Token: 1, Symbol: "A", TickSize: 5, BasePrice: 10000}, {Token: 2, Symbol: "B", TickSize: 5, BasePrice: 10000}}}
}
func TestWindowsAndMapping(t *testing.T) {
	for _, s := range []string{"", "2,1", "0", "100000", "2,2"} {
		if _, e := windows(s); e == nil {
			t.Fatal(s)
		}
	}
	w, e := windows("4,12")
	if e != nil || rawPosition(4, w) != 4 || rawPosition(5, w) != 6 || rawPosition(12, w) != 13 {
		t.Fatal(w, e)
	}
}
func TestTwoTickerIndependentOracle(t *testing.T) {
	m := manifest()
	w := expected(m, []int{4, 12})
	if len(w) != 20 {
		t.Fatal(len(w))
	}
	for _, n := range []int{1, 2} {
		x := w[outcomes.EventID(1, int32(n+2), identity(n))]
		if x.order.RemainingQty != 70 || x.order.FilledQty != "30" || x.order.Status != "PARTIALLY_FILLED" {
			t.Fatal(x)
		}
		x = w[outcomes.EventID(1, int32(n+4), identity(n))]
		if x.order.RemainingQty != 0 || x.order.FilledQty != "100" || x.order.Status != "FILLED" {
			t.Fatal(x)
		}
	}
	if w[outcomes.EventID(1, 5, identity(5))].broker != "6" {
		t.Fatal("HB position")
	}
}
func TestRawCorruption(t *testing.T) {
	m := manifest()
	d, _ := m.Digest()
	now := time.Now()
	p := feed.Packet{Stream: 1, Sequence: 3, Type: feed.New, Timestamp: now.Add(-time.Millisecond).UnixNano(), OrderID: identity(3), Token: 1, Side: feed.Sell, Price: 10000, Quantity: 30}
	raw, _ := feed.Encode(p)
	h := nats.Header{}
	h.Set(broker.ProfileHeader, feed.Profile)
	h.Set(broker.ManifestHeader, d)
	h.Set(broker.VersionHeader, "2")
	h.Set(broker.ReceivedHeader, ""+strconv.FormatInt(now.UnixNano(), 10))
	r := &nats.RawStreamMsg{Subject: "mtbt.1", Sequence: 3, Time: now.Add(time.Millisecond), Header: h, Data: raw}
	if _, e := validateRaw(m, d, r, 3, 0); e != nil {
		t.Fatal(e)
	}
	p.Quantity = 70
	r.Data, _ = feed.Encode(p)
	if _, e := validateRaw(m, d, r, 3, 0); e == nil {
		t.Fatal("bad quantity")
	}
	p.Quantity = 30
	r.Data, _ = feed.Encode(p)
	h.Set("mtbt-run", "")
	if _, e := validateRaw(m, d, r, 3, 0); e == nil {
		t.Fatal("legacy empty")
	}
}
func TestOutcomeExactness(t *testing.T) {
	m := manifest()
	w := expected(m, []int{4, 12})
	key := outcomes.EventID(1, 3, identity(1))
	want := w[key]
	now := time.Now()
	ev, e := outcomes.New(1, 3, 3, feed.New, "MATCH", want.order, now.Add(-time.Millisecond))
	if e != nil {
		t.Fatal(e)
	}
	raw, _ := json.Marshal(ev)
	h := nats.Header{}
	h.Set(nats.MsgIdHdr, key)
	r := &nats.RawStreamMsg{Subject: "mtbt.outcomes.1", Sequence: 1, Time: now, Data: raw, Header: h}
	seen := map[string]bool{}
	versions := map[string]int32{}
	if _, e := validateEvent(m, r, w, seen, versions); e != nil {
		t.Fatal(e)
	}
	if _, e := validateEvent(m, r, w, seen, versions); e == nil {
		t.Fatal("duplicate")
	}
	ev.Cause = "NEW"
	r.Data, _ = json.Marshal(ev)
	if _, e := validateEvent(m, r, w, map[string]bool{}, map[string]int32{}); e == nil {
		t.Fatal("wrong cause")
	}
	ev.Cause = "MATCH"
	ev.InputBrokerSequence = "4"
	r.Data, _ = json.Marshal(ev)
	if _, e := validateEvent(m, r, w, map[string]bool{}, map[string]int32{}); e == nil {
		t.Fatal("wrong provenance")
	}
}
