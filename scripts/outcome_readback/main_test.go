package main

import (
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/outcomes"
	"reflect"
	"testing"
	"time"
)

func TestWindows(t *testing.T) {
	for _, s := range []string{"", "0", "1,1", "2,1", "1,", "100000", " 1"} {
		if _, e := parseWindows(s); e == nil {
			t.Fatalf("accepted %q", s)
		}
	}
	w, e := parseWindows("2,5")
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(rawLayout(w), []int32{1, 2, 0, 3, 4, 5, 0}) {
		t.Fatal(rawLayout(w))
	}
}
func TestFanoutAndTails(t *testing.T) {
	for n := int32(1); n < 100; n++ {
		var count uint64
		for i := int32(1); i <= n; i++ {
			count += uint64(len(snapshots(i, 1)))
		}
		if count != expectedCount(n) {
			t.Fatal(n, count)
		}
	}
}
func TestIndependentSnapshots(t *testing.T) {
	a := snapshots(1, 42)
	if len(a) != 1 || a[0].Status != "OPEN" || a[0].RemainingQty != 100 {
		t.Fatal(a)
	}
	for n := int32(2); n <= 3; n++ {
		a = snapshots(n, 42)
		if len(a) != 2 {
			t.Fatal(a)
		}
		for _, s := range a {
			cause := "MATCH"
			if s.OrderIDHex == wireID(n) {
				cause = "NEW"
			}
			e, err := outcomes.New(7, n, uint64(n+1), feed.New, cause, s, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			if err = checkEvent(e, 7, n, uint64(n+1), s); err != nil {
				t.Fatal(err)
			}
			e.InputBrokerSequence = "100"
			if checkEvent(e, 7, n, uint64(n+1), s) == nil {
				t.Fatal("bad provenance accepted")
			}
		}
	}
}
func TestSnapshotMutationRejected(t *testing.T) {
	s := snapshots(2, 1)[0]
	cause := "MATCH"
	if s.OrderIDHex == wireID(2) {
		cause = "NEW"
	}
	e, err := outcomes.New(1, 2, 2, feed.New, cause, s, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	e.Order.Token = 2
	if checkEvent(e, 1, 2, 2, s) == nil {
		t.Fatal("mutated token accepted")
	}
}
