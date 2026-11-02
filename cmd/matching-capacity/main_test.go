package main

import "testing"

func TestSmallCapacityOracle(t *testing.T) {
	for _, layout := range []string{"dense", "sparse"} {
		r, e := run(421, layout, 2)
		if e != nil {
			t.Fatal(e)
		}
		if !r.Verified || !r.Restored || r.BurstFills == 0 {
			t.Fatalf("incomplete verification %+v", r)
		}
		for _, s := range r.Samples {
			if s.Phase == "loaded_gc" || s.Phase == "restored_gc" || s.Phase == "churn_gc" {
				if s.Active != 421 || s.HeapAlloc == 0 {
					t.Fatalf("bad memory phase %+v", s)
				}
			}
		}
	}
}
func TestCapacityBounds(t *testing.T) {
	for _, n := range []int{0, -1, 1000001} {
		if _, e := run(n, "dense", 1); e == nil {
			t.Fatal("accepted count", n)
		}
	}
	if _, e := run(1, "unknown", 1); e == nil {
		t.Fatal("layout")
	}
}
