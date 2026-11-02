package timingtrace

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"
)

func TestTrace(t *testing.T) {
	p := filepath.Join(t.TempDir(), "trace.csv")
	r, e := New(p, 2)
	if e != nil {
		t.Fatal(e)
	}
	if _, e := New(p, 2); e == nil {
		t.Fatal("overwrote trace")
	}
	r.Record(Sample{Sequence: 1, Type: "N", StartUnixNS: 10, EndUnixNS: 20, DurationNS: 10})
	if e = r.Close(); e != nil {
		t.Fatal(e)
	}
	if e = r.Close(); e != nil {
		t.Fatal(e)
	}
	f, _ := os.Open(p)
	defer f.Close()
	rows, e := csv.NewReader(f).ReadAll()
	if e != nil || len(rows) != 2 || rows[1][0] != "1" || rows[1][4] != "10" {
		t.Fatal(rows, e)
	}
}
func TestDisabledAndTruncation(t *testing.T) {
	r, e := New("", 0)
	if e != nil || r != nil {
		t.Fatal(e)
	}
	r.Record(Sample{})
	if e = r.Close(); e != nil {
		t.Fatal(e)
	}
	p := filepath.Join(t.TempDir(), "trace.csv")
	r, e = New(p, 1)
	if e != nil {
		t.Fatal(e)
	}
	r.Record(Sample{})
	r.Record(Sample{})
	if e = r.Close(); e == nil {
		t.Fatal("silent truncation")
	}
	if _, e = New(filepath.Join(t.TempDir(), "bad"), 0); e == nil {
		t.Fatal("accepted invalid capacity")
	}
}
