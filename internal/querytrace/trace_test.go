package querytrace

import (
	"context"
	"encoding/csv"
	"nse-mtbt-handler/internal/feed"
	"os"
	"path/filepath"
	"testing"
)

func TestDisabled(t *testing.T) {
	ctx := context.Background()
	r, e := New("", 0)
	if e != nil || r != nil {
		t.Fatal(e)
	}
	if WithEvent(ctx, r, nil, 1, 1) != ctx {
		t.Fatal("disabled changed context")
	}
	Start(ctx, "no-trace").End(true)
	if e = r.Close(); e != nil {
		t.Fatal(e)
	}
}
func TestCorrelatedSpansAndExclusiveCreate(t *testing.T) {
	p := filepath.Join(t.TempDir(), "ops.csv")
	r, e := New(p, 10)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = New(p, 10); e == nil {
		t.Fatal("overwrote evidence")
	}
	raw, e := feed.Encode(feed.Packet{Type: feed.Heartbeat, Stream: 1, LastSequence: 10})
	if e != nil {
		t.Fatal(e)
	}
	ctx := WithEvent(context.Background(), r, raw, 11, 2)
	Start(ctx, "checkpoint_select").End(true)
	Start(ctx, "commit").End(false)
	if e = r.Close(); e != nil {
		t.Fatal(e)
	}
	if e = r.Close(); e != nil {
		t.Fatal(e)
	}
	f, e := os.Open(p)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	rows, e := csv.NewReader(f).ReadAll()
	if e != nil || len(rows) != 3 {
		t.Fatal(rows, e)
	}
	if rows[1][0] != "11" || rows[1][1] != "0" || rows[1][2] != "Z" || rows[1][3] != "2" || rows[1][4] != "checkpoint_select" || rows[1][8] != "true" || rows[2][8] != "false" {
		t.Fatal(rows)
	}
}
func TestCapacity(t *testing.T) {
	p := filepath.Join(t.TempDir(), "ops.csv")
	r, e := New(p, 1)
	if e != nil {
		t.Fatal(e)
	}
	ctx := WithEvent(context.Background(), r, nil, 1, 1)
	Start(ctx, "one").End(true)
	Start(ctx, "two").End(true)
	if e = r.Close(); e == nil {
		t.Fatal("silent truncation")
	}
	if _, e = New(filepath.Join(t.TempDir(), "bad"), 0); e == nil {
		t.Fatal("invalid capacity")
	}
}
