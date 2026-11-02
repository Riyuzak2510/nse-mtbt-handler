package store

import (
	"context"
	"encoding/csv"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/querytrace"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWriteOperationTrace(t *testing.T) {
	s, m := setup(t)
	path := filepath.Join(t.TempDir(), "queries.csv")
	rec, e := querytrace.New(path, 100)
	if e != nil {
		t.Fatal(e)
	}
	defer rec.Close()
	packets := []feed.Packet{order(1, 1, feed.Buy), {Stream: 1, Type: feed.Heartbeat, LastSequence: 1}}
	for i, p := range packets {
		raw, e := feed.Encode(p)
		if e != nil {
			t.Fatal(e)
		}
		ctx := querytrace.WithEvent(context.Background(), rec, raw, uint64(i+1), 1)
		if e = s.Process(ctx, m, raw, uint64(i+1), time.Now()); e != nil {
			t.Fatal(e)
		}
	}
	if e = rec.Close(); e != nil {
		t.Fatal(e)
	}
	f, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	rows, e := csv.NewReader(f).ReadAll()
	if e != nil {
		t.Fatal(e)
	}
	want := []string{"begin", "checkpoint_select_for_update", "order_select", "order_upsert", "checkpoint_update", "commit", "begin", "checkpoint_select_for_update", "checkpoint_update", "commit"}
	if len(rows) != len(want)+1 {
		t.Fatal("wrong operation count", rows)
	}
	for i, op := range want {
		row := rows[i+1]
		if row[4] != op || row[8] != "true" {
			t.Fatal("query span identity/success", row)
		}
	}
	// New-order lookup has ErrNoRows internally: it must be classified as a successful SELECT.
	if rows[3][4] != "order_select" || rows[3][8] != "true" {
		t.Fatal("missing-order lookup wrongly marked failed")
	}
}
