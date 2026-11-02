package sqldiagnostics

import (
	"context"
	"github.com/jackc/pgx/v5"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestConcurrentRecordsAndClose(t *testing.T) {
	r, e := NewRecorder(filepath.Join(t.TempDir(), "m"))
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				if e := r.recordQuery("order_select", 1000, nil); e != nil {
					t.Error(e)
				}
			}
		}()
	}
	wg.Wait()
	if r.histograms["order_select"].Count != 8000 {
		t.Fatal("lost count")
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := r.Close(); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
}
func TestTracerContextAndDuration(t *testing.T) {
	r, e := NewRecorder(filepath.Join(t.TempDir(), "m"))
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	tr := NewQueryTracer(r)
	ctx := tr.TraceQueryStart(context.Background(), nil, pgx.TraceQueryStartData{SQL: "SELECT * FROM mtbt.orders"})
	time.Sleep(time.Millisecond)
	tr.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})
	tr.TraceQueryEnd(context.Background(), nil, pgx.TraceQueryEndData{})
	if r.histograms["order_select"].Count != 1 || r.histograms["order_select"].Min < 100000 {
		t.Fatal("wrong query context/timing")
	}
}
