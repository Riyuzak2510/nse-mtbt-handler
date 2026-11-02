package main

import (
	"context"
	"net"
	"net/http/httptest"
	"nse-mtbt-handler/internal/outcomerelay"
	"nse-mtbt-handler/internal/outcomes"
	"testing"
	"time"
)

func TestDeliveryConfiguration(t *testing.T) {
	for _, mode := range []string{"notify", "poll"} {
		if e := validateDelivery(mode, 5*time.Second); e != nil {
			t.Fatal(e)
		}
	}
	for _, tc := range []struct {
		mode     string
		interval time.Duration
	}{
		{"other", 5 * time.Second}, {"notify", 0}, {"notify", 999 * time.Millisecond}, {"notify", 61 * time.Second},
	} {
		if e := validateDelivery(tc.mode, tc.interval); e == nil {
			t.Fatal("invalid delivery configuration accepted", tc)
		}
	}
}

func TestReadinessReflectsDeliveryState(t *testing.T) {
	h := &health{}
	w := httptest.NewRecorder()
	h.routes().ServeHTTP(w, httptest.NewRequest("GET", "/readyz", nil))
	if w.Code != 503 {
		t.Fatal(w.Code)
	}
	h.set(true, "", outcomerelay.Stats{PendingCount: 3})
	w = httptest.NewRecorder()
	h.routes().ServeHTTP(w, httptest.NewRequest("GET", "/readyz", nil))
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
}
func TestServeCancelsDuringStartupRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	if e := serve(ctx, "127.0.0.1:0", "postgres://u:p@127.0.0.1:1/db?sslmode=disable", "nats://127.0.0.1:1", 1, "hash", outcomes.DefaultConfig()); e != nil {
		t.Fatal(e)
	}
	if time.Since(started) > 3*time.Second {
		t.Fatal("shutdownblocked")
	}
}
func TestListenerErrorPropagates(t *testing.T) {
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	if e = serve(context.Background(), l.Addr().String(), "", "", 1, "x", outcomes.DefaultConfig()); e == nil {
		t.Fatal("ignoredbindfailure")
	}
}
