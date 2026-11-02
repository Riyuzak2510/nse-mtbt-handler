package main

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"nse-mtbt-handler/internal/durable"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/runconfig"
)

type reader struct {
	status string
	fail   bool
}

func (r reader) Snapshot(context.Context, int32, int, time.Time, time.Duration) (durable.Snapshot, error) {
	if r.fail {
		return durable.Snapshot{}, errors.New("database unavailable")
	}
	return durable.Snapshot{Status: r.status, Synthetic: true}, nil
}
func (r reader) Order(context.Context, feed.OrderID) (durable.Order, bool, error) {
	return durable.Order{Status: "PARTIALLY_FILLED", Remaining: 70, Filled: 30}, !r.fail, nil
}
func TestDurableRoutes(t *testing.T) {
	m := runconfig.Manifest{Instruments: []runconfig.Instrument{{Token: 1}}}
	for _, tc := range []struct {
		path, status string
		code         int
	}{
		{"/healthz", "INVALID", 200}, {"/readyz", "READY", 200}, {"/readyz", "REBUILDING", 503},
		{"/book?token=1", "READY", 200}, {"/book", "READY", 400}, {"/book?token=2", "READY", 400},
		{"/book?token=1&depth=101", "READY", 400}, {"/book?token=1&depth=0", "READY", 400},
		{"/orders?id=4607182418800017408", "READY", 200}, {"/orders?id=0", "READY", 400}, {"/orders?id=9221120237041090560", "READY", 400}, {"/orders", "READY", 400},
	} {
		t.Run(tc.path+tc.status, func(t *testing.T) {
			w := httptest.NewRecorder()
			durableRoutes(reader{status: tc.status}, m, time.Minute).ServeHTTP(w, httptest.NewRequest("GET", tc.path, nil))
			if w.Code != tc.code {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
			if tc.code == 200 && tc.path != "/healthz" && w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("missing no-store")
			}
		})
	}
	w := httptest.NewRecorder()
	durableRoutes(reader{fail: true}, m, time.Minute).ServeHTTP(w, httptest.NewRequest("GET", "/book?token=1", nil))
	if w.Code != 503 {
		t.Fatal(w.Code)
	}
}
func TestDurableValidationBeforeDatabase(t *testing.T) {
	if e := runDurable("missing", "", "127.0.0.1:0", false, 100, 0, "", 10, ""); e == nil {
		t.Fatal("accepted zero freshness")
	}
	if e := runDurable("missing", "", "0.0.0.0:0", false, 100, time.Minute, "", 10, ""); e == nil {
		t.Fatal("accepted unapproved remote bind")
	}
}
