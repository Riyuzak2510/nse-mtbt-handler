package main

import (
	"net/http/httptest"
	"testing"
)

func TestHealthIsIndependentOfBrokerReadiness(t *testing.T) {
	for _, connected := range []bool{false, true} {
		h := healthRoutes(func() bool { return connected })
		for _, path := range []string{"/healthz", "/readyz"} {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
			want := 200
			if path == "/readyz" && !connected {
				want = 503
			}
			if w.Code != want {
				t.Fatalf("connected=%v path=%s got=%d want=%d", connected, path, w.Code, want)
			}
		}
	}
}
