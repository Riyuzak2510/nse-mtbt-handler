package main

import "net/http"

// Serving these routes implies that startup has bound UDP and connected to NATS.
// Readiness tracks connectivity; liveness deliberately does not depend on traffic.
func healthRoutes(connected func() bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"process":"alive","synthetic":true}`))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if !connected() {
			http.Error(w, "broker not connected", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
