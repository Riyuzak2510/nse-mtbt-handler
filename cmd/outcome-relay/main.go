package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"nse-mtbt-handler/internal/appconfig"
	"nse-mtbt-handler/internal/outcomerelay"
	"nse-mtbt-handler/internal/outcomes"
	"nse-mtbt-handler/internal/runconfig"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type health struct {
	sync.RWMutex
	Ready  bool
	Reason string
	Stats  outcomerelay.Stats
}

func (h *health) set(ok bool, reason string, s outcomerelay.Stats) {
	h.Lock()
	defer h.Unlock()
	h.Ready = ok
	h.Reason = reason
	h.Stats = s
}
func (h *health) routes() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("alive")) })
	for _, path := range []string{"/readyz", "/metrics"} {
		m.HandleFunc("GET "+path, func(w http.ResponseWriter, r *http.Request) {
			h.RLock()
			defer h.RUnlock()
			w.Header().Set("Content-Type", "application/json")
			if r.URL.Path == "/readyz" && !h.Ready {
				w.WriteHeader(503)
			}
			json.NewEncoder(w).Encode(struct {
				Ready  bool               `json:"ready"`
				Reason string             `json:"reason,omitempty"`
				Stats  outcomerelay.Stats `json:"outbox"`
			}{h.Ready, h.Reason, h.Stats})
		})
	}
	return m
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	manifest := flag.String("manifest", "run/demo.json", "existing matching native manifest")
	addr := flag.String("http", "127.0.0.1:19005", "relay diagnostic HTTP")
	remote := flag.Bool("allow-non-loopback", false, "allow isolated remote bind")
	maxBytes := flag.Int64("stream-max-bytes", 64<<20, "explicit outcome stream byte limit")
	age := flag.Duration("stream-max-age", 24*time.Hour, "outcome retention")
	dedup := flag.Duration("dedup-window", 10*time.Minute, "bounded broker duplicate suppression")
	replicas := flag.Int("replicas", 1, "outcome stream replicas; cluster must support")
	mode := flag.String("delivery-mode", "notify", "notify (commit-triggered) or poll (100ms compatibility control)")
	reconcile := flag.Duration("reconcile-interval", 5*time.Second, "safety scan interval for missed commit hints; 1s..1m")
	flag.Parse()
	if e := validateDelivery(*mode, *reconcile); e != nil {
		return e
	}
	if e := appconfig.ValidateBindAddress(*addr, *remote); e != nil {
		return e
	}
	m, e := runconfig.Load(*manifest)
	if e != nil {
		return e
	}
	cfg := outcomes.Config{MaxBytes: *maxBytes, MaxAge: *age, DuplicateWindow: *dedup, Replicas: *replicas}
	if e = cfg.Validate(); e != nil {
		return e
	}
	hash, e := m.Digest()
	if e != nil {
		return e
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return serveWithDelivery(ctx, *addr, appconfig.Env("DATABASE_URL", appconfig.DefaultDB), appconfig.Env("NATS_URL", appconfig.DefaultNATS), m.StreamID, hash, cfg, *mode, *reconcile)
}

func validateDelivery(mode string, reconcile time.Duration) error {
	if mode != "notify" && mode != "poll" {
		return fmt.Errorf("delivery-mode must be notify or poll")
	}
	if reconcile < time.Second || reconcile > time.Minute {
		return fmt.Errorf("reconcile-interval must be 1s..1m")
	}
	return nil
}

func serve(ctx context.Context, addr, dsn, url string, stream int16, hash string, cfg outcomes.Config) error {
	return serveWithDelivery(ctx, addr, dsn, url, stream, hash, cfg, "notify", 5*time.Second)
}

func serveWithDelivery(ctx context.Context, addr, dsn, url string, stream int16, hash string, cfg outcomes.Config, mode string, reconcile time.Duration) error {
	if e := validateDelivery(mode, reconcile); e != nil {
		return e
	}
	listener, e := net.Listen("tcp", addr)
	if e != nil {
		return e
	}
	h := &health{}
	server := &http.Server{Handler: h.routes(), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	errs := make(chan error, 1)
	go func() {
		err := server.Serve(listener)
		if !errors.Is(err, http.ErrServerClosed) {
			errs <- err
		}
	}()
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		server.Shutdown(c)
	}()
	var relay *outcomerelay.Relay
	var pub *outcomes.Publisher
	cleanup := func() {
		if pub != nil {
			pub.Close()
			pub = nil
		}
		if relay != nil {
			c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			relay.Close(c)
			cancel()
			relay = nil
		}
	}
	defer cleanup()
	backoff := 100 * time.Millisecond
	lastStats := time.Time{}
	wait := func(d time.Duration) error {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-errs:
			return e
		case <-timer.C:
			return nil
		}
	}
	for ctx.Err() == nil {
		select {
		case e := <-errs:
			return e
		default:
		}
		op, cancel := context.WithTimeout(ctx, 5*time.Second)
		if relay == nil {
			relay, e = outcomerelay.OpenWithWakeups(op, dsn, stream, hash, mode == "notify")
		}
		if e == nil && pub == nil {
			pub, e = outcomes.Connect(url, stream, cfg)
		}
		var worked bool
		if e == nil {
			worked, e = relay.Step(op, pub)
		}
		if e == nil && time.Since(lastStats) >= time.Second {
			var s outcomerelay.Stats
			s, e = relay.Status(op)
			if e == nil {
				h.set(true, "", s)
				lastStats = time.Now()
			}
		}
		cancel()
		if e == nil && !worked {
			if mode == "notify" {
				e = relay.Wait(ctx, reconcile)
			} else {
				e = wait(100 * time.Millisecond)
			}
		}
		if e != nil {
			h.set(false, "delivery unavailable; inspect relay logs", outcomerelay.Stats{StreamID: stream})
			if outcomerelay.IsPermanent(e) {
				return e
			}
			cleanup()
			if ctx.Err() != nil {
				return nil
			}
			if err := wait(backoff + time.Duration(rand.Int64N(int64(backoff/4+1)))); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			backoff *= 2
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
			e = nil
			continue
		}
		backoff = 100 * time.Millisecond
	}
	return nil
}
