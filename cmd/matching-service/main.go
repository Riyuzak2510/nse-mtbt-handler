// matching-service runs an ephemeral synthetic shadow, never an order router or NSE authority.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/nats-io/nats.go"
	"log"
	"net"
	"net/http"
	"nse-mtbt-handler/internal/appconfig"
	"nse-mtbt-handler/internal/broker"
	"nse-mtbt-handler/internal/durable"
	"nse-mtbt-handler/internal/matching"
	"nse-mtbt-handler/internal/runconfig"
	"nse-mtbt-handler/internal/timingtrace"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() (runErr error) {
	manifestPath := flag.String("manifest", "run/demo.json", "existing feed manifest")
	url := flag.String("nats", appconfig.Env("NATS_URL", appconfig.DefaultNATS), "existing JetStream server; prefer NATS_URL")
	addr := flag.String("http", "127.0.0.1:19004", "shadow diagnostic HTTP bind")
	remote := flag.Bool("allow-non-loopback", false, "explicitly permit remote bind; isolated lab only")
	capacity := flag.Int("max-active", 100000, "maximum active shadow orders;1..1000000; fail closed, not byte-memory guarantee")
	stale := flag.Duration("stale-after", 5*time.Second, "maximum publisher receive age for READY shadow reads")
	tracePath := flag.String("timings", "", "optional new CSV: shadow=RAM apply, durable=PostgreSQL commit")
	traceLimit := flag.Int("timings-limit", 100000, "maximum timing rows; preallocated only if enabled")
	mode := flag.String("mode", "shadow", "matching mode: shadow (ephemeral) or durable (database-backed)")
	database := flag.String("database", appconfig.Env("DATABASE_URL", appconfig.DefaultDB), "PostgreSQL connection string; required for -mode=durable; prefer DATABASE_URL")
	outcomesEnabled := flag.Bool("outcomes", false, "enable reliable pending outcomes; fresh native matching stream only")
	outRows := flag.Int64("outcomes-max-pending", 100000, "pending row bound; persisted per stream")
	outBytes := flag.Int64("outcomes-max-bytes", 128<<20, "serialized pending byte bound; excludes table/index/WAL")
	flag.Parse()
	if *stale <= 0 {
		return fmt.Errorf("stale-after must be positive")
	}
	if err := appconfig.ValidateBindAddress(*addr, *remote); err != nil {
		return err
	}

	// Dispatch based on mode
	switch *mode {
	case "shadow":
		if *outcomesEnabled {
			return fmt.Errorf("outcomes require durable mode")
		}
		return runShadow(*manifestPath, *url, *addr, *remote, *capacity, *stale, *tracePath, *traceLimit)
	case "durable":
		return runDurable(*manifestPath, *url, *addr, *remote, *capacity, *stale, *tracePath, *traceLimit, *database, durable.OutcomeOptions{Enabled: *outcomesEnabled, MaxPendingRows: *outRows, MaxPendingBytes: *outBytes})
	default:
		return fmt.Errorf("unknown mode: %q; must be shadow or durable", *mode)
	}
}

// runShadow runs the original ephemeral shadow matcher (unchanged logic).
func runShadow(
	manifestPath string,
	natsURL string,
	httpAddr string,
	allowNonLoopback bool,
	maxActive int,
	staleAfter time.Duration,
	tracePath string,
	traceLimit int,
) (runErr error) {
	m, err := runconfig.Load(manifestPath)
	if err != nil {
		return err
	}
	trace, err := timingtrace.New(tracePath, traceLimit)
	if err != nil {
		return err
	}
	defer func() {
		if e := trace.Close(); runErr == nil {
			runErr = e
		}
	}()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	shadow, err := broker.OpenShadow(ctx, natsURL, m)
	if err != nil {
		return err
	}
	defer func() {
		if e := shadow.Close(); runErr == nil {
			runErr = e
		}
	}()
	controller, err := matching.NewController(m, maxActive, shadow.ReplayThrough)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: routes(controller, m, staleAfter), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	serverErrors := make(chan error, 1)
	go func() {
		e := srv.Serve(listener)
		if !errors.Is(e, http.ErrServerClosed) {
			serverErrors <- e
		}
	}()
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	log.Printf("shadow matching process started stream=%d replay_through=%d http=%s max_active=%d policy=shadow-price-time-replace-v1 ephemeral=true", m.StreamID, shadow.ReplayThrough, httpAddr, maxActive)
	err = consume(ctx, shadow, controller, trace, serverErrors)
	if err != nil {
		controller.Invalidate(err.Error())
	}
	return err
}
func consume(ctx context.Context, shadow *broker.Shadow, c *matching.Controller, trace *timingtrace.Recorder, serverErrors <-chan error) error {
	for ctx.Err() == nil {
		select {
		case e := <-serverErrors:
			return e
		default:
		}
		if !shadow.Conn.IsConnected() {
			c.SetConnected(false)
			return fmt.Errorf("shadow NATS disconnected; restart must rebuild")
		}
		msgs, err := shadow.Subscription.Fetch(1, nats.MaxWait(time.Second))
		if errors.Is(err, nats.ErrTimeout) {
			continue
		}
		if err != nil {
			return err
		}
		delivery := time.Now()
		msg := msgs[0]
		received, err := shadow.Validate(msg)
		if err != nil {
			return err
		}
		meta, err := msg.Metadata()
		if err != nil {
			return err
		}
		started := time.Now()
		_, err = c.Process(msg.Data, meta.Sequence.Stream, received)
		applied := time.Now()
		if err != nil {
			return err
		}
		ackStart := time.Now()
		err = msg.AckSync(nats.AckWait(3 * time.Second))
		acked := time.Now()
		ackNS := int64(0)
		if err == nil {
			ackNS = acked.UnixNano()
		}
		seq, kind := timingtrace.Identity(msg.Data)
		trace.Record(timingtrace.Sample{Sequence: seq, Type: kind, StartUnixNS: started.UnixNano(), EndUnixNS: applied.UnixNano(), DurationNS: applied.Sub(started).Nanoseconds(), ReceivedUnixNS: received.UnixNano(), DeliveryUnixNS: delivery.UnixNano(), AckUnixNS: ackNS, AckDurationNS: acked.Sub(ackStart).Nanoseconds(), Attempts: 1, SourceBroker: meta.Sequence.Stream})
		if err != nil {
			return fmt.Errorf("shadow memory applied, ack failed: restart requires full rebuild: %w", err)
		}
	}
	return nil
}
func routes(c *matching.Controller, m runconfig.Manifest, stale time.Duration) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"process":"alive","synthetic":true,"ephemeral":true}`))
	})
	read := func(w http.ResponseWriter, r *http.Request, readyOnly bool) {
		token := m.Instruments[0].Token
		depth := 5
		if !readyOnly {
			n, e := strconv.ParseInt(r.URL.Query().Get("token"), 10, 32)
			if e != nil || !m.TokenAllowed(int32(n)) {
				http.Error(w, "known token required", 400)
				return
			}
			token = int32(n)
			if q := r.URL.Query().Get("depth"); q != "" {
				var e error
				depth, e = strconv.Atoi(q)
				if e != nil || depth < 1 || depth > 100 {
					http.Error(w, "depth must be1..100", 400)
					return
				}
			}
		}
		s, e := c.Snapshot(token, depth, time.Now(), stale)
		if e != nil {
			http.Error(w, "shadow snapshot unavailable", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if s.Status != "READY" {
			w.WriteHeader(503)
		}
		_ = json.NewEncoder(w).Encode(s)
	}
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) { read(w, r, true) })
	mux.HandleFunc("GET /shadow/book", func(w http.ResponseWriter, r *http.Request) { read(w, r, false) })
	return mux
}
