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
	"nse-mtbt-handler/internal/querytrace"
	"nse-mtbt-handler/internal/runconfig"
	"nse-mtbt-handler/internal/store"
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
	path := flag.String("manifest", "run/demo.json", "source configuration")
	shards := flag.Int("shards", 0, "parallel instrument subscribers: 1..16 for a fresh run; 0 keeps serial mode")
	parallelPath := flag.String("parallel-timings", "", "new timing directory: independent dispatcher/shard CSVs; requires shards")
	dsn := flag.String("database", appconfig.Env("DATABASE_URL", appconfig.DefaultDB), "PostgreSQL DSN; prefer DATABASE_URL")
	url := flag.String("nats", appconfig.Env("NATS_URL", appconfig.DefaultNATS), "NATS URL")
	addr := flag.String("http", "127.0.0.1:19002", "loopback HTTP address")
	allowNonLoopback := flag.Bool("allow-non-loopback", false, "allow binding to non-loopback addresses")
	stale := flag.Duration("stale-after", 5*time.Second, "maximum receive age for trusted reads")
	timingsPath := flag.String("timings", "", "optional bounded CSV timing trace, flushed on shutdown")
	timingsLimit := flag.Int("timings-limit", 100000, "maximum trace samples; overflow makes the run fail")
	queryPath := flag.String("query-timings", "", "optional bounded client-observed SQL-operation CSV, flushed on shutdown")
	queryLimit := flag.Int("query-timings-limit", 800000, "maximum SQL operation samples; overflow fails the trace")
	flag.Parse()
	queries, err := querytrace.New(*queryPath, *queryLimit)
	if err != nil {
		return err
	}
	defer func() {
		if e := queries.Close(); runErr == nil {
			runErr = e
		}
	}()
	trace, err := timingtrace.New(*timingsPath, *timingsLimit)
	if err != nil {
		return err
	}
	defer func() {
		if e := trace.Close(); runErr == nil {
			runErr = e
		}
	}()
	if *stale <= 0 {
		return fmt.Errorf("stale-after must be positive")
	}
	m, err := runconfig.Load(*path)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *shards < 0 || *shards > 16 {
		return fmt.Errorf("shards must be 0..16")
	}
	if *shards > 0 && (*timingsPath != "" || *queryPath != "") {
		return fmt.Errorf("serial timing recorders cannot be shared by shard workers; use parallel integration measurements")
	}
	if *parallelPath != "" && *shards == 0 {
		return fmt.Errorf("parallel-timings requires positive shards")
	}
	parallelTimings, err := timingtrace.NewParallel(*parallelPath, *shards, *timingsLimit)
	if err != nil {
		return err
	}
	defer func() {
		if e := parallelTimings.Close(); runErr == nil {
			runErr = e
		}
	}()
	poolMax := int32(6)
	if *shards > 0 {
		poolMax = int32(*shards + 4)
	}
	db, err := store.OpenWithPool(ctx, *dsn, poolMax)
	if err != nil {
		return err
	}
	defer db.Close()
	if err = db.Migrate(ctx); err != nil {
		return err
	}
	release, err := db.AcquireLease(ctx, m)
	if err != nil {
		return err
	}
	defer release()
	if err = db.Init(ctx, m); err != nil {
		return err
	}
	if *shards > 0 {
		if err = db.InitParallel(ctx, m, *shards); err != nil {
			return err
		}
	} else if err = db.CheckSerial(ctx, m); err != nil {
		return err
	}
	c, err := broker.Connect(ctx, *url, m)
	if err != nil {
		return err
	}
	defer c.Close()
	sub, err := c.Subscribe()
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()
	if err := appconfig.ValidateBindAddress(*addr, *allowNonLoopback); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: routes(db, m, *stale), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	serverErr := make(chan error, 1)
	go func() { serverErr <- srv.Serve(listener) }()
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	if *shards > 0 {
		return runParallelTimed(ctx, db, c, sub, m, *shards, serverErr, listener.Addr().String(), parallelTimings)
	}
	log.Printf("book service ready stream=%d http=%s", m.StreamID, listener.Addr())
	for ctx.Err() == nil {
		select {
		case e := <-serverErr:
			if !errors.Is(e, http.ErrServerClosed) {
				return e
			}
			return nil
		default:
		}
		msgs, e := sub.Fetch(1, nats.MaxWait(time.Second))
		if errors.Is(e, nats.ErrTimeout) {
			continue
		}
		if e != nil {
			return e
		}
		var delivery, processingStart time.Time
		var attempts int64
		var queryAttempt int64
		if trace != nil {
			delivery = time.Now()
		}
		msg := msgs[0]
		received, e := c.Validate(msg)
		if e != nil {
			if inv := db.Invalidate(ctx, m, e.Error()); inv != nil {
				return inv
			}
			return e
		}
		meta, e := msg.Metadata()
		if e != nil {
			return e
		}
		for ctx.Err() == nil {
			if trace != nil {
				if attempts == 0 {
					processingStart = time.Now()
				}
				attempts++
			}
			opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			queryAttempt++
			opCtx = querytrace.WithEvent(opCtx, queries, msg.Data, meta.Sequence.Stream, queryAttempt)
			e = db.Process(opCtx, m, msg.Data, meta.Sequence.Stream, received)
			cancel()
			if e == nil {
				var committed, ackStart time.Time
				if trace != nil {
					committed = time.Now()
					ackStart = time.Now()
				}
				e = msg.AckSync(nats.AckWait(3 * time.Second))
				if trace != nil {
					end := time.Now()
					seq, kind := timingtrace.Identity(msg.Data)
					ackNS := int64(0)
					if e == nil {
						ackNS = end.UnixNano()
					}
					trace.Record(timingtrace.Sample{Sequence: seq, Type: kind, StartUnixNS: processingStart.UnixNano(), EndUnixNS: committed.UnixNano(), DurationNS: committed.Sub(processingStart).Nanoseconds(), ReceivedUnixNS: received.UnixNano(), DeliveryUnixNS: delivery.UnixNano(), AckUnixNS: ackNS, AckDurationNS: end.Sub(ackStart).Nanoseconds(), Attempts: attempts})
				}
				if e != nil {
					return fmt.Errorf("commit succeeded, ack failed; safe to restart: %w", e)
				}
				break
			}
			if store.IsFatal(e) {
				return fmt.Errorf("book invalid: %w", e)
			}
			log.Printf("database processing failed; retrying without ack: %v", e)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
	return nil
}
func routes(db *store.Store, m runconfig.Manifest, stale time.Duration) http.Handler {
	mux := http.NewServeMux()
	snapshot := func(r *http.Request, token int32, depth int) (store.Snapshot, bool, error) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		s, e := db.Snapshot(ctx, m, token, depth)
		fresh := s.LastReceived != nil && time.Since(*s.LastReceived) <= stale
		return s, fresh, e
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"process":"alive","synthetic":true}`))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		s, f, e := snapshot(r, m.Instruments[0].Token, 1)
		if e != nil || s.Status != "VALID" || !f {
			http.Error(w, "book is not valid and fresh", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /book", func(w http.ResponseWriter, r *http.Request) {
		token, e := strconv.ParseInt(r.URL.Query().Get("token"), 10, 32)
		if e != nil || !m.TokenAllowed(int32(token)) {
			http.Error(w, "known token required", 400)
			return
		}
		depth := 5
		if value := r.URL.Query().Get("depth"); value != "" {
			depth, e = strconv.Atoi(value)
			if e != nil || depth < 1 || depth > 100 {
				http.Error(w, "depth must be 1..100", 400)
				return
			}
		}
		s, f, e := snapshot(r, int32(token), depth)
		if e != nil {
			http.Error(w, "book storage unavailable", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if s.Status != "VALID" || !f {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(struct {
			store.Snapshot
			Token     int32 `json:"token"`
			Synthetic bool  `json:"synthetic"`
			Fresh     bool  `json:"fresh"`
		}{s, int32(token), true, f})
	})
	return mux
}
