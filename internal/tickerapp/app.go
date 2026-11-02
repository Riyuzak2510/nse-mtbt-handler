// Package tickerapp runs the separate admission and instrument matching processes.
package tickerapp

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"net"
	"net/http"
	"nse-mtbt-handler/internal/appconfig"
	"nse-mtbt-handler/internal/broker"
	"nse-mtbt-handler/internal/durable"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/runconfig"
	"nse-mtbt-handler/internal/tickerwire"
	"nse-mtbt-handler/internal/timingtrace"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

func Run(args []string, matcher bool) (result error) {
	f := flag.NewFlagSet("ticker-service", flag.ContinueOnError)
	manifest := f.String("manifest", "run/demo.json", "configured native history manifest")
	token := f.Int("token", 0, "one canonical instrument token; matcher only")
	addr := f.String("http", "127.0.0.1:19006", "HTTP status bind")
	remote := f.Bool("allow-non-loopback", false, "allow isolated cluster bind")
	max := f.Int("max-active", 100000, "per instrument active order cap")
	timings := f.String("timings", "", "opt-in benchmark trace CSV path")
	timingsLimit := f.Int("timings-limit", 50000, "max samples per trace")
	if e := f.Parse(args); e != nil {
		return e
	}
	if f.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	if e := appconfig.ValidateBindAddress(*addr, *remote); e != nil {
		return e
	}
	m, e := runconfig.Load(*manifest)
	if e != nil {
		return e
	}
	if len(m.Instruments) > 16 {
		return fmt.Errorf("at most16 configured tickers supported")
	}
	if matcher && (*token <= 0 || int64(*token) > 2147483647 || !m.TokenAllowed(int32(*token))) {
		return fmt.Errorf("known token required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	dsn := appconfig.Env("DATABASE_URL", appconfig.DefaultDB)
	url := appconfig.Env("NATS_URL", appconfig.DefaultNATS)
	var intake *durable.Intake
	var match *durable.Service
	if matcher {
		match, e = durable.OpenTicker(ctx, dsn, m, int32(*token), *max)
		if e != nil {
			return e
		}
		defer match.Close()
	} else {
		intake, e = durable.OpenIntake(ctx, dsn, m)
		if e != nil {
			return e
		}
		defer intake.Close()
	}
	rec, e := timingtrace.New(*timings, *timingsLimit)
	if e != nil {
		return e
	}
	defer func() {
		if closeErr := rec.Close(); result == nil {
			result = closeErr
		}
	}()
	client, e := broker.Connect(ctx, url, m)
	if e != nil {
		return e
	}
	defer client.Close()
	bus, e := tickerwire.Open(ctx, client.Conn, client.JS, m)
	if e != nil {
		return e
	}
	errorsCh := make(chan error, 18)
	var wg sync.WaitGroup
	launch := func(fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := fn(); e != nil && ctx.Err() == nil {
				select {
				case errorsCh <- e:
				default:
				}
			}
		}()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"alive": true, "synthetic": true})
	})
	status := func(ctx context.Context) (map[string]any, error) {
		if matcher {
			return match.TickerStatus(ctx)
		}
		return intake.Status(ctx)
	}
	for _, path := range []string{"GET /status", "GET /readyz"} {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			v, e := status(r.Context())
			if e != nil {
				http.Error(w, "database unavailable", 503)
				return
			}
			if !client.Conn.IsConnected() || r.URL.Path == "/readyz" && v["status"] != "VALID" {
				w.WriteHeader(503)
			}
			json.NewEncoder(w).Encode(v)
		})
	}
	mux.HandleFunc("GET /orders", func(w http.ResponseWriter, r *http.Request) {
		b, e := hex.DecodeString(r.URL.Query().Get("id"))
		if e != nil {
			http.Error(w, "wire hex id required", 400)
			return
		}
		id, e := feed.IDFromBytes(b)
		if e != nil {
			http.Error(w, "wire hex id required", 400)
			return
		}
		if matcher {
			o, found, e := match.Order(r.Context(), id)
			if e != nil {
				http.Error(w, "order unavailable", 503)
				return
			}
			if !found || o.Token != int32(*token) {
				http.NotFound(w, r)
				return
			}
			json.NewEncoder(w).Encode(o)
		} else {
			v, e := intake.OrderView(r.Context(), id)
			if errors.Is(e, pgx.ErrNoRows) {
				http.NotFound(w, r)
				return
			}
			if e != nil {
				http.Error(w, "order unavailable", 503)
				return
			}
			json.NewEncoder(w).Encode(v)
		}
	})
	if matcher {
		mux.HandleFunc("GET /book", func(w http.ResponseWriter, r *http.Request) {
			depth := 5
			if q := r.URL.Query().Get("depth"); q != "" {
				var e error
				depth, e = strconv.Atoi(q)
				if e != nil || depth < 1 || depth > 100 {
					http.Error(w, "depth1..100", 400)
					return
				}
			}
			v, e := match.TickerSnapshot(r.Context(), depth)
			if e != nil {
				http.Error(w, "book unavailable", 503)
				return
			}
			if v.Status != "READY" && v.Status != "VALID" {
				w.WriteHeader(503)
			}
			json.NewEncoder(w).Encode(v)
		})
	}
	listener, e := net.Listen("tcp", *addr)
	if e != nil {
		return e
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	launch(func() error {
		e := server.Serve(listener)
		if errors.Is(e, http.ErrServerClosed) {
			return nil
		}
		return e
	})
	if matcher {
		match.Connected(true)
		sub, e := bus.Subscribe(ctx, int32(*token))
		if e != nil {
			server.Close()
			cancel()
			wg.Wait()
			return e
		}
		defer sub.Unsubscribe()
		launch(func() error {
			return consume(ctx, client, sub, rec, func(msg *nats.Msg) (traceInput, error) {
				c, e := bus.Validate(msg)
				if e != nil {
					return traceInput{}, e
				}
				return traceInput{raw: c.Raw, received: c.Received, sourceBroker: c.SourceBroker, token: c.Token, local: c.Sequence, apply: func(op context.Context) error { return match.ProcessCommand(op, c) }}, nil
			})
		})
	} else {
		sub, e := client.Subscribe()
		if e != nil {
			server.Close()
			cancel()
			wg.Wait()
			return e
		}
		defer sub.Unsubscribe()
		launch(func() error {
			return consume(ctx, client, sub, rec, func(msg *nats.Msg) (traceInput, error) {
				received, e := client.Validate(msg)
				if e != nil {
					return traceInput{}, e
				}
				meta, e := msg.Metadata()
				if e != nil {
					return traceInput{}, e
				}
				return traceInput{raw: msg.Data, received: received, sourceBroker: meta.Sequence.Stream, apply: func(op context.Context) error { return intake.Process(op, msg.Data, meta.Sequence.Stream, received) }}, nil
			})
		})
		for _, i := range m.Instruments {
			token := i.Token
			launch(func() error {
				d, e := durable.OpenDispatcher(ctx, dsn, m, token)
				if e != nil {
					return e
				}
				defer d.Close()
				for ctx.Err() == nil {
					op, done := context.WithTimeout(ctx, 10*time.Second)
					worked, e := d.Step(op, bus)
					done()
					if e != nil {
						return e
					}
					if !worked {
						if e = d.Wait(ctx); e != nil {
							return e
						}
					}
				}
				return nil
			})
		}
	}
	select {
	case <-ctx.Done():
	case result = <-errorsCh:
	}
	cancel()
	shutdown, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	server.Shutdown(shutdown)
	wg.Wait()
	return result
}

// traceInput is loop-owned; dispatchers never access its recorder.
type traceInput struct {
	raw          []byte
	received     time.Time
	sourceBroker uint64
	token        int32
	local        uint64
	apply        func(context.Context) error
}

func consume(ctx context.Context, client *broker.Client, sub *nats.Subscription, rec *timingtrace.Recorder, validate func(*nats.Msg) (traceInput, error)) error {
	for ctx.Err() == nil {
		if !client.Conn.IsConnected() {
			return fmt.Errorf("NATS disconnected; restart from committed state")
		}
		msgs, e := sub.Fetch(1, nats.MaxWait(time.Second))
		if errors.Is(e, nats.ErrTimeout) {
			continue
		}
		if e != nil {
			return e
		}
		delivery := time.Now()
		msg := msgs[0]
		input, e := validate(msg)
		if e != nil {
			return e
		}
		meta, e := msg.Metadata()
		if e != nil {
			return e
		}
		op, cancel := context.WithTimeout(ctx, 10*time.Second)
		start := time.Now()
		e = input.apply(op)
		committed := time.Now()
		cancel()
		if e != nil {
			return e
		}
		ackStart := time.Now()
		e = msg.AckSync(nats.AckWait(3 * time.Second))
		acked := time.Now()
		ackNS := int64(0)
		if e == nil {
			ackNS = acked.UnixNano()
		}
		seq, kind := timingtrace.Identity(input.raw)
		rec.Record(timingtrace.Sample{Sequence: seq, Type: kind, StartUnixNS: start.UnixNano(), EndUnixNS: committed.UnixNano(), DurationNS: committed.Sub(start).Nanoseconds(), ReceivedUnixNS: input.received.UnixNano(), DeliveryUnixNS: delivery.UnixNano(), AckUnixNS: ackNS, AckDurationNS: acked.Sub(ackStart).Nanoseconds(), Attempts: int64(meta.NumDelivered), Shard: int(input.token), ShardSequence: input.local, SourceBroker: input.sourceBroker})
		if e != nil {
			return fmt.Errorf("committed input ack failed; safe restart required: %w", e)
		}
	}
	return nil
}
