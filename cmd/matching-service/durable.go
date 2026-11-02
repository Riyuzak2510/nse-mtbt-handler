package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"nse-mtbt-handler/internal/appconfig"
	"nse-mtbt-handler/internal/broker"
	"nse-mtbt-handler/internal/durable"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/loadmetrics"
	"nse-mtbt-handler/internal/runconfig"
	"nse-mtbt-handler/internal/timingtrace"
)

func runDurable(manifestPath, natsURL, httpAddr string, allowNonLoopback bool, maxActive int, staleAfter time.Duration, tracePath string, traceLimit int, databaseURL string, options ...durable.OutcomeOptions) (runErr error) {
	if staleAfter <= 0 {
		return fmt.Errorf("stale-after must be positive")
	}
	if err := appconfig.ValidateBindAddress(httpAddr, allowNonLoopback); err != nil {
		return err
	}
	m, err := runconfig.Load(manifestPath)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var outcomeOptions durable.OutcomeOptions
	if len(options) > 0 {
		outcomeOptions = options[0]
	}
	svc, err := durable.OpenWithOutcomes(ctx, databaseURL, m, maxActive, outcomeOptions)
	if err != nil {
		return err
	}
	defer svc.Close()
	client, err := broker.Connect(ctx, natsURL, m)
	if err != nil {
		return err
	}
	defer client.Close()
	info, err := client.JS.StreamInfo(client.Stream)
	if err != nil {
		return err
	}
	position := svc.Position()
	if info.State.LastSeq < position || info.State.FirstSeq > position+1 {
		return fmt.Errorf("broker history cannot continue database checkpoint%d (first%d last%d)", position, info.State.FirstSeq, info.State.LastSeq)
	}
	consumerInfo, err := client.JS.ConsumerInfo(client.Stream, broker.Consumer)
	if err != nil {
		return err
	}
	if consumerInfo.AckFloor.Stream > position {
		return fmt.Errorf("broker acknowledged beyond database checkpoint; coherent recovery required")
	}
	svc.CatchUpThrough(info.State.LastSeq)
	sub, err := client.Subscribe()
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()
	trace, err := timingtrace.New(tracePath, traceLimit)
	if err != nil {
		return err
	}
	metrics, err := loadmetrics.NewRecorder(os.Getenv("MTBT_LOAD_METRICS"))
	if err != nil {
		return err
	}
	defer func() {
		if e := metrics.Close(); runErr == nil {
			runErr = e
		}
	}()
	defer func() {
		if e := trace.Close(); runErr == nil {
			runErr = e
		}
	}()
	svc.Connected(true)
	defer svc.Connected(false)
	listener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: durableRoutes(svc, m, staleAfter), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
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
	log.Printf("durable synthetic matcher stream=%d restored through broker=%d max_active=%d HTTP=%s; timings end at PostgreSQL commit", m.StreamID, position, maxActive, httpAddr)
	return consumeDurable(ctx, svc, client, sub, trace, serverErrors, metrics)
}

func consumeDurable(ctx context.Context, svc *durable.Service, client *broker.Client, sub *nats.Subscription, trace *timingtrace.Recorder, serverErrors <-chan error, metricsRecorders ...*loadmetrics.Recorder) error {
	for ctx.Err() == nil {
		if e := svc.DiagnosticsError(); e != nil {
			return fmt.Errorf("SQL diagnostics failed: %w", e)
		}
		select {
		case e := <-serverErrors:
			return e
		default:
		}
		if !client.Conn.IsConnected() {
			return fmt.Errorf("durable matcher NATS disconnected; restart from PostgreSQL")
		}
		msgs, err := sub.Fetch(1, nats.MaxWait(time.Second))
		if errors.Is(err, nats.ErrTimeout) {
			continue
		}
		if err != nil {
			return err
		}
		delivery := time.Now()
		msg := msgs[0]
		received, err := client.Validate(msg)
		if err != nil {
			return err
		}
		meta, err := msg.Metadata()
		if err != nil {
			return err
		}
		start := time.Now()
		err = svc.Process(ctx, msg.Data, meta.Sequence.Stream, received)
		committed := time.Now()
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
		if len(metricsRecorders) > 0 && metricsRecorders[0] != nil {
			p, decodeErr := feed.Decode(msg.Data)
			if decodeErr != nil {
				return decodeErr
			}
			if e := metricsRecorders[0].Record(loadmetrics.Sample{MonotonicDurations: true, ProcessDurationNS: committed.Sub(start).Nanoseconds(), CommitToAckDurationNS: acked.Sub(committed).Nanoseconds(), Sequence: p.Sequence, SourceBroker: meta.Sequence.Stream, Attempts: meta.NumDelivered, EmittedUnixNS: p.Timestamp, ReceivedUnixNS: received.UnixNano(), DeliveryUnixNS: delivery.UnixNano(), StartUnixNS: start.UnixNano(), CommittedUnixNS: committed.UnixNano(), AckUnixNS: ackNS}); e != nil {
				return e
			}
		}
		seq, kind := timingtrace.Identity(msg.Data)
		trace.Record(timingtrace.Sample{Sequence: seq, Type: kind, StartUnixNS: start.UnixNano(), EndUnixNS: committed.UnixNano(), DurationNS: committed.Sub(start).Nanoseconds(), ReceivedUnixNS: received.UnixNano(), DeliveryUnixNS: delivery.UnixNano(), AckUnixNS: ackNS, AckDurationNS: acked.Sub(ackStart).Nanoseconds(), Attempts: int64(meta.NumDelivered), SourceBroker: meta.Sequence.Stream})
		if err != nil {
			return fmt.Errorf("database committed but broker acknowledgement failed; restart safely: %w", err)
		}
	}
	return nil
}

type durableReader interface {
	Snapshot(context.Context, int32, int, time.Time, time.Duration) (durable.Snapshot, error)
	Order(context.Context, feed.OrderID) (durable.Order, bool, error)
}

func durableRoutes(svc durableReader, m runconfig.Manifest, stale time.Duration) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"process":"alive","synthetic":true,"ephemeral":false}`))
	})
	read := func(w http.ResponseWriter, r *http.Request, ready bool) {
		token := m.Instruments[0].Token
		depth := 5
		if !ready {
			n, e := strconv.ParseInt(r.URL.Query().Get("token"), 10, 32)
			if e != nil || !m.TokenAllowed(int32(n)) {
				http.Error(w, "known token required", 400)
				return
			}
			token = int32(n)
			if q := r.URL.Query().Get("depth"); q != "" {
				depth, e = strconv.Atoi(q)
				if e != nil || depth < 1 || depth > 100 {
					http.Error(w, "depth must be1..100", 400)
					return
				}
			}
		}
		snap, e := svc.Snapshot(r.Context(), token, depth, time.Now(), stale)
		if e != nil {
			http.Error(w, "matching snapshot unavailable", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if snap.Status != "READY" {
			w.WriteHeader(503)
		}
		_ = json.NewEncoder(w).Encode(snap)
	}
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) { read(w, r, true) })
	mux.HandleFunc("GET /book", func(w http.ResponseWriter, r *http.Request) { read(w, r, false) })
	mux.HandleFunc("GET /orders", func(w http.ResponseWriter, r *http.Request) {
		bits, e := strconv.ParseUint(r.URL.Query().Get("id"), 10, 64)
		id := feed.OrderID(bits)
		if e == nil {
			_, e = feed.IDFromFloat64(id.Float64())
		}
		if e != nil || id == 0 {
			http.Error(w, "positive valid order ID bits required", 400)
			return
		}
		var o any
		var found bool
		if coherent, ok := svc.(interface {
			OrderView(context.Context, feed.OrderID) (durable.OrderView, bool, error)
		}); ok {
			o, found, e = coherent.OrderView(r.Context(), id)
		} else {
			o, found, e = svc.Order(r.Context(), id)
		}
		if e != nil {
			http.Error(w, "order unavailable", 503)
			return
		}
		if !found {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(o)
	})
	return mux
}
