package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"nse-mtbt-handler/internal/appconfig"
	"nse-mtbt-handler/internal/broker"
	"nse-mtbt-handler/internal/ingestdiagnostics"
	"nse-mtbt-handler/internal/publisherpipeline"
	"nse-mtbt-handler/internal/runconfig"
	"nse-mtbt-handler/internal/timingtrace"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() (runErr error) {
	manifest := flag.String("manifest", "run/demo.json", "source configuration")
	addr := flag.String("listen", "127.0.0.1:19001", "loopback UDP listen address")
	allowNonLoopback := flag.Bool("allow-non-loopback", false, "allow binding to non-loopback addresses")
	healthHTTP := flag.String("health-http", "", "optional health check HTTP address (empty disables)")
	url := flag.String("nats", appconfig.Env("NATS_URL", appconfig.DefaultNATS), "NATS URL")
	timingsPath := flag.String("timings", "", "optional bounded CSV timing trace, flushed on shutdown")
	timingsLimit := flag.Int("timings-limit", 100000, "maximum trace samples; overflow makes the run fail")

	mode := flag.String("publish-mode", "async", "async bounded pipeline or sync comparison")
	queue := flag.Int("queue-capacity", 4096, "maximum queued datagrams")
	queueBytes := flag.Int64("queue-bytes", 16<<20, "maximum owned queued/in-flight payload bytes")
	inflight := flag.Int("max-inflight", 256, "maximum unretired publications")
	ackTimeout := flag.Duration("ack-timeout", 3*time.Second, "publish acknowledgement deadline")
	drainTimeout := flag.Duration("drain-timeout", 10*time.Second, "bounded graceful shutdown drain")
	flag.Parse()
	cfg := publisherpipeline.Config{Queue: *queue, MaxBytes: *queueBytes, InFlight: *inflight, AckTimeout: *ackTimeout, DrainTimeout: *drainTimeout}
	if *mode != "async" && *mode != "sync" {
		return fmt.Errorf("publish-mode must be async or sync")
	}
	if e := cfg.Validate(); e != nil {
		return e
	}

	trace, err := timingtrace.New(*timingsPath, *timingsLimit)
	if err != nil {
		return err
	}
	defer func() {
		if e := trace.Close(); runErr == nil {
			runErr = e
		}
	}()
	m, err := runconfig.Load(*manifest)
	if err != nil {
		return err
	}
	if err := appconfig.ValidateBindAddress(*addr, *allowNonLoopback); err != nil {
		return err
	}
	if *healthHTTP != "" {
		if err := appconfig.ValidateBindAddress(*healthHTTP, *allowNonLoopback); err != nil {
			return err
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	c, err := broker.Connect(ctx, *url, m)
	if err != nil {
		return err
	}
	defer c.Close()
	a, err := net.ResolveUDPAddr("udp4", *addr)
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp4", a)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err = conn.SetReadBuffer(1 << 20); err != nil {
		return err
	}
	diagnostics, err := ingestdiagnostics.New(conn, os.Getenv("MTBT_INGEST_METRICS"))
	if err != nil {
		return err
	}
	defer func() {
		if e := diagnostics.Close(); runErr == nil {
			runErr = e
		}
	}()
	// Only one reusable application buffer: publish synchronously before reuse. Kernel buffering is bounded.
	// Oversized or malformed payloads are preserved so the consumer can persist INVALID, not silently skip.
	buf := make([]byte, 65535)

	// Setup optional health HTTP server
	var healthSrv *http.Server
	var healthListener net.Listener
	healthErr := make(chan error, 1)

	if *healthHTTP != "" {
		var err error
		healthListener, err = net.Listen("tcp", *healthHTTP)
		if err != nil {
			return err
		}
		defer healthListener.Close()

		healthSrv = &http.Server{Handler: healthRoutes(c.Conn.IsConnected), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
		go func() { healthErr <- healthSrv.Serve(healthListener) }()
		defer func() {
			shutdown, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			_ = healthSrv.Shutdown(shutdown)
		}()
	}

	log.Printf("publisher ready stream=%d udp=%s subject=%s", m.StreamID, conn.LocalAddr(), c.Subject)
	if *healthHTTP != "" {
		log.Printf("publisher health http=%s", healthListener.Addr())
	}

	if *mode == "async" {
		ap, e := c.AsyncPublisher(*inflight, *ackTimeout)
		if e != nil {
			return e
		}
		defer ap.Close()
		observer, e := newAsyncObserver(diagnostics, trace, os.Getenv("MTBT_INGEST_METRICS"))
		if e != nil {
			return e
		}
		defer func() {
			if e := observer.Close(); runErr == nil {
				runErr = e
			}
		}()
		pipelineCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		healthFailure := make(chan error, 1)
		watchDone := make(chan struct{})
		defer close(watchDone)
		go func() {
			select {
			case healthError := <-healthErr:
				healthFailure <- healthError
				cancel()
			case <-watchDone:
			}
		}()
		log.Printf("publisher async queue=%d bytes=%d inflight=%d", cfg.Queue, cfg.MaxBytes, cfg.InFlight)
		final, e := publisherpipeline.Run(pipelineCtx, conn, ap, cfg, observer)
		log.Printf("async final received=%d submitted=%d confirmed=%d unconfirmed=%d overflow=%d owned_bytes=%d", final.Received, final.Submitted, final.Confirmed, final.Unconfirmed, final.Overflow, final.OwnedBytes)
		select {
		case healthError := <-healthFailure:
			if e == nil {
				e = healthError
			}
		default:
		}
		return e
	}
	var count uint64
	for ctx.Err() == nil {
		// Check if health server encountered an error
		select {
		case err := <-healthErr:
			if err != http.ErrServerClosed {
				return err
			}
		default:
		}

		if err = conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			return err
		}
		var readStart time.Time
		if diagnostics.Enabled() {
			readStart = time.Now()
		}
		n, _, e := conn.ReadFromUDP(buf)
		if e != nil {
			if ne, ok := e.(net.Error); ok && ne.Timeout() {
				if err = diagnostics.Tick(); err != nil {
					return err
				}
				continue
			}
			return e
		}
		receivedInstant := time.Now()
		received := receivedInstant.UTC()
		publishCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		var publishStart time.Time
		if trace != nil || diagnostics.Enabled() {
			publishStart = time.Now()
		}
		e = c.Publish(publishCtx, buf[:n], received)
		if trace != nil && e == nil {
			end := time.Now()
			seq, kind := timingtrace.Identity(buf[:n])
			trace.Record(timingtrace.Sample{Sequence: seq, Type: kind, StartUnixNS: publishStart.UnixNano(), EndUnixNS: end.UnixNano(), DurationNS: end.Sub(publishStart).Nanoseconds(), ReceivedUnixNS: received.UnixNano()})
		}
		if diagnostics.Enabled() {
			if err = diagnostics.Record(buf[:n], receivedInstant.Sub(readStart), time.Since(publishStart), receivedInstant, e); err != nil {
				cancel()
				return err
			}
		}
		cancel()
		if e != nil {
			return fmt.Errorf("publication failed; UDP loss possible, stop/reinitialize run: %w", e)
		}
		count++
		if count%1000 == 0 {
			log.Printf("published=%d stream=%d", count, m.StreamID)
		}
	}
	return nil
}
