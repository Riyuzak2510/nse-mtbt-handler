package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"net"
	"nse-mtbt-handler/internal/appconfig"
	"nse-mtbt-handler/internal/book"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/runconfig"
	"nse-mtbt-handler/internal/simulator"
	"nse-mtbt-handler/internal/timingtrace"
	"os"
	"os/signal"
	"path/filepath"
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
	initMode := flag.Bool("init", false, "create a source configuration, refusing overwrite")
	path := flag.String("manifest", "run/demo.json", "source configuration")
	addr := flag.String("addr", "127.0.0.1:19001", "UDP receiver")
	allowNonLoopback := flag.Bool("allow-non-loopback", false, "allow binding to non-loopback addresses")
	rate := flag.Int("rate", 100, "data packets/sec (local target, not NSE capacity)")
	count := flag.Int64("count", 0, "data events to generate; zero is continuous")
	expected := flag.String("expected", "", "write final reference state JSON at the generated sequence")
	seed := flag.Int64("seed", 1, "initialization seed; immutable once manifest exists")
	instruments := flag.Int("instruments", 3, "initialization instrument count")
	stream := flag.Int("stream", 1, "initialization stream ID")
	drop := flag.Int64("drop-sequence", 0, "omit this generated data sequence, zero disables")
	dup := flag.Int64("duplicate-sequence", 0, "send this generated sequence twice, zero disables")
	timingsPath := flag.String("timings", "", "optional bounded CSV timing trace, flushed on shutdown")
	timingsLimit := flag.Int("timings-limit", 100000, "maximum trace samples; overflow makes the run fail")
	flag.Parse()
	if *initMode {
		if *instruments < 1 || *instruments > 10000 || *stream < 1 || *stream > math.MaxInt16 {
			return fmt.Errorf("invalid instrument count/stream")
		}
		m := runconfig.Manifest{Version: 2, Profile: feed.Profile, StreamID: int16(*stream), Seed: *seed, Baseline: "empty", CreatedAt: time.Now().UTC()}
		for i := 0; i < *instruments; i++ {
			m.Instruments = append(m.Instruments, runconfig.Instrument{Token: int32(900001 + i), Symbol: fmt.Sprintf("SIM%04d", i+1), TickSize: 5, BasePrice: 100000 + int32(i)*1000})
		}
		if err := m.Validate(); err != nil {
			return err
		}
		if err := writeNew(*path, m); err != nil {
			return err
		}
		fmt.Println("created stream configuration", m.StreamID, "manifest", *path)
		return nil
	}
	if *rate < 1 || *rate > 1000000 || *count < 0 || *count > math.MaxInt32 || *drop < 0 || *drop > math.MaxInt32 || *dup < 0 || *dup > math.MaxInt32 {
		return fmt.Errorf("invalid rate/count/fault sequence")
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
	m, err := runconfig.Load(*path)
	if err != nil {
		return err
	}
	g, err := simulator.New(m)
	if err != nil {
		return err
	}
	if err := appconfig.ValidateDestAddress(*addr, *allowNonLoopback); err != nil {
		return err
	}
	a, err := net.ResolveUDPAddr("udp4", *addr)
	if err != nil {
		return err
	}
	conn, err := net.DialUDP("udp4", nil, a)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ticker := time.NewTicker(time.Second / time.Duration(*rate))
	defer ticker.Stop()
	// UTC1980 is explicitly a SYNTHETIC convention, not verified NSE timezone conversion.
	epoch := time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)
	send := func(p feed.Packet, scheduled time.Time) error {
		b, e := feed.Encode(p)
		if e != nil {
			return e
		}
		if trace == nil {
			_, e = conn.Write(b)
			return e
		}
		start := time.Now()
		_, e = conn.Write(b)
		end := time.Now()
		if e == nil {
			trace.Record(timingtrace.Sample{Sequence: p.Sequence, Type: string(byte(p.Type)), StartUnixNS: start.UnixNano(), EndUnixNS: end.UnixNano(), DurationNS: end.Sub(start).Nanoseconds(), ScheduledUnixNS: scheduled.UnixNano()})
		}
		return e
	}
	fmt.Println("generating synthetic CM traffic; stream", m.StreamID, "destination", *addr)
loop:
	for *count == 0 || int64(g.Sequence()) < *count {
		select {
		case <-ctx.Done():
			break loop
		case now := <-ticker.C:
			p, e := g.Next(now.Sub(epoch).Nanoseconds())
			if e != nil {
				return e
			}
			if int64(p.Sequence) != *drop {
				if e := send(p, now); e != nil {
					return e
				}
				if int64(p.Sequence) == *dup {
					if e := send(p, now); e != nil {
						return e
					}
				}
			}
		}
	}
	if err := send(g.Heartbeat(), time.Now()); err != nil {
		return err
	}
	if *expected != "" {
		state := struct {
			StreamID int16        `json:"stream_id"`
			Sequence int32        `json:"sequence"`
			Orders   []book.Order `json:"orders"`
		}{m.StreamID, g.Sequence(), g.Orders()}
		if err := writeNew(*expected, state); err != nil {
			return err
		}
	}
	fmt.Printf("finished stream=%d generated_sequence=%d active_orders=%d\n", m.StreamID, g.Sequence(), len(g.Orders()))
	return nil
}
func writeNew(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	e := json.NewEncoder(f)
	e.SetIndent("", "  ")
	err = e.Encode(v)
	closeErr := f.Close()
	if err != nil {
		return err
	}
	return closeErr
}
