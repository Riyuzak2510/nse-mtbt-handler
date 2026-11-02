// ticker-workload emits bounded deterministic interleaved synthetic inputs.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"nse-mtbt-handler/internal/appconfig"
	"nse-mtbt-handler/internal/feed"
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
func fixture(m runconfig.Manifest, n int) (feed.Packet, error) {
	if n < 1 || len(m.Instruments) < 1 || len(m.Instruments) > 16 {
		return feed.Packet{}, fmt.Errorf("invalid fixture identity")
	}
	i := (n - 1) % len(m.Instruments)
	local := (n-1)/len(m.Instruments) + 1
	q := int32(100)
	side := feed.Buy
	if local%3 == 2 {
		q = 30
		side = feed.Sell
	} else if local%3 == 0 {
		q = 70
		side = feed.Sell
	}
	id, e := feed.IDFromFloat64(float64(n))
	if e != nil {
		return feed.Packet{}, e
	}
	return feed.Packet{Stream: m.StreamID, Sequence: int32(n), Type: feed.New, Timestamp: time.Now().UnixNano(), OrderID: id, Token: m.Instruments[i].Token, Side: side, Price: m.Instruments[i].BasePrice, Quantity: q}, nil
}
func run() (result error) {
	path := flag.String("manifest", "", "required manifest")
	addr := flag.String("addr", "127.0.0.1:19001", "UDP destination")
	remote := flag.Bool("allow-non-loopback", false, "isolated cluster destination")
	start := flag.Int("start", 1, "absolute first source sequence")
	end := flag.Int("end", 12, "absolute last sequence")
	rate := flag.Int("rate", 10, "requested inputs/sec, max1000")
	timings := flag.String("timings", "", "optional bounded CSV, flushed after traffic")
	flag.Parse()
	if *start < 1 || *end < *start || *end > 100000 || *rate < 1 || *rate > 1000 {
		return fmt.Errorf("invalid bounded fixture range/rate")
	}
	if *timings != "" && *timings == *path {
		return fmt.Errorf("timings must not overwrite manifest")
	}
	m, e := runconfig.Load(*path)
	if e != nil {
		return e
	}
	if len(m.Instruments) > 16 {
		return fmt.Errorf("at most16 tickers")
	}
	if e = appconfig.ValidateDestAddress(*addr, *remote); e != nil {
		return e
	}
	trace, e := timingtrace.New(*timings, *end-*start+2)
	if e != nil {
		return e
	}
	defer func() {
		if e := trace.Close(); result == nil {
			result = e
		}
	}()
	c, e := net.Dial("udp4", *addr)
	if e != nil {
		return e
	}
	defer c.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	send := func(raw []byte, scheduled time.Time) error {
		start := time.Now()
		_, e := c.Write(raw)
		end := time.Now()
		if e != nil {
			return e
		}
		seq, kind := timingtrace.Identity(raw)
		trace.Record(timingtrace.Sample{Sequence: seq, Type: kind, StartUnixNS: start.UnixNano(), EndUnixNS: end.UnixNano(), DurationNS: end.Sub(start).Nanoseconds(), ScheduledUnixNS: scheduled.UnixNano()})
		return nil
	}
	deadline := time.Now()
	interval := time.Second / time.Duration(*rate)
	for n := *start; n <= *end; n++ {
		if e = waitUntil(ctx, deadline); e != nil {
			return e
		}
		scheduled := deadline
		p, e := fixture(m, n)
		if e != nil {
			return e
		}
		raw, e := feed.Encode(p)
		if e != nil {
			return e
		}
		if e = send(raw, scheduled); e != nil {
			return e
		}
		// No catch-up: delayed sends shift the next deadline instead of bursting.
		deadline = time.Now().Add(interval)
	}
	hb, e := feed.Encode(feed.Packet{Stream: m.StreamID, Type: feed.Heartbeat, LastSequence: int32(*end)})
	if e != nil {
		return e
	}
	return send(hb, time.Now())
}
func waitUntil(ctx context.Context, deadline time.Time) error {
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
