// load-source sends a synthetic, native-style UDP feed without completion-based
// pacing. A scheduling pause aborts the test rather than hiding omitted load.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"nse-mtbt-handler/internal/appconfig"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/runconfig"
)

type stage struct {
	Count int64 `json:"count"`
	Rate  int64 `json:"rate"`
}

func parseProfile(text string) ([]stage, error) {
	var result []stage
	var total int64
	for _, entry := range strings.Split(text, ",") {
		parts := strings.Split(entry, "@")
		if len(parts) != 2 {
			return nil, fmt.Errorf("profile requires count@rate segments")
		}
		n, e := strconv.ParseInt(parts[0], 10, 32)
		r, f := strconv.ParseInt(parts[1], 10, 32)
		if e != nil || f != nil || n < 1 || r < 1 || r > 1000000 {
			return nil, fmt.Errorf("invalid count/rate %q", entry)
		}
		total += n
		if total > 2147483647 {
			return nil, fmt.Errorf("profile exceeds native sequence range")
		}
		result = append(result, stage{n, r})
	}
	return result, nil
}

// Each cycle places all catalog buys, partial sells, then final sells. The
// number of active orders is bounded by catalog size, not total event history.
func packet(seq int32, m runconfig.Manifest, ts int64) (feed.Packet, error) {
	n := int64(len(m.Instruments))
	if n == 0 || seq < 1 {
		return feed.Packet{}, fmt.Errorf("empty catalog or invalid sequence")
	}
	slot := (int64(seq) - 1) % (3 * n)
	i := m.Instruments[slot%n]
	side := feed.Buy
	q := int32(100)
	if slot >= n {
		side = feed.Sell
		q = 30
	}
	if slot >= 2*n {
		q = 70
	}
	id, e := feed.IDFromFloat64(float64(seq))
	return feed.Packet{Stream: m.StreamID, Sequence: seq, Type: feed.New, Timestamp: ts, OrderID: id, Token: i.Token, Side: side, Quantity: q, Price: i.BasePrice}, e
}

type sourceReport struct {
	Kind              string  `json:"kind"`
	Reason            string  `json:"reason,omitempty"`
	Sent              int64   `json:"sent"`
	LastSequence      int64   `json:"last_sequence"`
	TargetRate        int64   `json:"target_rate"`
	ActualIntervalEPS float64 `json:"actual_interval_eps"`
	StartedUnixNS     int64   `json:"started_unix_ns"`
	AtUnixNS          int64   `json:"at_unix_ns"`
	ElapsedSeconds    float64 `json:"elapsed_seconds"`
	MaxScheduleLagNS  int64   `json:"max_schedule_lag_ns"`
	MeanScheduleLagNS float64 `json:"mean_schedule_lag_ns"`
	ScheduledByNow    int64   `json:"scheduled_by_now"`
	StageCounts       []int64 `json:"stage_counts"`
	Profile           []stage `json:"profile"`
	EnvironmentPause  bool    `json:"scheduling_interruption"`
}
type writer interface{ Write([]byte) (int, error) }

func waitUntil(ctx context.Context, t time.Time) error {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// One scheduling quantum is at most1ms. Packets in a quantum share a due time;
// this intentional microburst is recorded, not claimed as uniform packet spacing.
func batchSize(rate, left int64) int64 {
	n := rate / 1000
	if n < 1 {
		n = 1
	}
	if n > left {
		n = left
	}
	return n
}
func generate(ctx context.Context, conn writer, m runconfig.Manifest, stages []stage, start int64, maxLag time.Duration, enc *json.Encoder) (runErr error) {
	if start < 1 || len(stages) == 0 || maxLag < 0 {
		return fmt.Errorf("invalid generator settings")
	}
	var total int64
	for _, s := range stages {
		if s.Count < 1 || s.Rate < 1 {
			return fmt.Errorf("invalid stage")
		}
		total += s.Count
	}
	if start+total-1 > 2147483647 {
		return fmt.Errorf("source sequence bounds exceeded")
	}
	began, lastReport := time.Now(), time.Now()
	sent, previous := int64(0), int64(0)
	stageCounts := make([]int64, len(stages))
	stageIndex := 0
	var maxObserved time.Duration
	var sumLag float64
	var lagCount uint64
	var scheduled int64
	pause := false
	report := func(kind string, rate int64, reason string) error {
		now := time.Now()
		elapsed := now.Sub(lastReport).Seconds()
		actual := 0.0
		if elapsed > 0 {
			actual = float64(sent-previous) / elapsed
		}
		mean := 0.0
		if lagCount > 0 {
			mean = sumLag / float64(lagCount)
		}
		err := enc.Encode(sourceReport{kind, reason, sent, start + sent - 1, rate, actual, began.UnixNano(), now.UnixNano(), now.Sub(began).Seconds(), maxObserved.Nanoseconds(), mean, scheduled, append([]int64(nil), stageCounts...), stages, pause})
		previous = sent
		lastReport = now
		return err
	}
	defer func() {
		if runErr != nil {
			kind := "failed"
			if errors.Is(runErr, context.Canceled) {
				kind = "interrupted"
			}
			rate := stages[stageIndex].Rate
			if e := report(kind, rate, runErr.Error()); e != nil {
				runErr = errors.Join(runErr, e)
			}
		}
	}()
	for idx, s := range stages {
		stageIndex = idx
		stageStart := time.Now()
		before := sent
		for stageCounts[idx] < s.Count {
			done := stageCounts[idx]
			due := stageStart.Add(time.Duration(done * int64(time.Second) / s.Rate))
			if e := waitUntil(ctx, due); e != nil {
				return e
			}
			lag := time.Since(due)
			if lag > maxObserved {
				maxObserved = lag
			}
			expected := (time.Since(stageStart).Nanoseconds()*s.Rate)/int64(time.Second) + 1
			if expected > s.Count {
				expected = s.Count
			}
			scheduled = before + expected
			if maxLag > 0 && lag > maxLag {
				pause = true
				return fmt.Errorf("scheduling lag %s exceeds maximum %s; environment interruption or generator cannot keep offered rate", lag, maxLag)
			}
			n := batchSize(s.Rate, s.Count-done)
			for j := int64(0); j < n; j++ {
				if e := ctx.Err(); e != nil {
					return e
				}
				ts := time.Now()
				packetLag := ts.Sub(due)
				if packetLag > maxObserved {
					maxObserved = packetLag
				}
				sumLag += float64(packetLag)
				lagCount++
				p, e := packet(int32(start+sent), m, ts.UnixNano())
				if e != nil {
					return e
				}
				b, e := feed.Encode(p)
				if e != nil {
					return e
				}
				written, e := conn.Write(b)
				if e != nil {
					return e
				}
				if written != len(b) {
					return fmt.Errorf("short UDP write")
				}
				sent++
				stageCounts[idx]++
			}
			if time.Since(lastReport) >= time.Second {
				if e := report("interval", s.Rate, ""); e != nil {
					return e
				}
			}
		}
		scheduled = sent
		if e := report("stage-complete", s.Rate, ""); e != nil {
			return e
		}
	}
	hb, e := feed.Encode(feed.Packet{Stream: m.StreamID, Type: feed.Heartbeat, LastSequence: int32(start + sent - 1)})
	if e != nil {
		return e
	}
	if _, e = conn.Write(hb); e != nil {
		return e
	}
	return report("complete", 0, "")
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	manifest := flag.String("manifest", "", "native catalog")
	address := flag.String("addr", "127.0.0.1:19001", "UDP publisher destination")
	remote := flag.Bool("allow-non-loopback", false, "explicitly allow cluster DNS")
	profile := flag.String("profile", "10000@1000", "comma-separated count@events-per-second")
	start := flag.Int64("start", 1, "absolute first sequence")
	output := flag.String("report", "", "exclusive JSONL report; empty uses stdout")
	maxLag := flag.Duration("max-schedule-lag", 2*time.Second, "abort on scheduling lag above this limit (0 disables)")
	flag.Parse()
	stages, e := parseProfile(*profile)
	if e != nil {
		return e
	}
	m, e := runconfig.Load(*manifest)
	if e != nil {
		return e
	}
	if *maxLag < 0 {
		return fmt.Errorf("max-schedule-lag must be nonnegative")
	}
	var count int64
	for _, s := range stages {
		count += s.Count
	}
	if *start < 1 || *start+count-1 > 2147483647 {
		return fmt.Errorf("source sequence bounds exceeded")
	}
	if e = appconfig.ValidateDestAddress(*address, *remote); e != nil {
		return e
	}
	a, e := net.ResolveUDPAddr("udp4", *address)
	if e != nil {
		return e
	}
	conn, e := net.DialUDP("udp4", nil, a)
	if e != nil {
		return e
	}
	defer conn.Close()
	out := os.Stdout
	if *output != "" {
		out, e = os.OpenFile(*output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return e
		}
		defer out.Close()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return generate(ctx, conn, m, stages, *start, *maxLag, json.NewEncoder(out))
}
