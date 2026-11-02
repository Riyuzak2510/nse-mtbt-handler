// Package main implements the matching-workload command for deterministic durable matching testing.
// Sends feed.Encode packets with N-cycle pattern (buy/sell/sell), no docker/kube calls, clean exit.
package main

import (
	"context"
	"encoding/json"
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
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type expectedOrder struct {
	ID                string `json:"id"`
	TokenID           int32  `json:"token"`
	Side              string `json:"side"`
	Price             int32  `json:"price"`
	AcceptedQuantity  int32  `json:"accepted_quantity"`
	RemainingQuantity int32  `json:"remaining_quantity"`
	FilledQuantity    int32  `json:"filled_quantity"`
	CancelledQuantity int32  `json:"cancelled_quantity"`
	Status            string `json:"status"`
}

func run() (runErr error) {
	manifest := flag.String("manifest", "", "source configuration (required, native v2 catalog)")
	addr := flag.String("addr", "127.0.0.1:19001", "publisher UDP address")
	allowNonLoopback := flag.Bool("allow-non-loopback", false, "allow non-loopback addresses")
	start := flag.Int("start", 1, "first sequence (absolute, inclusive 1..100000)")
	end := flag.Int("end", 10000, "last sequence (absolute, inclusive 1..100000)")
	total := flag.Int("total", 10000, "expected total orders")
	rate := flag.Int("rate", 100, "data packets/sec")
	timingsPath := flag.String("timings", "", "optional timing trace CSV")
	expectedPath := flag.String("expected", "", "optional expected final orders JSON")
	heartbeat := flag.Bool("heartbeat", true, "send final heartbeat (Z packet)")
	flag.Parse()

	// Validate manifest
	if *manifest == "" {
		return fmt.Errorf("flag -manifest is required")
	}

	// Load and validate manifest
	m, err := runconfig.Load(*manifest)
	if err != nil {
		return err
	}
	if len(m.Instruments) != 1 || 10000%m.Instruments[0].TickSize != 0 {
		return fmt.Errorf("workload requires one instrument with tick-aligned price10000")
	}
	if *timingsPath != "" && (*timingsPath == *expectedPath || *timingsPath == *manifest) {
		return fmt.Errorf("output paths must be distinct from configuration")
	}
	if *expectedPath != "" && *expectedPath == *manifest {
		return fmt.Errorf("expected output must not overwrite manifest")
	}

	// Validate bounds
	if *start < 1 || *start > 100000 {
		return fmt.Errorf("flag -start must be 1..100000, got %d", *start)
	}
	if *end < 1 || *end > 100000 {
		return fmt.Errorf("flag -end must be 1..100000, got %d", *end)
	}
	if *start > *end {
		return fmt.Errorf("flag -start must be <= -end")
	}
	if *total < 1 || *total > 100000 {
		return fmt.Errorf("flag -total must be 1..100000, got %d", *total)
	}
	if *end > *total {
		return fmt.Errorf("end must not exceed absolute total")
	}
	if *rate < 1 || *rate > 1000000 {
		return fmt.Errorf("flag -rate must be >0, got %d", *rate)
	}

	// Create timing trace if requested
	trace, err := timingtrace.New(*timingsPath, 100000)
	if err != nil {
		return err
	}
	defer func() {
		if e := trace.Close(); runErr == nil {
			runErr = e
		}
	}()

	// Validate destination address
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

	// Setup signal handling
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Use the first instrument's token
	token := m.Instruments[0].Token

	// Deterministic pattern: seq%3 → 1:buy100, 2:sell30, 0:sell70 at price 10000
	// Cycle triplets fill completely (buy100 matches with sell30+sell70).
	// If total%3 != 0, trailing partial: buy100 open or buy70+sell30 filled/partial.
	sendPacket := func(seq int32, scheduled time.Time) error {
		nativeID := float64(seq) // Use seq as the native float-based ID
		orderID, err := feed.IDFromFloat64(nativeID)
		if err != nil {
			return fmt.Errorf("seq %d: invalid order ID: %w", seq, err)
		}

		// Determine side and quantity based on seq%3
		var side feed.Side
		var quantity int32
		mod := seq % 3
		if mod == 1 {
			side = feed.Buy
			quantity = 100
		} else if mod == 2 {
			side = feed.Sell
			quantity = 30
		} else {
			side = feed.Sell
			quantity = 70
		}

		p := feed.Packet{
			Stream:    m.StreamID,
			Sequence:  seq,
			Type:      feed.New,
			Timestamp: time.Now().UTC().UnixNano(),
			OrderID:   orderID,
			Token:     token,
			Side:      side,
			Price:     10000,
			Quantity:  quantity,
		}

		b, err := feed.Encode(p)
		if err != nil {
			return fmt.Errorf("seq %d encode: %w", seq, err)
		}

		if trace == nil {
			_, err = conn.Write(b)
			return err
		}

		sendStart := time.Now()
		_, err = conn.Write(b)
		sendEnd := time.Now()
		if err == nil {
			trace.Record(timingtrace.Sample{
				Sequence:        seq,
				Type:            string(byte(p.Type)),
				StartUnixNS:     sendStart.UnixNano(),
				EndUnixNS:       sendEnd.UnixNano(),
				DurationNS:      sendEnd.Sub(sendStart).Nanoseconds(),
				ScheduledUnixNS: scheduled.UnixNano(),
			})
		}
		return err
	}

	// One ticker gate per packet. A second sleep after receiving a tick can
	// accidentally halve the achieved rate. Ticker drops missed ticks rather
	// than sending an unbounded catch-up burst; always report actual send rate.
	ticker := time.NewTicker(time.Second / time.Duration(*rate))
	defer ticker.Stop()
	count := 0
	lastSeq := int32(*start - 1)

	fmt.Printf("matching-workload manifest=%s stream=%d addr=%s start=%d end=%d total=%d rate=%d\n",
		*manifest, m.StreamID, *addr, *start, *end, *total, *rate)

	if err := sendPaced(ctx, ticker.C, int32(*start), int32(*end), func(seq int32, scheduled time.Time) error {
		if err := sendPacket(seq, scheduled); err != nil {
			return err
		}
		lastSeq = seq
		count++
		return nil
	}); err != nil {
		return err
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}
	// Send heartbeat if enabled
	if *heartbeat {
		hb := feed.Packet{
			Stream:       m.StreamID,
			Sequence:     0,
			Type:         feed.Heartbeat,
			LastSequence: lastSeq,
		}
		b, err := feed.Encode(hb)
		if err != nil {
			return err
		}
		_, err = conn.Write(b)
		if err != nil {
			return err
		}
		if trace != nil {
			trace.Record(timingtrace.Sample{
				Sequence:    0,
				Type:        string(byte(feed.Heartbeat)),
				StartUnixNS: time.Now().UnixNano(),
			})
		}
	}

	// Generate expected output if requested
	if *expectedPath != "" {
		expected := generateExpected(int32(*total), token)
		if err := writeJSON(*expectedPath, expected); err != nil {
			return err
		}
	}

	fmt.Printf("finished stream=%d sent_sequence=%d total_orders=%d\n",
		m.StreamID, lastSeq, count)

	return nil
}

// sendPaced consumes one scheduling tick per packet. It neither adds a second
// sleep nor invents ticks to catch up after a delayed scheduler.
func sendPaced(ctx context.Context, ticks <-chan time.Time, first, last int32, emit func(int32, time.Time) error) error {
	for seq := first; seq <= last; seq++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case scheduled, ok := <-ticks:
			if !ok {
				return fmt.Errorf("source pacing channel closed before completion")
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := emit(seq, scheduled); err != nil {
				return err
			}
		}
	}
	return nil
}

// generateExpected creates the oracle expected state for orders 1..total.
// Triplets (seq%3: 1=buy100, 2=sell30, 0=sell70) are fully filled.
// Trailing orders (if total%3 != 0) depend on total%3:
// - total%3 == 1: seq is buy100 (open, remaining=100, filled=0)
// - total%3 == 2: seq-1 is buy100 (partial: remaining=70, filled=30), seq is sell30 (filled, remaining=0)
func generateExpected(total int32, token int32) []expectedOrder {
	result := make([]expectedOrder, total)

	for i := int32(1); i <= total; i++ {
		mod := i % 3
		nativeID := float64(i)
		orderID, _ := feed.IDFromFloat64(nativeID)
		idBytes := orderID.Bytes()
		idHex := fmt.Sprintf("%02x", idBytes)

		var side string
		var qty, filled, remaining int32
		var status string

		if i <= (total/3)*3 {
			// Full triplet cycles: all filled
			if mod == 1 {
				side = "B"
				qty = 100
				filled = 100
				remaining = 0
			} else if mod == 2 {
				side = "S"
				qty = 30
				filled = 30
				remaining = 0
			} else {
				side = "S"
				qty = 70
				filled = 70
				remaining = 0
			}
			status = "FILLED"
		} else {
			// Trailing partial orders
			remainder := total % 3
			if remainder == 1 {
				// i is buy100 (open)
				side = "B"
				qty = 100
				filled = 0
				remaining = 100
				status = "OPEN"
			} else {
				// remainder == 2
				if mod == 1 {
					// buy100 partial (50% filled by sell30)
					side = "B"
					qty = 100
					filled = 30
					remaining = 70
					status = "PARTIALLY_FILLED"
				} else {
					// sell30 (fully filled)
					side = "S"
					qty = 30
					filled = 30
					remaining = 0
					status = "FILLED"
				}
			}
		}

		result[i-1] = expectedOrder{
			ID:                idHex,
			TokenID:           token,
			Side:              side,
			Price:             10000,
			AcceptedQuantity:  qty,
			FilledQuantity:    filled,
			RemainingQuantity: remaining,
			CancelledQuantity: 0,
			Status:            status,
		}
	}

	return result
}

func writeJSON(path string, v any) error {
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
