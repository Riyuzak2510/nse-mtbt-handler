// Package publisherpipeline separates UDP reads from ordered asynchronous sends.
package publisherpipeline

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"net"
	"nse-mtbt-handler/internal/broker"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	Queue                    int
	MaxBytes                 int64
	InFlight                 int
	AckTimeout, DrainTimeout time.Duration
}

func (c Config) Validate() error {
	if c.Queue < 1 || c.Queue > 65536 || c.InFlight < 1 || c.InFlight > 4096 || c.MaxBytes < 65535 || c.MaxBytes > 256<<20 || c.AckTimeout <= 0 || c.AckTimeout > time.Minute || c.DrainTimeout <= 0 || c.DrainTimeout > time.Minute {
		return fmt.Errorf("invalid bounded publisher config")
	}
	return nil
}

type Publisher interface {
	Submit([]byte, time.Time) (nats.PubAckFuture, error)
	Baseline() uint64
	ExpectedStream() string
}
type Datagram struct {
	Raw      []byte
	Received time.Time
	ReadWait time.Duration
}
type Pending struct {
	Packet   Datagram
	Start    time.Time
	Future   nats.PubAckFuture
	Expected uint64
}
type Snapshot struct {
	Received            uint64 `json:"received"`
	Submitted           uint64 `json:"submitted"`
	Confirmed           uint64 `json:"confirmed"`
	Overflow            uint64 `json:"overflow"`
	AckErrors           uint64 `json:"ack_errors"`
	Queued              int    `json:"queued"`
	InFlight            int    `json:"in_flight"`
	OwnedBytes          int64  `json:"owned_bytes"`
	HighBytes           int64  `json:"high_bytes"`
	HighQueue           int    `json:"high_queue"`
	HighInFlight        int    `json:"high_in_flight"`
	LastConfirmedBroker uint64 `json:"last_confirmed_broker"`
	Unconfirmed         uint64 `json:"unconfirmed"`
	Mode                string `json:"mode"`
}
type Observer interface {
	Receive(Datagram) error
	Confirm(Datagram, time.Time, time.Time, error) error
	Status(Snapshot) error
}
type noop struct{}

func (noop) Receive(Datagram) error                              { return nil }
func (noop) Confirm(Datagram, time.Time, time.Time, error) error { return nil }
func (noop) Status(Snapshot) error                               { return nil }

type counters struct {
	received, submitted, confirmed, overflow, ackErrors atomic.Uint64
	bytes, highBytes                                    atomic.Int64
	queue, highQueue, flight, highFlight                atomic.Int64
	last                                                atomic.Uint64
}

func high(v *atomic.Int64, n int64) {
	for {
		old := v.Load()
		if n <= old || v.CompareAndSwap(old, n) {
			return
		}
	}
}
func (s *counters) snapshot() Snapshot {
	r, confirmed := s.received.Load(), s.confirmed.Load()
	return Snapshot{r, s.submitted.Load(), confirmed, s.overflow.Load(), s.ackErrors.Load(), int(s.queue.Load()), int(s.flight.Load()), s.bytes.Load(), s.highBytes.Load(), int(s.highQueue.Load()), int(s.highFlight.Load()), s.last.Load(), r - confirmed, "async"}
}

// Run uses one receiver goroutine and one ordered submission/ack coordinator.
// Futures are retired in submission order; later accepted suffixes are ambiguous
// on any failure. No automatic retries, skipping or source replay is attempted.
func Run(ctx context.Context, conn *net.UDPConn, p Publisher, cfg Config, observer Observer) (result Snapshot, runErr error) {
	if e := cfg.Validate(); e != nil {
		return result, e
	}
	if observer == nil {
		observer = noop{}
	}
	var s counters
	s.last.Store(p.Baseline())
	queue := make(chan Datagram, cfg.Queue)
	fatal := make(chan error, 1)
	readCtx, stopRead := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	report := func(e error) {
		select {
		case fatal <- e:
		default:
		}
		stopRead()
	}
	wgReader := func() {
		defer wg.Done()
		defer close(queue)
		buf := make([]byte, 65535)
		for readCtx.Err() == nil {
			if e := conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); e != nil {
				report(e)
				return
			}
			began := time.Now()
			n, _, e := conn.ReadFromUDP(buf)
			if e != nil {
				if x, ok := e.(net.Error); ok && x.Timeout() {
					continue
				}
				if readCtx.Err() != nil {
					return
				}
				report(e)
				return
			}
			at := time.Now()
			s.received.Add(1)
			// The budget covers all copied queued/in-flight data, not only UDP payloads
			// currently in the queue. At most one reusable65535B scratch buffer is extra.
			next := s.bytes.Add(int64(n))
			if next > cfg.MaxBytes {
				s.bytes.Add(-int64(n))
				s.overflow.Add(1)
				report(fmt.Errorf("publisher byte budget exceeded; packet read but not admitted"))
				return
			}
			high(&s.highBytes, next)
			packet := Datagram{append([]byte(nil), buf[:n]...), at, at.Sub(began)}
			if e := observer.Receive(packet); e != nil {
				s.bytes.Add(-int64(n))
				report(e)
				return
			}
			// Count-before-send avoids races with a fast coordinator decrement.
			q := s.queue.Add(1)
			high(&s.highQueue, q)
			select {
			case queue <- packet:
			default:
				s.queue.Add(-1)
				s.bytes.Add(-int64(n))
				s.overflow.Add(1)
				report(fmt.Errorf("publisher receive queue full; no silent drop"))
				return
			}
		}
	}
	go wgReader()
	defer func() {
		stopRead()
		wg.Wait()
		result = s.snapshot()
		if e := observer.Status(result); runErr == nil && e != nil {
			runErr = e
		}
	}()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	parent := ctx.Done()
	var drain <-chan time.Time
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	pending := make([]Pending, 0, cfg.InFlight)
	input := queue
	stopping := false
	for {
		select {
		case e := <-fatal:
			return s.snapshot(), e
		default:
		}
		if !stopping && !connOpen(p) {
			return s.snapshot(), fmt.Errorf("broker connection closed")
		}
		if input == nil && len(pending) == 0 {
			return s.snapshot(), nil
		}
		var next <-chan Datagram = input
		if len(pending) >= cfg.InFlight {
			next = nil
		}
		var ok <-chan *nats.PubAck
		var failed <-chan error
		var timeout <-chan time.Time
		var headTimer *time.Timer
		if len(pending) > 0 {
			head := pending[0]
			ok = head.Future.Ok()
			failed = head.Future.Err()
			remaining := cfg.AckTimeout - time.Since(head.Start)
			if remaining < 0 {
				remaining = 0
			}
			headTimer = time.NewTimer(remaining)
			timeout = headTimer.C
		}
		select {
		case <-parent:
			stopping = true
			parent = nil
			stopRead()
			timer = time.NewTimer(cfg.DrainTimeout)
			drain = timer.C
		case <-drain:
			if headTimer != nil {
				headTimer.Stop()
			}
			return s.snapshot(), fmt.Errorf("publisher shutdown drain timed out; accepted publications ambiguous")
		case e := <-fatal:
			if headTimer != nil {
				headTimer.Stop()
			}
			return s.snapshot(), e
		case packet, open := <-next:
			if !open {
				input = nil
				break
			}
			s.queue.Add(-1)
			began := time.Now()
			future, e := p.Submit(packet.Raw, packet.Received)
			if e != nil {
				s.ackErrors.Add(1)
				_ = observer.Confirm(packet, began, time.Now(), e)
				if headTimer != nil {
					headTimer.Stop()
				}
				return s.snapshot(), fmt.Errorf("asynchronous submission failed; no retry: %w", e)
			}
			if future == nil {
				if headTimer != nil {
					headTimer.Stop()
				}
				return s.snapshot(), errors.New("nil publication future")
			}
			offset := s.submitted.Add(1)
			pending = append(pending, Pending{packet, began, future, p.Baseline() + offset})
			flight := s.flight.Add(1)
			high(&s.highFlight, flight)
		case ack := <-ok:
			head := pending[0]
			e := broker.ValidateAck(ack, p.ExpectedStream(), head.Expected)
			if e != nil {
				s.ackErrors.Add(1)
				_ = observer.Confirm(head.Packet, head.Start, time.Now(), e)
				if headTimer != nil {
					headTimer.Stop()
				}
				return s.snapshot(), e
			}
			at := time.Now()
			if e = observer.Confirm(head.Packet, head.Start, at, nil); e != nil {
				if headTimer != nil {
					headTimer.Stop()
				}
				return s.snapshot(), e
			}
			s.confirmed.Add(1)
			s.last.Store(ack.Sequence)
			s.bytes.Add(-int64(len(head.Packet.Raw)))
			s.flight.Add(-1)
			copy(pending, pending[1:])
			pending[len(pending)-1] = Pending{}
			pending = pending[:len(pending)-1]
		case e := <-failed:
			s.ackErrors.Add(1)
			head := pending[0]
			_ = observer.Confirm(head.Packet, head.Start, time.Now(), e)
			if headTimer != nil {
				headTimer.Stop()
			}
			return s.snapshot(), fmt.Errorf("publish acknowledgement failed; no retry: %w", e)
		case <-timeout:
			s.ackErrors.Add(1)
			if headTimer != nil {
				headTimer.Stop()
			}
			return s.snapshot(), fmt.Errorf("publish acknowledgement deadline exceeded; broker admission ambiguous")
		case <-ticker.C:
			if e := observer.Status(s.snapshot()); e != nil {
				if headTimer != nil {
					headTimer.Stop()
				}
				return s.snapshot(), e
			}
		}
		if headTimer != nil {
			headTimer.Stop()
		}
	}
}

// Real adapter supplies this optional signal; fakes can focus on ack semantics.
func connOpen(p Publisher) bool {
	if c, ok := p.(interface{ Connected() bool }); ok {
		return c.Connected()
	}
	return true
}
