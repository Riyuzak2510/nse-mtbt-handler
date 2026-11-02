package publisherpipeline

import (
	"bytes"
	"context"
	"errors"
	"github.com/nats-io/nats.go"
	"net"
	"sync"
	"testing"
	"time"
)

type future struct {
	ok   chan *nats.PubAck
	fail chan error
}

func (f *future) Ok() <-chan *nats.PubAck { return f.ok }
func (f *future) Err() <-chan error       { return f.fail }
func (f *future) Msg() *nats.Msg          { return nil }

type fakePublisher struct {
	mu        sync.Mutex
	packets   [][]byte
	futures   []*future
	auto      bool
	submitErr error
	notify    chan struct{}
}

func (f *fakePublisher) Baseline() uint64       { return 7 }
func (f *fakePublisher) ExpectedStream() string { return "MTBT_1" }
func (f *fakePublisher) Submit(b []byte, _ time.Time) (nats.PubAckFuture, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.submitErr != nil {
		return nil, f.submitErr
	}
	u := &future{make(chan *nats.PubAck, 1), make(chan error, 1)}
	f.packets = append(f.packets, b)
	f.futures = append(f.futures, u)
	n := len(f.packets)
	if f.auto {
		u.ok <- &nats.PubAck{Stream: "MTBT_1", Sequence: 7 + uint64(n)}
	}
	if f.notify != nil {
		select {
		case f.notify <- struct{}{}:
		default:
		}
	}
	return u, nil
}
func sockets(t *testing.T) (*net.UDPConn, *net.UDPConn) {
	t.Helper()
	r, e := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if e != nil {
		t.Fatal(e)
	}
	w, e := net.DialUDP("udp4", nil, r.LocalAddr().(*net.UDPAddr))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { r.Close(); w.Close() })
	return r, w
}
func config() Config {
	return Config{Queue: 32, MaxBytes: 1 << 20, InFlight: 8, AckTimeout: time.Second, DrainTimeout: time.Second}
}
func wait(t *testing.T, predicate func() bool) {
	t.Helper()
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		if predicate() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("wait timed out")
}
func TestAsyncCopiesPacketsAndDrains(t *testing.T) {
	r, w := sockets(t)
	p := &fakePublisher{auto: true}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	var result Snapshot
	go func() { var e error; result, e = Run(ctx, r, p, config(), nil); done <- e }()
	b := []byte{1, 0, 0}
	for i := byte(1); i <= 20; i++ {
		b[0] = i
		w.Write(b)
		time.Sleep(time.Millisecond)
	}
	wait(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return len(p.packets) == 20 })
	cancel()
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if result.Confirmed != 20 || result.OwnedBytes != 0 || result.Unconfirmed != 0 {
		t.Fatal(result)
	}
	for i, b := range p.packets {
		if !bytes.Equal(b, []byte{byte(i + 1), 0, 0}) {
			t.Fatal(i, b)
		}
	}
}
func TestManyInFlightBeforeAcknowledgementAndOrderedRetirement(t *testing.T) {
	r, w := sockets(t)
	p := &fakePublisher{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, e := Run(ctx, r, p, config(), nil); done <- e }()
	for i := byte(1); i <= 8; i++ {
		w.Write([]byte{i})
		time.Sleep(time.Millisecond)
	}
	wait(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return len(p.futures) == 8 })
	p.mu.Lock()
	for i := 7; i >= 0; i-- {
		p.futures[i].ok <- &nats.PubAck{Stream: "MTBT_1", Sequence: 8 + uint64(i)}
	}
	p.mu.Unlock()
	cancel()
	if e := <-done; e != nil {
		t.Fatal(e)
	}
}
func TestFailuresAreVisible(t *testing.T) {
	for _, mode := range []string{"error", "wrongstream", "wrongseq", "duplicate", "timeout", "submit"} {
		t.Run(mode, func(t *testing.T) {
			r, w := sockets(t)
			p := &fakePublisher{}
			if mode == "submit" {
				p.submitErr = errors.New("submit")
			}
			cfg := config()
			cfg.AckTimeout = 30 * time.Millisecond
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { _, e := Run(ctx, r, p, cfg, nil); done <- e }()
			w.Write([]byte{1})
			if mode != "submit" {
				wait(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return len(p.futures) == 1 })
				p.mu.Lock()
				f := p.futures[0]
				p.mu.Unlock()
				switch mode {
				case "error":
					f.fail <- errors.New("reject")
				case "wrongstream":
					f.ok <- &nats.PubAck{Stream: "bad", Sequence: 8}
				case "wrongseq":
					f.ok <- &nats.PubAck{Stream: "MTBT_1", Sequence: 9}
				case "duplicate":
					f.ok <- &nats.PubAck{Stream: "MTBT_1", Sequence: 8, Duplicate: true}
				}
			}
			if e := <-done; e == nil {
				t.Fatal("silent failure")
			}
		})
	}
}
func TestQueueOverflowAndByteBudgetFailClosed(t *testing.T) {
	for _, byteLimit := range []bool{false, true} {
		r, w := sockets(t)
		p := &fakePublisher{}
		cfg := config()
		cfg.Queue = 1
		cfg.InFlight = 1
		if byteLimit {
			cfg.MaxBytes = 65535
			cfg.Queue = 32
			cfg.InFlight = 32
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		done := make(chan Snapshot, 1)
		errs := make(chan error, 1)
		go func() { s, e := Run(ctx, r, p, cfg, nil); done <- s; errs <- e }()
		payload := []byte{1}
		if byteLimit {
			payload = make([]byte, 4000)
		}
		for i := 0; i < 20; i++ {
			w.Write(payload)
			time.Sleep(time.Millisecond)
		}
		s := <-done
		if e := <-errs; e == nil || s.Overflow != 1 {
			t.Fatal(s, e)
		}
		cancel()
	}
}
func TestDrainDeadline(t *testing.T) {
	r, w := sockets(t)
	p := &fakePublisher{}
	cfg := config()
	cfg.DrainTimeout = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := Run(ctx, r, p, cfg, nil); done <- e }()
	w.Write([]byte{1})
	wait(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return len(p.futures) == 1 })
	cancel()
	if e := <-done; e == nil {
		t.Fatal("silently lost in-flight message")
	}
}
func TestInvalidConfig(t *testing.T) {
	for _, c := range []Config{{}, {Queue: 1, MaxBytes: 1, InFlight: 1, AckTimeout: time.Second, DrainTimeout: time.Second}} {
		if c.Validate() == nil {
			t.Fatal(c)
		}
	}
}
