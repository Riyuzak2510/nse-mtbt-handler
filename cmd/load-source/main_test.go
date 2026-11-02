package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/runconfig"
	"testing"
	"time"
)

func catalog() runconfig.Manifest {
	return runconfig.Manifest{StreamID: 1, Instruments: []runconfig.Instrument{{Token: 10, BasePrice: 10000}, {Token: 20, BasePrice: 20000}}}
}
func TestProfiles(t *testing.T) {
	s, e := parseProfile("100000@1000,100000@5000,19800000@10000")
	if e != nil || len(s) != 3 || s[2].Count != 19800000 {
		t.Fatal(s, e)
	}
	for _, s := range []string{"", "1@0", "0@10", "-1@20", "3@1000001", "2147483647@1,1@1", "one@two"} {
		if _, e := parseProfile(s); e == nil {
			t.Fatal(s)
		}
	}
}
func TestCatalogCycle(t *testing.T) {
	m := catalog()
	for n := int32(1); n <= 12; n++ {
		p, e := packet(n, m, 1234)
		if e != nil || p.Sequence != n || p.Timestamp != 1234 || p.Token != m.Instruments[(n-1)%2].Token || p.OrderID.Float64() != float64(n) {
			t.Fatal(p, e)
		}
		slot := (n - 1) % 6
		if slot < 2 && p.Side != feed.Buy || slot >= 2 && p.Side != feed.Sell {
			t.Fatal(p)
		}
	}
}

type fakeWriter struct {
	count int
	err   error
	pause time.Duration
}

func (f *fakeWriter) Write(b []byte) (int, error) {
	f.count++
	if f.pause > 0 {
		time.Sleep(f.pause)
	}
	if f.err != nil {
		return 0, f.err
	}
	return len(b), nil
}
func finalReport(t *testing.T, b *bytes.Buffer) sourceReport {
	t.Helper()
	scan := bufio.NewScanner(b)
	var r sourceReport
	for scan.Scan() {
		if e := json.Unmarshal(scan.Bytes(), &r); e != nil {
			t.Fatal(e)
		}
	}
	return r
}
func TestUDPSendsCountAndHeartbeat(t *testing.T) {
	receiver, e := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if e != nil {
		t.Fatal(e)
	}
	defer receiver.Close()
	sender, e := net.DialUDP("udp4", nil, receiver.LocalAddr().(*net.UDPAddr))
	if e != nil {
		t.Fatal(e)
	}
	defer sender.Close()
	var out bytes.Buffer
	if e = generate(context.Background(), sender, catalog(), []stage{{11, 10000}}, 1, time.Second, json.NewEncoder(&out)); e != nil {
		t.Fatal(e)
	}
	receiver.SetReadDeadline(time.Now().Add(time.Second))
	b := make([]byte, 1024)
	for i := int32(1); i <= 12; i++ {
		n, _, e := receiver.ReadFromUDP(b)
		if e != nil {
			t.Fatal(e)
		}
		p, e := feed.Decode(b[:n])
		if e != nil {
			t.Fatal(e)
		}
		if i < 12 && (p.Sequence != i || p.Timestamp <= 0) {
			t.Fatal(p)
		}
		if i == 12 && (p.Type != feed.Heartbeat || p.LastSequence != 11) {
			t.Fatal(p)
		}
	}
	r := finalReport(t, &out)
	if r.Kind != "complete" || r.Sent != 11 || r.ScheduledByNow != 11 || r.StageCounts[0] != 11 {
		t.Fatal(r)
	}
}
func TestRuntimeErrorsReported(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		ctx, stop := context.WithCancel(context.Background())
		if cancel {
			stop()
		}
		var out bytes.Buffer
		w := &fakeWriter{err: errors.New("write failed")}
		e := generate(ctx, w, catalog(), []stage{{10, 1000}}, 1, time.Second, json.NewEncoder(&out))
		stop()
		if e == nil {
			t.Fatal("lost error")
		}
		r := finalReport(t, &out)
		if r.Reason == "" || r.Sent != 0 || r.Kind != "failed" && r.Kind != "interrupted" {
			t.Fatal(r)
		}
	}
}
func TestSchedulingPauseAborts(t *testing.T) {
	var out bytes.Buffer
	w := &fakeWriter{pause: 10 * time.Millisecond}
	e := generate(context.Background(), w, catalog(), []stage{{100, 10000}}, 1, time.Millisecond, json.NewEncoder(&out))
	if e == nil {
		t.Fatal("missed scheduling pause")
	}
	r := finalReport(t, &out)
	if !r.EnvironmentPause || r.Kind != "failed" || r.Sent >= 100 || r.ScheduledByNow < r.Sent {
		t.Fatal(r)
	}
}
func TestBatchBounds(t *testing.T) {
	for _, r := range []int64{1, 100, 1000, 10000, 1000000} {
		for _, left := range []int64{1, 2, 11, 10000} {
			n := batchSize(r, left)
			if n < 1 || n > left || n > r/1000 && n != 1 {
				t.Fatal(r, left, n)
			}
		}
	}
}
