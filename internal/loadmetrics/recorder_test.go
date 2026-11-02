package loadmetrics

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestMonotonicDurationOverridesWallAdjustment(t *testing.T) {
	r, err := NewRecorder(filepath.Join(t.TempDir(), "m"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	err = r.Record(Sample{Sequence: 1, Attempts: 1, EmittedUnixNS: 50, ReceivedUnixNS: 60, DeliveryUnixNS: 70, StartUnixNS: 100, CommittedUnixNS: 90, AckUnixNS: 95, MonotonicDurations: true, ProcessDurationNS: 20, CommitToAckDurationNS: 5})
	if err != nil {
		t.Fatal(err)
	}
	s := r.total.snapshot()
	if s.Histograms["db_process"].Invalid != 0 || s.Histograms["db_process"].MeanMS != .00002 || s.Histograms["ack"].MeanMS != .000005 {
		t.Fatal(s)
	}
}

func TestRecorder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.jsonl")
	r, e := NewRecorder(path)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = NewRecorder(path); e == nil {
		t.Fatal("overwritten")
	}
	for i := int32(1); i <= 3; i++ {
		n := int64(i) * 2000000000
		e = r.Record(Sample{Sequence: i, SourceBroker: uint64(i), Attempts: 1, ScheduledUnixNS: n, EmittedUnixNS: n + 100, ReceivedUnixNS: n + 200, DeliveryUnixNS: n + 300, StartUnixNS: n + 400, CommittedUnixNS: n + 500})
		if e != nil {
			t.Fatal(e)
		}
	}
	if e = r.Close(); e != nil {
		t.Fatal(e)
	}
	r.Close()
	f, _ := os.Open(path)
	defer f.Close()
	s := bufio.NewScanner(f)
	var last Report
	lines := 0
	for s.Scan() {
		if e = json.Unmarshal(s.Bytes(), &last); e != nil {
			t.Fatal(e)
		}
		lines++
	}
	if last.Kind != "final" || last.Cumulative.Count != 3 || last.Cumulative.Histograms["emitted_to_commit"].Count != 3 || lines != 3 {
		t.Fatal(last, lines)
	}
}
func TestNilAndErrors(t *testing.T) {
	r, e := NewRecorder("")
	if e != nil || r != nil {
		t.Fatal(r, e)
	}
	if e = r.Record(Sample{}); e != nil {
		t.Fatal(e)
	}
	r.Close()
	r, e = NewRecorder(filepath.Join(t.TempDir(), "m"))
	if e != nil {
		t.Fatal(e)
	}
	r.file.Close()
	if e = r.Close(); e == nil {
		t.Fatal("lost write error")
	}
}
func TestInvalidAndRetry(t *testing.T) {
	r, _ := NewRecorder(filepath.Join(t.TempDir(), "m"))
	defer r.Close()
	r.Record(Sample{Sequence: 1, Attempts: 2, EmittedUnixNS: 20, ReceivedUnixNS: 21, DeliveryUnixNS: 22, StartUnixNS: 23, CommittedUnixNS: 10})
	s := r.total.snapshot()
	if s.AckFailures != 1 || s.RetriedSamples != 1 || s.Histograms["emitted_to_commit"].Invalid != 1 {
		t.Fatal(s)
	}
}

// TestRecorderInvalidTimestamps verifies zero/negative EmittedUnixNS are rejected
func TestRecorderInvalidTimestamps(t *testing.T) {
	r, _ := NewRecorder(filepath.Join(t.TempDir(), "m"))
	defer r.Close()

	// Record with zero EmittedUnixNS - should be rejected and counted
	r.Record(Sample{Sequence: 1, EmittedUnixNS: 0})
	r.Record(Sample{Sequence: 2, EmittedUnixNS: -1})

	// Record valid sample
	r.Record(Sample{Sequence: 3, EmittedUnixNS: 100, ReceivedUnixNS: 110, DeliveryUnixNS: 120, StartUnixNS: 130, CommittedUnixNS: 200})

	s := r.total.snapshot()
	if s.Count != 1 || s.MissingTimestamps != 2 {
		t.Fatalf("expected count=1 missing=2, got count=%d missing=%d", s.Count, s.MissingTimestamps)
	}
}

// TestRecorderScheduledLag verifies scheduled vs actual lag tracking
func TestRecorderScheduledLag(t *testing.T) {
	r, _ := NewRecorder(filepath.Join(t.TempDir(), "m"))
	defer r.Close()

	// Record samples with scheduled times
	base := int64(1000000)
	expectedTotalLag := int64(0)

	for i := 1; i <= 10; i++ {
		scheduled := base + int64(i*100)
		emitted := scheduled + int64(i*10) // i*10 ns lag each
		expectedTotalLag += int64(i * 10)
		r.Record(Sample{
			Sequence:        int32(i),
			ScheduledUnixNS: scheduled,
			EmittedUnixNS:   emitted,
			ReceivedUnixNS:  emitted + 1, DeliveryUnixNS: emitted + 2, StartUnixNS: emitted + 3,
			CommittedUnixNS: emitted + 100,
		})
	}

	s := r.total.snapshot()
	if s.ScheduledCount != 10 {
		t.Fatalf("expected scheduled_count=10, got %d", s.ScheduledCount)
	}
	if s.ScheduledLagNS != expectedTotalLag {
		t.Fatalf("expected lag=%d, got %d", expectedTotalLag, s.ScheduledLagNS)
	}
}

// TestRecorderHeartbeatExclusion verifies heartbeats are excluded
func TestRecorderHeartbeatExclusion(t *testing.T) {
	r, _ := NewRecorder(filepath.Join(t.TempDir(), "m"))
	defer r.Close()

	// Mix real samples with heartbeats (Sequence <= 0)
	r.Record(Sample{Sequence: 1, EmittedUnixNS: 100, ReceivedUnixNS: 110, DeliveryUnixNS: 120, StartUnixNS: 130, CommittedUnixNS: 200})
	r.Record(Sample{Sequence: 0, EmittedUnixNS: 300, CommittedUnixNS: 400})  // heartbeat
	r.Record(Sample{Sequence: -1, EmittedUnixNS: 500, CommittedUnixNS: 600}) // heartbeat
	r.Record(Sample{Sequence: 2, EmittedUnixNS: 700, ReceivedUnixNS: 710, DeliveryUnixNS: 720, StartUnixNS: 730, CommittedUnixNS: 800})

	s := r.total.snapshot()
	if s.Count != 2 || s.FirstSequence != 1 || s.LastSequence != 2 {
		t.Fatalf("expected count=2 first=1 last=2, got count=%d first=%d last=%d",
			s.Count, s.FirstSequence, s.LastSequence)
	}
}

// TestRecorderMonotonicPercentiles verifies percentiles are monotonic
func TestRecorderMonotonicPercentiles(t *testing.T) {
	r, _ := NewRecorder(filepath.Join(t.TempDir(), "m"))
	defer r.Close()

	// Record samples with increasing latencies
	for i := int32(1); i <= 1000; i++ {
		latency := int64(i * 100) // 100, 200, 300, ... ns
		r.Record(Sample{
			Sequence:       i,
			EmittedUnixNS:  1000 + latency,
			ReceivedUnixNS: 1001 + latency, DeliveryUnixNS: 1002 + latency, StartUnixNS: 1003 + latency,
			CommittedUnixNS: 1000 + latency + latency,
		})
	}

	s := r.total.snapshot()
	hist := s.Histograms["emitted_to_commit"]

	// Verify percentiles are monotonic
	if hist.P50MS > hist.P95MS {
		t.Fatalf("P50 > P95: %f > %f", hist.P50MS, hist.P95MS)
	}
	if hist.P95MS > hist.P99MS {
		t.Fatalf("P95 > P99: %f > %f", hist.P95MS, hist.P99MS)
	}
	if hist.P99MS > hist.P999MS {
		t.Fatalf("P99 > P999: %f > %f", hist.P99MS, hist.P999MS)
	}
	if hist.P999MS > hist.MaxMS {
		t.Fatalf("P999 > Max: %f > %f", hist.P999MS, hist.MaxMS)
	}
	if hist.MaxMS < hist.P999MS {
		t.Fatalf("Max < P999: %f < %f", hist.MaxMS, hist.P999MS)
	}
}
