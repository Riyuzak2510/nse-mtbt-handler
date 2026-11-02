package loadmetrics

import (
	"encoding/json"
	"fmt"
	"os"
)

type Sample struct {
	// Same-process durations may use monotonic Go clock readings even when wall
	// time adjusts. Cross-process boundaries remain wall-clock measurements.
	MonotonicDurations                                                                                      bool
	ProcessDurationNS, CommitToAckDurationNS                                                                int64
	Sequence                                                                                                int32
	SourceBroker, Attempts                                                                                  uint64
	ScheduledUnixNS, EmittedUnixNS, ReceivedUnixNS, DeliveryUnixNS, StartUnixNS, CommittedUnixNS, AckUnixNS int64
}

type Window struct {
	Count             uint64             `json:"count"`
	FirstSequence     int32              `json:"first_sequence"`
	LastSequence      int32              `json:"last_sequence"`
	LastBroker        uint64             `json:"last_broker"`
	StartedUnixNS     int64              `json:"started_unix_ns"`
	EndedUnixNS       int64              `json:"ended_unix_ns"`
	RetriedSamples    uint64             `json:"retried_samples"`
	AckFailures       uint64             `json:"ack_failures"`
	MissingTimestamps uint64             `json:"missing_timestamps"`
	ScheduledCount    uint64             `json:"scheduled_count"`
	ScheduledLagNS    int64              `json:"scheduled_lag_ns"`
	Histograms        map[string]Summary `json:"histograms"`
}
type accumulator struct {
	w Window
	h [5]Histogram
}

var names = [5]string{"emitted_to_commit", "received_to_delivery", "db_process", "ack", "emitted_to_ack"}

func (a *accumulator) record(s Sample) error {
	// Validate required timestamps (EmittedUnixNS must be positive, non-zero)
	if s.EmittedUnixNS <= 0 || s.ReceivedUnixNS <= 0 || s.DeliveryUnixNS <= 0 || s.StartUnixNS <= 0 || s.CommittedUnixNS <= 0 {
		a.w.MissingTimestamps++
		return nil // Skip invalid sample
	}

	if a.w.Count == 0 {
		a.w.FirstSequence = s.Sequence
		a.w.StartedUnixNS = s.StartUnixNS
	}
	a.w.Count++
	a.w.LastSequence = s.Sequence
	a.w.LastBroker = s.SourceBroker
	a.w.EndedUnixNS = s.CommittedUnixNS
	if s.Attempts > 1 {
		a.w.RetriedSamples++
	}

	// Track scheduled vs actual if available
	if s.ScheduledUnixNS > 0 && s.EmittedUnixNS > 0 {
		a.w.ScheduledCount++
		scheduledLag := s.EmittedUnixNS - s.ScheduledUnixNS
		if scheduledLag >= 0 {
			a.w.ScheduledLagNS += scheduledLag
		}
	}

	// Record latency histograms
	a.h[0].Record(s.CommittedUnixNS - s.EmittedUnixNS)
	a.h[1].Record(s.DeliveryUnixNS - s.ReceivedUnixNS)
	processNS := s.CommittedUnixNS - s.StartUnixNS
	if s.MonotonicDurations {
		processNS = s.ProcessDurationNS
	}
	a.h[2].Record(processNS)
	if s.AckUnixNS <= 0 {
		a.w.AckFailures++
	} else {
		a.w.EndedUnixNS = s.AckUnixNS
		ackNS := s.AckUnixNS - s.CommittedUnixNS
		if s.MonotonicDurations {
			ackNS = s.CommitToAckDurationNS
		}
		a.h[3].Record(ackNS)
		a.h[4].Record(s.AckUnixNS - s.EmittedUnixNS)
	}
	return nil
}
func (a *accumulator) snapshot() Window {
	w := a.w
	w.Histograms = make(map[string]Summary, 5)
	for i, name := range names {
		w.Histograms[name] = a.h[i].Snapshot()
	}
	return w
}

// Recorder belongs to one processing loop. Memory is fixed; one JSON record is
// written per elapsed second of processing and at graceful shutdown. No goroutine
// races with its accumulators. O_EXCL refuses overwrite; errors stop the test.
type Recorder struct {
	file            *os.File
	encoder         *json.Encoder
	total, interval accumulator
	lastFlush       int64
	closed          bool
}
type Report struct {
	Version      int    `json:"version"`
	Kind         string `json:"kind"`
	BucketPolicy string `json:"bucket_policy"`
	Cumulative   Window `json:"cumulative"`
	Interval     Window `json:"interval"`
}

func NewRecorder(path string) (*Recorder, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	return &Recorder{file: f, encoder: json.NewEncoder(f)}, nil
}
func (r *Recorder) Record(s Sample) error {
	if r == nil {
		return nil
	}
	if r.closed {
		return fmt.Errorf("metrics recorder closed")
	}
	if s.Sequence <= 0 {
		return nil
	} // Heartbeats are not workload events.

	// Record to both accumulators and handle errors
	if err := r.total.record(s); err != nil {
		return err
	}
	if err := r.interval.record(s); err != nil {
		return err
	}

	end := s.CommittedUnixNS
	if s.AckUnixNS > end {
		end = s.AckUnixNS
	}
	if r.lastFlush == 0 {
		r.lastFlush = end
	}
	if end-r.lastFlush >= 1e9 {
		if err := r.flush("interval"); err != nil {
			return err
		}
		r.lastFlush = end
		r.interval = accumulator{}
	}
	return nil
}
func (r *Recorder) flush(kind string) error {
	return r.encoder.Encode(Report{1, kind, "32 subdivisions per power-of-two nanoseconds; percentiles are conservative bucket upper bounds (~3.125% max relative width above32ns)", r.total.snapshot(), r.interval.snapshot()})
}
func (r *Recorder) Close() error {
	if r == nil || r.closed {
		return nil
	}
	r.closed = true
	err := r.flush("final")
	closeErr := r.file.Close()
	if err != nil {
		return err
	}
	return closeErr
}
