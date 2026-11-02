// Package timingtrace is opt-in, bounded-memory benchmark instrumentation.
// Each recorder belongs to one process loop; it is deliberately not concurrent.
// Files are created exclusively before traffic; samples are written only at shutdown.
package timingtrace

import (
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
)

type Sample struct {
	Sequence                                          int32
	Type                                              string
	StartUnixNS, EndUnixNS, DurationNS                int64
	ReceivedUnixNS, ScheduledUnixNS, DeliveryUnixNS   int64
	AckUnixNS, AckDurationNS                          int64
	Attempts                                          int64
	Shard                                             int
	ShardSequence, SourceBroker                       uint64
	RouteStartUnixNS, RouteEndUnixNS, RouteDurationNS int64
}
type Recorder struct {
	file    *os.File
	samples []Sample
	limit   int
	dropped int
	closed  bool
}

func New(path string, limit int) (*Recorder, error) {
	if path == "" {
		return nil, nil
	}
	if limit < 1 || limit > 2000000 {
		return nil, fmt.Errorf("timings-limit must be 1..2000000")
	}
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return nil, e
	}
	return &Recorder{file: f, samples: make([]Sample, 0, limit), limit: limit}, nil
}
func (r *Recorder) Record(s Sample) {
	if r == nil {
		return
	}
	if len(r.samples) == r.limit {
		r.dropped++
		return
	}
	r.samples = append(r.samples, s)
}
func (r *Recorder) Close() error {
	if r == nil || r.closed {
		return nil
	}
	r.closed = true
	w := csv.NewWriter(r.file)
	e := w.Write([]string{"sequence", "type", "start_unix_ns", "end_unix_ns", "duration_ns", "received_unix_ns", "scheduled_unix_ns", "delivery_unix_ns", "ack_unix_ns", "ack_duration_ns", "attempts", "shard", "shard_sequence", "source_broker", "route_start_unix_ns", "route_end_unix_ns", "route_duration_ns"})
	for _, s := range r.samples {
		if e != nil {
			break
		}
		e = w.Write([]string{strconv.FormatInt(int64(s.Sequence), 10), s.Type, i(s.StartUnixNS), i(s.EndUnixNS), i(s.DurationNS), i(s.ReceivedUnixNS), i(s.ScheduledUnixNS), i(s.DeliveryUnixNS), i(s.AckUnixNS), i(s.AckDurationNS), i(s.Attempts), strconv.Itoa(s.Shard), strconv.FormatUint(s.ShardSequence, 10), strconv.FormatUint(s.SourceBroker, 10), i(s.RouteStartUnixNS), i(s.RouteEndUnixNS), i(s.RouteDurationNS)})
	}
	w.Flush()
	if e == nil {
		e = w.Error()
	}
	if closeErr := r.file.Close(); e == nil {
		e = closeErr
	}
	if e == nil && r.dropped > 0 {
		e = fmt.Errorf("timing trace truncated: dropped %d samples", r.dropped)
	}
	return e
}
func i(n int64) string { return strconv.FormatInt(n, 10) }

// Identity extracts only correlation metadata, not a validation result.
func Identity(raw []byte) (int32, string) {
	if len(raw) < 9 {
		return 0, "?"
	}
	return int32(binary.LittleEndian.Uint32(raw[4:8])), string(raw[8:9])
}
