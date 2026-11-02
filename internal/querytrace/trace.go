// Package querytrace captures optional, bounded client-observed SQL-call timings.
// It does not measure PostgreSQL server execution alone or record SQL parameters.
// One recorder belongs exclusively to the serial consumer loop, not HTTP query handlers.
package querytrace

import (
	"context"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Sample struct {
	BrokerSequence                     uint64
	Sequence                           int32
	Type                               string
	Attempt                            int64
	Operation                          string
	StartUnixNS, EndUnixNS, DurationNS int64
	Success                            bool
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
	if limit < 1 || limit > 4000000 {
		return nil, fmt.Errorf("query-timings-limit must be 1..4000000")
	}
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return nil, e
	}
	return &Recorder{file: f, samples: make([]Sample, 0, limit), limit: limit}, nil
}

type eventKey struct{}
type event struct {
	recorder *Recorder
	broker   uint64
	sequence int32
	kind     string
	attempt  int64
}

// WithEvent binds correlation only. No wire or SQL parameter values are logged.
func WithEvent(ctx context.Context, r *Recorder, raw []byte, broker uint64, attempt int64) context.Context {
	if r == nil {
		return ctx
	}
	var sequence int32
	kind := "?"
	if len(raw) >= 9 {
		sequence = int32(binary.LittleEndian.Uint32(raw[4:8]))
		kind = string(raw[8:9])
	}
	return context.WithValue(ctx, eventKey{}, &event{r, broker, sequence, kind, attempt})
}

type Span struct {
	event     *event
	operation string
	start     time.Time
}

func Start(ctx context.Context, operation string) Span {
	e, _ := ctx.Value(eventKey{}).(*event)
	if e == nil {
		return Span{}
	}
	return Span{e, operation, time.Now()}
}

// End excludes subsequent validation. Reads must call End AFTER Scan consumes the row.
// SELECT missing order is a successful lookup outcome even when pgx reports ErrNoRows.
func (s Span) End(success bool) {
	if s.event == nil {
		return
	}
	end := time.Now()
	r := s.event.recorder
	if len(r.samples) >= r.limit {
		r.dropped++
		return
	}
	r.samples = append(r.samples, Sample{BrokerSequence: s.event.broker, Sequence: s.event.sequence, Type: s.event.kind, Attempt: s.event.attempt, Operation: s.operation, StartUnixNS: s.start.UnixNano(), EndUnixNS: end.UnixNano(), DurationNS: end.Sub(s.start).Nanoseconds(), Success: success})
}
func (r *Recorder) Close() error {
	if r == nil || r.closed {
		return nil
	}
	r.closed = true
	w := csv.NewWriter(r.file)
	err := w.Write([]string{"broker_sequence", "sequence", "type", "attempt", "operation", "start_unix_ns", "end_unix_ns", "duration_ns", "success"})
	for _, s := range r.samples {
		if err != nil {
			break
		}
		err = w.Write([]string{strconv.FormatUint(s.BrokerSequence, 10), strconv.FormatInt(int64(s.Sequence), 10), s.Type, strconv.FormatInt(s.Attempt, 10), s.Operation, strconv.FormatInt(s.StartUnixNS, 10), strconv.FormatInt(s.EndUnixNS, 10), strconv.FormatInt(s.DurationNS, 10), strconv.FormatBool(s.Success)})
	}
	w.Flush()
	if err == nil {
		err = w.Error()
	}
	if e := r.file.Close(); err == nil {
		err = e
	}
	if err == nil && r.dropped > 0 {
		err = fmt.Errorf("query trace truncated: dropped %d operations", r.dropped)
	}
	return err
}
