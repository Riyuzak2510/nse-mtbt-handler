package main

import (
	"encoding/json"
	"nse-mtbt-handler/internal/ingestdiagnostics"
	"nse-mtbt-handler/internal/publisherpipeline"
	"nse-mtbt-handler/internal/timingtrace"
	"os"
	"sync"
	"time"
)

// All recorder access is serialized, including Receive on the UDP goroutine.
// No packet slice is retained by this observer beyond its call.
type asyncObserver struct {
	mu      sync.Mutex
	ingest  *ingestdiagnostics.Recorder
	trace   *timingtrace.Recorder
	file    *os.File
	encoder *json.Encoder
}

func newAsyncObserver(d *ingestdiagnostics.Recorder, t *timingtrace.Recorder, path string) (*asyncObserver, error) {
	o := &asyncObserver{ingest: d, trace: t}
	if path != "" {
		f, e := os.OpenFile(path+".pipeline.jsonl", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return nil, e
		}
		o.file = f
		o.encoder = json.NewEncoder(f)
	}
	return o, nil
}
func (o *asyncObserver) Receive(p publisherpipeline.Datagram) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.ingest.Receive(p.Raw, p.ReadWait, p.Received)
}
func (o *asyncObserver) Confirm(p publisherpipeline.Datagram, start, end time.Time, publishErr error) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if publishErr == nil && o.trace != nil {
		seq, kind := timingtrace.Identity(p.Raw)
		o.trace.Record(timingtrace.Sample{Sequence: seq, Type: kind, StartUnixNS: start.UnixNano(), EndUnixNS: end.UnixNano(), DurationNS: end.Sub(start).Nanoseconds(), ReceivedUnixNS: p.Received.UnixNano()})
	}
	return o.ingest.Publication(end.Sub(start), publishErr)
}
func (o *asyncObserver) Status(s publisherpipeline.Snapshot) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.encoder != nil {
		if e := o.encoder.Encode(struct {
			AtUnixNS int64 `json:"at_unix_ns"`
			publisherpipeline.Snapshot
		}{time.Now().UnixNano(), s}); e != nil {
			return e
		}
	}
	return o.ingest.Tick()
}
func (o *asyncObserver) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.file != nil {
		return o.file.Close()
	}
	return nil
}
