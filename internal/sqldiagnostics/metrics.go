package sqldiagnostics

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"nse-mtbt-handler/internal/loadmetrics"
)

// Record represents a single JSONL row for SQL diagnostics.
type Record struct {
	TimestampUnixNS int64                       `json:"timestamp_unix_ns"`
	Kind            string                      `json:"kind"` // "interval" or "final"
	Operations      map[string]OperationMetrics `json:"operations"`
}

// OperationMetrics holds histogram statistics for one operation label.
type OperationMetrics struct {
	Count   uint64  `json:"count"`
	Invalid uint64  `json:"invalid"`
	Errors  uint64  `json:"errors"`
	MinMS   float64 `json:"min_ms"`
	MeanMS  float64 `json:"mean_ms"`
	MaxMS   float64 `json:"max_ms"`
	P50MS   float64 `json:"p50_ms"`
	P95MS   float64 `json:"p95_ms"`
	P99MS   float64 `json:"p99_ms"`
	P999MS  float64 `json:"p999_ms"`
}

// Recorder collects SQL timing metrics and writes JSONL output.
// It is safe for concurrent access from pgx query tracer callbacks.
type Recorder struct {
	file       *os.File
	encoder    *json.Encoder
	mu         sync.Mutex
	histograms map[string]*loadmetrics.Histogram
	errors     map[string]uint64 // errors per operation
	lastFlush  int64
	closed     bool
	lastError  error // Captured for DiagnosticsError()
}

// NewRecorder creates a new SQL diagnostics Recorder.
// If path is empty, returns nil (no-op mode).
// File is created with O_EXCL to prevent accidental overwrites.
func NewRecorder(path string) (*Recorder, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("create sql diagnostics file: %w", err)
	}
	return &Recorder{
		file:       f,
		encoder:    json.NewEncoder(f),
		histograms: make(map[string]*loadmetrics.Histogram),
		errors:     make(map[string]uint64),
		lastFlush:  time.Now().UnixNano(),
	}, nil
}

// recordQuery records a single query execution with its duration.
// Called synchronously by QueryTracer.TraceQueryEnd with bounded histogram memory.
func (r *Recorder) recordQuery(operation string, durationNS int64, err error) error {
	if r == nil {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return fmt.Errorf("sql diagnostics recorder closed")
	}
	switch operation {
	case "begin", "commit", "rollback", "checkpoint_lock", "checkpoint_update", "order_select", "order_upsert", "other":
	default:
		operation = "other"
	}

	// Get or create histogram for this operation.
	h, ok := r.histograms[operation]
	if !ok {
		h = &loadmetrics.Histogram{}
		r.histograms[operation] = h
	}

	// Record the duration.
	h.Record(durationNS)

	// Track errors per operation.
	if err != nil {
		r.errors[operation]++
	}

	// Flush every second (1e9 nanoseconds).
	now := time.Now().UnixNano()
	if now-r.lastFlush >= 1e9 {
		if flushErr := r.flushLocked("interval", now); flushErr != nil {
			r.lastError = flushErr
			return flushErr
		}
		r.lastFlush = now

	}

	return nil
}

// flushLocked writes a JSONL record and must be called with mu held.
func (r *Recorder) flushLocked(kind string, nowNS int64) error {
	// Snapshot current histograms.
	opsMetrics := make(map[string]OperationMetrics, len(r.histograms))
	for op, h := range r.histograms {
		summary := h.Snapshot()
		opsMetrics[op] = OperationMetrics{
			Count:   summary.Count,
			Invalid: summary.Invalid,
			Errors:  r.errors[op],
			MinMS:   summary.MinMS,
			MeanMS:  summary.MeanMS,
			MaxMS:   summary.MaxMS,
			P50MS:   summary.P50MS,
			P95MS:   summary.P95MS,
			P99MS:   summary.P99MS,
			P999MS:  summary.P999MS,
		}
	}

	rec := Record{
		TimestampUnixNS: nowNS,
		Kind:            kind,
		Operations:      opsMetrics,
	}

	if err := r.encoder.Encode(rec); err != nil {
		return fmt.Errorf("encode sql diagnostics record: %w", err)
	}
	return nil
}

// Close gracefully shuts down the recorder, flushing a final record and closing the file.
// Errors are captured and returned.
func (r *Recorder) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return r.lastError
	}
	r.closed = true

	// Flush final record.
	if err := r.flushLocked("final", time.Now().UnixNano()); err != nil {
		r.lastError = err
	}

	// Close file.
	if err := r.file.Close(); err != nil {
		if r.lastError == nil {
			r.lastError = err
		}
		return err
	}

	return r.lastError
}

// LastError returns the most recent error that occurred during recording or closing.
// This is used by Store.DiagnosticsError() to expose any diagnostics-related errors.
func (r *Recorder) LastError() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastError
}

// SetLastError allows Store to record initialization errors.
func (r *Recorder) SetLastError(err error) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastError == nil && err != nil {
		r.lastError = err
	}
}
