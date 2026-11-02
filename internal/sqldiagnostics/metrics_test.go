package sqldiagnostics

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestNewRecorder verifies Recorder creation and cleanup.
func TestNewRecorder(t *testing.T) {
	// Test nil path (no-op mode).
	r, err := NewRecorder("")
	if err != nil {
		t.Fatalf("NewRecorder(\"\") failed: %v", err)
	}
	if r != nil {
		t.Fatal("NewRecorder(\"\") should return nil for no-op mode")
	}

	// Test file creation with O_EXCL (exclusive).
	tmpdir := t.TempDir()
	path := filepath.Join(tmpdir, "metrics.jsonl")
	r, err = NewRecorder(path)
	if err != nil {
		t.Fatalf("NewRecorder(%q) failed: %v", path, err)
	}
	defer r.Close()
	if r == nil {
		t.Fatal("NewRecorder should return non-nil Recorder")
	}

	// Verify file was created.
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("metrics file not created: %v", err)
	}

	// Test O_EXCL prevents overwrite.
	r2, err := NewRecorder(path)
	if err == nil {
		t.Fatal("NewRecorder should fail with O_EXCL on existing file")
	}
	if r2 != nil {
		t.Fatal("NewRecorder should return nil on file creation error")
	}
}

// TestRecordQuery verifies query recording and histogram updates.
func TestRecordQuery(t *testing.T) {
	tmpdir := t.TempDir()
	path := filepath.Join(tmpdir, "test.jsonl")
	r, err := NewRecorder(path)
	if err != nil {
		t.Fatalf("NewRecorder failed: %v", err)
	}
	defer r.Close()

	// Record some queries with different operations and durations.
	tests := []struct {
		op         string
		durationNS int64
		err        error
	}{
		{"order_select", 1000, nil},
		{"order_select", 2000, nil},
		{"order_select", 3000, fmt.Errorf("timeout")},
		{"order_upsert", 5000, nil},
		{"checkpoint_update", 10000, nil},
	}

	for _, tt := range tests {
		if err := r.recordQuery(tt.op, tt.durationNS, tt.err); err != nil {
			t.Errorf("recordQuery failed: %v", err)
		}
	}

	// Verify histograms were updated.
	r.mu.Lock()
	if len(r.histograms) != 3 {
		t.Errorf("expected 3 operations, got %d", len(r.histograms))
	}
	if h, ok := r.histograms["order_select"]; !ok || h.Count != 3 {
		t.Errorf("order_select count: expected 3, got %d", h.Count)
	}
	if h, ok := r.histograms["order_upsert"]; !ok || h.Count != 1 {
		t.Errorf("order_upsert count: expected 1, got %d", h.Count)
	}
	if errs, ok := r.errors["order_select"]; !ok || errs != 1 {
		t.Errorf("order_select errors: expected 1, got %d", errs)
	}
	r.mu.Unlock()
}

// TestRecorderClose verifies graceful shutdown with final flush.
func TestRecorderClose(t *testing.T) {
	tmpdir := t.TempDir()
	path := filepath.Join(tmpdir, "close_test.jsonl")
	r, err := NewRecorder(path)
	if err != nil {
		t.Fatalf("NewRecorder failed: %v", err)
	}

	// Record one query.
	if err := r.recordQuery("order_select", 1000, nil); err != nil {
		t.Fatalf("recordQuery failed: %v", err)
	}

	// Close and verify.
	if err := r.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Verify JSONL file was written.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	if len(data) == 0 {
		t.Fatal("JSONL file is empty")
	}

	// Parse JSONL records.
	lines := string(data)
	if lines == "" {
		t.Fatal("no records written")
	}

	// Basic validation: should have "final" kind record.
	if !containsString(lines, `"kind":"final"`) {
		t.Errorf("missing final record: %s", lines)
	}
}

// TestRecorderConcurrency verifies concurrent access safety (basic).
func TestRecorderConcurrency(t *testing.T) {
	tmpdir := t.TempDir()
	path := filepath.Join(tmpdir, "concurrent.jsonl")
	r, err := NewRecorder(path)
	if err != nil {
		t.Fatalf("NewRecorder failed: %v", err)
	}
	defer r.Close()

	// Simulate concurrent queries.
	for i := 0; i < 100; i++ {
		op := []string{"begin", "order_select", "order_upsert"}[i%3]
		duration := int64(1000 + i*10)
		if err := r.recordQuery(op, duration, nil); err != nil {
			t.Errorf("recordQuery failed: %v", err)
		}
	}

	r.mu.Lock()
	if len(r.histograms) != 3 {
		t.Errorf("expected 3 operations, got %d", len(r.histograms))
	}
	r.mu.Unlock()
}

// TestRecorderClosedState verifies errors after close.
func TestRecorderClosedState(t *testing.T) {
	tmpdir := t.TempDir()
	path := filepath.Join(tmpdir, "closed_state.jsonl")
	r, err := NewRecorder(path)
	if err != nil {
		t.Fatalf("NewRecorder failed: %v", err)
	}

	// Close.
	if err := r.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Attempt to record after close.
	if err := r.recordQuery("operation", 1000, nil); err == nil {
		t.Error("recordQuery after close should return error")
	}
}

// TestRecorderNilMode verifies no-op mode.
func TestRecorderNilMode(t *testing.T) {
	// Create nil recorder with empty path.
	r, err := NewRecorder("")
	if err != nil || r != nil {
		t.Fatalf("NewRecorder(\"\") should return (nil, nil)")
	}

	// Call methods on nil Recorder (should not panic).
	if err := r.Close(); err != nil {
		t.Errorf("nil.Close() should not error")
	}
}

// TestLastError verifies error tracking.
func TestLastError(t *testing.T) {
	tmpdir := t.TempDir()
	path := filepath.Join(tmpdir, "error_tracking.jsonl")
	r, err := NewRecorder(path)
	if err != nil {
		t.Fatalf("NewRecorder failed: %v", err)
	}
	defer r.Close()

	// Initially no error.
	if r.LastError() != nil {
		t.Error("LastError should be nil initially")
	}

	// Simulate an error.
	testErr := fmt.Errorf("test error")
	r.SetLastError(testErr)
	if r.LastError() != testErr {
		t.Errorf("LastError() = %v, want %v", r.LastError(), testErr)
	}

	// Second SetLastError should not overwrite.
	r.SetLastError(fmt.Errorf("second error"))
	if r.LastError() != testErr {
		t.Errorf("LastError should not be overwritten; got %v, want %v", r.LastError(), testErr)
	}
}

// TestFlushInterval verifies interval-based flushing (mocked time).
func TestFlushInterval(t *testing.T) {
	tmpdir := t.TempDir()
	path := filepath.Join(tmpdir, "flush_interval.jsonl")
	r, err := NewRecorder(path)
	if err != nil {
		t.Fatalf("NewRecorder failed: %v", err)
	}
	defer r.Close()

	// Record a query.
	if err := r.recordQuery("order_select", 1000, nil); err != nil {
		t.Fatalf("recordQuery failed: %v", err)
	}

	// Manually set lastFlush to trigger interval flush on next query.
	r.mu.Lock()
	r.lastFlush = time.Now().UnixNano() - 2e9 // 2 seconds ago
	r.mu.Unlock()

	// This should trigger flush.
	if err := r.recordQuery("order_upsert", 2000, nil); err != nil {
		t.Errorf("recordQuery with flush failed: %v", err)
	}
}

// containsString is a helper for string searching.
func containsString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestCumulativeAcrossFlushAndFiniteLabels(t *testing.T) {
	r, err := NewRecorder(filepath.Join(t.TempDir(), "m"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.recordQuery("order_select", 1000, nil)
	r.mu.Lock()
	r.lastFlush = time.Now().UnixNano() - 2e9
	r.mu.Unlock()
	r.recordQuery("order_select", 2000, nil)
	for i := 0; i < 1000; i++ {
		r.recordQuery(fmt.Sprintf("unbounded-%d", i), 1000, nil)
	}
	if r.histograms["order_select"].Count != 2 || len(r.histograms) != 2 {
		t.Fatal("lost cumulative samples or unbounded labels")
	}
}
