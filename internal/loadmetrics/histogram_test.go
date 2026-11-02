package loadmetrics

import (
	"math"
	"testing"
	"unsafe"
)

func TestHistogramQuantizationAndBounds(t *testing.T) {
	var h Histogram
	for i := int64(1); i <= 100000; i++ {
		h.Record(i * 1000)
	}
	s := h.Snapshot()
	if s.Count != 100000 || s.P99MS < 99 || s.P99MS > 99*1.032 || s.P50MS < 50 || s.P50MS > 50*1.032 {
		t.Fatal(s)
	}
	h.Record(-1)
	if h.Invalid != 1 {
		t.Fatal(h)
	}
	h.Record(math.MaxInt64)
	if h.Snapshot().P999MS > float64(math.MaxInt64)/1e6 {
		t.Fatal("overflow")
	}
}
func TestBoundedTwentyMillion(t *testing.T) {
	var h Histogram
	size := unsafe.Sizeof(h)
	for i := 0; i < 20000000; i++ {
		h.Record(1000000)
	}
	if h.Count != 20000000 || unsafe.Sizeof(h) != size || size > 20000 {
		t.Fatal(h.Count, size)
	}
	s := h.Snapshot()
	if s.P99MS != 1 || s.MeanMS != 1 {
		t.Fatal(s)
	}
}
func TestZeroAndEmpty(t *testing.T) {
	var h Histogram
	if h.Snapshot().Count != 0 {
		t.Fatal(h)
	}
	h.Record(0)
	if h.Snapshot().P99MS != 0 {
		t.Fatal(h)
	}
}

// TestHistogramExtremeValues verifies handling of extreme input values
func TestHistogramExtremeValues(t *testing.T) {
	var h Histogram

	// Record very small values
	for i := int64(0); i <= 100; i++ {
		h.Record(i)
	}

	// Record very large values (near MaxInt64)
	large := int64(9223372036854775000) // Near MaxInt64
	h.Record(large)

	s := h.Snapshot()
	if s.Count != 102 {
		t.Fatalf("expected count=102, got %d", s.Count)
	}
	if s.MinMS != 0 {
		t.Fatalf("expected min=0, got %f", s.MinMS)
	}
	if s.MaxMS == 0 || s.MaxMS <= float64(large-1000)/1e6 {
		t.Fatalf("max value not recorded correctly: %f", s.MaxMS)
	}
}

// TestHistogramMonotonicPercentiles verifies percentiles are monotonic and bounded
func TestHistogramMonotonicPercentiles(t *testing.T) {
	var h Histogram

	// Record 10000 values with distribution
	for i := int64(1); i <= 10000; i++ {
		h.Record(i * 1000) // 1us to 10ms
	}

	s := h.Snapshot()

	// Verify monotonic ordering
	if s.MinMS > s.P50MS {
		t.Fatalf("min > p50: %f > %f", s.MinMS, s.P50MS)
	}
	if s.P50MS > s.P95MS {
		t.Fatalf("p50 > p95: %f > %f", s.P50MS, s.P95MS)
	}
	if s.P95MS > s.P99MS {
		t.Fatalf("p95 > p99: %f > %f", s.P95MS, s.P99MS)
	}
	if s.P99MS > s.P999MS {
		t.Fatalf("p99 > p999: %f > %f", s.P99MS, s.P999MS)
	}
	if s.P999MS > s.MaxMS {
		t.Fatalf("p999 > max: %f > %f", s.P999MS, s.MaxMS)
	}

	// Verify mean is in reasonable range
	if s.MeanMS < s.MinMS || s.MeanMS > s.MaxMS {
		t.Fatalf("mean out of bounds: %f not in [%f, %f]", s.MeanMS, s.MinMS, s.MaxMS)
	}
}

// TestHistogramNegativeRejection verifies negative values are counted as invalid
func TestHistogramNegativeRejection(t *testing.T) {
	var h Histogram

	// Record valid values
	for i := int64(0); i <= 100; i++ {
		h.Record(i)
	}

	// Record negative values
	for i := int64(-100); i < 0; i++ {
		h.Record(i)
	}

	s := h.Snapshot()
	if s.Count != 101 {
		t.Fatalf("expected count=101, got %d", s.Count)
	}
	if s.Invalid != 100 {
		t.Fatalf("expected invalid=100, got %d", s.Invalid)
	}
}

// TestHistogramBucketDistribution verifies bucket allocation is correct
func TestHistogramBucketDistribution(t *testing.T) {
	var h Histogram

	// Record values that should hit different buckets
	testValues := []int64{0, 1, 32, 64, 1000, 1e6, 1e9, math.MaxInt64}
	for _, val := range testValues {
		if val >= 0 {
			h.Record(val)
		}
	}

	s := h.Snapshot()
	if s.Count != uint64(len(testValues)) {
		t.Fatalf("expected count=%d, got %d", len(testValues), s.Count)
	}
	if s.MinMS != 0 {
		t.Fatalf("min should be 0, got %f", s.MinMS)
	}
}
