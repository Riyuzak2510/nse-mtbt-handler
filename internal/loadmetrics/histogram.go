// Package loadmetrics supplies fixed-memory, opt-in benchmark histograms.
package loadmetrics

import "math"

// Histogram uses 32 equal subdivisions per power of two, in integer nanoseconds.
// Percentiles are bucket upper bounds (at most ~3.125% relative quantization
// above 32ns), clamped to the observed maximum. No sample slice is retained.
type Histogram struct {
	Buckets  [2049]uint64 `json:"-"`
	Count    uint64       `json:"count"`
	Invalid  uint64       `json:"invalid"`
	Sum      float64      `json:"-"`
	Min, Max int64        `json:"-"`
}

type Summary struct {
	Count   uint64  `json:"count"`
	Invalid uint64  `json:"invalid"`
	MinMS   float64 `json:"min_ms"`
	MeanMS  float64 `json:"mean_ms"`
	MaxMS   float64 `json:"max_ms"`
	P50MS   float64 `json:"p50_ms"`
	P95MS   float64 `json:"p95_ms"`
	P99MS   float64 `json:"p99_ms"`
	P999MS  float64 `json:"p999_ms"`
}

func (h *Histogram) Record(ns int64) {
	if ns < 0 {
		h.Invalid++
		return
	}
	h.Count++
	h.Sum += float64(ns)
	if h.Count == 1 || ns < h.Min {
		h.Min = ns
	}
	if ns > h.Max {
		h.Max = ns
	}
	if ns == 0 {
		h.Buckets[0]++
		return
	}
	exp := 0
	for v := uint64(ns); v > 1; v >>= 1 {
		exp++
	}
	base := uint64(1) << exp
	var sub int
	if exp >= 5 {
		sub = int((uint64(ns) - base) >> (exp - 5))
	} else {
		sub = int((uint64(ns) - base) * 32 / base)
	}
	h.Buckets[1+exp*32+sub]++
}
func (h *Histogram) percentile(q float64) float64 {
	if h.Count == 0 {
		return 0
	}
	target := uint64(math.Ceil(float64(h.Count) * q))
	var seen uint64
	for i, n := range h.Buckets {
		seen += n
		if seen < target {
			continue
		}
		if i == 0 {
			return 0
		}
		exp, sub := (i-1)/32, (i-1)%32
		upper := math.Ldexp(1, exp) * (1 + float64(sub+1)/32)
		if upper > float64(h.Max) {
			upper = float64(h.Max)
		}
		return upper / 1e6
	}
	return float64(h.Max) / 1e6
}
func (h *Histogram) Snapshot() Summary {
	s := Summary{Count: h.Count, Invalid: h.Invalid}
	if h.Count == 0 {
		return s
	}
	s.MinMS = float64(h.Min) / 1e6
	s.MaxMS = float64(h.Max) / 1e6
	s.MeanMS = h.Sum / float64(h.Count) / 1e6
	s.P50MS = h.percentile(.5)
	s.P95MS = h.percentile(.95)
	s.P99MS = h.percentile(.99)
	s.P999MS = h.percentile(.999)
	return s
}
