// Package metrics records latency as a distribution, because the mean is the
// number that hides the incident.
//
// A fan-out whose mean is 2ms and whose p99 is 400ms visibly stutters for one
// user in a hundred, every second, forever. The mean says it is fine. Only the
// tail describes what anyone actually sees, so the tail is what gets
// published.
package metrics

import (
	"sync"
	"sync/atomic"
)

// boundsUS is fine-grained low down and coarse where we stop caring. Buckets
// rather than keeping every sample: at tens of thousands of ticks a second the
// measurement must not become the load.
var boundsUS = [...]int64{
	50, 100, 200, 400, 800, 1_500, 2_500, 4_000, 6_000, 9_000,
	12_000, 16_000, 20_000, 25_000, 30_000, 40_000, 50_000, 65_000,
	80_000, 100_000, 130_000, 160_000, 200_000, 250_000, 320_000,
	400_000, 500_000, 650_000, 800_000, 1_000_000, 1_600_000,
}

// Histogram is safe for concurrent recording.
type Histogram struct {
	buckets [len(boundsUS) + 1]atomic.Uint64
	count   atomic.Uint64
	sumUS   atomic.Int64
	maxUS   atomic.Int64
}

func (h *Histogram) Record(microseconds int64) {
	if microseconds < 0 {
		microseconds = 0
	}
	h.count.Add(1)
	h.sumUS.Add(microseconds)
	for {
		current := h.maxUS.Load()
		if microseconds <= current || h.maxUS.CompareAndSwap(current, microseconds) {
			break
		}
	}

	index := 0
	for index < len(boundsUS) && microseconds > boundsUS[index] {
		index++
	}
	h.buckets[index].Add(1)
}

// Quantile returns the upper bound of the bucket the quantile falls in, in
// milliseconds.
//
// An upper bound rather than an interpolated value: interpolating inside a
// bucket invents precision the histogram never had, and a latency figure
// claiming more precision than it has is exactly the kind of number this
// project is meant to be better than.
func (h *Histogram) Quantile(q float64) float64 {
	total := h.count.Load()
	if total == 0 {
		return 0
	}
	observedMax := float64(h.maxUS.Load()) / 1000
	target := q * float64(total)
	var seen float64
	for index := range h.buckets {
		seen += float64(h.buckets[index].Load())
		if seen >= target {
			if index >= len(boundsUS) {
				return observedMax
			}
			bound := float64(boundsUS[index]) / 1000
			// A bucket's upper bound can exceed everything actually seen,
			// and reporting a p99 larger than the maximum is nonsense on its
			// face. Clamping keeps the guarantee the bound was making — the
			// true value is no larger than this — while never claiming a
			// latency nobody observed.
			if bound > observedMax {
				return observedMax
			}
			return bound
		}
	}
	return observedMax
}

type Summary struct {
	Count uint64  `json:"count"`
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
	Max   float64 `json:"max"`
	Mean  float64 `json:"mean"`
}

func (h *Histogram) Summary() Summary {
	count := h.count.Load()
	mean := 0.0
	if count > 0 {
		mean = float64(h.sumUS.Load()) / float64(count) / 1000
	}
	return Summary{
		Count: count,
		P50:   h.Quantile(0.50),
		P95:   h.Quantile(0.95),
		P99:   h.Quantile(0.99),
		Max:   float64(h.maxUS.Load()) / 1000,
		Mean:  round(mean),
	}
}

func (h *Histogram) Reset() {
	for index := range h.buckets {
		h.buckets[index].Store(0)
	}
	h.count.Store(0)
	h.sumUS.Store(0)
	h.maxUS.Store(0)
}

func round(value float64) float64 {
	return float64(int64(value*1000+0.5)) / 1000
}

// Mutex-guarded wrapper for callers that want to reset and read atomically.
type Locked struct {
	mu sync.Mutex
	H  Histogram
}

func (l *Locked) SummaryAndReset() Summary {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.H.Summary()
	l.H.Reset()
	return s
}
