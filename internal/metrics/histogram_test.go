package metrics

import "testing"

// The bug the first benchmark run surfaced: buckets report an upper bound, so
// with coarse bounds a p99 came back larger than the largest value ever seen.
func TestAQuantileNeverExceedsTheObservedMaximum(t *testing.T) {
	var h Histogram
	for i := 0; i < 1000; i++ {
		h.Record(120_000) // 120ms, inside a bucket whose bound is 130ms
	}
	if p99, max := h.Quantile(0.99), h.Summary().Max; p99 > max {
		t.Fatalf("p99 %v exceeds max %v, which is not a thing that can happen", p99, max)
	}
}

func TestQuantilesTrackTheDistribution(t *testing.T) {
	var h Histogram
	// 2% in the tail, so the 99th percentile has to land inside it. An
	// earlier version of this test put exactly 1% there and then complained
	// that p99 was fast — which it correctly was.
	for i := 0; i < 980; i++ {
		h.Record(1_000) // 1ms
	}
	for i := 0; i < 20; i++ {
		h.Record(300_000) // 300ms, the tail
	}
	if p50 := h.Quantile(0.5); p50 > 2 {
		t.Fatalf("p50 should sit with the bulk, got %v", p50)
	}
	if p99 := h.Quantile(0.99); p99 < 100 {
		t.Fatalf("p99 must see the tail the mean would hide, got %v", p99)
	}
}
