package hub

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/vantionlabs/dispatch/internal/feed"
)

type recorder struct {
	mu     sync.Mutex
	frames [][]byte
	fail   bool
}

func (r *recorder) Write(payload []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail {
		return errRefused
	}
	r.frames = append(r.frames, append([]byte(nil), payload...))
	return nil
}

func (r *recorder) Close() error { return nil }

func (r *recorder) decoded() []tickFrame {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]tickFrame, 0, len(r.frames))
	for _, raw := range r.frames {
		var frame tickFrame
		if err := json.Unmarshal(raw, &frame); err == nil && frame.Type == "ticks" {
			out = append(out, frame)
		}
	}
	return out
}

type refusedError struct{}

func (refusedError) Error() string { return "refused" }

var errRefused = refusedError{}

func tick(symbol string, price float64, ingest int64) feed.Tick {
	return feed.Tick{Symbol: symbol, Price: price, ExchangeTS: ingest, IngestTS: ingest}
}

// The claim the whole design rests on: a subscriber behind the feed gets the
// current price, not a queue of old ones.
func TestCoalescingKeepsOnlyTheLatestPrice(t *testing.T) {
	h := New(Options{FlushInterval: time.Hour}) // flush only when we say so
	sink := &recorder{}
	h.Subscribe(sink, nil)

	now := time.Now().UnixMilli()
	for i := 1; i <= 50; i++ {
		h.OnTick(tick("BTC-USDT", float64(i), now))
	}
	h.Flush()

	frames := sink.decoded()
	if len(frames) != 1 {
		t.Fatalf("want one frame, got %d", len(frames))
	}
	if got := len(frames[0].Ticks); got != 1 {
		t.Fatalf("fifty ticks for one symbol must collapse to one, got %d", got)
	}
	if got := frames[0].Ticks[0].P; got != 50 {
		t.Fatalf("want the newest price 50, got %v", got)
	}
	if coalesced := h.Stats().Coalesced; coalesced != 49 {
		t.Fatalf("the 49 replaced ticks must be counted, got %d", coalesced)
	}
}

// Dropping is the design; hiding it is not. Whatever else changes, ticks in
// must equal ticks delivered plus ticks coalesced away.
func TestEveryTickIsEitherDeliveredOrCounted(t *testing.T) {
	h := New(Options{FlushInterval: time.Hour})
	sink := &recorder{}
	h.Subscribe(sink, nil)

	now := time.Now().UnixMilli()
	symbols := []string{"BTC-USDT", "ETH-USDT", "SOL-USDT"}
	for round := 0; round < 4; round++ {
		for i, symbol := range symbols {
			h.OnTick(tick(symbol, float64(round*10+i), now))
			h.OnTick(tick(symbol, float64(round*10+i)+0.5, now))
		}
		h.Flush()
	}

	stats := h.Stats()
	delivered := uint64(0)
	for _, frame := range sink.decoded() {
		delivered += uint64(len(frame.Ticks))
	}
	if delivered+stats.Coalesced != stats.TicksIn {
		t.Fatalf("delivered %d + coalesced %d != ticks in %d",
			delivered, stats.Coalesced, stats.TicksIn)
	}
}

// A dirty set held while nobody is watching would burst stale symbols at
// whoever connects next, which is the queuing behaviour this design rejects.
func TestNothingIsHeldWhileNobodyIsSubscribed(t *testing.T) {
	h := New(Options{FlushInterval: time.Hour})
	now := time.Now().UnixMilli()
	for i := 0; i < 10; i++ {
		h.OnTick(tick("BTC-USDT", float64(i), now))
	}
	h.Flush() // no subscribers

	sink := &recorder{}
	h.Subscribe(sink, nil)
	h.Flush()

	if frames := sink.decoded(); len(frames) != 0 {
		t.Fatalf("a new subscriber must not receive a backlog, got %d frames", len(frames))
	}
}

// A subscriber arriving mid-session needs a correct screen immediately, not a
// blank one until every symbol happens to trade.
func TestSnapshotCarriesEverySymbolSeen(t *testing.T) {
	h := New(Options{FlushInterval: time.Hour})
	now := time.Now().UnixMilli()
	h.OnTick(tick("BTC-USDT", 70000, now))
	h.OnTick(tick("ETH-USDT", 2500, now))

	var frame tickFrame
	if err := json.Unmarshal(h.Snapshot(nil), &frame); err != nil {
		t.Fatal(err)
	}
	if frame.Type != "snapshot" || len(frame.Ticks) != 2 {
		t.Fatalf("want a two-symbol snapshot, got %s with %d", frame.Type, len(frame.Ticks))
	}
}

// A subscriber asking for two symbols must not be sent the other fifty.
func TestFilteredSubscribersGetOnlyWhatTheyAskedFor(t *testing.T) {
	h := New(Options{FlushInterval: time.Hour})
	sink := &recorder{}
	h.Subscribe(sink, []string{"BTC-USDT"})

	now := time.Now().UnixMilli()
	h.OnTick(tick("BTC-USDT", 70000, now))
	h.OnTick(tick("ETH-USDT", 2500, now))
	h.Flush()

	frames := sink.decoded()
	if len(frames) != 1 || len(frames[0].Ticks) != 1 || frames[0].Ticks[0].S != "BTC-USDT" {
		t.Fatalf("filtered subscriber got the wrong payload: %+v", frames)
	}
}

// One bad connection must not become an outage: a refusing sink is dropped
// rather than retried into the flush loop.
func TestARefusingSubscriberIsDroppedNotRetried(t *testing.T) {
	h := New(Options{FlushInterval: time.Hour})
	bad := &recorder{fail: true}
	good := &recorder{}
	h.Subscribe(bad, nil)
	h.Subscribe(good, nil)

	now := time.Now().UnixMilli()
	h.OnTick(tick("BTC-USDT", 70000, now))
	h.Flush()

	if got := h.Stats().Subscribers; got != 1 {
		t.Fatalf("the refusing subscriber should be gone, %d remain", got)
	}
	if len(good.decoded()) != 1 {
		t.Fatal("the healthy subscriber must still have been served")
	}
}
