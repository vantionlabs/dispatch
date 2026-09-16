// Package hub takes every tick in and puts one frame per subscriber out.
//
// # Coalescing, not queuing
//
// The decision the whole system turns on. When a subscriber is slower than the
// feed — a laptop that went to sleep, a phone on a train — the obvious move is
// to buffer their ticks and drain when they come back. That is wrong here. A
// queued tick is a price that was true thirty seconds ago, and putting it on a
// screen is worse than putting nothing there, because the user cannot tell it
// is stale and will act on it.
//
// So the hub keeps the latest tick per symbol and a set of symbols that have
// moved since the last flush. Everything follows from that:
//
//   - Load turns into a lower update rate, never into lag. Someone watching a
//     number wants the current one, not all of them.
//   - Memory is bounded by the symbol count, not the tick rate, so neither a
//     slow client nor a market surge can grow it.
//   - Ticks are dropped deliberately, which makes the drop rate a real number
//     that gets published next to the latency instead of folded into it.
//
// # One encoding, many sockets
//
// Because the dirty set is held once at the hub rather than per subscriber,
// every subscriber taking the full stream receives byte-identical frames. So
// the frame is encoded once per flush and the same buffer is written to all of
// them. That is worth more than any amount of tuning underneath: serialisation
// was the dominant cost, and it now happens once instead of N times.
//
// # Flushing on a timer
//
// Writing per tick means a syscall per tick per subscriber, which at a few
// thousand ticks a second across a few thousand subscribers is the entire CPU.
// A single ticker drains everyone instead. The flush interval is therefore the
// floor on our own latency, and it is published as part of the result rather
// than quietly tuned until the number looks good.
package hub

import (
	"encoding/json"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vantionlabs/dispatch/internal/feed"
	"github.com/vantionlabs/dispatch/internal/metrics"
)

// Sink is one connected client. Write must not block indefinitely; returning
// an error drops the subscriber.
type Sink interface {
	Write(payload []byte) error
	Close() error
}

type Subscriber struct {
	ID      uint64
	sink    Sink
	symbols map[string]struct{} // empty means everything
	dropped atomic.Uint64
}

// Filtered reports whether this subscriber asked for a subset.
func (s *Subscriber) Filtered() bool { return len(s.symbols) > 0 }

type Options struct {
	// FlushInterval is the floor on our own latency. 25ms by default: fast
	// enough that a price never looks stuck, slow enough that a surge
	// coalesces instead of melting the CPU.
	FlushInterval time.Duration
}

type Hub struct {
	mu     sync.RWMutex
	latest map[string]feed.Tick
	dirty  map[string]struct{}
	status feed.Status

	subMu sync.RWMutex
	subs  map[uint64]*Subscriber

	nextID          atomic.Uint64
	ticksIn         atomic.Uint64
	coalesced       atomic.Uint64
	flushes         atomic.Uint64
	flushesWithData atomic.Uint64
	symbolsFlushed  atomic.Uint64
	framesOut       atomic.Uint64
	dropped         atomic.Uint64
	encodes         atomic.Uint64

	flushInterval time.Duration
	fanout        metrics.Histogram

	stop chan struct{}
	once sync.Once
}

func New(opts Options) *Hub {
	interval := opts.FlushInterval
	if interval <= 0 {
		interval = 25 * time.Millisecond
	}
	return &Hub{
		latest:        make(map[string]feed.Tick),
		dirty:         make(map[string]struct{}),
		subs:          make(map[uint64]*Subscriber),
		flushInterval: interval,
		stop:          make(chan struct{}),
	}
}

func (h *Hub) FlushInterval() time.Duration { return h.flushInterval }

// OnTick implements feed.Handler. Deliberately cheap: it runs on the feed's
// goroutine, and anything slow here becomes backpressure on the exchange
// connection itself.
func (h *Hub) OnTick(tick feed.Tick) {
	h.ticksIn.Add(1)
	h.mu.Lock()
	h.latest[tick.Symbol] = tick
	// Already dirty means the previous tick for this symbol will never be
	// sent as its own update: a newer price replaced it before the flush.
	// Counted here, where it actually happens, rather than inferred later
	// from the gap between ticks in and ticks out — that inference also
	// swallows every tick that arrived while nobody was subscribed, which
	// is a different thing and would quietly inflate the number.
	if _, already := h.dirty[tick.Symbol]; already {
		h.coalesced.Add(1)
	}
	h.dirty[tick.Symbol] = struct{}{}
	h.mu.Unlock()
}

func (h *Hub) OnStatus(status feed.Status) {
	h.mu.Lock()
	h.status = status
	h.mu.Unlock()
	h.broadcast(mustEncode(statusFrame{Type: "status", Connected: status.Connected,
		Detail: status.Detail, Reconnects: status.Reconnects}))
}

func (h *Hub) Subscribe(sink Sink, symbols []string) *Subscriber {
	set := make(map[string]struct{}, len(symbols))
	for _, s := range symbols {
		set[feed.Normalise(s)] = struct{}{}
	}
	sub := &Subscriber{ID: h.nextID.Add(1), sink: sink, symbols: set}

	h.subMu.Lock()
	h.subs[sub.ID] = sub
	h.subMu.Unlock()
	return sub
}

func (h *Hub) Unsubscribe(id uint64) {
	h.subMu.Lock()
	delete(h.subs, id)
	h.subMu.Unlock()
}

// Snapshot is what a subscriber gets on connect, so a screen is correct
// immediately rather than blank until every symbol happens to trade.
func (h *Hub) Snapshot(symbols map[string]struct{}) []byte {
	h.mu.RLock()
	defer h.mu.RUnlock()

	ticks := make([]wireTick, 0, len(h.latest))
	for symbol, tick := range h.latest {
		if len(symbols) > 0 {
			if _, ok := symbols[symbol]; !ok {
				continue
			}
		}
		ticks = append(ticks, toWire(tick))
	}
	return mustEncode(tickFrame{Type: "snapshot", SentAt: time.Now().UnixMilli(), Ticks: ticks})
}

func (h *Hub) Run() {
	ticker := time.NewTicker(h.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-h.stop:
			return
		case <-ticker.C:
			h.Flush()
		}
	}
}

func (h *Hub) Stop() {
	h.once.Do(func() { close(h.stop) })
}

// Flush writes one frame per subscriber to everyone holding anything.
//
// The fan-out clock stops here, per tick in the frame, measured from the
// instant ingest accepted it. That deliberately includes the time the tick sat
// waiting for this tick of the timer: a tick that waited 24ms was 24ms late to
// the socket, whatever the code did in between, and excluding it would be
// measuring the part we like rather than the part that happened.
func (h *Hub) Flush() {
	h.flushes.Add(1)
	h.subMu.RLock()
	count := len(h.subs)
	h.subMu.RUnlock()

	h.mu.Lock()
	if len(h.dirty) == 0 || count == 0 {
		// Nobody listening, or nothing moved. Clear regardless: holding a
		// dirty set while unwatched would deliver a burst of stale symbols
		// to whoever connects next, which is the queuing behaviour this
		// design exists to avoid.
		clear(h.dirty)
		h.mu.Unlock()
		return
	}
	moved := make([]feed.Tick, 0, len(h.dirty))
	for symbol := range h.dirty {
		if tick, ok := h.latest[symbol]; ok {
			moved = append(moved, tick)
		}
	}
	clear(h.dirty)
	h.mu.Unlock()

	h.flushesWithData.Add(1)
	h.symbolsFlushed.Add(uint64(len(moved)))

	now := time.Now().UnixMilli()

	// Encoded once. Every unfiltered subscriber gets these same bytes.
	full := encodeTicks(moved, now)
	h.encodes.Add(1)

	// Filtered subscribers are rarer and each needs its own frame; cache by
	// the set they asked for so two identical filters still encode once.
	filtered := make(map[string][]byte)

	h.subMu.RLock()
	subs := make([]*Subscriber, 0, len(h.subs))
	for _, sub := range h.subs {
		subs = append(subs, sub)
	}
	h.subMu.RUnlock()

	// Filtered frames are built up front, single threaded, because the cache
	// they share is not worth a lock. Filtered subscribers are the rare case.
	payloads := make([][]byte, len(subs))
	for index, sub := range subs {
		if !sub.Filtered() {
			payloads[index] = full
			continue
		}
		key := filterKey(sub.symbols)
		cached, ok := filtered[key]
		if !ok {
			subset := make([]feed.Tick, 0, len(moved))
			for _, tick := range moved {
				if _, want := sub.symbols[tick.Symbol]; want {
					subset = append(subset, tick)
				}
			}
			if len(subset) > 0 {
				cached = encodeTicks(subset, now)
				h.encodes.Add(1)
			}
			filtered[key] = cached
		}
		payloads[index] = cached
	}

	// Writing is a syscall per subscriber, and doing them in one loop makes
	// the fan-out serial: at forty flushes a second across two thousand
	// sockets that is eighty thousand syscalls through a single goroutine,
	// and the measured cost was the last subscriber receiving a frame 25ms
	// after the first. Sharding across workers is the fix, and it is safe
	// only because every payload here is already encoded and read-only —
	// which is the second thing the shared-buffer design bought.
	var (
		staleMu sync.Mutex
		stale   []uint64
		wg      sync.WaitGroup
	)
	workers := runtime.NumCPU()
	if workers > len(subs) {
		workers = len(subs)
	}
	if workers < 1 {
		workers = 1
	}

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			for index := start; index < len(subs); index += workers {
				payload := payloads[index]
				if payload == nil {
					continue
				}
				sub := subs[index]
				if err := sub.sink.Write(payload); err != nil {
					// The transport refused or the client vanished. Distinct
					// from coalescing: the socket pushing back, not us
					// choosing.
					sub.dropped.Add(1)
					h.dropped.Add(1)
					staleMu.Lock()
					stale = append(stale, sub.ID)
					staleMu.Unlock()
					continue
				}
				h.framesOut.Add(1)
			}
		}(worker)
	}
	wg.Wait()

	for _, tick := range moved {
		h.fanout.Record((now - tick.IngestTS) * 1000)
	}

	for _, id := range stale {
		h.subMu.Lock()
		if sub, ok := h.subs[id]; ok {
			delete(h.subs, id)
			sub.sink.Close()
		}
		h.subMu.Unlock()
	}
}

func (h *Hub) broadcast(payload []byte) {
	h.subMu.RLock()
	defer h.subMu.RUnlock()
	for _, sub := range h.subs {
		_ = sub.sink.Write(payload)
	}
}

type Stats struct {
	Subscribers     int             `json:"subscribers"`
	Symbols         int             `json:"symbols"`
	TicksIn         uint64          `json:"ticks_in"`
	FramesOut       uint64          `json:"frames_out"`
	Coalesced       uint64          `json:"coalesced"`
	Dropped         uint64          `json:"dropped"`
	Encodes         uint64          `json:"encodes"`
	Flushes         uint64          `json:"flushes"`
	FlushesWithData uint64          `json:"flushes_with_data"`
	SymbolsFlushed  uint64          `json:"symbols_flushed"`
	FlushMS         int64           `json:"flush_ms"`
	Connected       bool            `json:"connected"`
	Fanout          metrics.Summary `json:"fanout_ms"`
}

func (h *Hub) Stats() Stats {
	h.subMu.RLock()
	subscribers := len(h.subs)
	h.subMu.RUnlock()

	h.mu.RLock()
	symbols := len(h.latest)
	connected := h.status.Connected
	h.mu.RUnlock()

	fanout := h.fanout.Summary()

	return Stats{
		Subscribers:     subscribers,
		Symbols:         symbols,
		TicksIn:         h.ticksIn.Load(),
		FramesOut:       h.framesOut.Load(),
		Coalesced:       h.coalesced.Load(),
		Dropped:         h.dropped.Load(),
		Encodes:         h.encodes.Load(),
		Flushes:         h.flushes.Load(),
		FlushesWithData: h.flushesWithData.Load(),
		SymbolsFlushed:  h.symbolsFlushed.Load(),
		FlushMS:         h.flushInterval.Milliseconds(),
		Connected:       connected,
		Fanout:          fanout,
	}
}

// --- wire format ---------------------------------------------------------

type wireTick struct {
	S string  `json:"s"`
	P float64 `json:"p"`
	Q float64 `json:"q"`
	E int64   `json:"e"` // exchange event time
	I int64   `json:"i"` // our ingest time
}

type tickFrame struct {
	Type   string     `json:"t"`
	SentAt int64      `json:"sentAt"`
	Ticks  []wireTick `json:"ticks"`
}

type statusFrame struct {
	Type       string `json:"t"`
	Connected  bool   `json:"connected"`
	Detail     string `json:"detail"`
	Reconnects int    `json:"reconnects"`
}

func toWire(tick feed.Tick) wireTick {
	return wireTick{S: tick.Symbol, P: tick.Price, Q: tick.Size, E: tick.ExchangeTS, I: tick.IngestTS}
}

func encodeTicks(ticks []feed.Tick, now int64) []byte {
	wire := make([]wireTick, len(ticks))
	for i, tick := range ticks {
		wire[i] = toWire(tick)
	}
	return mustEncode(tickFrame{Type: "ticks", SentAt: now, Ticks: wire})
}

func mustEncode(v any) []byte {
	payload, err := json.Marshal(v)
	if err != nil {
		return []byte(`{"t":"error"}`)
	}
	return payload
}

func filterKey(symbols map[string]struct{}) string {
	keys := make([]string, 0, len(symbols))
	for symbol := range symbols {
		keys = append(keys, symbol)
	}
	// Small sets; insertion sort keeps the key stable without importing sort
	// machinery into the hot path.
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	key := ""
	for _, k := range keys {
		key += k + ","
	}
	return key
}
