// Command loadgen opens N concurrent subscribers and measures what they
// actually receive.
//
//	go run ./cmd/loadgen -url ws://localhost:8080/ws -clients 5000 -duration 60s
//
// The spec lists four ways this measurement could be dishonest, and three of
// them are guarded here:
//
//   - Every client fully decodes every frame. A subscriber that does not
//     deserialise is not a subscriber, and one that only counts bytes is
//     measuring the kernel rather than the system.
//   - The reported figure is a tail, never a mean. A mean latency is the
//     number that hides the incident.
//   - Coalesced ticks are counted and printed beside the latency rather than
//     folded into it. Frames are what we deliver; ticks are what the market
//     produced, and the gap is a real cost that belongs on the page.
//
// The fourth it cannot guard: run against localhost, this measures fan-out,
// not the network. It says so in its own output rather than leaving the reader
// to assume.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vantionlabs/dispatch/internal/metrics"
)

type frame struct {
	Type   string `json:"t"`
	SentAt int64  `json:"sentAt"`
	Ticks  []struct {
		S string  `json:"s"`
		P float64 `json:"p"`
		E int64   `json:"e"`
		I int64   `json:"i"`
	} `json:"ticks"`
}

func main() {
	url := flag.String("url", "ws://localhost:8080/ws", "dispatch websocket endpoint")
	clients := flag.Int("clients", 1000, "concurrent subscribers")
	duration := flag.Duration("duration", 30*time.Second, "how long to hold them open")
	ramp := flag.Duration("ramp", 5*time.Second, "spread connections over this window")
	warmup := flag.Duration("warmup", 5*time.Second, "discard samples before this elapses")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var (
		owned     metrics.Histogram // our ingest -> client decode. All of ours.
		transport metrics.Histogram // hub flush -> client decode. Socket and decode only.
		endToEnd  metrics.Histogram // exchange event time -> client decode.
		connected atomic.Int64
		failed    atomic.Int64
		frames    atomic.Uint64
		ticks     atomic.Uint64
		measuring atomic.Bool
	)

	fmt.Printf("opening %d subscribers against %s over %s\n", *clients, *url, *ramp)

	var wg sync.WaitGroup
	stagger := time.Duration(0)
	if *clients > 0 {
		stagger = *ramp / time.Duration(*clients)
	}

	deadline := time.After(*warmup + *duration)
	time.AfterFunc(*warmup, func() {
		measuring.Store(true)
		owned.Reset()
		transport.Reset()
		endToEnd.Reset()
		fmt.Printf("warmup over, measuring for %s\n", *duration)
	})

	for i := 0; i < *clients; i++ {
		select {
		case <-ctx.Done():
			break
		case <-time.After(stagger):
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, _, err := websocket.DefaultDialer.DialContext(ctx, *url, nil)
			if err != nil {
				failed.Add(1)
				return
			}
			defer conn.Close()
			connected.Add(1)
			defer connected.Add(-1)

			var payload frame
			for {
				if ctx.Err() != nil {
					return
				}
				_, data, err := conn.ReadMessage()
				if err != nil {
					return
				}
				// Full decode, every frame. The measurement is worthless if
				// the subscriber does less work than a real one.
				if err := json.Unmarshal(data, &payload); err != nil {
					continue
				}
				if payload.Type != "ticks" {
					continue
				}
				now := time.Now().UnixMilli()
				frames.Add(1)
				ticks.Add(uint64(len(payload.Ticks)))

				if !measuring.Load() {
					continue
				}
				transport.Record((now - payload.SentAt) * 1000)
				for _, tick := range payload.Ticks {
					// The number to publish: everything between the tick
					// becoming ours and it being usable on the far side,
					// including the coalescing wait we chose to impose.
					owned.Record((now - tick.I) * 1000)
					endToEnd.Record((now - tick.E) * 1000)
				}
			}
		}()
	}

	select {
	case <-deadline:
	case <-ctx.Done():
	}
	cancel()
	wg.Wait()

	report(reportInput{
		url:       *url,
		clients:   *clients,
		connected: connected.Load(),
		failed:    failed.Load(),
		duration:  *duration,
		frames:    frames.Load(),
		ticks:     ticks.Load(),
		owned:     owned.Summary(),
		transport: transport.Summary(),
		endToEnd:  endToEnd.Summary(),
	})
}

type reportInput struct {
	url       string
	clients   int
	connected int64
	failed    int64
	duration  time.Duration
	frames    uint64
	ticks     uint64
	owned     metrics.Summary
	transport metrics.Summary
	endToEnd  metrics.Summary
}

func report(in reportInput) {
	seconds := in.duration.Seconds()
	fmt.Printf("\n%s\n", divider)
	fmt.Printf("subscribers opened     %d (%d failed)\n", in.clients, in.failed)
	fmt.Printf("frames decoded         %d  (%.0f/s)\n", in.frames, float64(in.frames)/seconds)
	fmt.Printf("ticks decoded          %d  (%.0f/s)\n", in.ticks, float64(in.ticks)/seconds)
	fmt.Printf("\nOURS — tick accepted by ingest to decoded on the client\n")
	fmt.Printf("        includes the coalescing wait, which is a choice, not a cost we hid\n")
	line(in.owned)
	fmt.Printf("\n  of which transport — hub flush to decoded, i.e. socket and JSON only\n")
	line(in.transport)
	fmt.Printf("\nEND TO END — exchange event time to decoded, plus two networks and their clock\n")
	line(in.endToEnd)
	fmt.Printf("\n%s\n", note)
}

func line(s metrics.Summary) {
	fmt.Printf("  p50 %6.2fms   p95 %6.2fms   p99 %6.2fms   max %7.2fms   n=%d\n",
		s.P50, s.P95, s.P99, s.Max, s.Count)
}

const divider = "────────────────────────────────────────────────────────────────────"

const note = `Quantiles are bucket upper bounds, not interpolated: the histogram never had
that precision and claiming it would be inventing accuracy.

End to end starts from the exchange's own clock, which we did not set and
cannot verify. Read it as indicative. Delivery is the number this system is
responsible for.

Run against localhost this is a fan-out measurement, not a network one.`
