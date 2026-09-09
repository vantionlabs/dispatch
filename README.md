# Dispatch

Live market data to many browsers at once, in Go, with an honest number for
how long it takes.

```
go run ./cmd/dispatch                       # :8080, dashboard at /
go run ./cmd/loadgen -clients 1000          # the benchmark
```

## The number

Measured on one Apple M-series laptop, against the live Binance trade feed,
with the load generator running **on the same machine**:

| subscribers | p99 (ours) | max | tick-deliveries/s | failures |
|---|---|---|---|---|
| 500 | 40 ms | 46 ms | 138,000 | 0 |
| 1,000 | 40 ms | 69 ms | 134,000 | 0 |
| 2,000 | 65 ms | 120 ms | 290,000 | 0 |

**p99 of 40 ms at 1,000 concurrent subscribers, 25 ms of which is the flush
interval we chose.** The system's own processing tail is the remaining ~15 ms.

The degradation at 2,000 is the load generator competing with the server for
cores, not the server: the tail tightens as the client count drops while
throughput keeps climbing. Measuring across a real network is D4, and until
that is done this is a fan-out number and is labelled as one.

## Three clocks, never conflated

Most published latency figures quietly include the public internet. Ours does
not, because we did not build the public internet.

| Number | From | To | Whose fault |
|---|---|---|---|
| **Ours** | tick accepted by ingest | decoded on the client | ours, all of it |
| *of which transport* | hub flush | decoded on the client | socket and JSON only |
| **End to end** | the exchange's own timestamp | decoded on the client | ours, plus two networks and a clock we did not set |

End to end runs around 160–250 ms and is dominated by the hop from Binance.
It is reported because hiding it would be dishonest, and captioned because
quoting it as ours would be worse.

## Coalescing, not queuing

The decision the system turns on. A slow subscriber does not get a backlog;
they get the latest price per symbol and nothing else.

A queued tick is a price that was true thirty seconds ago. Putting it on a
screen is worse than putting nothing there, because the viewer cannot tell it
is stale and will act on it. So load turns into a lower update rate rather
than lag, memory is bounded by the symbol count rather than the tick rate, and
ticks are dropped **on purpose** — which makes the drop rate a real number
that gets published beside the latency instead of folded into it.

Measured against the live feed: **578 ticks/second in, about 47 price updates
per second out to each subscriber, 92% coalesced away.** Binance delivers
trades in bursts — thirty for BTC arrive together, then a quiet window — so
the browser gets one current price per burst instead of thirty stale ones.

## Two things the benchmark found

**The fan-out loop was serial.** Writing to every subscriber from one
goroutine meant 80,000 syscalls a second through a single thread at 2,000
connections, and the last subscriber received its frame 25 ms after the first.
Sharding the writes across workers took throughput from 36k to 290k
tick-deliveries per second. That is only safe because every frame is encoded
once and shared read-only, which is the second thing the shared-buffer design
bought.

**A p99 above the maximum.** The first run reported an end-to-end p99 of 200 ms
and a max of 167 ms, which is impossible. The histogram reports bucket upper
bounds rather than interpolating — deliberately, because interpolating invents
precision the histogram never had — and the bounds jumped 100 ms to 200 ms, so
everything landed on one edge. Fixed with finer buckets and by clamping a
quantile to the largest value actually observed.

## Layout

```
cmd/dispatch     the server
cmd/loadgen      the benchmark harness
internal/feed    Tick, the Feed interface, and the Binance adapter
internal/hub     coalescing fan-out, the part that matters
internal/metrics latency as a distribution, because the mean hides the incident
web/             the dashboard, which measures latency in the browser itself
```

The feed is an interface. Binance is the demo source because it is free and
genuinely fast, which makes the number earned rather than simulated; energy
prices, transit positions and odds are the same system with a slower clock and
a different adapter.

## What would make this dishonest

Kept in `dispatch-spec.md` §5 so it stays checkable. Three of the four are
guarded in code — every benchmark client fully decodes every frame, the report
is a tail rather than a mean, and coalesced ticks are counted and printed
beside the latency. The fourth is the same-machine caveat above, and it is
stated everywhere the number appears rather than left for the reader to
notice.
