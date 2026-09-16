<p align="center">
  <a href="https://vantion.co">
    <img src="https://raw.githubusercontent.com/vantionlabs/.github/main/profile/banner.png" alt="Vantion Labs" width="100%" />
  </a>
</p>

<h1 align="center">Dispatch</h1>

<p align="center">
  <b>Live market data to thousands of browsers at once, in Go.</b><br />
  With a latency number that survives being asked how it was measured.
</p>

<p align="center">
  <a href="https://github.com/vantionlabs/dispatch/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/vantionlabs/dispatch/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://go.dev"><img alt="Go 1.25" src="https://img.shields.io/badge/go-1.25-00ADD8?style=flat-square&logo=go&logoColor=white" /></a>
  <a href="LICENSE"><img alt="MIT" src="https://img.shields.io/badge/licence-MIT-f4f4f6?style=flat-square" /></a>
  <a href="https://vantion.co"><img alt="Vantion Labs" src="https://img.shields.io/badge/by-Vantion_Labs-2233f0?style=flat-square" /></a>
</p>

---

A reference build from [Vantion Labs](https://vantion.co). Not every problem is
an LLM problem: before a CTO believes the interesting half, the boring half has
to hold up. This is the boring half, measured properly.

```bash
go run ./cmd/dispatch                       # :8080, dashboard at /
go run ./cmd/loadgen -clients 1000          # the benchmark
go test -race ./...                         # the hub and the histogram
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

## What you get

- **Coalescing fan-out, not queuing.** A slow subscriber gets the latest price
  per symbol and nothing else. Load turns into a lower update rate rather than
  lag, and memory is bounded by the symbol count rather than the tick rate.
- **Three clocks, never conflated.** Ours, the transport half of ours, and end
  to end including two networks we do not own. Each is reported with what it
  includes.
- **A drop rate published beside the latency.** Ticks are dropped on purpose,
  so the number is real instead of folded into an average.
- **Sharded writes.** Every frame is encoded once and shared read-only, so
  fan-out scales across workers instead of through one goroutine.
- **Latency as a distribution.** A histogram with bucket upper bounds, never an
  interpolated quantile, because the mean hides the incident.
- **A feed interface with one real implementation.** Binance is the demo source
  because it is free and genuinely fast, which makes the number earned rather
  than simulated.
- **A dashboard that measures in the browser**, because that is where the
  viewer's latency actually is.

## Coalescing, not queuing

The decision the system turns on.

A queued tick is a price that was true thirty seconds ago. Putting it on a
screen is worse than putting nothing there, because the viewer cannot tell it
is stale and will act on it. So ticks are dropped **on purpose**, and the drop
rate gets published beside the latency.

Measured against the live feed: **578 ticks/second in, about 47 price updates
per second out to each subscriber, 92% coalesced away.** Binance delivers
trades in bursts, so the browser gets one current price per burst instead of
thirty stale ones.

## Two things the benchmark found

**The fan-out loop was serial.** Writing to every subscriber from one goroutine
meant 80,000 syscalls a second through a single thread at 2,000 connections,
and the last subscriber received its frame 25 ms after the first. Sharding the
writes took throughput from 36k to 290k tick-deliveries per second.

**A p99 above the maximum.** The first run reported an end-to-end p99 of 200 ms
and a max of 167 ms, which is impossible. The histogram reports bucket upper
bounds rather than interpolating, and the bounds jumped 100 ms to 200 ms, so
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

The feed is an interface. Energy prices, transit positions and odds are the
same system with a slower clock and a different adapter.

## What would make this dishonest

Kept in [`dispatch-spec.md`](dispatch-spec.md) §5 so it stays checkable. Three
of the four are guarded in code: every benchmark client fully decodes every
frame, the report is a tail rather than a mean, and coalesced ticks are counted
and printed beside the latency. The fourth is the same-machine caveat above,
and it is stated everywhere the number appears rather than left for the reader
to notice.

## Licence

MIT. See [LICENSE](LICENSE). Built by [Vantion Labs](https://vantion.co); if
you want help putting something like this into production,
[talk to the founder](https://vantion.co/book-a-call).
