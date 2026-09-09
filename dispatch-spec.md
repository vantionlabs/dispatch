# Dispatch

Live market data to many browsers at once, and an honest number for how long
it takes.

## 1. Why this exists

The portfolio needs one piece that is not an LLM. A prospect's CTO has to
believe the boring half can be built — the sockets, the backpressure, the
thing that stays up when the market moves — before they will believe the
interesting half. Docket sells to finance and compliance. This sells to
whoever has to keep a screen correct.

The deliverable is a sentence with a number in it, and the number has to
survive someone asking how it was measured.

## 2. The claim, and what it is not

> *p99 fan-out under N ms at M concurrent connections, sustained over a
> firehose of real exchange ticks.*

Most published latency numbers quietly include the public internet. Ours
does not, because we did not build the public internet. Dispatch reports two
numbers and never conflates them:

| Number | Clock start | Clock stop | Whose fault is it |
|---|---|---|---|
| **Fan-out** | tick accepted by our ingest | tick written to the last subscriber's socket | ours |
| **End-to-end** | exchange's own event timestamp | tick handled in the browser | ours, plus two networks and a clock we do not own |

Fan-out is the engineering claim. End-to-end is the user's experience and is
reported alongside so nobody thinks we are hiding it. Publishing only the
second is how a 200ms number gets quoted for a system whose own contribution
was 3ms; publishing only the first is how you dodge the question.

Exchange clocks drift, so end-to-end is reported with that caveat attached
rather than presented as precise.

## 3. Shape

    exchange ws  ->  feed adapter  ->  hub (in memory)  ->  subscriber sockets
                     normalise         coalesce             one frame per flush

**No database in the hot path.** The current price of BTC is a variable, not
a row. Persistence, if it ever arrives, hangs off the side for history and
never sits between a tick and a screen.

**Coalescing, not queuing.** This is the decision the whole system turns on.
When a subscriber is slower than the feed — a laptop asleep, a phone on a
train — the obvious move is to queue their ticks and drain when they return.
That is wrong for market data: a queued tick is a price that was true
thirty seconds ago, and delivering it is worse than delivering nothing. So
a slow subscriber holds *the latest tick per symbol* and nothing else. Load
does not become lag; it becomes a lower update rate, which is what a human
watching a number actually wants. It also bounds memory per connection to
the number of symbols, so a slow client cannot make the server fall over.

**The feed is an interface.** Binance is one implementation because it is
free, needs no key, and pushes thousands of real ticks a second, which makes
the number earned rather than simulated. Energy prices, transit positions
and odds are the same system with a different adapter and a slower clock.

## 4. Milestones

- **D1** ingest, hub, fan-out, one page that shows live prices.
- **D2** the load harness: N concurrent subscribers, latency histogram,
  the number.
- **D3** the dashboard that shows the number while it happens, so a demo is
  a thing you watch rather than a slide.
- **D4** deploy, and re-measure from somewhere that is not a laptop.

## 5. What would make this dishonest

Kept here so it stays checkable:

- Measuring with subscribers that never read their socket. A subscriber that
  does not deserialise is not a subscriber.
- Reporting a mean. The mean latency of a fan-out is the number that hides
  the incident; p99 is the one a user feels.
- Counting a coalesced tick as delivered. If three ticks collapse into one
  frame, that is two ticks dropped on purpose, and the drop rate is
  published beside the latency, not instead of it.
- Running the load generator and the server on the same machine and calling
  the result a network measurement. D2 is a fan-out measurement and says so;
  D4 is the network one.
