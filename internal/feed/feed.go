// Package feed defines what a market data source produces, and nothing about
// where it comes from.
//
// Everything downstream of this file is exchange-agnostic on purpose. Binance
// is the demo source because it is free and fast; energy prices, transit
// positions and betting odds are the same system with a slower clock, and the
// adapter should be the only thing that has to change.
package feed

import "context"

// Tick is one observed price.
type Tick struct {
	// Symbol is normalised, e.g. "BTC-USDT". Uppercase, dash separated.
	Symbol string
	Price  float64
	Size   float64

	// ExchangeTS is the source's own event time in epoch milliseconds.
	//
	// Kept apart from our own clock everywhere, because it is the one number
	// in the system we did not measure and cannot trust. Exchange clocks
	// drift, and a drifted clock silently flatters or ruins an end-to-end
	// latency figure. Anything derived from it is reported with that caveat
	// attached rather than presented as precise.
	ExchangeTS int64

	// IngestTS is when our process accepted the tick. This clock we own, and
	// it is the one the published fan-out number starts from.
	IngestTS int64
}

// Status says whether the source is currently reachable.
//
// A feed that reconnects quietly is a feed that can be down for an hour while
// every screen shows the last price it ever saw, which is the failure this
// system exists to avoid. Status is fanned out to subscribers like any tick.
type Status struct {
	Connected  bool
	Detail     string
	Reconnects int
}

// Handler receives everything a feed produces. Implementations are called from
// the feed's own goroutine and must not block.
type Handler interface {
	OnTick(Tick)
	OnStatus(Status)
}

// Feed is a source of ticks that runs until its context is cancelled.
type Feed interface {
	Name() string
	Symbols() []string
	Run(ctx context.Context, h Handler) error
}
