package feed

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// DefaultSymbols are liquid pairs, so the stream is busy at any hour.
var DefaultSymbols = []string{
	"btcusdt", "ethusdt", "solusdt", "xrpusdt", "bnbusdt",
	"adausdt", "dogeusdt", "avaxusdt", "linkusdt", "dotusdt",
	"ltcusdt", "trxusdt", "nearusdt", "atomusdt", "uniusdt",
	"aptusdt", "arbusdt", "opusdt", "injusdt", "filusdt",
	"suiusdt", "seiusdt", "tiausdt", "pepeusdt", "shibusdt",
	"wifusdt", "bonkusdt", "jupusdt", "pythusdt", "rndrusdt",
	"imxusdt", "grtusdt", "aaveusdt", "mkrusdt", "ldousdt",
	"crvusdt", "sandusdt", "manausdt", "axsusdt", "galausdt",
	"ftmusdt", "algousdt", "vetusdt", "hbarusdt", "icpusdt",
	"etcusdt", "bchusdt", "xlmusdt", "eosusdt", "thetausdt",
	"ethbtc", "solbtc", "xrpbtc", "adabtc", "linkbtc",
	"btceur", "etheur", "soleur", "xrpeur", "adaeur",
}

const binanceHost = "wss://stream.binance.com:9443/stream"

// Binance reads public trade streams. No key, no account.
//
// Chosen over a friendlier-sounding source because it is genuinely fast. A
// fan-out benchmark run against something that emits once a second measures
// nothing; the point of the published number is that real ticks arrived faster
// than a naive implementation could push them, and something had to give.
//
// The @trade stream rather than @bookTicker: bookTicker updates more often,
// but its payload carries no exchange timestamp, and without one the
// end-to-end figure would have to be invented.
type Binance struct {
	symbols []string
	streams string
}

func NewBinance(symbols []string) *Binance {
	if len(symbols) == 0 {
		symbols = DefaultSymbols
	}
	parts := make([]string, 0, len(symbols))
	normalised := make([]string, 0, len(symbols))
	for _, s := range symbols {
		parts = append(parts, strings.ToLower(s)+"@trade")
		normalised = append(normalised, Normalise(strings.ToUpper(s)))
	}
	return &Binance{symbols: normalised, streams: strings.Join(parts, "/")}
}

func (b *Binance) Name() string      { return "binance" }
func (b *Binance) Symbols() []string { return b.symbols }

type tradeFrame struct {
	Data struct {
		Event  string `json:"e"`
		Time   int64  `json:"E"`
		Symbol string `json:"s"`
		Price  string `json:"p"`
		Qty    string `json:"q"`
	} `json:"data"`
}

// Run connects and keeps reconnecting until the context is cancelled.
func (b *Binance) Run(ctx context.Context, h Handler) error {
	backoff := 250 * time.Millisecond
	reconnects := 0

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := b.stream(ctx, h, &backoff)

		if ctx.Err() != nil {
			return ctx.Err()
		}
		reconnects++
		h.OnStatus(Status{Connected: false, Detail: errText(err), Reconnects: reconnects})

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > 10*time.Second {
			backoff = 10 * time.Second
		}
	}
}

func (b *Binance) stream(ctx context.Context, h Handler, backoff *time.Duration) error {
	url := fmt.Sprintf("%s?streams=%s", binanceHost, b.streams)
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		return err
	}
	defer conn.Close()

	*backoff = 250 * time.Millisecond
	h.OnStatus(Status{Connected: true, Detail: fmt.Sprintf("%d symbols", len(b.symbols))})

	// Binance pings; the read deadline is what notices a silently dead link.
	conn.SetReadDeadline(time.Now().Add(90 * time.Second))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(90 * time.Second))
	})
	conn.SetPingHandler(func(data string) error {
		conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		return conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(5*time.Second))
	})

	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	var frame tradeFrame
	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		// The clock starts here: the first instant the tick is ours. Taken
		// before decoding, so our own deserialisation cost sits inside the
		// number we publish rather than outside it.
		ingest := time.Now().UnixMilli()

		frame.Data.Event = ""
		if err := json.Unmarshal(payload, &frame); err != nil {
			continue // a frame we cannot read is one we drop, on purpose
		}
		if frame.Data.Event != "trade" {
			continue
		}
		price, err := strconv.ParseFloat(frame.Data.Price, 64)
		if err != nil {
			continue
		}
		size, _ := strconv.ParseFloat(frame.Data.Qty, 64)

		h.OnTick(Tick{
			Symbol:     Normalise(frame.Data.Symbol),
			Price:      price,
			Size:       size,
			ExchangeTS: frame.Data.Time,
			IngestTS:   ingest,
		})
	}
}

var quoteCurrencies = []string{"USDT", "USDC", "EUR", "USD", "BTC", "ETH"}

// Normalise turns "BTCUSDT" into "BTC-USDT", once, here, so that no consumer
// downstream has to guess where the pair splits.
//
// Idempotent, because it is applied both to what the exchange sends and to
// what a subscriber asks for, and those arrive in different shapes. Without
// the guard "BTC-USDT" matched the USDT suffix and split a second time into
// "BTC--USDT", so every filtered subscription silently matched nothing.
func Normalise(symbol string) string {
	if strings.Contains(symbol, "-") {
		return symbol
	}
	for _, quote := range quoteCurrencies {
		if strings.HasSuffix(symbol, quote) && len(symbol) > len(quote) {
			return symbol[:len(symbol)-len(quote)] + "-" + quote
		}
	}
	return symbol
}

func errText(err error) string {
	if err == nil {
		return "closed"
	}
	return err.Error()
}
