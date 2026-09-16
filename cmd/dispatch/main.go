// Command dispatch serves live market data to many browsers at once.
//
//	go run ./cmd/dispatch -addr :8080 -flush 25ms
//
// Endpoints:
//
//	GET /ws?symbols=BTC-USDT,ETH-USDT   subscribe; omit symbols for everything
//	GET /stats                          what the hub is doing, as JSON
//	GET /                               the dashboard
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vantionlabs/dispatch/internal/feed"
	"github.com/vantionlabs/dispatch/internal/hub"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	flush := flag.Duration("flush", 25*time.Millisecond, "flush interval, the floor on fan-out latency")
	writeTimeout := flag.Duration("write-timeout", 250*time.Millisecond, "per-frame write deadline before a subscriber is dropped")
	symbols := flag.String("symbols", "", "comma separated exchange symbols; empty uses the defaults")
	flag.Parse()

	var wanted []string
	if *symbols != "" {
		wanted = strings.Split(*symbols, ",")
	}

	source := feed.NewBinance(wanted)
	h := hub.New(hub.Options{FlushInterval: *flush})
	go h.Run()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		if err := source.Run(ctx, h); err != nil && ctx.Err() == nil {
			log.Printf("feed stopped: %v", err)
		}
	}()

	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 16 * 1024,
		// The dashboard is served from anywhere during a demo, and the data
		// is a public price feed carrying no authority. Nothing here reads a
		// cookie, so there is no session for another origin to ride.
		CheckOrigin: func(*http.Request) bool { return true },
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		var requested []string
		if raw := r.URL.Query().Get("symbols"); raw != "" {
			requested = strings.Split(strings.ToUpper(raw), ",")
		}

		sink := hub.NewSocketSink(conn, *writeTimeout)
		sub := h.Subscribe(sink, requested)

		// A screen must be correct on arrival, not blank until every symbol
		// happens to trade.
		_ = sink.Write(h.Snapshot(subscriberSymbols(requested)))

		// Reading is what notices the client going away; the payloads are
		// ignored because subscribers have nothing to say.
		go func() {
			defer func() {
				h.Unsubscribe(sub.ID)
				sink.Close()
			}()
			conn.SetReadLimit(1024)
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}()
	})

	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		_ = json.NewEncoder(w).Encode(h.Stats())
	})

	mux.Handle("/", http.FileServer(http.Dir("web")))

	server := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdown, done := context.WithTimeout(context.Background(), 3*time.Second)
		defer done()
		_ = server.Shutdown(shutdown)
		h.Stop()
	}()

	log.Printf("dispatch listening on %s, flushing every %s", *addr, *flush)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func subscriberSymbols(requested []string) map[string]struct{} {
	set := make(map[string]struct{}, len(requested))
	for _, s := range requested {
		set[feed.Normalise(strings.ToUpper(s))] = struct{}{}
	}
	return set
}
