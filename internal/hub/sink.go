package hub

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// SocketSink writes frames to one browser.
//
// The write deadline is the backpressure mechanism and it is deliberately
// short. A subscriber that cannot accept a frame within it is one whose screen
// is already behind; blocking the flush loop to wait for them would make
// everyone else late too, which is how one bad connection becomes an outage.
// They are dropped, and the drop is counted.
type SocketSink struct {
	mu      sync.Mutex
	conn    *websocket.Conn
	timeout time.Duration
	closed  bool
}

func NewSocketSink(conn *websocket.Conn, timeout time.Duration) *SocketSink {
	if timeout <= 0 {
		timeout = 250 * time.Millisecond
	}
	return &SocketSink{conn: conn, timeout: timeout}
}

func (s *SocketSink) Write(payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return websocket.ErrCloseSent
	}
	if err := s.conn.SetWriteDeadline(time.Now().Add(s.timeout)); err != nil {
		return err
	}
	return s.conn.WriteMessage(websocket.TextMessage, payload)
}

func (s *SocketSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.conn.Close()
}
