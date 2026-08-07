package websocket

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Client struct {
	ID        string // MAC Address (Camera) atau Viewer-ID (Viewer)
	Role      string // "camera" atau "viewer"
	Target    string // MAC Address target kamera (Khusus Viewer)
	conn      *websocket.Conn
	server    *Server
	send      chan []byte // Buffered channel untuk melempar frame secara non-blocking
	closeOnce sync.Once   // Mencegah close channel ganda saat reconnect beruntun
}

// WritePump menangani pengiriman data dari Go Channel ke WebSocket Connection.
func (c *Client) WritePump() {
	ticker := time.NewTicker(30 * time.Second) // Ping internal setiap 30 detik
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			// FIX: Dikembalikan ke BinaryMessage karena terbukti payload Redis adalah raw biner
			// Browser frontend otomatis mendeteksi objek ini sebagai Instance of Blob.
			if err := c.conn.WriteMessage(websocket.BinaryMessage, message); err != nil {
				return
			}

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// ReadPump menangani incoming message dari WebSocket Connection.
func (c *Client) ReadPump() {
	defer func() {
		c.server.RemoveClient(c)
		c.conn.Close()

		// sync.Once menjaga agar penutupan channel aman dari race condition reconnect instan
		c.closeOnce.Do(func() {
			close(c.send)
		})

		log.Printf("🔴 [%s] Disconnected (%s)\n", c.ID, c.Role)
	}()

	// Batasi max size payload (10MB per frame)
	c.conn.SetReadLimit(10 * 1024 * 1024)
	_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))

	// Reset deadline setiap kali dapat Pong dari client browser/hardware
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		messageType, payload, err := c.conn.ReadMessage()
		if err != nil {
			break
		}

		// Jika data masuk dari Kamera berbentuk biner, arahkan langsung ke Redis Raw Channel
		if c.Role == "camera" && messageType == websocket.BinaryMessage {
			redisChannel := fmt.Sprintf("urken:frame:raw:%s", c.ID)
			err := c.server.Redis.Publish(ctx, redisChannel, payload).Err()
			if err != nil {
				log.Printf("❌ Gagal publish Redis dari %s: %v\n", c.ID, err)
			} else {
				log.Printf("📤 [REDIS PUBLISH SUCCESS] ──> Frame Mentah Kamera [%s] Berhasil Masuk Redis Raw\n", c.ID)
			}
		}
	}
}
