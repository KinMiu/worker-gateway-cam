package websocket

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Client struct {
	ID        string // MAC Address (Camera) atau Viewer-ID unik (Viewer)
	Role      string // "camera" or "viewer"
	Target    string // MAC Address kamera yang ingin ditonton (Khusus Viewer)
	conn      *websocket.Conn
	server    *Server
	send      chan []byte // Buffered channel untuk melempar frame secara non-blocking
	closeOnce sync.Once   // Mencegah double-close channel saat crash/reconnect cepat
}

// WritePump menangani pengiriman data dari Go Channel ke WebSocket Connection.
func (c *Client) WritePump() {
	ticker := time.NewTicker(30 * time.Second) // Ping tiap 30 detik
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			if !ok {
				// Channel ditutup oleh proses cleanup ReadPump
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			// FIX: Menggunakan TextMessage jika data enhanced dari Redis PubSub berupa Base64/JSON String
			// Agar frontend browser bisa langsung menangkap payloadnya dengan mudah.
			var err error
			if c.Role == "viewer" {
				err = c.conn.WriteMessage(websocket.TextMessage, message)
			} else {
				err = c.conn.WriteMessage(websocket.BinaryMessage, message)
			}

			if err != nil {
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

		// Gunakan sync.Once agar penutupan channel aman dari race condition reconnect instan
		c.closeOnce.Do(func() {
			close(c.send)
		})

		log.Printf("🔴 [%s] Disconnected (%s)\n", c.ID, c.Role)
	}()

	// Batasi max size payload (10MB per frame)
	c.conn.SetReadLimit(10 * 1024 * 1024)
	_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))

	// Reset deadline setiap kali dapat Pong dari client
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		messageType, payload, err := c.conn.ReadMessage()
		if err != nil {
			break
		}

		// Jika dari Kamera dan berupa biner frame, publish ke Redis Channel
		if c.Role == "camera" && messageType == websocket.BinaryMessage {
			redisChannel := fmt.Sprintf("urken:frame:raw:%s", c.ID)
			err := c.server.Redis.Publish(ctx, redisChannel, payload).Err()
			if err != nil {
				log.Printf("❌ Gagal publish Redis dari %s: %v\n", c.ID, err)
			}
		}
	}
}
