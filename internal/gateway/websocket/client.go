package websocket

import (
	"context"
	"fmt"
	"log"

	"github.com/gorilla/websocket"
)

var ctx = context.Background()

type Client struct {
	ID     string // MAC Address jika kamera, acak/timestamp jika viewer
	Role   string // "camera" atau "viewer"
	Target string // MAC Address kamera yang ditargetkan (khusus viewer)
	conn   *websocket.Conn
	server *Server
}

// Membuat instance client baru
func NewClient(conn *websocket.Conn, server *Server, role string, id string) *Client {
	return &Client{
		ID:     id,
		Role:   role,
		conn:   conn,
		server: server,
	}
}

func (c *Client) ReadPump() {
	defer func() {
		c.server.RemoveDevice(c.ID)
		log.Printf("🔴 [%s] Disconnected (%s)\n", c.ID, c.Role)
		c.conn.Close()
	}()

	log.Printf("🔥 [%s] ReadPump mulai berjalan untuk %s\n", c.ID, c.Role)

	for {
		messageType, payload, err := c.conn.ReadMessage()
		if err != nil {
			log.Printf("⚠️ [%s] Error ReadMessage: %v\n", c.ID, err)
			break
		}

		// DEBUG LOG: Cetak setiap kali ada pesan masuk dari WebSocket
		log.Printf("📥 [%s] Menerima pesan! Type: %d, Ukuran: %d bytes\n", c.ID, messageType, len(payload))

		// Hanya memproses data biner stream dari Kamera
		if c.Role == "camera" {
			if messageType == websocket.BinaryMessage {
				redisChannel := fmt.Sprintf("urken:frame:raw:%s", c.ID)

				err := c.server.Redis.Publish(ctx, redisChannel, payload).Err()
				if err != nil {
					log.Printf("❌ Gagal publish ke Redis untuk %s: %v\n", c.ID, err)
				} else {
					log.Printf("🚀 Berhasil Publish biner %s ke Redis channel [%s]\n", c.ID, redisChannel)
				}
			} else {
				log.Printf("ℹ️ [%s] Pesan diabaikan karena tipe data bukan Binary (Type: %d)\n", c.ID, messageType)
			}
		}
	}
}
