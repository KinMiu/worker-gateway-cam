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
		log.Printf("[%s] Disconnected (%s)\n", c.ID, c.Role)
		c.conn.Close()
	}()

	for {
		messageType, payload, err := c.conn.ReadMessage()
		if err != nil {
			break
		}

		// Hanya memproses data biner stream dari Kamera
		if c.Role == "camera" && messageType == websocket.BinaryMessage {
			// Channel Redis dinamis: urken:frame:raw:240AC4XXXXXX
			redisChannel := fmt.Sprintf("urken:frame:raw:%s", c.ID)

			err := c.server.Redis.Publish(ctx, redisChannel, payload).Err()
			if err != nil {
				log.Printf("Gagal publish ke Redis untuk %s: %v\n", c.ID, err)
			}
		}
	}
}
