package websocket

import (
	"fmt"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

type Client struct {
	ID     string
	Role   string // Tambahkan Role: "camera" atau "viewer"
	conn   *websocket.Conn
	server *Server
}

// Tambahkan parameter 'role'
func NewClient(conn *websocket.Conn, server *Server, role string) *Client {
	id := fmt.Sprintf("%s-%d", role, time.Now().UnixNano())
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
		log.Printf("[%s] Disconnected\n", c.ID)
		c.conn.Close()
	}()

	for {
		messageType, payload, err := c.conn.ReadMessage()
		if err != nil {
			break
		}

		// Hanya izinkan "camera" yang bisa mem-publish ke Redis
		if c.Role == "camera" && messageType == websocket.BinaryMessage {
			err := c.server.Redis.Publish(ctx, "urken:frame:raw", payload).Err()
			if err != nil {
				log.Printf("Gagal publish ke Redis: %v\n", err)
			}
		}
	}
}
