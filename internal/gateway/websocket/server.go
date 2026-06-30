package websocket

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type Server struct {
	devices map[string]*Client
	mu      sync.RWMutex
	Redis   *redis.Client
}

func NewServer() *Server {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6380",
	})
	s := &Server{
		devices: make(map[string]*Client),
		Redis:   rdb,
	}
	go s.ListenToEnhancedFrames()
	return s
}

func (s *Server) ListenToEnhancedFrames() {
	pubsub := s.Redis.Subscribe(ctx, "urken:frame:enhanced")
	defer pubsub.Close()
	ch := pubsub.Channel()

	for msg := range ch {
		s.BroadcastToFrontend([]byte(msg.Payload))
	}
}

func (s *Server) AddDevice(c *Client) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.devices[c.ID] = c
}

func (s *Server) RemoveDevice(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.devices, id)
}

func (s *Server) BroadcastToFrontend(data []byte) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for id, client := range s.devices {
		// HANYA kirim ke Frontend (Viewer), JANGAN kirim balik ke Kamera!
		if client.Role == "viewer" {
			// Cegah macet: Jika dalam 100ms gambar gagal terkirim, lewati (drop frame)
			client.conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))

			err := client.conn.WriteMessage(websocket.BinaryMessage, data)
			if err != nil {
				log.Printf("[%s] Frame drop / gagal kirim: %v\n", id, err)
			}
		}
	}
}

// Endpoint khusus Kamera
func (s *Server) HandleCamera(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	client := NewClient(conn, s, "camera")
	s.AddDevice(client)
	log.Printf("[%s] Camera Connected\n", client.ID)
	go client.ReadPump()
}

// Endpoint khusus Frontend / Viewer
func (s *Server) HandleViewer(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	client := NewClient(conn, s, "viewer")
	s.AddDevice(client)
	log.Printf("[%s] Viewer Connected\n", client.ID)
	go client.ReadPump()
}
