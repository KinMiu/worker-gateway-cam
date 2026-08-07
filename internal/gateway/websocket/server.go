package websocket

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

var upgrader = websocket.Upgrader{
	ReadBufferSize:  65536, // 64 KB
	WriteBufferSize: 65536, // 64 KB
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type Server struct {
	cameras map[string]*Client            // mac -> client
	viewers map[string]map[string]*Client // targetMac -> map[viewerID]*Client
	mu      sync.RWMutex
	Redis   *redis.Client
}

func NewServer() *Server {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6380", // Port Redis internal VPS kamu
	})

	s := &Server{
		cameras: make(map[string]*Client),
		viewers: make(map[string]map[string]*Client),
		Redis:   rdb,
	}

	// Jalankan listener Redis di background goroutine
	go s.ListenToEnhancedFrames()
	return s
}

func (s *Server) AddClient(c *Client) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if c.Role == "camera" {
		s.cameras[c.ID] = c
	} else if c.Role == "viewer" {
		if _, exists := s.viewers[c.Target]; !exists {
			s.viewers[c.Target] = make(map[string]*Client)
		}
		s.viewers[c.Target][c.ID] = c
	}
}

func (s *Server) RemoveClient(c *Client) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if c.Role == "camera" {
		if _, exists := s.cameras[c.ID]; exists {
			delete(s.cameras, c.ID)
		}
	} else if c.Role == "viewer" {
		if targetMap, exists := s.viewers[c.Target]; exists {
			delete(targetMap, c.ID)
			if len(targetMap) == 0 {
				delete(s.viewers, c.Target)
			}
		}
	}
}

func (s *Server) ListenToEnhancedFrames() {
	pubsub := s.Redis.PSubscribe(ctx, "urken:frame:enhanced:*")
	defer pubsub.Close()
	ch := pubsub.Channel()

	log.Println("⚡ Redis PSubscribe aktif pada urken:frame:enhanced:*")

	prefixLen := len("urken:frame:enhanced:")
	for msg := range ch {
		if len(msg.Channel) > prefixLen {
			macKamera := msg.Channel[prefixLen:]
			// Teruskan payload binary mentah ke viewer yang tepat
			s.BroadcastToTargetViewer(macKamera, []byte(msg.Payload))
		}
	}
}

func (s *Server) BroadcastToTargetViewer(macKamera string, data []byte) {
	s.mu.RLock()
	targetViewers, exists := s.viewers[macKamera]
	if !exists || len(targetViewers) == 0 {
		s.mu.RUnlock()
		return
	}

	// Copy pointer client agar durasi Lock Mutex mikrodetik
	clients := make([]*Client, 0, len(targetViewers))
	for _, client := range targetViewers {
		clients = append(clients, client)
	}
	s.mu.RUnlock()

	// Kirim frame ke channel viewer secara NON-BLOCKING
	for _, client := range clients {
		select {
		case client.send <- data:
		default:
			// Auto drop frame jika client lagging (mencegah memory leak)
			log.Printf("⚠️ Frame dropped untuk viewer %s (Buffer Full)\n", client.ID)
		}
	}
}

func (s *Server) HandleCamera(w http.ResponseWriter, r *http.Request) {
	mac := r.URL.Query().Get("mac")
	if mac == "" {
		http.Error(w, "Query parameter 'mac' wajib disertakan", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Gagal upgrade koneksi kamera: %v\n", err)
		return
	}

	client := &Client{
		ID:     mac,
		Role:   "camera",
		conn:   conn,
		server: s,
		send:   make(chan []byte, 30),
	}
	s.AddClient(client)

	log.Printf("📸 [Camera Connected] MAC: %s\n", client.ID)
	go client.WritePump()
	go client.ReadPump()
}

func (s *Server) HandleViewer(w http.ResponseWriter, r *http.Request) {
	targetMac := r.URL.Query().Get("mac")
	if targetMac == "" {
		http.Error(w, "Query parameter target 'mac' wajib disertakan", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Gagal upgrade koneksi viewer: %v\n", err)
		return
	}

	viewerID := fmt.Sprintf("viewer-%d", time.Now().UnixNano())
	client := &Client{
		ID:     viewerID,
		Role:   "viewer",
		Target: targetMac,
		conn:   conn,
		server: s,
		send:   make(chan []byte, 30),
	}
	s.AddClient(client)

	log.Printf("🖥️  [Viewer Connected] ID: %s -> Target MAC: %s\n", client.ID, client.Target)
	go client.WritePump()
	go client.ReadPump()
}
