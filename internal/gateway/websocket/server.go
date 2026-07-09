package websocket

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Izinkan CORS untuk kebutuhan Frontend
	},
}

type Server struct {
	devices map[string]*Client
	mu      sync.RWMutex
	Redis   *redis.Client
}

func NewServer() *Server {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6380", // Sesuaikan port Redis kamu
	})

	s := &Server{
		devices: make(map[string]*Client),
		Redis:   rdb,
	}

	// Menjalankan background worker untuk mendengarkan hasil olah frame dari sistem AI
	go s.ListenToEnhancedFrames()
	return s
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

// Background Worker mendengarkan channel bermotif Wildcard (*) di Redis
func (s *Server) ListenToEnhancedFrames() {
	// Mendengarkan channel seperti: urken:frame:enhanced:240AC4XXXXXX
	pubsub := s.Redis.PSubscribe(ctx, "urken:frame:enhanced:*")
	defer pubsub.Close()
	ch := pubsub.Channel()

	log.Println("⚡ Redis PSubscribe aktif pada urken:frame:enhanced:*")

	for msg := range ch {
		// Ekstrak MAC Address dari nama channel Redis yang mengirim pesan
		// "urken:frame:enhanced:240AC4XXXXXX" -> diambil "240AC4XXXXXX"
		prefixLen := len("urken:frame:enhanced:")
		if len(msg.Channel) > prefixLen {
			macKamera := msg.Channel[prefixLen:]

			// Teruskan biner gambar hanya ke viewer yang meminta MAC ini
			s.BroadcastToTargetViewer(macKamera, []byte(msg.Payload))
		}
	}
}

// Mengirimkan gambar spesifik ke viewer yang memintanya
func (s *Server) BroadcastToTargetViewer(macKamera string, data []byte) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for id, client := range s.devices {
		// Filter: Harus bertindak sebagai viewer DAN Target MAC kamera yang diminta harus cocok
		if client.Role == "viewer" && client.Target == macKamera {
			// Timeout pengiriman agar tidak membebani performa server jika client lag/putus sinyal
			client.conn.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))

			err := client.conn.WriteMessage(websocket.BinaryMessage, data)
			if err != nil {
				log.Printf("[%s] Frame drop / gagal kirim ke viewer: %v\n", id, err)
			}
		}
	}
}

// Handler khusus untuk Hardware ESP32-CAM
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

	// Untuk kamera, ID di dalam map diisi langsung dengan MAC Address-nya
	client := NewClient(conn, s, "camera", mac)
	s.AddDevice(client)

	log.Printf("📸 [Camera Connected] MAC: %s\n", client.ID)
	go client.ReadPump()
}

// Handler khusus untuk Frontend Web/Mobile Apps
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

	// Membuat ID unik untuk viewer menggunakan timestamp nano
	viewerID := fmt.Sprintf("viewer-%d", time.Now().UnixNano())
	client := NewClient(conn, s, "viewer", viewerID)
	client.Target = targetMac // Kunci target MAC kamera yang ingin ditonton

	s.AddDevice(client)

	log.Printf("🖥️  [Viewer Connected] ID: %s mendengarkan Kamera: %s\n", client.ID, client.Target)
	go client.ReadPump()
}
