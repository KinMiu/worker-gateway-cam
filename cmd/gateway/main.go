package main

import (
	"log"
	"net/http"

	"urken/internal/gateway/websocket" // Pastikan modul path ini sesuai dengan go.mod kamu
)

func main() {
	// Inisialisasi WebSocket dan PubSub server
	wsServer := websocket.NewServer()

	// Endpoint khusus kamera (ESP / RTSP Worker)
	http.HandleFunc("/ws/camera", wsServer.HandleCamera)

	// Endpoint khusus client frontend / viewer
	http.HandleFunc("/ws/viewer", wsServer.HandleViewer)

	port := ":5093"
	log.Printf("🚀 Gateway running on port %s\n", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
