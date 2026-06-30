package main

import (
	"log"
	"net/http"

	"urken/internal/gateway/websocket" // Sesuaikan dengan module kamu
)

func main() {
	wsServer := websocket.NewServer()

	// Sekarang kita punya 2 pintu yang jelas fungsinya:
	http.HandleFunc("/ws/camera", wsServer.HandleCamera)
	http.HandleFunc("/ws/viewer", wsServer.HandleViewer)

	port := ":8080"
	log.Printf("🚀 Gateway running on port %s", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
