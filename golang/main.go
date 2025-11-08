package main

import (
	"context"
	"log"
	"net/http"

	"github.com/chickooooo/go-kafka-python/internal/kafka"
)

func main() {
	ctx := context.Background()

	// Initialize kafka service
	kafkaService := kafka.NewService()
	defer kafkaService.Close()
	// Create new topic
	kafkaService.CreateTopic(ctx)

	// Initialize handlers
	handlers := NewHandlers(kafkaService)

	// Start http server
	StartServer(handlers)
}

// StartServer defines API routes and start the http server
func StartServer(handlers Handlers) {
	// Define API routes
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/order", handlers.Order)
	mux.HandleFunc("/", handlers.Health)

	// Start HTTP server
	log.Println("Server started at port 8080...")
	http.ListenAndServe(":8080", mux)
}
