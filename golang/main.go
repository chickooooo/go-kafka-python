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

	// Check if kafka is ready
	if err := kafkaService.IsReady(ctx); err != nil {
		log.Fatalf("Kafka not ready: %v", err)
	}

	// Create new topic
	if err := kafkaService.CreateTopic(ctx); err != nil {
		log.Fatalf("failed to create topic: %v\n", err)
	}

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
