package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/chickooooo/go-kafka-python/internal/kafka"
)

type Handlers interface {
	// Health checks if the server is healthy
	Health(w http.ResponseWriter, r *http.Request)
	// Order processes the order using orderId
	Order(w http.ResponseWriter, r *http.Request)
}

type handlers struct {
	kafkaService kafka.Service
}

func NewHandlers(kafkaService kafka.Service) Handlers {
	return handlers{
		kafkaService: kafkaService,
	}
}

func (h handlers) Order(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var requestBody struct {
		OrderID string `json:"order_id"`
	}
	defer r.Body.Close()
	err := json.NewDecoder(r.Body).Decode(&requestBody)
	if err != nil {
		log.Printf("failed to parse request body: %v", err)
		response := ErrorResponse{
			Message: "Cannot parse request body",
			Details: err.Error(),
		}
		WriteJSON(w, http.StatusBadRequest, response)
		return
	}

	// Send event to kafka
	err = h.kafkaService.SendEvent(r.Context(), requestBody.OrderID)
	if err != nil {
		log.Printf("failed to send Kafka event: %v", err)
		response := ErrorResponse{
			Message: "Failed to process order",
			Details: err.Error(),
		}
		WriteJSON(w, http.StatusServiceUnavailable, response)
		return
	}

	// Success reponse
	response := SuccessResponse{
		Message: "Order processed successfully",
	}
	WriteJSON(w, http.StatusOK, response)
}

func (h handlers) Health(w http.ResponseWriter, r *http.Request) {
	healthy := struct {
		Status string `json:"status"`
	}{"Healthy!"}
	WriteJSON(w, http.StatusCreated, healthy)
}
