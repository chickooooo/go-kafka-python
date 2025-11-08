package kafka

import (
	"context"
	"fmt"
	"log"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	brokerAddr = "localhost:9092" // broker address
	topic      = "orders"         // kafka topic

	partitions  = int32(3) // number of partitions
	replication = int16(1) // replication factor
)

type Service interface {
	// CreateTopic will create new kafka topic if it does not already exists
	CreateTopic(ctx context.Context) error
	// SendEvent sends the given message to kafka topic
	SendEvent(ctx context.Context, message string) error
	// Close will close all the kafka clients
	Close()
}

type service struct {
	admin  *kadm.Client // for admin operations
	client *kgo.Client  // for producing/consuming messages
}

func NewService() Service {
	// Create kafka client
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddr),
		kgo.ClientID("go-producer-1"), // client ID
	)
	if err != nil {
		panic(fmt.Sprintf("failed to create kafka client: %v\n", err))
	}

	// Create the admin client
	admin := kadm.NewClient(client)

	return service{
		admin:  admin,
		client: client,
	}
}

func (s service) CreateTopic(ctx context.Context) error {
	// Get list of all topics
	details, err := s.admin.ListTopics(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to list topics: %w", err)
	}

	// Check if topic exists and is valid (no error)
	if detail, ok := details[topic]; ok && detail.Err == nil {
		log.Printf("topic '%s' already exists. Skipping creation.", topic)
		return nil
	}

	// Create a new topic if it doesn't exist
	_, err = s.admin.CreateTopics(ctx, partitions, replication, nil, topic)
	if err != nil {
		return fmt.Errorf("failed to create topic '%s': %w", topic, err)
	}

	log.Printf("topic '%s' created successfully\n", topic)
	return nil
}

func (s service) SendEvent(ctx context.Context, message string) error {
	// Done channel to wait for response from Kafka
	done := make(chan error)

	// Create a record that will be sent to Kafka
	record := kgo.Record{
		Topic: topic,
		Value: []byte(message),
	}

	// Sends the record asynchronously.
	s.client.Produce(
		ctx,
		&record,
		// This callback will be called when the request completes
		func(r *kgo.Record, err error) {
			if err != nil {
				log.Printf("failed to send event to topic '%s': %v\n", r.Topic, err)
			} else {
				log.Printf(
					"successfully sent event to topic '%s' at partition %d, offset %d\n",
					r.Topic,
					r.Partition,
					r.Offset,
				)
			}
			done <- err
		},
	)

	select {
	// Processing completed
	case err := <-done:
		if err != nil {
			return fmt.Errorf("failed to send event: %w", err)
		}
		return nil
	// Context cancelled / completed
	case <-ctx.Done():
		return fmt.Errorf("Kafka send canceled: %w", ctx.Err())
	}
}

func (s service) Close() {
	s.admin.Close()
	s.client.Close()
}
