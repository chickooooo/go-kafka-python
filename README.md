# Go Kafka Python

<br>
<br>

### About

This repository demonstrates how we can achieve asynchronous communication between a **Go web service** & a **Python service** in a microservice architecture. This asynchronous communication is facilitated through **Kafka**.

<br>

### Overview

The Go web service has a POST API endpoint `/order`, through which user can place an order. This order is then published to a Kafka topic. On the other side of this topic, we have a Python consumer processing these orders.

<br>

### Technologies

- We are using `docker compose` to run the Kafka service + Go producer + Python consumer.
- Kafka will be running in **KRaft** mode.
- Go tech-stack: `net/http` (for REST APIs), `franz-go` (for Kafka).
- Python tech-stack: `kafka-python` (for Kafka).

<br>

### Project Structure

```
- golang/                   // Go producer
    - internal/
        - kafka/            // Kafka package
            - service.go    // Kafka service
    - utils.go              // JSON response utils
    - handlers.go           // REST API handlers
    - main.go               // Go application entry-point
    - go.mod
    - go.sum
    - Dockerfile            // Go dockerfile
- python/                   // Python consumer
    - consumer.py           // Kafka consumer
    - requirements.txt
    - Dockerfile            // Python dockerfile
- .gitignore
- docker-compose.yml        // Kafka, Go, Python services
- README.md
```

<br>

### Start all 3 services

```bash
docker compose up --build
```

<br>

### Placing order

cURL

```bash
curl --location 'localhost:8080/api/v1/order' \
--header 'Content-Type: application/json' \
--data '{
    "order_id": "f5a2e7c9-0e6a-4a56-bb97-d9a3d6b5a01c"
}'
```

Response
```json
{"message":"Order processed successfully"}
```

<br>

### Docker logs

```log
kafka-broker     | [2025-11-10 10:17:56,336] INFO [KafkaRaftServer nodeId=1] Kafka Server started (kafka.server.KafkaRaftServer)

go-producer      | 2025/11/10 10:18:01 Kafka connection established successfully.
go-producer      | 2025/11/10 10:18:01 topic 'orders' already exists. Skipping creation.
go-producer      | 2025/11/10 10:18:01 Server started at port 8080...

python-consumer  | INFO:consumer:Checking Kafka readiness on broker: broker:9092
python-consumer  | INFO:consumer:Attempt 1/10 to connect to Kafka...
python-consumer  | INFO:consumer:Kafka is ready!
python-consumer  | INFO:consumer:Consumer 'python-consumer-1' listening to topic 'orders'...

go-producer      | 2025/11/10 10:20:17 successfully sent event to topic 'orders' at partition 0, offset 5
python-consumer  | INFO:consumer:Processed message: topic='orders' partition=0 offset=5 value='f5a2e7c9-0e6a-4a56-bb97-d9a3d6b5a01c'
```
