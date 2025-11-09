import os
import time
import signal
import logging
from typing import Callable, Optional, Any

# from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition, errors
from kafka.consumer.fetcher import ConsumerRecord


# Initialize logger
logging.basicConfig(level=logging.INFO)
logging.getLogger("kafka").setLevel(logging.WARNING)
logger = logging.getLogger("consumer")


# Kafka topic name
TOPIC_NAME = "orders"

# Delay before checking for new events (in seconds)
CHECK_DELAY = 1
CONSUMER_BLOCK_TIME = 1000

# Consumer details
CONSUMER_GROUP_ID = "delivery"
CONSUMER_ID = "python-consumer-1"

# Retry configuration
MAX_RETRIES = 10
INITIAL_BACKOFF = 1  # seconds


def wait_for_kafka(broker_address: str) -> None:
    """
    Check if Kafka broker is ready, retrying with exponential backoff.
    Raises an exception if broker cannot be reached after all retries.
    """
    logger.info("Checking Kafka readiness on broker: %s", broker_address)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Attempt %d/%d to connect to Kafka...", attempt, MAX_RETRIES)
            consumer = KafkaConsumer(
                bootstrap_servers=broker_address,
                request_timeout_ms=2000,
                api_version_auto_timeout_ms=2000,
            )
            # If we can get metadata, Kafka is ready
            consumer.topics()
            consumer.close()
            logger.info("Kafka is ready!")
            return
        except errors.NoBrokersAvailable:
            logger.warning("Kafka broker not available yet.")
        except Exception as e:
            logger.warning("Kafka connection failed: %s", str(e))
        
        # Exponential backoff
        backoff = INITIAL_BACKOFF * (2 ** (attempt - 1))
        backoff = min(backoff, 30) # cap backoff to 30s

        logger.info("Retrying in %.1f seconds...", backoff)
        time.sleep(backoff)

    raise RuntimeError(f"Kafka broker at '{broker_address}' not ready after {MAX_RETRIES} retries.")


class MyKafkaConsumer:
    def __init__(self, handler: Callable[[ConsumerRecord], None]) -> None:
        # Get broker address from environment
        broker_address = os.getenv("KAFKA_BROKER")
        if broker_address is None:
            logger.error("Key 'KAFKA_BROKER' not found in the environment")

        # Handles incoming messages
        self._handler = handler
        # Used to terminate running consumer
        self._running = True

        # Ensure Kafka is up before continuing
        wait_for_kafka(broker_address)

        # Create consumer
        self._consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=broker_address,
            group_id=CONSUMER_GROUP_ID,
            client_id=CONSUMER_ID,
            consumer_timeout_ms=CONSUMER_BLOCK_TIME,
        )

        # Handle graceful consumer termination
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, sig_num: int, frame: Optional[Any]) -> None:
        logger.info("Signal %s received, shutting down..", sig_num)
        self._running = False

    def run(self) -> None:
        logger.info("Consumer '%s' listening to topic '%s'...", CONSUMER_ID, TOPIC_NAME)

        while self._running:
            # For each received record
            for record in self._consumer:
                # If consumer shutdown is requested
                if not self._running:
                    break
                # Process record
                self._handler(record)

            # Check again after some delay
            time.sleep(CHECK_DELAY)

        # Close consumer & related connections
        consumer.close()

        logger.info("Consumer '%s' stopped", CONSUMER_ID)

    def run_2(self) -> None:
        """Run the consumer loop (blocking)."""
        try:
            while self._running:
                try:
                    for record in self._consumer:
                        if not self._running:
                            break
                        # Process message
                        try:
                            self._handler(record)
                        except Exception:
                            # If processing fails, log and do NOT commit; message will be retried.
                            logger.exception(
                                "Message processing failed; offset=%s partition=%s",
                                getattr(record, "offset", None),
                                getattr(record, "partition", None),
                            )
                            # optionally: push to DLQ or alert here
                            continue

                        # On success, commit offset for that partition
                        tp = TopicPartition(record.topic, record.partition)
                        self._commit_offset(tp, int(record.offset))

                    # if consumer iteration exhausted (consumer_timeout_ms), sleep a bit
                    time.sleep(self.cfg.poll_interval_secs)
                except Exception:
                    # transient consumer-level errors (network/broker). Backoff then continue.
                    logger.exception("Consumer poll error, backing off briefly")
                    time.sleep(1.0)
        finally:
            self.close()

    def close(self) -> None:
        logger.info("Closing consumer")
        try:
            self._consumer.close()
        except Exception:
            logger.exception("Error closing consumer")


# Example handler (replace with real business logic)
def example_handler(record: ConsumerRecord) -> None:
    # Decode record.value from bytes to string
    decoded_value = record.value.decode("utf-8")
    logger.info(
        "Processed message: topic='%s' partition=%s offset=%s value='%s'",
        record.topic,
        record.partition,
        record.offset,
        decoded_value,
    )


if __name__ == "__main__":
    # Create new consumer
    consumer = MyKafkaConsumer(example_handler)
    # Start consumer
    consumer.run()
