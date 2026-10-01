"""Create the Kafka topic for the book."""

# %% 1 - Imports and configuration
import os

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "gutenberg-book-lines")


# %% 2 - Function to create the topic
def create_topic() -> None:
    """Create our topic and show all topics."""
    # We connect to Kafka on the local Docker container.
    admin = AdminClient(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "socket.timeout.ms": 10_000,
        }
    )
    futures = admin.create_topics(
        # One partition is enough and it keeps the book lines in order.
        [NewTopic(TOPIC, num_partitions=1, replication_factor=1)]
    )

    try:
        futures[TOPIC].result(timeout=15)
        print(f"Created topic: {TOPIC}")
    except TimeoutError as exc:
        raise SystemExit(
            f"Cannot connect to Kafka at {BOOTSTRAP_SERVERS}. "
            "Start Docker and the Kafka container first."
        ) from exc
    except KafkaException as exc:
        error = exc.args[0]
        if error.code() == KafkaError.TOPIC_ALREADY_EXISTS:
            print(f"Topic already exists: {TOPIC}")
        else:
            raise

    metadata = admin.list_topics(timeout=10)
    print("Available topics:")
    for name in sorted(metadata.topics):
        print(f"- {name}")


# %% 3 - Run this cell to create the topic
if __name__ == "__main__":
    create_topic()
