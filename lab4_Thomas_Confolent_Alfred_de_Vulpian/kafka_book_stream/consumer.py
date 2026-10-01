"""Consume book lines from Kafka, clean them and write them to a text file."""

import argparse
import os
from pathlib import Path

from confluent_kafka import Consumer, KafkaError

from text_cleaning import clean_line


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT = BASE_DIR / "output" / "cleaned_book.txt"
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "gutenberg-book-lines")
END_OF_BOOK = "__END_OF_BOOK__"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Destination text file (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--group-id",
        default="book-cleaner-v1",
        help="Kafka consumer group. Use a new value to replay the topic.",
    )
    parser.add_argument(
        "--max-idle-polls",
        type=int,
        default=15,
        help="Stop after this many one-second polls without a message.",
    )
    return parser.parse_args()


def consume_book(output_path: Path, group_id: str, max_idle_polls: int) -> None:
    """Consume and clean records until the producer's end marker is received."""
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])
    output_path.parent.mkdir(parents=True, exist_ok=True)

    received = 0
    written = 0
    idle_polls = 0
    reached_end = False

    try:
        with output_path.open("w", encoding="utf-8", newline="\n") as output:
            while idle_polls < max_idle_polls:
                message = consumer.poll(1.0)

                if message is None:
                    idle_polls += 1
                    continue

                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(f"Kafka consumer error: {message.error()}")

                idle_polls = 0
                value = message.value().decode("utf-8")

                if value == END_OF_BOOK:
                    reached_end = True
                    consumer.commit(message=message, asynchronous=False)
                    break

                received += 1
                cleaned = clean_line(value)
                if cleaned:
                    output.write(cleaned + "\n")
                    written += 1

        if received and not reached_end:
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()

    print(f"Received {received:,} book lines from topic '{TOPIC}'.")
    print(f"Wrote {written:,} cleaned lines to '{output_path}'.")
    if not reached_end:
        print("Warning: stopped after the idle timeout without an end marker.")


if __name__ == "__main__":
    arguments = parse_args()
    consume_book(arguments.output.resolve(), arguments.group_id, arguments.max_idle_polls)
