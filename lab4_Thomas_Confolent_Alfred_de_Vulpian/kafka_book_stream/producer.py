"""Send a Project Gutenberg book to Kafka, one record per source line."""

import argparse
import os
import socket
from pathlib import Path

from confluent_kafka import Producer


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_BOOK = BASE_DIR / "data" / "around_the_world_in_80_days.txt"
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "gutenberg-book-lines")
END_OF_BOOK = "__END_OF_BOOK__"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--book",
        type=Path,
        default=DEFAULT_BOOK,
        help=f"UTF-8 text file to stream (default: {DEFAULT_BOOK})",
    )
    return parser.parse_args()


def send_book(book_path: Path) -> None:
    """Read *book_path* and send every line to the configured Kafka topic."""
    if not book_path.is_file():
        raise FileNotFoundError(f"Book not found: {book_path}")

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "client.id": socket.gethostname(),
            "message.timeout.ms": 10_000,
        }
    )
    delivery_failures: list[str] = []

    def delivery_report(error, message) -> None:
        if error is not None:
            delivery_failures.append(str(error))

    line_count = 0
    with book_path.open("r", encoding="utf-8") as book:
        for line_count, line in enumerate(book, start=1):
            value = line.rstrip("\r\n")

            while True:
                try:
                    # A shared key keeps all lines in the same partition and preserves order.
                    producer.produce(
                        topic=TOPIC,
                        key=book_path.name,
                        value=value,
                        callback=delivery_report,
                    )
                    break
                except BufferError:
                    producer.poll(0.5)

            producer.poll(0)

    producer.produce(
        topic=TOPIC,
        key=book_path.name,
        value=END_OF_BOOK,
        callback=delivery_report,
    )
    undelivered = producer.flush(30)

    if delivery_failures or undelivered:
        details = delivery_failures[0] if delivery_failures else "flush timeout"
        raise RuntimeError(
            f"Kafka did not deliver every record ({undelivered} pending): {details}"
        )

    print(f"Sent {line_count:,} lines to topic '{TOPIC}'.")
    print("Sent the end-of-book marker.")


if __name__ == "__main__":
    send_book(parse_args().book.resolve())
