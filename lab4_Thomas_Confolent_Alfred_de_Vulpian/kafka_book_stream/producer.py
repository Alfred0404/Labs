"""Send the Gutenberg book to Kafka line by line."""

# %% 1 - Imports and configuration
import argparse
import os
import socket
from pathlib import Path

from confluent_kafka import Producer


# __file__ exist in script mode. Path.cwd() is used when we execute a VS Code cell.
BASE_DIR = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
DEFAULT_BOOK = BASE_DIR / "data" / "around_the_world_in_80_days.txt"
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "gutenberg-book-lines")
END_OF_BOOK = "__END_OF_BOOK__"


# %% 2 - Command line option
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--book",
        type=Path,
        default=DEFAULT_BOOK,
        help=f"UTF-8 text file to stream (default: {DEFAULT_BOOK})",
    )
    # parse_known_args work also when we run the file cell by cell in VS Code.
    return parser.parse_known_args()[0]


# %% 3 - Function to send the book
def send_book(book_path: Path) -> None:
    """Read the book and send each line in our topic."""
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
                    # We use the same key for keep the good order of the lines.
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


# %% 4 - Run this cell to send the book
if __name__ == "__main__":
    send_book(parse_args().book.resolve())
