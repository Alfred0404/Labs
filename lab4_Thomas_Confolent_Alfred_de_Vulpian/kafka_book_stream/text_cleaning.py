"""Pure text-cleaning functions used by the Kafka consumer."""

import re


HTML_TAG = re.compile(r"<[^>]+>")
UNWANTED_CHARACTER = re.compile(r"[^\w\s']", flags=re.UNICODE)
WHITESPACE = re.compile(r"\s+")


def clean_line(text: str) -> str:
    """Lowercase a line and remove HTML, punctuation and extra whitespace."""
    text = text.lower()
    text = HTML_TAG.sub(" ", text)
    text = UNWANTED_CHARACTER.sub(" ", text)
    text = text.replace("_", " ")
    return WHITESPACE.sub(" ", text).strip()
