"""Small text cleaning used by the consumer."""

# %% 1 - Regular expressions
import re


HTML_TAG = re.compile(r"<[^>]+>")
UNWANTED_CHARACTER = re.compile(r"[^\w\s']", flags=re.UNICODE)
WHITESPACE = re.compile(r"\s+")


# %% 2 - Cleaning function
def clean_line(text: str) -> str:
    """Put lowercase and remove HTML, punctuation and extra spaces."""
    text = text.lower()
    text = HTML_TAG.sub(" ", text)
    text = UNWANTED_CHARACTER.sub(" ", text)
    text = text.replace("_", " ")
    return WHITESPACE.sub(" ", text).strip()
