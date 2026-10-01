import sys
import unittest
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_DIR))

from text_cleaning import clean_line


class CleanLineTests(unittest.TestCase):
    def test_lowercase_and_punctuation(self) -> None:
        self.assertEqual(clean_line("Hello, WORLD!"), "hello world")

    def test_html_and_extra_spaces(self) -> None:
        self.assertEqual(clean_line("A  <br>   new line."), "a new line")

    def test_apostrophes_and_unicode(self) -> None:
        self.assertEqual(clean_line("It's déjà ready."), "it's déjà ready")

    def test_empty_line(self) -> None:
        self.assertEqual(clean_line("  ---  "), "")


if __name__ == "__main__":
    unittest.main()
