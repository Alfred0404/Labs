# Lab 4 — Kafka book streaming

This project implements the producer/topic/consumer pipeline requested in the Kafka overview lab.

The producer reads *Around the World in Eighty Days* from Project Gutenberg line by line and sends every line to Kafka. The consumer reads the topic, cleans each line and writes the result to `output/cleaned_book.txt`.

## Project structure

```text
kafka_book_stream/
├── data/
│   └── around_the_world_in_80_days.txt
├── output/
├── tests/
│   └── test_text_cleaning.py
├── admin.py
├── consumer.py
├── docker-compose.yml
├── producer.py
├── requirements.txt
└── text_cleaning.py
```

## 1. Open the project

```bash
cd lab4_Thomas_Confolent_Alfred_de_Vulpian/kafka_book_stream
```

## 2. Create the Python environment

On Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

On Linux or macOS:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

## 3. Start Kafka

Make sure Docker Desktop is running, then start the Kafka broker:

```bash
docker compose up -d
docker compose ps
```

The compose file uses the `apache/kafka-native:4.1.1` image requested in the lab and exposes Kafka on `localhost:9092`.

## 4. Create the topic

```bash
python admin.py
```

This creates `gutenberg-book-lines` with one partition and a replication factor of one. Running the script again is safe: it reports that the topic already exists.

### Run with VS Code cells

The Python files also contain `# %%` cells. Open the `kafka_book_stream` folder directly in VS Code, select the `.venv` Python interpreter and run the cells in this order:

1. `admin.py`: cells 1, 2 and 3;
2. `producer.py`: cells 1, 2, 3 and 4;
3. `consumer.py`: cells 1, 2, 3 and 4.

We run the cells from top to bottom because each part uses variables and functions from the previous part.

## 5. Send the book

```bash
python producer.py
```

The producer sends the 8,312 lines of the book, including empty lines, followed by an end-of-book marker.

## 6. Consume and clean the text

```bash
python consumer.py
```

The consumer:

1. reads the records from the beginning of the topic;
2. converts text to lowercase;
3. removes punctuation and repeated whitespace;
4. skips lines that become empty;
5. writes the result to `output/cleaned_book.txt`.

To replay the topic, use a new consumer group:

```bash
python consumer.py --group-id book-cleaner-run-2
```

## 7. Check the result

On PowerShell:

```powershell
Get-Content .\output\cleaned_book.txt -TotalCount 20
(Get-Content .\output\cleaned_book.txt | Measure-Object -Line).Lines
```

On Linux or macOS:

```bash
head -n 20 output/cleaned_book.txt
wc -l output/cleaned_book.txt
```

The expected output contains 6,427 non-empty cleaned lines.

## 8. Run the cleaning tests

```bash
python -m unittest discover -s tests -v
```

## 9. Stop Kafka

```bash
docker compose down
```

## Configuration

The scripts use these optional environment variables:

- `KAFKA_BOOTSTRAP_SERVERS` (default: `localhost:9092`)
- `KAFKA_TOPIC` (default: `gutenberg-book-lines`)

Book source: [Project Gutenberg — Around the World in Eighty Days](https://www.gutenberg.org/ebooks/103).
