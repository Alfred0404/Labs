# Lab 4 — Kafka book streaming

This project make the producer/topic/consumer pipeline asked in the Kafka overview lab.

The producer read *Around the World in Eighty Days* from Project Gutenberg line by line and send each line in Kafka. After, the consumer read the topic, clean the lines and write the result in `output/cleaned_book.txt`.

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

We need to verify Docker Desktop is started, then we can start the Kafka broker:

```bash
docker compose up -d
docker compose ps
```

The compose file use the image `apache/kafka-native:4.1.1` asked in the lab. Kafka is available on `localhost:9092`.

## 4. Create the topic

```bash
python admin.py
```

This create `gutenberg-book-lines` with one partition and a replication factor of one. If we launch again, it just say the topic already exist.

### Run with VS Code cells

The Python files contain also `# %%` cells. We open the folder `kafka_book_stream` in VS Code, select the Python `.venv` and run cells in this order:

1. `admin.py`: cells 1, 2 and 3;
2. `producer.py`: cells 1, 2, 3 and 4;
3. `consumer.py`: cells 1, 2, 3 and 4.

We run cells from top to bottom because every part need variables and functions created before.

## 5. Send the book

```bash
python producer.py
```

The producer send the 8,312 lines of the book, also the empty lines. At the end it send a marker to say the book is finished.

## 6. Consume and clean the text

```bash
python consumer.py
```

The consumer do these steps:

1. reads the records from the beginning of the topic;
2. converts text to lowercase;
3. removes punctuation and repeated whitespace;
4. skips lines that become empty;
5. writes the result to `output/cleaned_book.txt`.

For read again the topic, we use a new consumer group:

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

Normally the output have 6,427 cleaned lines who are not empty.

## 8. Run the cleaning tests

```bash
python -m unittest discover -s tests -v
```

## 9. Stop Kafka

```bash
docker compose down
```

## Configuration

The scripts can use these optional environment variables:

- `KAFKA_BOOTSTRAP_SERVERS` (default: `localhost:9092`)
- `KAFKA_TOPIC` (default: `gutenberg-book-lines`)

Book source: [Project Gutenberg — Around the World in Eighty Days](https://www.gutenberg.org/ebooks/103).
