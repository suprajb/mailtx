# MailTx

A local-first AI spend analyzer in Python. Heavily in development.
Currently supports (via CLI) queries like "How much did I spend on Uber last month?"

> **Note:** Current v0 supports receipt ingestion via Gmail only.


## Setup

1.  Install `uv` (if not installed).
2.  Place `credentials.json` (Gmail API) in the root directory.
3.  Install dependencies:
    ```bash
    uv sync
    ```

## Usage

The main entry point is `main.py`.

### 1. Ingest Emails
Download and parse emails from Gmail.
```bash
uv run main.py ingest --days 90
```

### 2. Generate Embeddings
Generate vector embeddings for semantic search.
```bash
uv run main.py embed
```

### 3. Extract Transactions
Run the LLM extraction pipeline to populate the ledger.
```bash
uv run main.py extract
```

### 4. Ask Questions
Query your spending data using natural language.
```bash
uv run main.py ask "How much did I spend on Amazon last month?"
```

## Requirements

- Python 3.12+
- Ollama (running locally with `llama3.2` and `nomic-embed-text` models)
