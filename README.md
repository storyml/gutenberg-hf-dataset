# Gutenberg HuggingFace Dataset Pipeline

Pipeline to build and maintain a comprehensive HuggingFace dataset from Project Gutenberg.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Usage

```bash
# Full build (local, one-time)
python -m src --full --repo-id user/gutenberg-corpus

# Incremental update (used by GitHub Actions)
python -m src --incremental --repo-id user/gutenberg-corpus
```
