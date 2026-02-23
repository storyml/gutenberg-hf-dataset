from pathlib import Path

from datasets import Dataset


def _rows_to_columnar(rows: list[dict]) -> dict[str, list]:
    if not rows:
        return {}
    keys = rows[0].keys()
    return {k: [row[k] for row in rows] for k in keys}


def upload_dataset(
    repo_id: str,
    book_rows: list[dict],
    chapter_rows: list[dict],
    paragraph_rows: list[dict],
) -> None:
    configs = {
        "books": book_rows,
        "chapters": chapter_rows,
        "paragraphs": paragraph_rows,
    }

    for config_name, rows in configs.items():
        ds = Dataset.from_dict(_rows_to_columnar(rows))
        ds.push_to_hub(
            repo_id,
            config_name=config_name,
            split="train",
            private=False,
        )


def upload_from_jsonl(
    repo_id: str,
    books_path: Path,
    chapters_path: Path,
    paragraphs_path: Path,
) -> None:
    """Upload dataset from JSONL files on disk (memory-efficient)."""
    configs = {
        "books": books_path,
        "chapters": chapters_path,
        "paragraphs": paragraphs_path,
    }

    for config_name, path in configs.items():
        ds = Dataset.from_json(str(path))
        ds.push_to_hub(
            repo_id,
            config_name=config_name,
            split="train",
            private=False,
        )
