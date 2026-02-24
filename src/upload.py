import json
import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub import HfApi
from datasets import Dataset

logger = logging.getLogger(__name__)

SHARD_SIZE = 500_000  # rows per parquet shard


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


def _jsonl_to_parquet_shards(jsonl_path: Path, output_dir: Path, config_name: str) -> list[Path]:
    """Convert a JSONL file to Parquet shards without loading all into memory."""
    output_dir.mkdir(parents=True, exist_ok=True)
    shard_paths = []
    batch = []
    shard_idx = 0

    with open(jsonl_path, "r") as f:
        for line in f:
            batch.append(json.loads(line))
            if len(batch) >= SHARD_SIZE:
                shard_path = _write_shard(batch, output_dir, config_name, shard_idx)
                shard_paths.append(shard_path)
                logger.info(f"  Wrote shard {shard_idx} ({len(batch)} rows)")
                shard_idx += 1
                batch = []

    if batch:
        shard_path = _write_shard(batch, output_dir, config_name, shard_idx)
        shard_paths.append(shard_path)
        logger.info(f"  Wrote shard {shard_idx} ({len(batch)} rows)")

    return shard_paths


def _write_shard(rows: list[dict], output_dir: Path, config_name: str, shard_idx: int) -> Path:
    """Write a batch of rows to a Parquet file."""
    columnar = _rows_to_columnar(rows)
    table = pa.table(columnar)
    total_shards = "PLACEHOLDER"  # will be renamed later
    shard_path = output_dir / f"train-{shard_idx:05d}.parquet"
    pq.write_table(table, shard_path)
    return shard_path


def _rename_shards(shard_paths: list[Path]) -> list[Path]:
    """Rename shards to HF convention: train-XXXXX-of-YYYYY.parquet."""
    total = len(shard_paths)
    renamed = []
    for i, path in enumerate(shard_paths):
        new_name = f"train-{i:05d}-of-{total:05d}.parquet"
        new_path = path.parent / new_name
        path.rename(new_path)
        renamed.append(new_path)
    return renamed


def upload_from_jsonl(
    repo_id: str,
    books_path: Path,
    chapters_path: Path,
    paragraphs_path: Path,
) -> None:
    """Upload dataset from JSONL files — converts to Parquet shards first."""
    api = HfApi()

    # Ensure repo exists
    api.create_repo(repo_id, repo_type="dataset", exist_ok=True)

    configs = {
        "books": books_path,
        "chapters": chapters_path,
        "paragraphs": paragraphs_path,
    }

    parquet_base = books_path.parent.parent / "parquet"

    for config_name, jsonl_path in configs.items():
        logger.info(f"Converting {config_name} to Parquet shards...")
        config_dir = parquet_base / config_name
        shard_paths = _jsonl_to_parquet_shards(jsonl_path, config_dir, config_name)
        shard_paths = _rename_shards(shard_paths)
        logger.info(f"  {config_name}: {len(shard_paths)} shards")

    # Upload all parquet directories
    logger.info(f"Uploading parquet files to {repo_id}...")
    api.upload_large_folder(
        repo_id=repo_id,
        repo_type="dataset",
        folder_path=str(parquet_base),
    )
