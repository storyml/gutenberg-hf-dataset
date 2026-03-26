import csv
import gzip
import logging
import time
from pathlib import Path

import requests

PG_BASE = "https://www.gutenberg.org"
PG_FEEDS = f"{PG_BASE}/cache/epub/feeds"
RATE_LIMIT_SECONDS = 2

logger = logging.getLogger(__name__)


def download_catalog(dest_dir: Path) -> Path:
    url = f"{PG_FEEDS}/pg_catalog.csv.gz"
    dest = dest_dir / "pg_catalog.csv.gz"
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    dest.write_bytes(resp.content)
    return dest


def parse_catalog_csv(csv_path: Path) -> list[dict]:
    if csv_path.suffix == ".gz":
        f = gzip.open(csv_path, "rt", encoding="utf-8")
    else:
        f = open(csv_path, "r", encoding="utf-8")

    with f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            book_id = row.get("Text#", row.get("id", ""))
            rows.append({"id": str(book_id).strip(), **row})
        return rows


def diff_catalogs(old_catalog: list[dict], new_catalog: list[dict]) -> set[str]:
    old_ids = {row["id"] for row in old_catalog}
    new_ids = {row["id"] for row in new_catalog}
    return new_ids - old_ids


def download_book_text(book_id: str, dest_dir: Path) -> Path:
    url = f"{PG_BASE}/cache/epub/{book_id}/pg{book_id}.txt"
    dest = dest_dir / f"{book_id}.txt"
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    dest.write_bytes(resp.content)
    time.sleep(RATE_LIMIT_SECONDS)
    return dest


def download_book_rdf(book_id: str, dest_dir: Path) -> Path:
    url = f"{PG_BASE}/cache/epub/{book_id}/pg{book_id}.rdf"
    dest = dest_dir / f"pg{book_id}.rdf"
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    dest.write_bytes(resp.content)
    time.sleep(RATE_LIMIT_SECONDS)
    return dest


def download_bulk_texts(dest_dir: Path) -> Path:
    url = f"{PG_FEEDS}/txt-files.tar.zip"
    dest = dest_dir / "txt-files.tar.zip"
    logger.info(f"Downloading {url} (this will take a while)...")
    resp = requests.get(url, timeout=3600, stream=True)
    resp.raise_for_status()
    downloaded = 0
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192 * 1024):
            f.write(chunk)
            downloaded += len(chunk)
            if downloaded % (100 * 1024 * 1024) < len(chunk):
                logger.info(f"  Downloaded {downloaded / 1024 / 1024:.0f} MB...")
    logger.info(f"  Download complete: {downloaded / 1024 / 1024:.0f} MB")
    return dest


def download_bulk_rdf(dest_dir: Path) -> Path:
    url = f"{PG_FEEDS}/rdf-files.tar.bz2"
    dest = dest_dir / "rdf-files.tar.bz2"
    logger.info(f"Downloading {url}...")
    resp = requests.get(url, timeout=600, stream=True)
    resp.raise_for_status()
    downloaded = 0
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192 * 1024):
            f.write(chunk)
            downloaded += len(chunk)
            if downloaded % (50 * 1024 * 1024) < len(chunk):
                logger.info(f"  Downloaded {downloaded / 1024 / 1024:.0f} MB...")
    logger.info(f"  Download complete: {downloaded / 1024 / 1024:.0f} MB")
    return dest
