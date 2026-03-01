import json
import logging
import shutil
import tarfile
import zipfile
from pathlib import Path

from src.chunk import detect_chapters, split_paragraphs
from src.download import (
    download_catalog,
    download_bulk_rdf,
    download_bulk_texts,
    parse_catalog_csv,
    download_book_text,
    download_book_rdf,
    diff_catalogs,
)
from src.metadata import parse_rdf
from src.clean import strip_gutenberg_headers
from src.dedup import deduplicate_catalog
from src.upload import upload_dataset, upload_from_jsonl

logger = logging.getLogger(__name__)


def process_book(meta: dict, text: str) -> dict:
    """Process a single book into rows for all three dataset configs."""
    chapters = detect_chapters(text)
    has_chapters = len(chapters) > 1 or (
        len(chapters) == 1 and chapters[0]["chapter_title"] is not None
    )

    book_row = {
        **meta,
        "has_chapters": has_chapters,
        "chapter_count": len(chapters),
        "text": text,
    }

    chapter_rows = []
    paragraph_rows = []

    for chapter in chapters:
        chapter_rows.append({
            "id": meta["id"],
            "chapter_index": chapter["chapter_index"],
            "chapter_title": chapter["chapter_title"],
            "text": chapter["text"],
        })

        paragraphs = split_paragraphs(chapter["text"])
        for para_idx, para_text in enumerate(paragraphs):
            paragraph_rows.append({
                "id": meta["id"],
                "chapter_index": chapter["chapter_index"],
                "paragraph_index": para_idx,
                "text": para_text,
            })

    return {
        "book_row": book_row,
        "chapter_rows": chapter_rows,
        "paragraph_rows": paragraph_rows,
    }


def full_build(repo_id: str, data_dir: Path, dedup: bool = True) -> None:
    """Run the full build: download everything, process, upload."""
    raw_dir = data_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    # 1. Download bulk files (skip if already present)
    catalog_path = raw_dir / "pg_catalog.csv.gz"
    if not catalog_path.exists():
        logger.info("Downloading catalog...")
        catalog_path = download_catalog(raw_dir)
    else:
        logger.info("Catalog already downloaded, reusing")
    catalog = parse_catalog_csv(catalog_path)
    logger.info(f"Catalog has {len(catalog)} entries")

    if dedup:
        catalog, removed = deduplicate_catalog(catalog)
        logger.info(f"After dedup: {len(catalog)} entries ({len(removed)} removed)")

    rdf_archive = raw_dir / "rdf-files.tar.bz2"
    if not rdf_archive.exists():
        logger.info("Downloading RDF metadata archive...")
        rdf_archive = download_bulk_rdf(raw_dir)
    else:
        logger.info("RDF archive already downloaded, reusing")

    txt_archive = raw_dir / "txt-files.tar.zip"
    if not txt_archive.exists():
        logger.info("Downloading text archive...")
        txt_archive = download_bulk_texts(raw_dir)
    else:
        logger.info("Text archive already downloaded, reusing")

    # 2. Extract RDF files (skip if already extracted)
    rdf_dir = raw_dir / "rdf"
    rdf_dir.mkdir(exist_ok=True)
    if not (rdf_dir / "cache" / "epub").exists():
        logger.info("Extracting RDF files...")
        with tarfile.open(rdf_archive, "r:bz2") as tar:
            tar.extractall(path=rdf_dir, filter="data")
    else:
        logger.info("RDF files already extracted, reusing")

    # 3. Extract text files (skip if already extracted)
    txt_dir = raw_dir / "txt"
    txt_dir.mkdir(exist_ok=True)
    inner_tar = txt_dir / "txt-files.tar"
    if not (txt_dir / "cache" / "epub").exists():
        logger.info("Extracting text files...")
        if not inner_tar.exists():
            with zipfile.ZipFile(txt_archive, "r") as zf:
                zf.extractall(path=txt_dir)
        with tarfile.open(inner_tar, "r") as tar:
            tar.extractall(path=txt_dir, filter="data")
    else:
        logger.info("Text files already extracted, reusing")

    # 4. Process all books — stream to JSONL files to avoid OOM
    jsonl_dir = data_dir / "jsonl"
    jsonl_dir.mkdir(exist_ok=True)
    books_path = jsonl_dir / "books.jsonl"
    chapters_path = jsonl_dir / "chapters.jsonl"
    paragraphs_path = jsonl_dir / "paragraphs.jsonl"

    errors = []
    total = len(catalog)
    book_count = 0
    chapter_count = 0
    paragraph_count = 0

    with open(books_path, "w") as bf, \
         open(chapters_path, "w") as cf, \
         open(paragraphs_path, "w") as pf:

        for i, entry in enumerate(catalog):
            book_id = entry["id"]
            if not book_id or not book_id.isdigit():
                continue

            if (i + 1) % 1000 == 0:
                logger.info(f"Progress: {i + 1}/{total} ({book_count} processed, {len(errors)} errors)")

            try:
                rdf_path = rdf_dir / "cache" / "epub" / book_id / f"pg{book_id}.rdf"
                if not rdf_path.exists():
                    continue

                meta = parse_rdf(rdf_path)

                txt_path = txt_dir / "cache" / "epub" / book_id / f"pg{book_id}.txt"
                if not txt_path.exists():
                    continue

                raw_bytes = txt_path.read_bytes()
                clean_text = strip_gutenberg_headers(raw_bytes)

                if not clean_text.strip():
                    continue

                result = process_book(meta, clean_text)
                bf.write(json.dumps(result["book_row"]) + "\n")
                book_count += 1
                for ch in result["chapter_rows"]:
                    cf.write(json.dumps(ch) + "\n")
                    chapter_count += 1
                for p in result["paragraph_rows"]:
                    pf.write(json.dumps(p) + "\n")
                    paragraph_count += 1

            except Exception as e:
                errors.append((book_id, str(e)))
                logger.error(f"Error processing book {book_id}: {e}")

    logger.info(
        f"Processed {book_count} books, "
        f"{chapter_count} chapters, "
        f"{paragraph_count} paragraphs. "
        f"{len(errors)} errors."
    )

    # 5. Upload from JSONL files
    logger.info(f"Uploading to {repo_id}...")
    upload_from_jsonl(repo_id, books_path, chapters_path, paragraphs_path)
    logger.info("Upload complete!")

    # 6. Save catalog snapshot
    snapshot_dir = data_dir / "snapshots"
    snapshot_dir.mkdir(exist_ok=True)
    shutil.copy2(catalog_path, snapshot_dir / "pg_catalog.csv.gz")

    if errors:
        errors_path = data_dir / "build_errors.json"
        errors_path.write_text(json.dumps(errors, indent=2))
        logger.warning(f"Errors saved to {errors_path}")


def incremental_build(repo_id: str, data_dir: Path, dedup: bool = True) -> None:
    """Run incremental update: diff catalog, process new books, append."""
    raw_dir = data_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    snapshot_dir = data_dir / "snapshots"

    logger.info("Downloading fresh catalog...")
    new_catalog_path = download_catalog(raw_dir)
    new_catalog = parse_catalog_csv(new_catalog_path)

    if dedup:
        new_catalog, removed = deduplicate_catalog(new_catalog)
        logger.info(f"After dedup: {len(new_catalog)} entries ({len(removed)} removed)")

    old_snapshot = snapshot_dir / "pg_catalog.csv.gz"
    if old_snapshot.exists():
        old_catalog = parse_catalog_csv(old_snapshot)
    else:
        old_catalog = []

    new_ids = diff_catalogs(old_catalog, new_catalog)
    logger.info(f"Found {len(new_ids)} new books")

    if not new_ids:
        logger.info("No new books, nothing to do")
        return

    # If there's no previous snapshot, this is effectively a first run.
    # Use bulk download instead of fetching books one at a time.
    if not old_catalog:
        logger.info(
            f"No previous snapshot — running full build via bulk download "
            f"instead of fetching {len(new_ids)} books individually"
        )
        full_build(repo_id, data_dir, dedup=dedup)
        return

    all_book_rows = []
    all_chapter_rows = []
    all_paragraph_rows = []
    errors = []

    for book_id in sorted(new_ids):
        try:
            rdf_path = download_book_rdf(book_id, raw_dir)
            meta = parse_rdf(rdf_path)

            txt_path = download_book_text(book_id, raw_dir)
            raw_bytes = txt_path.read_bytes()
            clean_text = strip_gutenberg_headers(raw_bytes)

            if not clean_text.strip():
                logger.warning(f"Empty text for book {book_id}, skipping")
                continue

            result = process_book(meta, clean_text)
            all_book_rows.append(result["book_row"])
            all_chapter_rows.extend(result["chapter_rows"])
            all_paragraph_rows.extend(result["paragraph_rows"])

        except Exception as e:
            errors.append((book_id, str(e)))
            logger.error(f"Error processing book {book_id}: {e}")

    logger.info(f"Processed {len(all_book_rows)} new books")

    if all_book_rows:
        logger.info(f"Uploading {len(all_book_rows)} new books to {repo_id}...")
        upload_dataset(repo_id, all_book_rows, all_chapter_rows, all_paragraph_rows)

    snapshot_dir.mkdir(exist_ok=True)
    shutil.copy2(new_catalog_path, old_snapshot)
    logger.info("Snapshot updated")
