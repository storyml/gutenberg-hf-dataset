"""Integration test: process a single real book end-to-end."""

import pytest
from pathlib import Path
from src.download import download_book_text, download_book_rdf
from src.metadata import parse_rdf
from src.clean import strip_gutenberg_headers
from src.build import process_book


@pytest.mark.integration
def test_end_to_end_single_book(tmp_path):
    """Download, parse, clean, and process Moby Dick."""
    book_id = "2701"

    # Download
    rdf_path = download_book_rdf(book_id, tmp_path)
    txt_path = download_book_text(book_id, tmp_path)

    # Parse metadata
    meta = parse_rdf(rdf_path)
    assert meta["title"] == "Moby Dick; Or, The Whale"
    assert meta["author"] == "Melville, Herman"
    assert meta["language"] == "en"

    # Clean text
    raw = txt_path.read_bytes()
    text = strip_gutenberg_headers(raw)
    assert "Call me Ishmael" in text
    assert "Project Gutenberg" not in text[:200]

    # Process into rows
    result = process_book(meta, text)
    assert result["book_row"]["id"] == "2701"
    assert result["book_row"]["has_chapters"] is True
    assert result["book_row"]["chapter_count"] > 10  # Moby Dick has 135 chapters
    assert len(result["chapter_rows"]) > 10
    assert len(result["paragraph_rows"]) > 100
