from pathlib import Path
from src.chunk import detect_chapters, split_paragraphs


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
