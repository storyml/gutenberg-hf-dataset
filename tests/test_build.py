from src.build import process_book


def test_process_book_returns_expected_keys():
    meta = {
        "id": "1", "title": "Test", "author": "Author",
        "author_birth_year": None, "author_death_year": None,
        "contributors": [], "subjects": [], "bookshelves": [],
        "locc": "", "language": "en", "release_date": "2000-01-01",
        "rights": "Public domain in the USA.", "summary": None,
    }
    text = "CHAPTER 1\n\nFirst paragraph.\n\nSecond paragraph.\n\nCHAPTER 2\n\nThird paragraph."
    result = process_book(meta, text)
    assert "book_row" in result
    assert "chapter_rows" in result
    assert "paragraph_rows" in result


def test_process_book_sets_has_chapters():
    meta = {
        "id": "1", "title": "Test", "author": "Author",
        "author_birth_year": None, "author_death_year": None,
        "contributors": [], "subjects": [], "bookshelves": [],
        "locc": "", "language": "en", "release_date": "",
        "rights": "", "summary": None,
    }
    text_with = "CHAPTER 1\n\nContent.\n\nCHAPTER 2\n\nMore content."
    result = process_book(meta, text_with)
    assert result["book_row"]["has_chapters"] is True
    assert result["book_row"]["chapter_count"] == 2

    text_without = "Just plain text here.\n\nAnother paragraph."
    result2 = process_book(meta, text_without)
    assert result2["book_row"]["has_chapters"] is False
    assert result2["book_row"]["chapter_count"] == 1


def test_process_book_paragraph_rows_have_correct_fields():
    meta = {
        "id": "42", "title": "Test", "author": "Author",
        "author_birth_year": None, "author_death_year": None,
        "contributors": [], "subjects": [], "bookshelves": [],
        "locc": "", "language": "en", "release_date": "",
        "rights": "", "summary": None,
    }
    text = "A substantial first paragraph with enough text.\n\nA substantial second paragraph with enough text."
    result = process_book(meta, text)
    for row in result["paragraph_rows"]:
        assert "id" in row
        assert "chapter_index" in row
        assert "paragraph_index" in row
        assert "text" in row
        assert row["id"] == "42"
