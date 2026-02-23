from src.dedup import deduplicate_catalog, normalize_title, is_split_volume


class TestNormalizeTitle:
    def test_strips_leading_articles(self):
        assert normalize_title("The Adventures of Tom Sawyer") == "adventures of tom sawyer"
        assert normalize_title("A Tale of Two Cities") == "tale of two cities"
        assert normalize_title("An Old Curiosity Shop") == "old curiosity shop"

    def test_lowercases(self):
        assert normalize_title("MOBY DICK") == "moby dick"

    def test_strips_subtitle_after_semicolon(self):
        assert normalize_title("Moby Dick; Or, The Whale") == "moby dick"

    def test_strips_subtitle_after_colon(self):
        assert normalize_title("Frankenstein: or, The Modern Prometheus") == "frankenstein"

    def test_strips_trailing_whitespace(self):
        assert normalize_title("  Moby Dick  ") == "moby dick"


class TestIsSplitVolume:
    def test_detects_part_n(self):
        assert is_split_volume("The Adventures of Tom Sawyer, Part 1") is True
        assert is_split_volume("The Adventures of Tom Sawyer, Part 7") is True

    def test_detects_volume_n(self):
        assert is_split_volume("War and Peace, Volume 1") is True
        assert is_split_volume("War and Peace, Vol. 2") is True

    def test_not_split_for_complete(self):
        assert is_split_volume("The Adventures of Tom Sawyer, Complete") is False

    def test_not_split_for_normal_title(self):
        assert is_split_volume("Moby Dick; Or, The Whale") is False

    def test_detects_book_n_of_m(self):
        assert is_split_volume("Don Quixote, Book 1 of 4") is True


class TestDeduplicateCatalog:
    def test_removes_split_volumes_when_complete_exists(self):
        catalog = [
            {"id": "74", "title": "The Adventures of Tom Sawyer, Complete", "author": "Twain, Mark", "release_date": "2006-01-01"},
            {"id": "7193", "title": "The Adventures of Tom Sawyer, Part 1", "author": "Twain, Mark", "release_date": "2004-01-01"},
            {"id": "7194", "title": "The Adventures of Tom Sawyer, Part 2", "author": "Twain, Mark", "release_date": "2004-01-01"},
        ]
        result, removed = deduplicate_catalog(catalog)
        assert len(result) == 1
        assert result[0]["id"] == "74"
        assert len(removed) == 2

    def test_keeps_split_volumes_when_no_complete(self):
        catalog = [
            {"id": "7193", "title": "The Adventures of Tom Sawyer, Part 1", "author": "Twain, Mark", "release_date": "2004-01-01"},
            {"id": "7194", "title": "The Adventures of Tom Sawyer, Part 2", "author": "Twain, Mark", "release_date": "2004-01-01"},
        ]
        result, removed = deduplicate_catalog(catalog)
        assert len(result) == 2  # keep them since no complete version
        assert len(removed) == 0

    def test_prefers_most_recent_among_duplicates(self):
        catalog = [
            {"id": "1", "title": "Moby Dick", "author": "Melville, Herman", "release_date": "1995-01-01"},
            {"id": "2", "title": "Moby Dick; Or, The Whale", "author": "Melville, Herman", "release_date": "2001-07-01"},
        ]
        result, removed = deduplicate_catalog(catalog)
        assert len(result) == 1
        assert result[0]["id"] == "2"  # most recent

    def test_different_authors_not_deduped(self):
        catalog = [
            {"id": "1", "title": "Hamlet", "author": "Shakespeare, William", "release_date": "2000-01-01"},
            {"id": "2", "title": "Hamlet", "author": "Different Author", "release_date": "2001-01-01"},
        ]
        result, removed = deduplicate_catalog(catalog)
        assert len(result) == 2

    def test_empty_catalog(self):
        result, removed = deduplicate_catalog([])
        assert result == []
        assert removed == []

    def test_single_entry(self):
        catalog = [{"id": "1", "title": "Test", "author": "Author", "release_date": "2000-01-01"}]
        result, removed = deduplicate_catalog(catalog)
        assert len(result) == 1
