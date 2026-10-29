import pytest
from unittest.mock import patch, MagicMock
from utils import load_history, add_to_history, convert_to_short_path


class TestLoadHistory:
    def test_returns_empty_when_no_file(self, tmp_path):
        with patch("utils.HISTORY_FILE", str(tmp_path / "nonexistent.json")):
            assert load_history() == []

    def test_filters_nonexistent_dirs(self, tmp_path):
        import json

        history_file = tmp_path / "history.json"
        existing_dir = tmp_path / "exists"
        existing_dir.mkdir()
        history_file.write_text(
            json.dumps([str(existing_dir), str(tmp_path / "gone")])
        )
        with patch("utils.HISTORY_FILE", str(history_file)):
            result = load_history()
        assert result == [str(existing_dir)]


class TestAddToHistory:
    def test_adds_new_path_to_front(self):
        history = ["/a", "/b"]
        result = add_to_history(history, "/c")
        assert result[0] == "/c"

    def test_moves_existing_path_to_front(self):
        history = ["/a", "/b", "/c"]
        result = add_to_history(history, "/b")
        assert result[0] == "/b"
        assert result.count("/b") == 1

    def test_caps_at_max_history(self):
        history = [f"/{i}" for i in range(10)]
        result = add_to_history(history, "/new")
        assert len(result) == 10
        assert result[0] == "/new"
