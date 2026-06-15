import pytest
from unittest.mock import MagicMock, patch


class TestLevelsValidation:
    def test_valid_levels_accepted(self):
        with patch("psr.factory.Study"):
            from PSRTools.PSRIOCommand import PSRIOCommand

            study = MagicMock()
            # Should not raise
            cmd = PSRIOCommand(study, "/path", "parquet", "YMD", "", "gerter", "")
            assert cmd.levels == "YMD"

    def test_empty_levels_defaults_to_X(self):
        with patch("psr.factory.Study"):
            from PSRTools.PSRIOCommand import PSRIOCommand

            study = MagicMock()
            cmd = PSRIOCommand(study, "/path", "parquet", "", "", "gerter", "")
            assert cmd.levels == "X"

    def test_invalid_levels_raises(self):
        with patch("psr.factory.Study"):
            from PSRTools.PSRIOCommand import PSRIOCommand

            study = MagicMock()
            with pytest.raises(ValueError, match="Invalid level characters"):
                PSRIOCommand(study, "/path", "parquet", "YZQ", "", "gerter", "")
