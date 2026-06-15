import pytest


class TestPsrioObjectInfo:
    def test_dict_entries_have_required_fields(self):
        from PSRTools.Parameters import DICT_PSRFILE_PSRIOOBJECT

        for key, info in DICT_PSRFILE_PSRIOOBJECT.items():
            assert info.object_type, f"{key} missing object_type"
            assert info.object_filename, f"{key} missing object_filename"
            assert info.operation in ("sum", "mean"), f"{key} has invalid operation"
            assert isinstance(info.factor, (int, float)), f"{key} factor not numeric"

    def test_psrio_commands_valid(self):
        from PSRTools.Parameters import PSRIO_COMMANDS

        assert "parquet" in PSRIO_COMMANDS
        assert "csv" in PSRIO_COMMANDS
