import pytest

from deltacat.storage import EntryParams


class TestEntryParams:
    def test_of_default(self):
        dp = EntryParams.of()
        assert dp == {}
        assert dp.equality_column_names is None

    def test_of_with_equality_column_names(self):
        equality_column_names = ["col1", "col2"]
        dp = EntryParams.of(equality_column_names=equality_column_names)
        assert dp == {"equality_column_names": equality_column_names}
        assert dp.equality_column_names == equality_column_names

    def test_equality_column_names_property(self):
        dp = EntryParams()
        assert dp.equality_column_names is None
        equality_column_names = ["col1", "col2"]
        dp["equality_column_names"] = equality_column_names
        assert dp.equality_column_names == equality_column_names
