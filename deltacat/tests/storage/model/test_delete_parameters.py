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

    def test_merge_parameters_single(self):
        equality_column_names = ["col1", "col2"]
        dps = [EntryParams.of(equality_column_names=equality_column_names)]
        merged = EntryParams.merge(dps)
        assert merged == dps

    def test_merge_parameters_multiple_same(self):
        equality_column_names = ["col1", "col2"]
        dps = [
            EntryParams.of(equality_column_names=equality_column_names),
            EntryParams.of(equality_column_names=equality_column_names),
        ]
        merged = EntryParams.merge(dps)
        assert merged == dps[0]

    def test_merge_parameters_multiple_different(self):
        dps = [
            EntryParams.of(equality_column_names=["col1", "col2"]),
            EntryParams.of(equality_column_names=["col2", "col3"]),
        ]
        with pytest.raises(AssertionError):
            EntryParams.merge(dps)

    def test_merge_parameters_empty(self):
        merged = EntryParams.merge([])
        assert merged == []

    def test_merge_parameters_single_element(self):
        dp = EntryParams.of(equality_column_names=["col1", "col2"])
        merged = EntryParams.merge([dp])
        assert merged[0] == dp
