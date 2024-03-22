import pytest

from deltacat.storage.model.delete_parameters import DeleteParameters


class TestDeleteParameters:
    def test_of_default(self):
        dp = DeleteParameters.of()
        assert dp == {}
        assert dp.equality_column_names is None

    def test_of_with_equality_column_names(self):
        equality_column_names = ["col1", "col2"]
        dp = DeleteParameters.of(equality_column_names=equality_column_names)
        assert dp == {"equality_column_names": equality_column_names}
        assert dp.equality_column_names == equality_column_names

    def test_equality_column_names_property(self):
        dp = DeleteParameters()
        assert dp.equality_column_names is None
        equality_column_names = ["col1", "col2"]
        dp["equality_column_names"] = equality_column_names
        assert dp.equality_column_names == equality_column_names

    def test_merge_delete_parameters_single(self):
        equality_column_names = ["col1", "col2"]
        dps = [DeleteParameters.of(equality_column_names=equality_column_names)]
        merged = DeleteParameters.merge_delete_parameters(dps)
        assert merged == dps

    def test_merge_delete_parameters_multiple_same(self):
        equality_column_names = ["col1", "col2"]
        dps = [
            DeleteParameters.of(equality_column_names=equality_column_names),
            DeleteParameters.of(equality_column_names=equality_column_names),
        ]
        merged = DeleteParameters.merge_delete_parameters(dps)
        assert merged == dps[0]

    def test_merge_delete_parameters_multiple_different(self):
        dps = [
            DeleteParameters.of(equality_column_names=["col1", "col2"]),
            DeleteParameters.of(equality_column_names=["col2", "col3"]),
        ]
        with pytest.raises(AssertionError):
            DeleteParameters.merge_delete_parameters(dps)

    def test_merge_delete_parameters_empty(self):
        merged = DeleteParameters.merge_delete_parameters([])
        assert merged == []

    def test_merge_delete_parameters_single_element(self):
        dp = DeleteParameters.of(equality_column_names=["col1", "col2"])
        merged = DeleteParameters.merge_delete_parameters([dp])
        assert merged[0] == dp
