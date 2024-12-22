from deltacat.storage import EntryParams


class TestEntryParams:
    def test_of_default(self):
        ep = EntryParams.of()
        assert ep == {}
        assert ep.equality_field_locators is None

    def test_of_with_equality_column_names(self):
        equality_column_names = ["col1", "col2"]
        ep = EntryParams.of(equality_field_locators=equality_column_names)
        assert ep == {"equality_field_locators": equality_column_names}
        assert ep.equality_field_locators == equality_column_names

    def test_equality_column_names_property(self):
        ep = EntryParams()
        assert ep.equality_field_locators is None
        equality_column_names = ["col1", "col2"]
        ep["equality_field_locators"] = equality_column_names
        assert ep.equality_field_locators == equality_column_names
