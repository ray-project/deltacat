import pytest


class TestIntegerRangeDict:
    def test_setitem_with_int_key(self):
        from deltacat.utils.rangedictionary import IntegerRangeDict

        d = IntegerRangeDict()
        d[1] = "a"
        assert d[1] == "a"

    def test_setitem_with_non_int_key(self):
        from deltacat.utils.rangedictionary import IntegerRangeDict

        d = IntegerRangeDict()
        with pytest.raises(ValueError):
            d["a"] = "b"

    def test_getexactitem_with_int_key(self):
        from deltacat.utils.rangedictionary import IntegerRangeDict

        d = IntegerRangeDict()
        d[1] = "a"
        d[3] = "c"
        assert d[1] == "a"
        assert d[3] == "c"

    def test_getitem_with_non_int_key(self):
        from deltacat.utils.rangedictionary import IntegerRangeDict

        d = IntegerRangeDict()
        with pytest.raises(ValueError):
            d["a"]

    def test_getitem_with_next_greater_key(self):
        from deltacat.utils.rangedictionary import IntegerRangeDict

        d = IntegerRangeDict()
        d[1] = "a"
        d[3] = "c"
        assert d[2] == "c"

    def test_getitem_with_no_next_greater_key(self):
        from deltacat.utils.rangedictionary import IntegerRangeDict

        d = IntegerRangeDict()
        d[1] = "a"
        d[3] = "c"
        with pytest.raises(KeyError):
            d[4]

    def test_rebalance_sorted(self):
        from deltacat.utils.rangedictionary import IntegerRangeDict

        d = IntegerRangeDict()
        d[1] = "a"
        d[2] = "b"
        d[3] = "c"
        assert d.rebalance() is d

    def test_rebalance_unsorted(self):
        from deltacat.utils.rangedictionary import IntegerRangeDict

        d = IntegerRangeDict()
        d[3] = "c"
        d[1] = "a"
        d[2] = "b"
        d.rebalance()
        assert isinstance(d.rebalance(), IntegerRangeDict)
        assert list(d.keys()) == [1, 2, 3]
        assert list(d.values()) == ["a", "b", "c"]
