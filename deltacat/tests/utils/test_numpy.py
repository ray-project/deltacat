import pytest

from dataclasses import dataclass, fields
from typing import List, Optional
import numpy as np


class TestObjectValueAttribute:
    def __init__(self, value):
        self.value = value


class TestObjectFooAttribute:
    def __init__(self, foo):
        self.foo = foo


@dataclass(frozen=True)
class SearchSortedByAttrTestCaseParams:
    """
    A pytest parameterized test case for the `prepare_deletes` function.
    """

    attribute: str
    obj_arr: List[object]
    values_to_insert: List[int]
    side: Optional[str]
    expected_result: List[int]
    expected_error: BaseException

    # makes TestCaseParams iterable which is required to build the list of pytest.param values to pass to pytest.mark.parametrize
    def __iter__(self):
        return (getattr(self, field.name) for field in fields(self))


TEST_CASES_SEARCHSORTED_BY_ATTR = {
    "1-test-search-sorted-all-valid-right-bisect": SearchSortedByAttrTestCaseParams(
        attribute="value",
        obj_arr=[
            TestObjectValueAttribute(1),
            TestObjectValueAttribute(3),
            TestObjectValueAttribute(5),
            TestObjectValueAttribute(7),
        ],
        values_to_insert=[2, 4, 6],
        side="right",
        expected_result=[1, 2, 3],
        expected_error=None,
    ),
    "2-test-search-sorted-all-valid-left-bisect": SearchSortedByAttrTestCaseParams(
        attribute="value",
        obj_arr=[
            TestObjectValueAttribute(1),
            TestObjectValueAttribute(3),
            TestObjectValueAttribute(5),
            TestObjectValueAttribute(7),
        ],
        values_to_insert=[2, 4, 6],
        side="left",
        expected_result=[1, 2, 3],
        expected_error=None,
    ),
    "3-test-search-sorted-empty-object-arr": SearchSortedByAttrTestCaseParams(
        attribute="value",
        obj_arr=[],
        values_to_insert=[2, 4, 6],
        side="right",
        expected_result=[0, 0, 0],
        expected_error=None,
    ),
    "4-test-search-sorted-empty-values-to-insert": SearchSortedByAttrTestCaseParams(
        attribute="value",
        obj_arr=[
            TestObjectValueAttribute(1),
            TestObjectValueAttribute(3),
            TestObjectValueAttribute(5),
            TestObjectValueAttribute(7),
        ],
        values_to_insert=[],
        side="right",
        expected_result=[],
        expected_error=None,
    ),
    "5-test-search-sorted-non-existing-attribute": SearchSortedByAttrTestCaseParams(
        attribute="foo",
        obj_arr=[
            TestObjectValueAttribute(1),
            TestObjectValueAttribute(3),
            TestObjectValueAttribute(5),
            TestObjectValueAttribute(7),
            TestObjectFooAttribute(9),
        ],
        values_to_insert=[2, 4, 6],
        side="right",
        expected_result=[],
        expected_error=AttributeError,
    ),
    "6-test-search-sorted-invalid-side-args": SearchSortedByAttrTestCaseParams(
        attribute="foo",
        obj_arr=[
            TestObjectValueAttribute(1),
            TestObjectValueAttribute(3),
            TestObjectValueAttribute(5),
            TestObjectValueAttribute(7),
        ],
        values_to_insert=[2, 4, 6],
        side="FUZZ",
        expected_result=[],
        expected_error=AssertionError,
    ),
}


class TestNumpy:
    @pytest.mark.parametrize(
        [
            "test_name",
            "attribute",
            "obj_arr",
            "values_to_insert",
            "side",
            "expected_result",
            "expected_error",
        ],
        [
            (
                test_name,
                attribute,
                obj_arr,
                values_to_insert,
                side,
                expected_result,
                expected_error,
            )
            for test_name, (
                attribute,
                obj_arr,
                values_to_insert,
                side,
                expected_result,
                expected_error,
            ) in TEST_CASES_SEARCHSORTED_BY_ATTR.items()
        ],
        ids=[test_name for test_name in TEST_CASES_SEARCHSORTED_BY_ATTR],
    )
    def test_searchsorted_by_attr(
        self,
        test_name: str,
        attribute,
        obj_arr,
        values_to_insert,
        side,
        expected_result,
        expected_error,
    ):
        from deltacat.utils.numpy import searchsorted_by_attr

        if expected_error is not None:
            with pytest.raises(expected_error):
                searchsorted_by_attr(attribute, obj_arr, values_to_insert, side=side)
            return
        assert np.array_equal(
            searchsorted_by_attr(attribute, obj_arr, values_to_insert, side=side),
            expected_result,
        )
        return
