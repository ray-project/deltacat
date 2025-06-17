import pytest

from deltacat.storage import (
    SortKey,
    SortScheme,
    SortOrder,
    NullOrder,
)


def test_sort_scheme_validates_empty_keys():
    # When creating a sort scheme with empty keys list
    with pytest.raises(ValueError, match="Sort scheme cannot have empty keys list"):
        SortScheme.of(
            keys=[],
            name="test_sort_scheme",
            scheme_id="test_sort_scheme_id",
        )


def test_sort_scheme_validates_duplicate_keys():
    # When creating a sort scheme with duplicate keys
    with pytest.raises(ValueError, match="Duplicate sort key found: col1"):
        SortScheme.of(
            keys=[
                SortKey.of(
                    key=["col1"],
                    sort_order=SortOrder.ASCENDING,
                    null_order=NullOrder.AT_END,
                ),
                SortKey.of(
                    key=["col1"],  # Duplicate key
                    sort_order=SortOrder.DESCENDING,
                    null_order=NullOrder.AT_START,
                ),
            ],
            name="test_sort_scheme",
            scheme_id="test_sort_scheme_id",
        )


def test_sort_scheme_allows_valid_keys():
    # When creating a sort scheme with valid keys
    sort_scheme = SortScheme.of(
        keys=[
            SortKey.of(
                key=["col1"],
                sort_order=SortOrder.ASCENDING,
                null_order=NullOrder.AT_END,
            ),
            SortKey.of(
                key=["col2"],
                sort_order=SortOrder.DESCENDING,
                null_order=NullOrder.AT_END,
            ),
        ],
        name="test_sort_scheme",
        scheme_id="test_sort_scheme_id",
    )

    # Then it should succeed
    assert sort_scheme is not None
    assert len(sort_scheme.keys) == 2
    assert sort_scheme.name == "test_sort_scheme"
    assert sort_scheme.id == "test_sort_scheme_id"


def test_sort_scheme_validates_null_order_consistency():
    # When creating a sort scheme with inconsistent null orders
    with pytest.raises(
        ValueError, match="All arrow sort keys must use the same null order"
    ):
        sort_scheme = SortScheme.of(
            keys=[
                SortKey.of(
                    key=["col1"],
                    sort_order=SortOrder.ASCENDING,
                    null_order=NullOrder.AT_END,
                ),
                SortKey.of(
                    key=["col2"],
                    sort_order=SortOrder.DESCENDING,
                    null_order=NullOrder.AT_START,  # Different null order
                ),
            ],
            name="test_sort_scheme",
            scheme_id="test_sort_scheme_id",
        )
        # Access arrow property to trigger validation
        sort_scheme.arrow
