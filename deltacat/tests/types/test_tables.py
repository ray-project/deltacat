import pytest
import pandas as pd
import pyarrow as pa

from deltacat.types.tables import (
    to_pandas,
    to_pyarrow,
    get_table_length,
)


def test_convert_to_pandas_error_cases():
    """Test convert_to_pandas with invalid inputs."""
    # Test None input
    with pytest.raises(
        ValueError, match="No pandas conversion function found for table type"
    ):
        to_pandas(None)

    # Test unsupported type
    with pytest.raises(
        ValueError, match="No pandas conversion function found for table type"
    ):
        to_pandas("invalid_string")

    # Test unsupported type with complex object
    with pytest.raises(
        ValueError, match="No pandas conversion function found for table type"
    ):
        to_pandas({"not": "a_dataframe"})


def test_convert_to_arrow_error_cases():
    """Test convert_to_arrow with invalid inputs."""
    # Test None input
    with pytest.raises(
        ValueError, match="No pyarrow conversion function found for table type"
    ):
        to_pyarrow(None)

    # Test unsupported type
    with pytest.raises(
        ValueError, match="No pyarrow conversion function found for table type"
    ):
        to_pyarrow("invalid_string")

    # Test unsupported type with complex object
    with pytest.raises(
        ValueError, match="No pyarrow conversion function found for table type"
    ):
        to_pyarrow({"not": "a_table"})


def test_conversion_functions_with_real_data():
    """Test conversion functions with actual data structures."""
    # Create test data
    test_df = pd.DataFrame({"id": [1, 2], "name": ["test1", "test2"]})
    test_table = pa.Table.from_pandas(test_df)

    # Test pandas conversion
    converted_df = to_pandas(test_df)
    assert isinstance(converted_df, pd.DataFrame)
    assert converted_df.equals(test_df)

    # Test arrow conversion
    converted_table = to_pyarrow(test_table)
    assert isinstance(converted_table, pa.Table)
    assert converted_table.equals(test_table)

    # Test cross-conversion
    df_from_table = to_pandas(test_table)
    table_from_df = to_pyarrow(test_df)
    assert isinstance(df_from_table, pd.DataFrame)
    assert isinstance(table_from_df, pa.Table)


def test_conversion_roundtrip_consistency():
    """Test that conversion functions maintain data integrity through roundtrips."""
    # Create test data
    original_df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Dave", "Eve"],
            "age": [25, 30, 35, 40, 45],
            "city": ["NYC", "LA", "Chicago", "Houston", "Phoenix"],
        }
    )

    # Test pandas -> arrow -> pandas roundtrip
    arrow_table = to_pyarrow(original_df)
    roundtrip_df = to_pandas(arrow_table)

    # Verify data integrity (allowing for potential type changes)
    assert get_table_length(original_df) == get_table_length(
        roundtrip_df
    ), "Row count should be preserved"
    assert list(original_df.columns) == list(
        roundtrip_df.columns
    ), "Column names should be preserved"

    # Verify ID column integrity (critical for merge operations)
    original_ids = sorted(original_df["id"].tolist())
    roundtrip_ids = sorted(roundtrip_df["id"].tolist())
    assert original_ids == roundtrip_ids, "ID column should be preserved exactly"
