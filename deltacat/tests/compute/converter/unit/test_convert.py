"""
Unit tests for convert step functions in deltacat.compute.converter.steps.convert.

This module contains comprehensive unit tests for the core convert functionality,
including equality delete conversion, position delete generation, and file processing logic.
"""

import pytest
import pyarrow as pa
from unittest.mock import Mock, patch
from typing import List

from deltacat.compute.converter.steps.convert import (
    get_additional_applicable_data_files,
    filter_rows_to_be_deleted,
    compute_pos_delete_converting_equality_deletes,
    compute_pos_delete_with_limited_parallelism,
)
import deltacat.compute.converter.utils.iceberg_columns as sc
from pyiceberg.manifest import DataFile


class TestConvertStepFunctions:
    """Test class for convert step functions."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        # Create mock DataFile objects
        self.mock_data_files = []
        for i in range(10):
            mock_file = Mock(spec=DataFile)
            mock_file.file_path = f"s3://bucket/data/file{i+1}.parquet"
            mock_file.file_size_in_bytes = 1000 * (i + 1)
            self.mock_data_files.append(mock_file)

    def create_mock_table_with_hash(
        self, identifier_values: List[str], file_path: str, record_values: List[int]
    ) -> pa.Table:
        """Helper method to create a mock PyArrow table with identifier hash column."""
        hash_values = [f"hash_{val}" for val in identifier_values]

        table = pa.Table.from_arrays(
            [
                pa.array(hash_values),
                pa.array([file_path] * len(identifier_values)),
                pa.array(record_values, type=pa.int64()),
            ],
            names=[
                sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME,
                sc._FILE_PATH_COLUMN_NAME,
                sc._ORDERED_RECORD_IDX_COLUMN_NAME,
            ],
        )

        return table

    def test_get_additional_applicable_data_files_basic(self):
        """Test basic functionality of getting additional applicable data files."""
        all_data_files = [
            (1, self.mock_data_files[0]),
            (2, self.mock_data_files[1]),
            (3, self.mock_data_files[2]),
            (4, self.mock_data_files[3]),
        ]

        data_files_downloaded = [
            (1, self.mock_data_files[0]),
            (2, self.mock_data_files[1]),
        ]

        result = get_additional_applicable_data_files(
            all_data_files, data_files_downloaded
        )

        # Should return files that are in all_data_files but not in data_files_downloaded
        expected = [
            (3, self.mock_data_files[2]),
            (4, self.mock_data_files[3]),
        ]

        # Convert to sets for comparison since order might differ
        result_set = set(result)
        expected_set = set(expected)
        assert result_set == expected_set

    def test_get_additional_applicable_data_files_no_downloaded_files(self):
        """Test when no files have been downloaded."""
        all_data_files = [
            (1, self.mock_data_files[0]),
            (2, self.mock_data_files[1]),
        ]

        data_files_downloaded = []

        result = get_additional_applicable_data_files(
            all_data_files, data_files_downloaded
        )

        # Should return all data files
        assert set(result) == set(all_data_files)

    def test_get_additional_applicable_data_files_all_downloaded(self):
        """Test when all files have been downloaded."""
        all_data_files = [
            (1, self.mock_data_files[0]),
            (2, self.mock_data_files[1]),
        ]

        data_files_downloaded = all_data_files.copy()

        result = get_additional_applicable_data_files(
            all_data_files, data_files_downloaded
        )

        # Should return empty list
        assert result == []

    def test_get_additional_applicable_data_files_assertion_error(self):
        """Test assertion error when downloaded files exceed all files."""
        all_data_files = [
            (1, self.mock_data_files[0]),
        ]

        data_files_downloaded = [
            (1, self.mock_data_files[0]),
            (2, self.mock_data_files[1]),  # Extra file not in all_data_files
        ]

        with pytest.raises(AssertionError, match="Length of all data files"):
            get_additional_applicable_data_files(all_data_files, data_files_downloaded)

    def test_filter_rows_to_be_deleted_with_matches(self):
        """Test filtering rows when there are matches between equality deletes and data."""
        # Create equality delete table
        equality_delete_table = pa.Table.from_arrays(
            [
                pa.array(["hash_user_1", "hash_user_3"]),
            ],
            names=[sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME],
        )

        # Create data file table
        data_file_table = self.create_mock_table_with_hash(
            ["user_1", "user_2", "user_3", "user_4"],
            "s3://bucket/data.parquet",
            [0, 1, 2, 3],
        )

        identifier_columns = ["user_id"]

        position_delete_table, remaining_data_table = filter_rows_to_be_deleted(
            equality_delete_table, data_file_table, identifier_columns
        )

        # Verify position delete table
        assert position_delete_table is not None
        assert len(position_delete_table) == 2  # user_1 and user_3 should be deleted

        # Verify remaining data table
        assert remaining_data_table is not None
        assert len(remaining_data_table) == 2  # user_2 and user_4 should remain

        # Verify total records match
        assert len(position_delete_table) + len(remaining_data_table) == len(
            data_file_table
        )

        # Verify position delete table doesn't have hash column
        assert (
            sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME
            not in position_delete_table.column_names
        )

    def test_filter_rows_to_be_deleted_no_matches(self):
        """Test filtering rows when there are no matches."""
        # Create equality delete table with non-matching hashes
        equality_delete_table = pa.Table.from_arrays(
            [
                pa.array(["hash_user_5", "hash_user_6"]),
            ],
            names=[sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME],
        )

        # Create data file table
        data_file_table = self.create_mock_table_with_hash(
            ["user_1", "user_2"], "s3://bucket/data.parquet", [0, 1]
        )

        identifier_columns = ["user_id"]

        position_delete_table, remaining_data_table = filter_rows_to_be_deleted(
            equality_delete_table, data_file_table, identifier_columns
        )

        # No matches, so position delete table should be empty
        assert len(position_delete_table) == 0

        # All data should remain
        assert len(remaining_data_table) == len(data_file_table)

    def test_filter_rows_to_be_deleted_empty_tables(self):
        """Test filtering with empty tables."""
        position_delete_table, remaining_data_table = filter_rows_to_be_deleted(
            None, None, ["user_id"]
        )

        assert position_delete_table is None
        assert remaining_data_table is None

    def test_filter_rows_to_be_deleted_empty_equality_delete_table(self):
        """Test filtering with empty equality delete table."""
        data_file_table = self.create_mock_table_with_hash(
            ["user_1", "user_2"], "s3://bucket/data.parquet", [0, 1]
        )

        position_delete_table, remaining_data_table = filter_rows_to_be_deleted(
            None, data_file_table, ["user_id"]
        )

        assert position_delete_table is None
        assert remaining_data_table is None

    def test_compute_pos_delete_converting_equality_deletes_with_deletes(self):
        """Test computing position deletes from equality deletes."""
        # Create equality delete table
        equality_delete_table = pa.Table.from_arrays(
            [
                pa.array(["hash_user_1"]),
            ],
            names=[sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME],
        )

        # Create data file table
        data_file_table = self.create_mock_table_with_hash(
            ["user_1", "user_2"], "s3://bucket/data.parquet", [0, 1]
        )

        identifier_columns = ["user_id"]
        warehouse_prefix = "s3://warehouse/table/data"

        (
            new_position_delete_table,
            remaining_data_table,
        ) = compute_pos_delete_converting_equality_deletes(
            equality_delete_table,
            data_file_table,
            identifier_columns,
            warehouse_prefix,
            None,
        )

        # Should have position deletes
        assert new_position_delete_table is not None
        assert len(new_position_delete_table) == 1

        # Should have remaining data
        assert remaining_data_table is not None
        assert len(remaining_data_table) == 1

    def test_compute_pos_delete_converting_equality_deletes_no_deletes(self):
        """Test computing position deletes when no deletes are needed."""
        # Create equality delete table with non-matching hashes
        equality_delete_table = pa.Table.from_arrays(
            [
                pa.array(["hash_user_3"]),
            ],
            names=[sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME],
        )

        # Create data file table
        data_file_table = self.create_mock_table_with_hash(
            ["user_1", "user_2"], "s3://bucket/data.parquet", [0, 1]
        )

        identifier_columns = ["user_id"]
        warehouse_prefix = "s3://warehouse/table/data"

        (
            new_position_delete_table,
            remaining_data_table,
        ) = compute_pos_delete_converting_equality_deletes(
            equality_delete_table,
            data_file_table,
            identifier_columns,
            warehouse_prefix,
            None,
        )

        # Should have no position deletes
        assert new_position_delete_table is None

        # Should have remaining data
        assert remaining_data_table is not None
        assert len(remaining_data_table) == 2

    def test_compute_pos_delete_converting_equality_deletes_empty_tables(self):
        """Test computing position deletes with empty tables."""
        (
            new_position_delete_table,
            remaining_data_table,
        ) = compute_pos_delete_converting_equality_deletes(
            None, None, ["user_id"], "s3://warehouse/table/data", None
        )

        assert new_position_delete_table is None
        assert remaining_data_table is None

    @patch(
        "deltacat.compute.converter.steps.convert.download_data_table_and_append_iceberg_columns"
    )
    @patch("deltacat.compute.converter.steps.convert.sort_data_files_maintaining_order")
    def test_compute_pos_delete_with_limited_parallelism_basic(
        self, mock_sort_files, mock_download_data
    ):
        """Test computing position deletes with limited parallelism."""
        # Setup input data
        data_files_list = [[(1, self.mock_data_files[0]), (2, self.mock_data_files[1])]]
        equality_delete_files_list = [[(3, self.mock_data_files[2])]]

        identifier_columns = ["user_id"]
        warehouse_prefix = "s3://warehouse/table/data"
        convert_task_index = 0
        max_parallel_download = 4

        # Mock sort function
        mock_sort_files.return_value = data_files_list[0]

        # Create mock tables
        data_table1 = self.create_mock_table_with_hash(
            ["user_1", "user_2"], "s3://bucket/file1.parquet", [0, 1]
        )
        data_table2 = self.create_mock_table_with_hash(
            ["user_3", "user_4"], "s3://bucket/file2.parquet", [2, 3]
        )
        equality_delete_table = pa.Table.from_arrays(
            [
                pa.array(["hash_user_1"]),
            ],
            names=[sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME],
        )

        # Mock download function to return different tables for different calls
        mock_download_data.side_effect = [
            data_table1,
            data_table2,
            equality_delete_table,
        ]

        (
            new_pos_delete_table,
            remaining_data_table,
        ) = compute_pos_delete_with_limited_parallelism(
            data_files_list,
            identifier_columns,
            equality_delete_files_list,
            warehouse_prefix,
            convert_task_index,
            max_parallel_download,
            None,
            None,
        )

        # Verify function calls
        assert (
            mock_download_data.call_count == 3
        )  # 2 data files + 1 equality delete file
        mock_sort_files.assert_called_once()

        # Verify results
        assert new_pos_delete_table is not None
        assert len(new_pos_delete_table) == 1  # One record should be deleted (user_1)

    @patch(
        "deltacat.compute.converter.steps.convert.download_data_table_and_append_iceberg_columns"
    )
    @patch("deltacat.compute.converter.steps.convert.sort_data_files_maintaining_order")
    def test_compute_pos_delete_with_limited_parallelism_no_deletes(
        self, mock_sort_files, mock_download_data
    ):
        """Test computing position deletes when no deletes are needed."""
        # Setup input data
        data_files_list = [[(1, self.mock_data_files[0])]]
        equality_delete_files_list = [[(2, self.mock_data_files[1])]]

        identifier_columns = ["user_id"]
        warehouse_prefix = "s3://warehouse/table/data"
        convert_task_index = 0
        max_parallel_download = 4

        # Mock sort function
        mock_sort_files.return_value = data_files_list[0]

        # Create mock tables with no matching records
        data_table = self.create_mock_table_with_hash(
            ["user_1", "user_2"], "s3://bucket/file1.parquet", [0, 1]
        )
        equality_delete_table = pa.Table.from_arrays(
            [
                pa.array(["hash_user_3"]),  # No match with data table
            ],
            names=[sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME],
        )

        mock_download_data.side_effect = [data_table, equality_delete_table]

        (
            new_pos_delete_table,
            remaining_data_table,
        ) = compute_pos_delete_with_limited_parallelism(
            data_files_list,
            identifier_columns,
            equality_delete_files_list,
            warehouse_prefix,
            convert_task_index,
            max_parallel_download,
            None,
            None,
        )

        # Should have no position deletes
        assert new_pos_delete_table is None

    def test_compute_pos_delete_with_limited_parallelism_assertion_error(self):
        """Test assertion error when data files and equality delete files lists have different lengths."""
        data_files_list = [[(1, self.mock_data_files[0])]]
        equality_delete_files_list = [
            [(2, self.mock_data_files[1])],
            [(3, self.mock_data_files[2])],  # Extra list
        ]

        with pytest.raises(
            AssertionError, match="Number of lists of data files should equal"
        ):
            compute_pos_delete_with_limited_parallelism(
                data_files_list,
                ["user_id"],
                equality_delete_files_list,
                "s3://warehouse/table/data",
                0,
                4,
                None,
                None,
            )


class TestConvertStepEdgeCases:
    """Test edge cases and error conditions for convert step functions."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_data_file = Mock(spec=DataFile)
        self.mock_data_file.file_path = "s3://bucket/test.parquet"
        self.mock_data_file.file_size_in_bytes = 1000

    def test_get_additional_applicable_data_files_empty_lists(self):
        """Test with empty input lists."""
        result = get_additional_applicable_data_files([], [])
        assert result == []

    def test_get_additional_applicable_data_files_duplicate_files(self):
        """Test behavior with duplicate files in lists."""
        all_data_files = [
            (1, self.mock_data_file),
            (1, self.mock_data_file),  # Duplicate
        ]

        data_files_downloaded = [
            (1, self.mock_data_file),
        ]

        # The function uses set operations, so duplicates should be handled
        result = get_additional_applicable_data_files(
            all_data_files, data_files_downloaded
        )

        # Should return empty since the file is already downloaded
        assert result == []

    def test_filter_rows_to_be_deleted_all_matches(self):
        """Test filtering when all data rows match equality deletes."""
        equality_delete_table = pa.Table.from_arrays(
            [
                pa.array(["hash_user_1", "hash_user_2"]),
            ],
            names=[sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME],
        )

        data_file_table = pa.Table.from_arrays(
            [
                pa.array(["hash_user_1", "hash_user_2"]),
                pa.array(["s3://bucket/data.parquet"] * 2),
                pa.array([0, 1], type=pa.int64()),
            ],
            names=[
                sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME,
                sc._FILE_PATH_COLUMN_NAME,
                sc._ORDERED_RECORD_IDX_COLUMN_NAME,
            ],
        )

        position_delete_table, remaining_data_table = filter_rows_to_be_deleted(
            equality_delete_table, data_file_table, ["user_id"]
        )

        # All records should be deleted
        assert len(position_delete_table) == 2
        assert len(remaining_data_table) == 0

        # Verify total still matches
        assert len(position_delete_table) + len(remaining_data_table) == len(
            data_file_table
        )

    def test_compute_pos_delete_converting_equality_deletes_all_deleted(self):
        """Test when all data records are deleted by equality deletes."""
        equality_delete_table = pa.Table.from_arrays(
            [
                pa.array(["hash_user_1", "hash_user_2"]),
            ],
            names=[sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME],
        )

        data_file_table = pa.Table.from_arrays(
            [
                pa.array(["hash_user_1", "hash_user_2"]),
                pa.array(["s3://bucket/data.parquet"] * 2),
                pa.array([0, 1], type=pa.int64()),
            ],
            names=[
                sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME,
                sc._FILE_PATH_COLUMN_NAME,
                sc._ORDERED_RECORD_IDX_COLUMN_NAME,
            ],
        )

        (
            new_position_delete_table,
            remaining_data_table,
        ) = compute_pos_delete_converting_equality_deletes(
            equality_delete_table,
            data_file_table,
            ["user_id"],
            "s3://warehouse/table/data",
            None,
        )

        # Should have position deletes for all records
        assert new_position_delete_table is not None
        assert len(new_position_delete_table) == 2

        # Should have no remaining data
        assert remaining_data_table is not None
        assert len(remaining_data_table) == 0

    @patch(
        "deltacat.compute.converter.steps.convert.download_data_table_and_append_iceberg_columns"
    )
    @patch("deltacat.compute.converter.steps.convert.sort_data_files_maintaining_order")
    def test_compute_pos_delete_with_limited_parallelism_empty_tables(
        self, mock_sort_files, mock_download_data
    ):
        """Test computing position deletes with empty tables from download."""
        data_files_list = [[(1, self.mock_data_file)]]
        equality_delete_files_list = [[(2, self.mock_data_file)]]

        mock_sort_files.return_value = data_files_list[0]

        # Return empty tables
        empty_data_table = pa.Table.from_arrays(
            [
                pa.array([], type=pa.string()),
                pa.array([], type=pa.string()),
                pa.array([], type=pa.int64()),
            ],
            names=[
                sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME,
                sc._FILE_PATH_COLUMN_NAME,
                sc._ORDERED_RECORD_IDX_COLUMN_NAME,
            ],
        )

        empty_equality_table = pa.Table.from_arrays(
            [
                pa.array([], type=pa.string()),
            ],
            names=[sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME],
        )

        mock_download_data.side_effect = [empty_data_table, empty_equality_table]

        (
            new_pos_delete_table,
            remaining_data_table,
        ) = compute_pos_delete_with_limited_parallelism(
            data_files_list,
            ["user_id"],
            equality_delete_files_list,
            "s3://warehouse/table/data",
            0,
            4,
            None,
            None,
        )

        # Should handle empty tables gracefully
        assert new_pos_delete_table is None

    @patch(
        "deltacat.compute.converter.steps.convert.download_data_table_and_append_iceberg_columns"
    )
    @patch("deltacat.compute.converter.steps.convert.sort_data_files_maintaining_order")
    def test_compute_pos_delete_with_limited_parallelism_multiple_file_groups(
        self, mock_sort_files, mock_download_data
    ):
        """Test computing position deletes with multiple file groups."""
        # Setup multiple groups of files
        data_files_list = [
            [(1, self.mock_data_file)],  # Group 1
            [(3, self.mock_data_file)],  # Group 2
        ]
        equality_delete_files_list = [
            [(2, self.mock_data_file)],  # Group 1
            [(4, self.mock_data_file)],  # Group 2
        ]

        mock_sort_files.side_effect = [
            data_files_list[0],  # First call
            data_files_list[1],  # Second call
        ]

        # Create mock tables for each group
        data_table1 = pa.Table.from_arrays(
            [
                pa.array(["hash_user_1"]),
                pa.array(["s3://bucket/file1.parquet"]),
                pa.array([0], type=pa.int64()),
            ],
            names=[
                sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME,
                sc._FILE_PATH_COLUMN_NAME,
                sc._ORDERED_RECORD_IDX_COLUMN_NAME,
            ],
        )

        equality_table1 = pa.Table.from_arrays(
            [
                pa.array(["hash_user_1"]),
            ],
            names=[sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME],
        )

        data_table2 = pa.Table.from_arrays(
            [
                pa.array(["hash_user_2"]),
                pa.array(["s3://bucket/file2.parquet"]),
                pa.array([1], type=pa.int64()),
            ],
            names=[
                sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME,
                sc._FILE_PATH_COLUMN_NAME,
                sc._ORDERED_RECORD_IDX_COLUMN_NAME,
            ],
        )

        equality_table2 = pa.Table.from_arrays(
            [
                pa.array(["hash_user_2"]),
            ],
            names=[sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME],
        )

        mock_download_data.side_effect = [
            data_table1,
            equality_table1,  # Group 1
            data_table2,
            equality_table2,  # Group 2
        ]

        (
            new_pos_delete_table,
            remaining_data_table,
        ) = compute_pos_delete_with_limited_parallelism(
            data_files_list,
            ["user_id"],
            equality_delete_files_list,
            "s3://warehouse/table/data",
            0,
            4,
            None,
            None,
        )

        # Should process both groups and combine results
        assert new_pos_delete_table is not None
        assert len(new_pos_delete_table) == 2  # One delete from each group

        # Verify function was called for both groups
        assert mock_sort_files.call_count == 2
        assert mock_download_data.call_count == 4
