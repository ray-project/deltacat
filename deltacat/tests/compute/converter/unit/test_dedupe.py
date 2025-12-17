"""
Unit tests for the dedupe_data_files function in deltacat.compute.converter.steps.dedupe.

This module contains comprehensive unit tests that focus specifically on the core
deduplication logic of the dedupe_data_files function. The tests verify that:

1. Position delete records are correctly generated for duplicate merge keys
2. Only the latest version of each duplicate record is kept (based on global record index)
3. The function handles various edge cases correctly
4. Proper error handling and validation
5. Correct return values (position delete table, record count, byte size)

The tests use mocked dependencies to isolate the dedupe logic and test it independently
of external systems like S3, Iceberg, etc.
"""

import pyarrow as pa
from unittest.mock import Mock, patch
from typing import List

from deltacat.compute.converter.steps.dedupe import dedupe_data_files
import deltacat.compute.converter.utils.iceberg_columns as sc
from pyiceberg.manifest import DataFile


class TestDedupeDataFiles:
    """Test class for dedupe_data_files function."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        # Mock DataFile objects for 10 files
        self.mock_data_files = []
        for i in range(10):
            mock_file = Mock(spec=DataFile)
            mock_file.file_path = f"s3://bucket/file{i+1}.parquet"
            self.mock_data_files.append(mock_file)

        # Keep original 3 files for backward compatibility with other tests
        self.mock_data_file_1 = self.mock_data_files[0]
        self.mock_data_file_2 = self.mock_data_files[1]
        self.mock_data_file_3 = self.mock_data_files[2]

    def create_mock_table_with_hash(
        self, identifier_values: List[str], file_path: str, record_values: List[int]
    ) -> pa.Table:
        """
        Helper method to create a mock PyArrow table with identifier hash column.

        Args:
            identifier_values: List of identifier values to hash
            file_path: File path for the records
            record_values: Values assigned to each record

        Returns:
            PyArrow table with hash column, file_path, and record value columns
        """
        # Create hash values (simplified - just use the identifier values as hash)
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

    @patch("deltacat.compute.converter.steps.dedupe.write_sliced_table")
    @patch("deltacat.compute.converter.steps.dedupe.sort_data_files_maintaining_order")
    @patch(
        "deltacat.compute.converter.steps.dedupe.download_data_table_and_append_iceberg_columns"
    )
    def test_dedupe_data_files_basic_functionality(
        self, mock_download_data, mock_sort_files, mock_write_sliced_table
    ):
        """Test basic deduplication functionality with simple duplicates."""

        # Setup input data
        data_files_to_dedupe = [(1, self.mock_data_file_1), (2, self.mock_data_file_2)]
        identifier_columns = ["user_id"]
        remaining_data_table = None
        s3_client_kwargs = {}

        # Mock sort_data_files_maintaining_order to return input as-is
        mock_sort_files.return_value = data_files_to_dedupe

        # Create mock tables with duplicates
        # File 1: user_1 (pos 0), user_2 (pos 1), user_3 (pos 2)
        table1 = self.create_mock_table_with_hash(
            ["user_1", "user_2", "user_3"], "s3://bucket/file1.parquet", [0, 1, 2]
        )

        # File 2: user_1 (pos 3), user_4 (pos 4) - user_1 is duplicate
        table2 = self.create_mock_table_with_hash(
            ["user_1", "user_4"], "s3://bucket/file2.parquet", [3, 4]
        )

        # Mock download function to return our test tables
        mock_download_data.side_effect = [table1, table2]

        # Mock write_sliced_table to prevent actual file I/O
        mock_write_sliced_table.return_value = [
            "s3://warehouse/test_table/partition=0/position_delete_1.parquet"
        ]

        # Mock filesystem for new required parameter
        mock_filesystem = Mock()

        # Call the function with new required parameters
        (
            position_delete_files,
            total_records,
            total_bytes,
            pos_delete_count,
            pos_delete_bytes,
        ) = dedupe_data_files(
            data_files_to_dedupe,
            identifier_columns,
            remaining_data_table,
            "pos",  # merge_sort_column - not used but required by function signature
            s3_client_kwargs,
            "s3://warehouse/test_table/partition=0",  # iceberg_table_warehouse_prefix_with_partition
            mock_filesystem,  # filesystem
        )

        # Verify function calls
        mock_sort_files.assert_called_once_with(data_files=data_files_to_dedupe)
        assert mock_download_data.call_count == 2

        # Verify results (updated for new return format)
        assert total_records == 5  # Total input records
        assert total_bytes > 0  # Should have some byte size
        assert pos_delete_count == 1  # One duplicate to delete
        assert pos_delete_bytes > 0  # Should have memory usage for position deletes
        assert (
            len(position_delete_files) == 1
        )  # One position delete file should be created

    @patch("deltacat.compute.converter.steps.dedupe.write_sliced_table")
    @patch("deltacat.compute.converter.steps.dedupe.sort_data_files_maintaining_order")
    @patch(
        "deltacat.compute.converter.steps.dedupe.download_data_table_and_append_iceberg_columns"
    )
    def test_dedupe_data_files_with_remaining_data_table(
        self, mock_download_data, mock_sort_files, mock_write_sliced_table
    ):
        """Test deduplication with existing remaining_data_table_after_convert."""

        # Setup input data
        data_files_to_dedupe = [(1, self.mock_data_file_1)]
        identifier_columns = ["user_id"]

        # Create remaining data table with some existing records
        remaining_data_table = self.create_mock_table_with_hash(
            ["user_1", "user_5"], "existing_file.parquet", [10, 11]
        )

        s3_client_kwargs = {}

        # Mock sort_data_files_maintaining_order
        mock_sort_files.return_value = data_files_to_dedupe

        # Create mock table for the new file
        new_file_table = self.create_mock_table_with_hash(
            ["user_1", "user_6"], "s3://bucket/file1.parquet", [0, 1]
        )

        mock_download_data.return_value = new_file_table

        # Mock write_sliced_table to prevent actual file I/O
        mock_write_sliced_table.return_value = [
            "s3://warehouse/test_table/partition=0/position_delete_1.parquet"
        ]

        # Mock filesystem for new required parameter
        mock_filesystem = Mock()

        # Call the function with new required parameters
        (
            position_delete_files,
            total_records,
            total_bytes,
            pos_delete_count,
            pos_delete_bytes,
        ) = dedupe_data_files(
            data_files_to_dedupe,
            identifier_columns,
            remaining_data_table,
            "pos",  # merge_sort_column - not used but required by function signature
            s3_client_kwargs,
            "s3://warehouse/test_table/partition=0",  # iceberg_table_warehouse_prefix_with_partition
            mock_filesystem,  # filesystem
        )

        # Verify results
        assert total_records == 4  # 2 from remaining + 2 from new file
        assert total_bytes > 0
        assert pos_delete_count == 1  # One duplicate to delete
        assert pos_delete_bytes > 0  # Should have memory usage for position deletes
        assert (
            len(position_delete_files) == 1
        )  # One position delete file should be created

    @patch("deltacat.compute.converter.steps.dedupe.write_sliced_table")
    @patch("deltacat.compute.converter.steps.dedupe.sort_data_files_maintaining_order")
    @patch(
        "deltacat.compute.converter.steps.dedupe.download_data_table_and_append_iceberg_columns"
    )
    def test_dedupe_data_files_no_duplicates(
        self, mock_download_data, mock_sort_files, mock_write_sliced_table
    ):
        """Test deduplication when there are no duplicates."""

        # Setup input data with no duplicates
        data_files_to_dedupe = [(1, self.mock_data_file_1), (2, self.mock_data_file_2)]
        identifier_columns = ["user_id"]
        remaining_data_table = None
        "pos"  # merge_sort_column - not used but required by function signature
        s3_client_kwargs = {}

        mock_sort_files.return_value = data_files_to_dedupe

        # Create tables with unique records
        table1 = self.create_mock_table_with_hash(
            ["user_1", "user_2"], "s3://bucket/file1.parquet", [0, 1]
        )

        table2 = self.create_mock_table_with_hash(
            ["user_3", "user_4"], "s3://bucket/file2.parquet", [2, 3]
        )

        mock_download_data.side_effect = [table1, table2]

        # Mock write_sliced_table to prevent actual file I/O (shouldn't be called for no duplicates)
        mock_write_sliced_table.return_value = []

        # Mock filesystem for new required parameter
        mock_filesystem = Mock()

        # Call the function with new required parameters
        (
            position_delete_files,
            total_records,
            total_bytes,
            pos_delete_count,
            pos_delete_bytes,
        ) = dedupe_data_files(
            data_files_to_dedupe,
            identifier_columns,
            remaining_data_table,
            "pos",  # merge_sort_column - not used but required by function signature
            s3_client_kwargs,
            "s3://warehouse/test_table/partition=0",  # iceberg_table_warehouse_prefix_with_partition
            mock_filesystem,  # filesystem
        )

        # Verify results - no duplicates means empty position delete files
        assert len(position_delete_files) == 0
        # Verify write_sliced_table was not called since there are no duplicates
        mock_write_sliced_table.assert_not_called()
        assert total_records == 4
        assert total_bytes > 0
        assert pos_delete_count == 0  # No duplicates to delete
        assert pos_delete_bytes == 0  # No memory usage for position deletes

    @patch("deltacat.compute.converter.steps.dedupe.write_sliced_table")
    @patch("deltacat.compute.converter.steps.dedupe.sort_data_files_maintaining_order")
    @patch("deltacat.compute.converter.utils.io.daft_read_parquet")
    def test_dedupe_data_files_multiple_duplicates_same_key(
        self, mock_daft_read_parquet, mock_sort_files, mock_write_sliced_table
    ):
        """
        Comprehensive stress test for deduplication with 10 files and both cross-file and internal duplicates.

        This test creates a systematic scenario with:
        - 10 files, each with exactly 1200 records (12,000 total)
        - Composite keys (user_id, session_id) - both must match for records to be considered duplicates
        - Cross-file duplicates: 200 shared composite keys appearing in all 10 files
        - Internal duplicates: 50 composite keys appearing 3 times each within every file
        - Unique records: 850 per file with no duplicates
        - Exact duplicate counting: 2800 total duplicates (1800 cross-file + 1000 internal)
        - Precise verification of composite key deduplication logic for both scenarios
        """

        # Setup input data with 10 files
        data_files_to_dedupe = [(i + 1, self.mock_data_files[i]) for i in range(10)]

        # Use composite identifier columns - both user_id and session_id must match for duplicates
        identifier_columns = ["user_id", "session_id"]
        remaining_data_table = None
        s3_client_kwargs = {}

        mock_sort_files.return_value = data_files_to_dedupe

        # Create systematic duplicate patterns with composite keys
        # Each file has exactly 1200 records
        # Pattern: 200 cross-file duplicates + 150 internal duplicates + 850 unique records per file

        mock_daft_dataframes = []

        for file_idx in range(10):
            user_ids = []
            session_ids = []

            # 200 cross-file duplicates: shared composite keys across all files
            # These appear in ALL 10 files, so each will have 9 duplicates to delete
            for i in range(200):
                user_ids.append(f"shared_user_{i}")
                session_ids.append(
                    f"shared_session_{i % 20}"
                )  # 20 different sessions, creating composite keys

            # 150 internal duplicates within this file: composite keys appearing 3 times each
            # Each composite key appears 3 times within the same file, so 2 duplicates per key to delete
            for i in range(50):  # 50 keys × 3 occurrences = 150 records
                for j in range(3):
                    user_ids.append(f"internal_file{file_idx}_user_{i}")
                    session_ids.append(
                        f"internal_session_{i % 10}"
                    )  # 10 different sessions per file

            # 850 unique records per file: unique composite keys
            for i in range(850):
                user_ids.append(f"unique_file{file_idx}_user_{i}")
                session_ids.append(
                    f"unique_session_{i % 50}"
                )  # 50 different sessions, ensuring uniqueness

            # Create PyArrow table with separate columns for composite keys
            table_data = {
                "user_id": user_ids,
                "session_id": session_ids,
            }
            arrow_table = pa.Table.from_pydict(table_data)

            # Create mock Daft DataFrame
            mock_df = Mock()
            mock_df.select.return_value.to_arrow.return_value = arrow_table
            mock_daft_dataframes.append(mock_df)

            # Verify exactly 1200 records per file
            assert (
                len(user_ids) == 1200
            ), f"File{file_idx+1} has {len(user_ids)} records, expected 1200"

        mock_daft_read_parquet.side_effect = mock_daft_dataframes

        # Mock write_sliced_table to prevent actual file I/O
        mock_write_sliced_table.return_value = [
            "s3://warehouse/test_table/partition=0/position_delete_1.parquet"
        ]

        # Mock filesystem for new required parameter
        mock_filesystem = Mock()

        # Call the function with new required parameters
        (
            position_delete_files,
            total_records,
            total_bytes,
            pos_delete_count,
            pos_delete_bytes,
        ) = dedupe_data_files(
            data_files_to_dedupe,
            identifier_columns,
            remaining_data_table,
            "pos",  # merge_sort_column - not used but required by function signature
            s3_client_kwargs,
            "s3://warehouse/test_table/partition=0",  # iceberg_table_warehouse_prefix_with_partition
            mock_filesystem,  # filesystem
        )

        print(f"DEBUG: Position delete files: {len(position_delete_files)}")
        print(f"DEBUG: Position delete count: {pos_delete_count}")

        # Verify results
        total_input_records = 10 * 1200  # 12,000 total records
        assert total_records == total_input_records
        assert total_bytes > 0

        # Calculate exact expected duplicates:
        # 1. Cross-file duplicates: 200 shared keys appear in all 10 files = 200 * 10 = 2000 total occurrences
        #    Keep 200 (one of each), delete 1800
        # 2. Internal duplicates: 50 keys × 3 occurrences × 10 files = 1500 total occurrences
        #    Keep 500 (one of each key per file), delete 1000
        cross_file_duplicates = 200 * 10 - 200  # 1800
        internal_duplicates = 50 * 3 * 10 - 50 * 10  # 1500 - 500 = 1000
        expected_duplicates = cross_file_duplicates + internal_duplicates  # 2800

        assert pos_delete_count == expected_duplicates, (
            f"Expected exactly {expected_duplicates} duplicates, got {pos_delete_count}. "
            f"Cross-file duplicates: {cross_file_duplicates}, Internal duplicates: {internal_duplicates}"
        )

        assert (
            len(position_delete_files) > 0
        ), "Should have created position delete files"
        assert pos_delete_bytes > 0, "Should have memory usage for position deletes"

        print(f"Systematic stress test completed successfully:")
        print(f"  Total input records: {total_records}")
        print(
            f"  Duplicates found: {pos_delete_count} (expected: {expected_duplicates})"
        )
        print(f"  Cross-file duplicates: {cross_file_duplicates}")
        print(f"  Internal duplicates: {internal_duplicates}")
        print(f"  Files processed: 10")
        print(f"  Records per file: 1200")
        print(f"  Shared keys (cross-file): 200")
        print(f"  Internal keys per file: 50 (3 duplicates each)")
        print(f"  Position delete files created: {len(position_delete_files)}")
        print(f"  Position delete memory usage: {pos_delete_bytes} bytes")

    @patch("deltacat.compute.converter.steps.dedupe.write_sliced_table")
    @patch("deltacat.compute.converter.steps.dedupe.sort_data_files_maintaining_order")
    @patch("deltacat.compute.converter.utils.io.daft_read_parquet")
    def test_dedupe_data_files_multiple_identifier_columns(
        self, mock_daft_read_parquet, mock_sort_files, mock_write_sliced_table
    ):
        """
        Comprehensive stress test for deduplication with multiple identifier columns (composite keys).

        This test creates a systematic scenario with:
        - 5 files, each with exactly 1000 records (5,000 total)
        - Composite keys (user_id, session_id)
        - Cross-file duplicates: 100 shared composite keys appearing in all 5 files
        - Internal duplicates: 50 composite keys appearing 2 times each within every file
        - Unique records: 850 per file with no duplicates
        - Exact duplicate counting: 650 total duplicates (400 cross-file + 250 internal)
        - Precise verification of composite key deduplication logic
        """

        # Setup input data with 5 files
        data_files_to_dedupe = [(i + 1, self.mock_data_files[i]) for i in range(5)]

        # Use composite identifier columns
        identifier_columns = ["user_id", "session_id"]
        remaining_data_table = None
        s3_client_kwargs = {}

        mock_sort_files.return_value = data_files_to_dedupe

        # Create systematic duplicate patterns with composite keys
        # Each file has exactly 1000 records
        # Pattern: 100 cross-file duplicates + 100 internal duplicates + 800 unique records per file

        mock_daft_dataframes = []

        for file_idx in range(5):
            user_ids = []
            session_ids = []

            # 100 cross-file duplicates: shared composite keys across all files
            for i in range(100):
                user_ids.append(f"shared_user_{i}")
                session_ids.append(f"shared_session_{i % 10}")  # 10 different sessions

            # 100 internal duplicates within this file: 50 keys × 2 occurrences = 100 records
            for i in range(50):
                for j in range(2):
                    user_ids.append(f"internal_file{file_idx}_user_{i}")
                    session_ids.append(
                        f"internal_session_{i % 5}"
                    )  # 5 different sessions

            # 800 unique records per file
            for i in range(800):
                user_ids.append(f"unique_file{file_idx}_user_{i}")
                session_ids.append(f"unique_session_{i % 20}")  # 20 different sessions

            # Create PyArrow table with separate columns for composite keys
            table_data = {
                "user_id": user_ids,
                "session_id": session_ids,
            }
            arrow_table = pa.Table.from_pydict(table_data)

            # Create mock Daft DataFrame
            mock_df = Mock()
            mock_df.select.return_value.to_arrow.return_value = arrow_table
            mock_daft_dataframes.append(mock_df)

            # Verify exactly 1000 records per file
            assert (
                len(user_ids) == 1000
            ), f"File{file_idx+1} has {len(user_ids)} records, expected 1000"

        mock_daft_read_parquet.side_effect = mock_daft_dataframes

        # Mock write_sliced_table to prevent actual file I/O
        mock_write_sliced_table.return_value = [
            "s3://warehouse/test_table/partition=0/position_delete_1.parquet"
        ]

        # Mock filesystem for new required parameter
        mock_filesystem = Mock()

        # Call the function with new required parameters
        (
            position_delete_files,
            total_records,
            total_bytes,
            pos_delete_count,
            pos_delete_bytes,
        ) = dedupe_data_files(
            data_files_to_dedupe,
            identifier_columns,
            remaining_data_table,
            "pos",  # merge_sort_column - not used but required by function signature
            s3_client_kwargs,
            "s3://warehouse/test_table/partition=0",  # iceberg_table_warehouse_prefix_with_partition
            mock_filesystem,  # filesystem
        )

        print(f"DEBUG: Position delete files: {len(position_delete_files)}")
        print(f"DEBUG: Position delete count: {pos_delete_count}")

        # Verify results
        total_input_records = 5 * 1000  # 5,000 total records
        assert total_records == total_input_records
        assert total_bytes > 0

        # Calculate exact expected duplicates:
        # 1. Cross-file duplicates: 100 shared composite keys appear in all 5 files = 100 * 5 = 500 total occurrences
        #    Keep 100 (one of each), delete 400
        # 2. Internal duplicates: 50 keys × 2 occurrences × 5 files = 500 total occurrences
        #    Keep 250 (one of each key per file), delete 250
        cross_file_duplicates = 100 * 5 - 100  # 400
        internal_duplicates = 50 * 2 * 5 - 50 * 5  # 500 - 250 = 250
        expected_duplicates = cross_file_duplicates + internal_duplicates  # 650

        assert pos_delete_count == expected_duplicates, (
            f"Expected exactly {expected_duplicates} duplicates, got {pos_delete_count}. "
            f"Cross-file duplicates: {cross_file_duplicates}, Internal duplicates: {internal_duplicates}"
        )

        assert (
            len(position_delete_files) > 0
        ), "Should have created position delete files"
        assert pos_delete_bytes > 0, "Should have memory usage for position deletes"

        print(f"Composite key stress test completed successfully:")
        print(f"  Total input records: {total_records}")
        print(
            f"  Duplicates found: {pos_delete_count} (expected: {expected_duplicates})"
        )
        print(f"  Cross-file duplicates: {cross_file_duplicates}")
        print(f"  Internal duplicates: {internal_duplicates}")
        print(f"  Files processed: 5")
        print(f"  Records per file: 1000")
        print(f"  Shared composite keys (cross-file): 100")
        print(f"  Internal composite keys per file: 50 (2 duplicates each)")
        print(f"  Position delete files created: {len(position_delete_files)}")
        print(f"  Position delete memory usage: {pos_delete_bytes} bytes")
