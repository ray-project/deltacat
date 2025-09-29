"""
Unit tests for converter session utilities in deltacat.compute.converter.utils.converter_session_utils.

This module contains comprehensive unit tests for critical utility functions used in the converter session,
including file grouping, sorting, snapshot type determination, and partition handling.
"""

from unittest.mock import Mock

from deltacat.compute.converter.utils.converter_session_utils import (
    check_data_files_sequence_number,
    construct_iceberg_table_prefix,
    partition_value_record_to_partition_value_string,
    group_all_files_to_each_bucket,
    sort_data_files_maintaining_order,
    SnapshotType,
    _get_snapshot_action_description,
    _determine_snapshot_type,
)
from pyiceberg.manifest import DataFile


class TestConverterSessionUtils:
    """Test class for converter session utility functions."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        # Create mock DataFile objects
        self.mock_data_files = []
        for i in range(10):
            mock_file = Mock(spec=DataFile)
            mock_file.file_path = f"s3://bucket/data/file{i+1}.parquet"
            mock_file.file_size_in_bytes = 1000 * (i + 1)
            self.mock_data_files.append(mock_file)

    def test_check_data_files_sequence_number_basic(self):
        """Test basic functionality of sequence number checking."""
        # Setup test data with sequence numbers
        data_files_list = [
            (1, self.mock_data_files[0]),  # seq 1
            (3, self.mock_data_files[1]),  # seq 3
            (5, self.mock_data_files[2]),  # seq 5
        ]

        equality_delete_files_list = [
            (2, self.mock_data_files[3]),  # seq 2 > data seq 1
            (4, self.mock_data_files[4]),  # seq 4 > data seq 3
            (6, self.mock_data_files[5]),  # seq 6 > data seq 5
        ]

        result_eq_files, result_data_files = check_data_files_sequence_number(
            data_files_list, equality_delete_files_list
        )

        print(
            f"DEBUG: result_eq_files={result_eq_files}, result_data_files={result_data_files}"
        )

        # Verify results - should group data files by their applicable equality delete files
        assert len(result_eq_files) == len(result_data_files)

        # Expected grouping based on which equality deletes apply to which data files:
        # Data file 1: applicable equality deletes [2, 4, 6] (all > 1)
        # Data file 3: applicable equality deletes [4, 6] (both > 3)
        # Data file 5: applicable equality deletes [6] (only 6 > 5)
        # So we should have 3 groups with different sets of applicable equality deletes

        # Verify that equality delete files have higher sequence numbers than their corresponding data files
        for eq_files_group, data_files_group in zip(result_eq_files, result_data_files):
            for eq_file in eq_files_group:
                for data_file in data_files_group:
                    assert (
                        eq_file[0] > data_file[0]
                    ), f"Equality delete seq {eq_file[0]} should be > data file seq {data_file[0]}"

        # Verify we have the expected number of groups (each data file gets its own group due to different applicable eq deletes)
        assert (
            len(result_eq_files) == 3
        ), f"Expected 3 groups, got {len(result_eq_files)}"

    def test_check_data_files_sequence_number_no_applicable_deletes(self):
        """Test when no equality deletes are applicable to data files."""
        data_files_list = [
            (5, self.mock_data_files[0]),  # seq 5
            (6, self.mock_data_files[1]),  # seq 6
        ]

        equality_delete_files_list = [
            (1, self.mock_data_files[2]),  # seq 1 < data seq 5
            (2, self.mock_data_files[3]),  # seq 2 < data seq 5
        ]

        result_eq_files, result_data_files = check_data_files_sequence_number(
            data_files_list, equality_delete_files_list
        )

        # No equality deletes should be applicable
        assert len(result_eq_files) == 0
        assert len(result_data_files) == 0

    def test_check_data_files_sequence_number_multiple_deletes_per_data_file(self):
        """Test when multiple equality deletes apply to the same data file."""
        data_files_list = [
            (1, self.mock_data_files[0]),  # seq 1
        ]

        equality_delete_files_list = [
            (2, self.mock_data_files[1]),  # seq 2 > data seq 1
            (3, self.mock_data_files[2]),  # seq 3 > data seq 1
            (4, self.mock_data_files[3]),  # seq 4 > data seq 1
        ]

        result_eq_files, result_data_files = check_data_files_sequence_number(
            data_files_list, equality_delete_files_list
        )

        # Should have one group with all three equality deletes
        assert len(result_eq_files) == 1
        assert len(result_data_files) == 1
        assert (
            len(result_eq_files[0]) == 3
        )  # All three equality deletes should be applicable
        assert len(result_data_files[0]) == 1  # One data file

    def test_check_data_files_sequence_number_optimization_example(self):
        """Test the optimization example: data files 1,2,3,5 with equality deletes 4,6."""
        # data file 1, 2, 3, 5; equality delete 4, 6
        # Expected: res_data_file_list = [[1, 2, 3], [5]] res_equality_delete_file_list = [[4, 6], [6]]
        data_files_list = [
            (1, self.mock_data_files[0]),  # seq 1
            (2, self.mock_data_files[1]),  # seq 2
            (3, self.mock_data_files[2]),  # seq 3
            (5, self.mock_data_files[3]),  # seq 5
        ]

        equality_delete_files_list = [
            (4, self.mock_data_files[4]),  # seq 4
            (6, self.mock_data_files[5]),  # seq 6
        ]

        result_eq_files, result_data_files = check_data_files_sequence_number(
            data_files_list, equality_delete_files_list
        )

        print(
            f"DEBUG optimization: result_eq_files={result_eq_files}, result_data_files={result_data_files}"
        )

        # Should have 2 groups
        assert len(result_eq_files) == 2
        assert len(result_data_files) == 2

        # Extract sequence numbers for easier comparison
        all_eq_seqs = [sorted([f[0] for f in eq_group]) for eq_group in result_eq_files]
        all_data_seqs = [
            sorted([f[0] for f in data_group]) for data_group in result_data_files
        ]

        # Expected groups: [[1,2,3], [5]] and [[4,6], [6]]
        # Sort by data group size to make comparison deterministic
        combined = list(zip(all_data_seqs, all_eq_seqs))
        combined.sort(key=lambda x: len(x[0]), reverse=True)  # Larger data group first

        # First group should have 3 data files [1,2,3] with equality deletes [4,6]
        assert combined[0][0] == [
            1,
            2,
            3,
        ], f"Expected data files [1,2,3], got {combined[0][0]}"
        assert combined[0][1] == [
            4,
            6,
        ], f"Expected equality deletes [4,6], got {combined[0][1]}"

        # Second group should have 1 data file [5] with equality deletes [6]
        assert combined[1][0] == [5], f"Expected data files [5], got {combined[1][0]}"
        assert combined[1][1] == [
            6
        ], f"Expected equality deletes [6], got {combined[1][1]}"

    def test_construct_iceberg_table_prefix(self):
        """Test construction of Iceberg table prefix."""
        bucket_name = "my-warehouse-bucket"
        table_name = "my_table"
        namespace = "my_namespace"

        result = construct_iceberg_table_prefix(bucket_name, table_name, namespace)

        expected = "my-warehouse-bucket/my_namespace/my_table/data"
        assert result == expected

    def test_construct_iceberg_table_prefix_with_special_characters(self):
        """Test construction with special characters in names."""
        bucket_name = "warehouse-bucket-123"
        table_name = "table_with_underscores"
        namespace = "namespace.with.dots"

        result = construct_iceberg_table_prefix(bucket_name, table_name, namespace)

        expected = (
            "warehouse-bucket-123/namespace.with.dots/table_with_underscores/data"
        )
        assert result == expected

    def test_partition_value_record_to_partition_value_string(self):
        """Test conversion of partition value record to string."""
        # Mock partition record
        mock_partition = Mock()
        mock_partition.__repr__ = Mock(return_value="Record[year=2023, month=12]")

        result = partition_value_record_to_partition_value_string(mock_partition)

        expected = "year=2023, month=12"
        assert result == expected

    def test_partition_value_record_to_partition_value_string_single_field(self):
        """Test conversion with single partition field."""
        mock_partition = Mock()
        mock_partition.__repr__ = Mock(return_value="Record[date=2023-12-01]")

        result = partition_value_record_to_partition_value_string(mock_partition)

        expected = "date=2023-12-01"
        assert result == expected

    def test_group_all_files_to_each_bucket_with_equality_deletes(self):
        """Test grouping files when equality deletes are present."""
        # Setup partition values
        partition1 = Mock()
        partition1.__repr__ = Mock(return_value="Record[year=2023]")
        partition2 = Mock()
        partition2.__repr__ = Mock(return_value="Record[year=2024]")

        # Setup file dictionaries
        data_file_dict = {
            partition1: [(1, self.mock_data_files[0]), (3, self.mock_data_files[1])],
            partition2: [(2, self.mock_data_files[2])],
        }

        equality_delete_dict = {
            partition1: [(2, self.mock_data_files[3]), (4, self.mock_data_files[4])],
        }

        pos_delete_dict = {}

        result = group_all_files_to_each_bucket(
            data_file_dict, equality_delete_dict, pos_delete_dict
        )

        # Should have 2 convert input files (one per partition)
        assert len(result) == 2

        # Find the partition with equality deletes
        partition_with_deletes = None
        partition_without_deletes = None

        for convert_input in result:
            if convert_input.applicable_equality_delete_files:
                partition_with_deletes = convert_input
            else:
                partition_without_deletes = convert_input

        # Verify partition with equality deletes
        assert partition_with_deletes is not None
        assert len(partition_with_deletes.applicable_data_files) > 0
        assert len(partition_with_deletes.applicable_equality_delete_files) > 0

        # Verify partition without equality deletes
        assert partition_without_deletes is not None
        assert partition_without_deletes.applicable_equality_delete_files is None

    def test_group_all_files_to_each_bucket_no_equality_deletes(self):
        """Test grouping files when no equality deletes are present."""
        partition1 = Mock()
        partition1.__repr__ = Mock(return_value="Record[year=2023]")

        data_file_dict = {
            partition1: [(1, self.mock_data_files[0]), (2, self.mock_data_files[1])],
        }

        equality_delete_dict = {}
        pos_delete_dict = {}

        result = group_all_files_to_each_bucket(
            data_file_dict, equality_delete_dict, pos_delete_dict
        )

        # Should have 1 convert input file
        assert len(result) == 1

        convert_input = result[0]
        assert convert_input.partition_value == partition1
        assert len(convert_input.all_data_files_for_dedupe) == 2
        assert convert_input.applicable_equality_delete_files is None

    def test_sort_data_files_maintaining_order(self):
        """Test deterministic sorting of data files."""
        # Create files with mixed sequence numbers and paths
        data_files = [
            (3, self.mock_data_files[2]),  # seq 3, path file3.parquet
            (1, self.mock_data_files[0]),  # seq 1, path file1.parquet
            (2, self.mock_data_files[1]),  # seq 2, path file2.parquet
            (
                1,
                self.mock_data_files[3],
            ),  # seq 1, path file4.parquet (same seq as file1)
        ]

        result = sort_data_files_maintaining_order(data_files)

        # Should be sorted by sequence number first, then by file path
        expected_order = [
            (1, self.mock_data_files[0]),  # seq 1, file1.parquet
            (1, self.mock_data_files[3]),  # seq 1, file4.parquet
            (2, self.mock_data_files[1]),  # seq 2, file2.parquet
            (3, self.mock_data_files[2]),  # seq 3, file3.parquet
        ]

        assert result == expected_order

    def test_sort_data_files_maintaining_order_empty_list(self):
        """Test sorting with empty list."""
        result = sort_data_files_maintaining_order([])
        assert result == []

    def test_sort_data_files_maintaining_order_single_file(self):
        """Test sorting with single file."""
        data_files = [(1, self.mock_data_files[0])]
        result = sort_data_files_maintaining_order(data_files)
        assert result == data_files

    def test_determine_snapshot_type_none(self):
        """Test snapshot type determination when no changes are needed."""
        to_be_deleted_files = []
        to_be_added_files = []

        result = _determine_snapshot_type(to_be_deleted_files, to_be_added_files)

        assert result == SnapshotType.NONE

    def test_determine_snapshot_type_append(self):
        """Test snapshot type determination for append operation."""
        to_be_deleted_files = []
        to_be_added_files = [self.mock_data_files[0]]

        result = _determine_snapshot_type(to_be_deleted_files, to_be_added_files)

        assert result == SnapshotType.APPEND

    def test_determine_snapshot_type_replace(self):
        """Test snapshot type determination for replace operation."""
        to_be_deleted_files = [[self.mock_data_files[0]]]
        to_be_added_files = [self.mock_data_files[1]]

        result = _determine_snapshot_type(to_be_deleted_files, to_be_added_files)

        assert result == SnapshotType.REPLACE

    def test_determine_snapshot_type_delete(self):
        """Test snapshot type determination for delete operation."""
        to_be_deleted_files = [[self.mock_data_files[0]]]
        to_be_added_files = []

        result = _determine_snapshot_type(to_be_deleted_files, to_be_added_files)

        assert result == SnapshotType.DELETE


class TestConverterSessionUtilsEdgeCases:
    """Test edge cases and error conditions for converter session utilities."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_data_file = Mock(spec=DataFile)
        self.mock_data_file.file_path = "s3://bucket/test.parquet"
        self.mock_data_file.file_size_in_bytes = 1000

    def test_check_data_files_sequence_number_assertion_error(self):
        """Test that assertion is raised when result lists have different lengths."""
        # This test verifies the internal assertion logic
        data_files_list = [(1, self.mock_data_file)]
        equality_delete_files_list = [(2, self.mock_data_file)]

        # Normal case should not raise assertion error
        result_eq_files, result_data_files = check_data_files_sequence_number(
            data_files_list, equality_delete_files_list
        )

        # Verify the assertion condition is met
        assert len(result_eq_files) == len(result_data_files)

    def test_group_all_files_to_each_bucket_empty_dicts(self):
        """Test grouping with empty file dictionaries."""
        result = group_all_files_to_each_bucket({}, {}, {})
        assert result == []

    def test_sort_data_files_maintaining_order_identical_sequences_and_paths(self):
        """Test sorting with identical sequence numbers and file paths."""
        # This shouldn't happen in practice, but test the behavior
        mock_file1 = Mock(spec=DataFile)
        mock_file1.file_path = "s3://bucket/same.parquet"
        mock_file2 = Mock(spec=DataFile)
        mock_file2.file_path = "s3://bucket/same.parquet"

        data_files = [
            (1, mock_file1),
            (1, mock_file2),
        ]

        result = sort_data_files_maintaining_order(data_files)

        # Should maintain stable sort order
        assert len(result) == 2
        assert all(item[0] == 1 for item in result)

    def test_partition_value_record_edge_cases(self):
        """Test partition value conversion with edge cases."""
        # Test with nested brackets
        mock_partition = Mock()
        mock_partition.__repr__ = Mock(return_value="Record[complex=[nested=value]]")

        result = partition_value_record_to_partition_value_string(mock_partition)

        # Should extract content between first [ and first ]
        expected = "complex=[nested=value"
        assert result == expected

    def test_snapshot_type_enum_values(self):
        """Test that SnapshotType enum has expected values."""
        assert SnapshotType.NONE.value == "none"
        assert SnapshotType.APPEND.value == "append"
        assert SnapshotType.REPLACE.value == "replace"
        assert SnapshotType.DELETE.value == "delete"

    def test_determine_snapshot_type_with_nested_delete_files(self):
        """Test snapshot type determination with nested delete file lists."""
        # Multiple groups of files to delete
        to_be_deleted_files = [
            [self.mock_data_file],  # Group 1
            [self.mock_data_file, self.mock_data_file],  # Group 2
        ]
        to_be_added_files = [self.mock_data_file]

        result = _determine_snapshot_type(to_be_deleted_files, to_be_added_files)

        assert result == SnapshotType.REPLACE

    def test_get_snapshot_action_description_with_nested_delete_files(self):
        """Test description generation with nested delete file lists."""
        files_to_delete = [
            [self.mock_data_file],  # 1 file in group 1
            [self.mock_data_file, self.mock_data_file],  # 2 files in group 2
        ]
        files_to_add = [self.mock_data_file]

        result = _get_snapshot_action_description(
            SnapshotType.REPLACE, files_to_delete, files_to_add
        )

        # Should count total files across all groups
        assert result == "Replacing 3 files with 1 new files"
