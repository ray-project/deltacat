import os
import pytest
import re
from functools import partial
from datetime import datetime, timezone

from pyarrow.fs import (
    FileType,
    LocalFileSystem,
)
from deltacat.utils.filesystem import (
    list_directory,
    list_directory_partitioned,
    get_file_info,
    get_file_info_partitioned,
    write_file,
    write_file_partitioned,
    read_file,
    resolve_path_and_filesystem,
    epoch_timestamp_partition_transform,
    parse_epoch_timestamp_partitions,
    exponential_partition_transform,
    parse_exponential_partitions,
    remove_exponential_partitions,
)
from deltacat.storage.model.metafile import MetafileRevisionInfo
from deltacat.constants import UNSIGNED_INT64_MAX_VALUE


def epoch_timestamp_file_parser(file_path: str) -> int:
    """
    Extract epoch timestamp from a partitioned file path.

    Expected path format: .../YYYY/MM/DD/HH/MM/filename
    Returns the epoch timestamp that would have been used for partitioning.
    """
    # Extract the partition path components (last 5 directories before filename)
    path_parts = file_path.split("/")
    if len(path_parts) < 6:  # Need at least 5 partition dirs + filename
        return None

    try:
        year = int(path_parts[-6])
        month = int(path_parts[-5])
        day = int(path_parts[-4])
        hour = int(path_parts[-3])
        minute = int(path_parts[-2])

        # Convert to epoch timestamp
        dt = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
        return int(dt.timestamp())
    except (ValueError, IndexError):
        return None


def exponential_file_parser(file_path: str) -> int:
    """
    Extract the original partition value from a filename in an exponential partition.

    Expected filename format: {zero_padded_value}_filename.ext
    Returns the integer value that would have been used for partitioning.
    """
    filename = os.path.basename(file_path)
    match = re.match(r"^(\d+)_", filename)
    if match:
        return int(match.group(1))
    return None


class TestResolvePathAndFilesystem:
    """Test resolve_path_and_filesystem function."""

    def test_resolve_local_path(self, temp_dir):
        """Test resolving a local path."""
        test_file = os.path.join(temp_dir, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")

        path, filesystem = resolve_path_and_filesystem(test_file)
        assert path == test_file
        assert filesystem is not None

    def test_resolve_with_provided_filesystem(self, temp_dir):
        """Test resolving with a provided filesystem."""
        test_file = os.path.join(temp_dir, "test.txt")
        local_fs = LocalFileSystem()

        path, filesystem = resolve_path_and_filesystem(test_file, local_fs)
        assert path == test_file
        assert filesystem is local_fs

    def test_resolve_nonexistent_path(self, temp_dir):
        """Test resolving a nonexistent path."""
        nonexistent = os.path.join(temp_dir, "nonexistent.txt")
        path, filesystem = resolve_path_and_filesystem(nonexistent)
        assert path == nonexistent
        assert filesystem is not None


class TestListDirectory:
    """Test list_directory function."""

    def test_list_empty_directory(self, temp_dir):
        """Test listing an empty directory."""
        _, filesystem = resolve_path_and_filesystem(temp_dir)
        files = list_directory(temp_dir, filesystem)
        assert files == []

    def test_list_directory_with_files(self, temp_dir):
        """Test listing a directory with files."""
        # Create test files
        file1 = os.path.join(temp_dir, "file1.txt")
        file2 = os.path.join(temp_dir, "file2.txt")

        for f in [file1, file2]:
            with open(f, "w") as fh:
                fh.write(f"content of {os.path.basename(f)}")

        _, filesystem = resolve_path_and_filesystem(temp_dir)
        files = list_directory(temp_dir, filesystem)
        assert len(files) == 2

        # Check file info
        file_paths = [f[0] for f in files]
        assert any("file1.txt" in p for p in file_paths)
        assert any("file2.txt" in p for p in file_paths)

    def test_list_directory_recursive(self, temp_dir):
        """Test recursive directory listing."""
        # Create nested structure
        subdir1 = os.path.join(temp_dir, "dir1")
        subdir2 = os.path.join(subdir1, "dir2")
        os.makedirs(subdir2)

        file1 = os.path.join(temp_dir, "root.txt")
        file2 = os.path.join(subdir1, "level1.txt")
        file3 = os.path.join(subdir2, "level2.txt")

        for f in [file1, file2, file3]:
            with open(f, "w") as fh:
                fh.write(f"content of {os.path.basename(f)}")

        _, filesystem = resolve_path_and_filesystem(temp_dir)
        files = list_directory(temp_dir, filesystem, recursive=True)
        # Filter out directories (entries with None size)
        files_only = [f for f in files if f[1] is not None]
        assert len(files_only) == 3

    def test_list_directory_with_prefixes(self, temp_dir):
        """Test listing with exclude prefixes."""
        # Create files with different prefixes
        files_to_create = ["file1.txt", "temp_file.txt", "_hidden.txt", "data.csv"]
        for filename in files_to_create:
            with open(os.path.join(temp_dir, filename), "w") as f:
                f.write("content")

        _, filesystem = resolve_path_and_filesystem(temp_dir)
        files = list_directory(temp_dir, filesystem, exclude_prefixes=["temp", "_"])
        assert (
            len(files) == 2
        )  # Should exclude temp_file.txt (starts with "temp") and _hidden.txt (starts with "_")

        file_names = [os.path.basename(f[0]) for f in files]
        assert "file1.txt" in file_names
        assert "data.csv" in file_names
        assert "temp_file.txt" not in file_names
        assert "_hidden.txt" not in file_names

    def test_list_directory_missing_path_ignore(self, temp_dir):
        """Test listing missing path with ignore_missing_path=True."""
        missing_dir = os.path.join(temp_dir, "missing")
        _, filesystem = resolve_path_and_filesystem(temp_dir)
        files = list_directory(missing_dir, filesystem, ignore_missing_path=True)
        assert files == []

    def test_list_directory_missing_path_no_ignore(self, temp_dir):
        """Test listing missing path with ignore_missing_path=False."""
        missing_dir = os.path.join(temp_dir, "missing")
        _, filesystem = resolve_path_and_filesystem(temp_dir)
        with pytest.raises(FileNotFoundError):
            list_directory(missing_dir, filesystem, ignore_missing_path=False)


class TestGetFileInfo:
    """Test get_file_info function."""

    def test_get_file_info_existing_file(self, temp_dir):
        """Test getting info for an existing file."""
        test_file = os.path.join(temp_dir, "test.txt")
        content = "Hello, World!"
        with open(test_file, "w") as f:
            f.write(content)

        _, filesystem = resolve_path_and_filesystem(temp_dir)
        file_info = get_file_info(test_file, filesystem)
        assert file_info.type == FileType.File
        assert file_info.size == len(content)

    def test_get_file_info_directory(self, temp_dir):
        """Test getting info for a directory."""
        _, filesystem = resolve_path_and_filesystem(temp_dir)
        file_info = get_file_info(temp_dir, filesystem)
        assert file_info.type == FileType.Directory

    def test_get_file_info_missing_file_ignore(self, temp_dir):
        """Test getting info for missing file with ignore_missing_path=True."""
        missing_file = os.path.join(temp_dir, "missing.txt")
        _, filesystem = resolve_path_and_filesystem(temp_dir)
        file_info = get_file_info(missing_file, filesystem, ignore_missing_path=True)
        assert file_info.type == FileType.NotFound

    def test_get_file_info_missing_file_no_ignore(self, temp_dir):
        """Test getting info for missing file with ignore_missing_path=False."""
        missing_file = os.path.join(temp_dir, "missing.txt")
        _, filesystem = resolve_path_and_filesystem(temp_dir)
        with pytest.raises(FileNotFoundError):
            get_file_info(missing_file, filesystem, ignore_missing_path=False)


class TestWriteFile:
    """Test write_file function."""

    def test_write_text_file(self, temp_dir):
        """Test writing a text file."""
        test_file = os.path.join(temp_dir, "test.txt")
        content = "Hello, World!"

        write_file(test_file, content)

        # Verify file was written
        with open(test_file, "r") as f:
            assert f.read() == content

    def test_write_binary_file(self, temp_dir):
        """Test writing a binary file."""
        test_file = os.path.join(temp_dir, "test.bin")
        content = b"\x00\x01\x02\x03"

        write_file(test_file, content)

        # Verify file was written
        with open(test_file, "rb") as f:
            assert f.read() == content

    def test_write_file_creates_directory(self, temp_dir):
        """Test that write_file creates parent directories."""
        nested_file = os.path.join(temp_dir, "nested", "dir", "file.txt")
        content = "nested content"

        write_file(nested_file, content)

        # Verify file was written
        with open(nested_file, "r") as f:
            assert f.read() == content


class TestReadFile:
    """Test read_file function."""

    def test_read_text_file(self, temp_dir):
        """Test reading a text file."""
        test_file = os.path.join(temp_dir, "test.txt")
        content = "Hello, World!"
        with open(test_file, "w") as f:
            f.write(content)

        read_content = read_file(test_file)
        assert read_content.decode("utf-8") == content

    def test_read_binary_file(self, temp_dir):
        """Test reading a binary file."""
        test_file = os.path.join(temp_dir, "test.bin")
        content = b"\x00\x01\x02\x03"
        with open(test_file, "wb") as f:
            f.write(content)

        read_content = read_file(test_file)
        assert read_content == content

    def test_read_missing_file_ignore(self, temp_dir):
        """Test reading missing file with fail_if_not_found=False."""
        missing_file = os.path.join(temp_dir, "missing.txt")
        content = read_file(missing_file, fail_if_not_found=False)
        assert content is None

    def test_read_missing_file_no_ignore(self, temp_dir):
        """Test reading missing file with fail_if_not_found=True."""
        missing_file = os.path.join(temp_dir, "missing.txt")
        with pytest.raises(FileNotFoundError):
            read_file(missing_file, fail_if_not_found=True)


class TestEpochTimestampPartitionTransform:
    """Test epoch_timestamp_partition_transform function."""

    def test_transform_seconds_precision(self):
        """Test transforming seconds precision timestamp."""
        # Dec 31, 2023 16:00:00 UTC
        timestamp = 1704038400
        result = epoch_timestamp_partition_transform(timestamp)

        expected = [
            "2023",  # Year
            "12",  # Month
            "31",  # Day
            "16",  # Hour (16:00 UTC)
            "00",  # Minute
        ]
        assert result == expected

    def test_transform_milliseconds_precision(self):
        """Test transforming milliseconds precision timestamp."""
        # Dec 31, 2023 16:00:00 UTC in milliseconds
        timestamp = 1704038400000
        result = epoch_timestamp_partition_transform(timestamp)

        # Should produce same result as seconds precision
        expected = [
            "2023",  # Year
            "12",  # Month
            "31",  # Day
            "16",  # Hour (16:00 UTC)
            "00",  # Minute
        ]
        assert result == expected

    def test_transform_nanoseconds_precision(self):
        """Test transforming nanoseconds precision timestamp."""
        # Dec 31, 2023 16:00:00 UTC in nanoseconds
        timestamp = 1704038400000000000
        result = epoch_timestamp_partition_transform(timestamp)

        # Should produce same result as seconds precision
        expected = [
            "2023",  # Year
            "12",  # Month
            "31",  # Day
            "16",  # Hour (16:00 UTC)
            "00",  # Minute
        ]
        assert result == expected

    def test_transform_invalid_input(self):
        """Test transforming invalid input."""
        with pytest.raises(TypeError):
            epoch_timestamp_partition_transform("not_a_number")

    def test_transform_negative_timestamp(self):
        """Test that negative timestamps raise ValueError."""
        with pytest.raises(ValueError, match="Epoch timestamp must be non-negative"):
            epoch_timestamp_partition_transform(-1)

        with pytest.raises(ValueError, match="Epoch timestamp must be non-negative"):
            epoch_timestamp_partition_transform(-1234567890)

    def test_transform_too_short_timestamp(self):
        """Test that timestamps shorter than 10 digits raise ValueError for seconds precision."""
        # Test various short timestamps
        short_timestamps = [0, 1, 123, 12345, 123456, 1234567, 12345678, 123456789]

        for timestamp in short_timestamps:
            with pytest.raises(
                ValueError, match="too short.*Expected at least 10 digits"
            ):
                epoch_timestamp_partition_transform(timestamp)

    def test_transform_valid_edge_cases(self):
        """Test valid edge cases for timestamp validation."""
        # Test minimum valid second-precision timestamp (10 digits)
        result = epoch_timestamp_partition_transform(
            1000000000
        )  # 2001-09-09 01:46:40 UTC
        assert len(result) == 5
        assert all(isinstance(part, str) for part in result)

        # Test very large valid timestamp
        large_timestamp = 2147483647  # Max 32-bit signed integer
        result = epoch_timestamp_partition_transform(large_timestamp)
        assert len(result) == 5
        assert result[0] == "2038"  # Year should be 2038

        # Test timestamp exactly at 10 digits
        result = epoch_timestamp_partition_transform(1704067200)  # 10 digits
        assert len(result) == 5
        assert result == ["2024", "01", "01", "00", "00"]


class TestEpochTimestampPartitionParser:
    """Test epoch timestamp partition parser function."""

    def test_parse_valid_timestamp(self):
        """Test parsing a valid timestamp from human-readable components."""
        result = parse_epoch_timestamp_partitions(["2024", "01", "01", "12", "00"])
        assert result == 1704110400  # 2024-01-01 12:00:00 UTC

    def test_parse_partial_components(self):
        """Test parsing with partial components."""
        # Just year and month
        result = parse_epoch_timestamp_partitions(["2024", "01"])
        assert result == 1704067200  # 2024-01-01 00:00:00 UTC

        # Year, month, day
        result = parse_epoch_timestamp_partitions(["2024", "01", "15"])
        assert result == 1705276800  # 2024-01-15 00:00:00 UTC

    def test_parse_invalid_components(self):
        """Test parsing invalid components."""
        # Invalid month
        result = parse_epoch_timestamp_partitions(["2024", "13", "01"])
        assert result is None

        # Invalid day
        result = parse_epoch_timestamp_partitions(["2024", "01", "32"])
        assert result is None

        # Invalid hour
        result = parse_epoch_timestamp_partitions(["2024", "01", "01", "25"])
        assert result is None

        # Invalid minute
        result = parse_epoch_timestamp_partitions(["2024", "01", "01", "12", "60"])
        assert result is None

    def test_parse_empty_list(self):
        """Test parsing an empty list."""
        result = parse_epoch_timestamp_partitions([])
        assert result is None

    def test_parse_invalid_format(self):
        """Test parsing components with invalid format."""
        result = parse_epoch_timestamp_partitions(["not_a_year", "01"])
        assert result is None

        result = parse_epoch_timestamp_partitions(["2024", "not_a_month"])
        assert result is None


class TestExponentialTransform:
    """Test exponential transform function."""

    def test_basic_two_level_transform(self):
        """Test basic 2-level exponential transform with default base=1000."""
        result = exponential_partition_transform(1500, base=1000, levels=2)
        expected = ["1000000", "2000"]
        assert result == expected

    def test_user_examples(self):
        """Test the specific examples provided by the user."""
        # First example: 1500 with base=1000, levels=2
        result = exponential_partition_transform(1500, base=1000, levels=2)
        expected = ["1000000", "2000"]
        assert result == expected

        # Second example: 1500000 with base=1000, levels=2
        result = exponential_partition_transform(1500000, base=1000, levels=2)
        expected = ["2000000", "500000"]
        assert result == expected

    def test_three_level_transform(self):
        """Test 3-level exponential transform."""
        result = exponential_partition_transform(2500, base=1000, levels=3)
        expected = ["1000000000", "1000000", "3000"]
        assert result == expected

    def test_single_level_transform(self):
        """Test single-level exponential transform."""
        result = exponential_partition_transform(2500, base=1000, levels=1)
        expected = ["3000"]
        assert result == expected

    def test_four_level_transform(self):
        """Test 4-level exponential transform."""
        result = exponential_partition_transform(1500, base=1000, levels=4)
        expected = ["1000000000000", "1000000000", "1000000", "2000"]
        assert result == expected

    def test_different_base_values(self):
        """Test exponential transform with different base values."""
        # Base 10
        result = exponential_partition_transform(123, base=10, levels=2)
        expected = ["200", "30"]
        assert result == expected

        # Base 2
        result = exponential_partition_transform(13, base=2, levels=3)
        expected = ["16", "8", "2"]
        assert result == expected

    def test_exact_multiples(self):
        """Test with values that are exact multiples of the base powers."""
        result = exponential_partition_transform(2000, base=1000, levels=2)
        expected = ["1000000", "2000"]
        assert result == expected

        result = exponential_partition_transform(1000000, base=1000, levels=3)
        expected = ["1000000000", "1000000", "1000000"]
        assert result == expected

    def test_exact_multiples_boundary(self):
        """Test values that are exact multiples +/- 1 to verify boundary conditions."""
        # Test 2000 +/- 1 (2000 is 2 * 1000^1 for levels=2)
        result = exponential_partition_transform(1999, base=1000, levels=2)
        expected = ["1000000", "2000"]  # 1999 rounds up to same partitions as 2000
        assert result == expected

        result = exponential_partition_transform(2001, base=1000, levels=2)
        expected = ["1000000", "3000"]  # 2001 requires higher total than 2000
        assert result == expected

        # Test 1000000 +/- 1 (1000000 is 1 * 1000^2 for levels=3)
        result = exponential_partition_transform(999999, base=1000, levels=3)
        expected = [
            "1000000000",
            "1000000",
            "1000000",
        ]  # 999999 rounds up to same as 1000000
        assert result == expected

        result = exponential_partition_transform(1000001, base=1000, levels=3)
        expected = [
            "1000000000",
            "2000000",
            "1000",
        ]  # 1000001 requires adjustment in lower levels
        assert result == expected

        # Test with smaller base for more obvious boundary effects
        result = exponential_partition_transform(99, base=10, levels=2)
        expected = ["100", "100"]  # 99 rounds up to 200 total
        assert result == expected

        result = exponential_partition_transform(101, base=10, levels=2)
        expected = ["200", "10"]  # 101 rounds up to 210 total
        assert result == expected

    def test_small_values(self):
        """Test with small values."""
        result = exponential_partition_transform(1, base=1000, levels=2)
        expected = ["1000000", "1000"]
        assert result == expected

        result = exponential_partition_transform(999, base=1000, levels=2)
        expected = ["1000000", "1000"]
        assert result == expected

    def test_default_parameters(self):
        """Test with default parameters (base=1000, levels=2)."""
        result = exponential_partition_transform(1500)
        expected = ["1000000", "2000"]
        assert result == expected

    def test_invalid_inputs(self):
        """Test error handling for invalid inputs."""
        # Non-integer value
        with pytest.raises(TypeError, match="value must be an integer"):
            exponential_partition_transform("1500")

        # Non-integer base
        with pytest.raises(TypeError, match="base must be an integer"):
            exponential_partition_transform(1500, base="1000")

        # Non-integer levels
        with pytest.raises(TypeError, match="levels must be an integer"):
            exponential_partition_transform(1500, levels="2")

        # Zero value
        with pytest.raises(ValueError, match="value must be a positive integer"):
            exponential_partition_transform(0)

        # Negative value
        with pytest.raises(ValueError, match="value must be a positive integer"):
            exponential_partition_transform(-1)

        # Invalid base (<= 1)
        with pytest.raises(ValueError, match="base must be greater than 1"):
            exponential_partition_transform(1500, base=1)

        with pytest.raises(ValueError, match="base must be greater than 1"):
            exponential_partition_transform(1500, base=0)

        # Invalid levels (<= 0)
        with pytest.raises(ValueError, match="levels must be positive"):
            exponential_partition_transform(1500, levels=0)

        with pytest.raises(ValueError, match="levels must be positive"):
            exponential_partition_transform(1500, levels=-1)

    def test_edge_cases(self):
        """Test edge cases and boundary conditions."""
        # Very large value
        large_value = 10**18
        result = exponential_partition_transform(large_value, base=1000, levels=3)
        assert len(result) == 3
        assert all(isinstance(part, str) for part in result)

        # Large number of levels
        result = exponential_partition_transform(1500, base=1000, levels=5)
        expected = [
            "1000000000000000",
            "1000000000000",
            "1000000000",
            "1000000",
            "2000",
        ]
        assert result == expected

    def test_uint64_max_validation(self):
        """Test that exponential transform validates partition values don't exceed UNSIGNED_INT64_MAX_VALUE."""
        # Test case that should work (just under the limit)
        # Using a large but valid value
        large_valid_value = (
            10**15
        )  # 1 quadrillion - should be fine with reasonable base/levels
        result = exponential_partition_transform(large_valid_value, base=1000, levels=3)
        assert len(result) == 3
        assert all(isinstance(part, str) for part in result)

        # Test case that should fail - very large base with high levels
        # This should cause partition values to exceed UNSIGNED_INT64_MAX_VALUE
        with pytest.raises(ValueError, match="exceeds maximum allowed value"):
            # Using a very large base that will cause overflow
            exponential_partition_transform(10**10, base=10**10, levels=5)

        # Test case that should fail - extremely large input value with large base
        with pytest.raises(ValueError, match="exceeds maximum allowed value"):
            # This combination should definitely overflow
            exponential_partition_transform(
                UNSIGNED_INT64_MAX_VALUE, base=10**6, levels=4
            )

        # Test edge case - value that results in a reasonable partition value (should work)
        # We'll test with a smaller case we can calculate
        # For base=2, levels=1: partition_value = multiplier * 2^1 = multiplier * 2
        # For value=5, algorithm needs multiplier=3 (since 3*2=6 >= 5), so result is ["6"]
        result = exponential_partition_transform(5, base=2, levels=1)
        assert result == ["6"]
        assert int(result[0]) <= UNSIGNED_INT64_MAX_VALUE


class TestWriteFilePartitioned:
    """Test write_file_partitioned function."""

    def test_write_partitioned_file(self, temp_dir):
        """Test writing a partitioned file."""
        # Create a test timestamp and transform
        timestamp = 1704038400  # Dec 31, 2023 16:00:00 UTC
        partitions = epoch_timestamp_partition_transform(timestamp)

        # Write partitioned file
        base_filename = "test.txt"
        content = "partitioned content"

        write_file_partitioned(
            path=os.path.join(temp_dir, base_filename),
            data=content,
            partition_value=timestamp,
            partition_transform=epoch_timestamp_partition_transform,
        )

        # Verify the partitioned directory structure was created
        expected_path = os.path.join(
            temp_dir, *partitions, base_filename  # Unpack partition directories
        )

        assert os.path.exists(expected_path)
        with open(expected_path, "r") as f:
            assert f.read() == content

    def test_write_partitioned_with_invalid_partition_name(self, temp_dir):
        """Test writing with partition directory containing path separators."""

        def bad_transform(ts):
            return ["invalid/path", "name"]  # Contains path separators

        with pytest.raises(
            ValueError, match="Partition directory name cannot contain path separators"
        ):
            write_file_partitioned(
                path=os.path.join(temp_dir, "test.txt"),
                data="content",
                partition_value=1234567890,
                partition_transform=bad_transform,
            )

    def test_write_partitioned_returns_correct_path(self, temp_dir):
        """Test that write_file_partitioned returns the correct write path."""
        # Create a test timestamp and transform
        timestamp = 1704038400  # Dec 31, 2023 16:00:00 UTC
        partitions = epoch_timestamp_partition_transform(timestamp)

        # Write partitioned file and capture the returned path
        base_filename = "test.txt"
        content = "partitioned content"

        returned_path = write_file_partitioned(
            path=os.path.join(temp_dir, base_filename),
            data=content,
            partition_value=timestamp,
            partition_transform=epoch_timestamp_partition_transform,
        )

        # Construct the expected path
        expected_path = os.path.join(
            temp_dir, *partitions, base_filename  # Unpack partition directories
        )

        # Validate that the returned path matches the expected path
        assert returned_path == expected_path

        # Verify that the file was actually written to the returned path
        assert os.path.exists(returned_path)
        with open(returned_path, "r") as f:
            assert f.read() == content

        # Test with different partition transform for additional validation
        def custom_transform(ts):
            return ["custom", "partition", str(ts)]

        custom_timestamp = 1234567890
        custom_content = "custom partitioned content"

        custom_returned_path = write_file_partitioned(
            path=os.path.join(temp_dir, "custom.txt"),
            data=custom_content,
            partition_value=custom_timestamp,
            partition_transform=custom_transform,
        )

        # Construct expected custom path
        custom_expected_path = os.path.join(
            temp_dir, "custom", "partition", str(custom_timestamp), "custom.txt"
        )

        # Validate custom path
        assert custom_returned_path == custom_expected_path
        assert os.path.exists(custom_returned_path)
        with open(custom_returned_path, "r") as f:
            assert f.read() == custom_content

    def test_write_partitioned_levels_above_file(self, temp_dir):
        """Test writing a partitioned file with partition_levels_above_file parameter."""
        # Create a test timestamp and transform
        timestamp = 1704038400  # Dec 31, 2023 16:00:00 UTC
        partitions = epoch_timestamp_partition_transform(timestamp)

        # Write partitioned file with partitions one level above
        base_filename = "subdir/test.txt"
        content = "partitioned above content"

        returned_path = write_file_partitioned(
            path=os.path.join(temp_dir, base_filename),
            data=content,
            partition_value=timestamp,
            partition_transform=epoch_timestamp_partition_transform,
            partition_levels_above_file=1,
        )

        # Verify the partitioned directory structure was created one level above
        # The partitions should be inserted between temp_dir and "subdir"
        expected_path = os.path.join(
            temp_dir, *partitions, "subdir", "test.txt"  # Unpack partition directories
        )

        # Validate that the returned path matches the expected path
        assert returned_path == expected_path

        # Verify that the file was actually written to the returned path
        assert os.path.exists(returned_path)
        with open(returned_path, "r") as f:
            assert f.read() == content


class TestGetFileInfoPartitioned:
    """Test get_file_info_partitioned function."""

    def test_get_partitioned_file_info(self, temp_dir):
        """Test getting info for a partitioned file."""
        # First write a partitioned file
        timestamp = 1704038400
        filename = "test.txt"
        content = "test content"

        write_file_partitioned(
            path=os.path.join(temp_dir, filename),
            data=content,
            partition_value=timestamp,
            partition_transform=epoch_timestamp_partition_transform,
        )

        # Now get its info
        _, filesystem = resolve_path_and_filesystem(temp_dir)
        file_info = get_file_info_partitioned(
            path=os.path.join(temp_dir, filename),
            filesystem=filesystem,
            partition_value=timestamp,
            partition_transform=epoch_timestamp_partition_transform,
        )

        assert file_info.type == FileType.File
        assert file_info.size == len(content)


class TestListDirectoryPartitioned:
    """Test list_directory_partitioned function."""

    def test_list_partitioned_directory_empty(self, temp_dir):
        """Test listing empty partitioned directory."""

        def simple_parser(partition_dirs):
            return parse_epoch_timestamp_partitions(partition_dirs)

        _, filesystem = resolve_path_and_filesystem(temp_dir)
        files = list_directory_partitioned(
            path=temp_dir,
            filesystem=filesystem,
            partition_value=1704038400,
            partition_transform=epoch_timestamp_partition_transform,
            partition_file_parser=epoch_timestamp_file_parser,
            limit=10,
            partition_dir_parser=simple_parser,
        )
        assert files == []

    def test_list_partitioned_directory_with_files(self, temp_dir):
        """Test listing partitioned directory with files."""

        def simple_parser(partition_dirs):
            return parse_epoch_timestamp_partitions(partition_dirs)

        # Create multiple partitioned files
        timestamps = [
            1704038400,  # Dec 31, 2023 16:00:00 UTC
            1703952000,  # Dec 30, 2023 16:00:00 UTC
            1704124800,  # Jan 1, 2024 16:00:00 UTC (target - should not be included)
        ]

        for i, timestamp in enumerate(timestamps):
            filename = f"file{i}.txt"
            content = f"content {i}"

            write_file_partitioned(
                path=os.path.join(temp_dir, filename),
                data=content,
                partition_value=timestamp,
                partition_transform=epoch_timestamp_partition_transform,
            )

        # List files prior to Jan 1, 2024 (inclusive)
        _, filesystem = resolve_path_and_filesystem(temp_dir)
        files = list_directory_partitioned(
            path=temp_dir,
            filesystem=filesystem,
            partition_value=1704124800,  # Jan 1, 2024
            partition_transform=epoch_timestamp_partition_transform,
            partition_file_parser=epoch_timestamp_file_parser,
            limit=10,
            partition_dir_parser=simple_parser,
        )

        # Should find 3 files (both from Dec 2023 + target file from Jan 1, 2024)
        assert len(files) == 3

    def test_list_partitioned_directory_invalid_limit(self, temp_dir):
        """Test listing with invalid limit."""

        def simple_parser(partition_dirs):
            return parse_epoch_timestamp_partitions(partition_dirs)

        _, filesystem = resolve_path_and_filesystem(temp_dir)

        with pytest.raises(
            ValueError, match="limit must be a positive integer or None"
        ):
            list_directory_partitioned(
                path=temp_dir,
                filesystem=filesystem,
                partition_value=1704038400,
                partition_transform=epoch_timestamp_partition_transform,
                partition_file_parser=epoch_timestamp_file_parser,
                limit=0,
                partition_dir_parser=simple_parser,
            )

    def test_list_partitioned_directory_invalid_partition_transform(self, temp_dir):
        """Test listing with invalid partition transform."""

        def simple_parser(partition_dirs):
            return parse_epoch_timestamp_partitions(partition_dirs)

        _, filesystem = resolve_path_and_filesystem(temp_dir)

        with pytest.raises(ValueError, match="partition_transform must return a list"):
            list_directory_partitioned(
                path=temp_dir,
                filesystem=filesystem,
                partition_value=1704038400,
                partition_transform=lambda x: "not_a_list",
                partition_file_parser=epoch_timestamp_file_parser,
                limit=10,
                partition_dir_parser=simple_parser,
            )

    def test_list_partitioned_directory_invalid_partition_names(self, temp_dir):
        """Test listing with invalid partition directory names."""

        def simple_parser(partition_dirs):
            return parse_epoch_timestamp_partitions(partition_dirs)

        _, filesystem = resolve_path_and_filesystem(temp_dir)

        def bad_transform(ts):
            return ["invalid/path"]  # Contains path separator

        with pytest.raises(
            ValueError, match="Partition directory name cannot contain path separators"
        ):
            list_directory_partitioned(
                path=temp_dir,
                filesystem=filesystem,
                partition_value=1704038400,
                partition_transform=bad_transform,
                partition_file_parser=epoch_timestamp_file_parser,
                limit=10,
                partition_dir_parser=simple_parser,
            )

    def test_list_partitioned_directory_limit(self, temp_dir):
        """Test that the limit parameter works correctly."""

        def simple_parser(partition_dirs):
            return parse_epoch_timestamp_partitions(partition_dirs)

        # Create multiple test files in different partitions
        timestamps = [
            1704067200,  # Jan 1, 2024 00:00:00 UTC
            1704067260,  # Jan 1, 2024 00:01:00 UTC
            1704067320,  # Jan 1, 2024 00:02:00 UTC
            1704067380,  # Jan 1, 2024 00:03:00 UTC
            1703952000,  # Dec 30, 2023 16:00:00 UTC
        ]

        for i, timestamp in enumerate(timestamps):
            filename = f"file{i}.txt"
            content = f"content {i}"

            write_file_partitioned(
                path=os.path.join(temp_dir, filename),
                data=content,
                partition_value=timestamp,
                partition_transform=epoch_timestamp_partition_transform,
            )

        # Test with limit of 2
        _, filesystem = resolve_path_and_filesystem(temp_dir)
        limited_files = list_directory_partitioned(
            path=temp_dir,
            filesystem=filesystem,
            partition_value=1704124800,  # Jan 1, 2024 16:00:00 UTC (target)
            partition_transform=epoch_timestamp_partition_transform,
            partition_file_parser=epoch_timestamp_file_parser,
            limit=2,
            partition_dir_parser=simple_parser,
        )

        # Should return exactly 2 files (or fewer if not enough exist)
        assert len(limited_files) <= 2
        assert len(limited_files) > 0  # Should find at least some files

    def test_list_partitioned_directory_correct_partitions(self, temp_dir):
        """Test that files are returned from correct partitions (prior to target)."""

        def simple_parser(partition_dirs):
            return parse_epoch_timestamp_partitions(partition_dirs)

        # Create test files in different partitions
        test_cases = [
            (1704038400, "file1_dec31_16h.txt"),  # Dec 31, 2023 16:00:00 UTC
            (1704124800, "file2_jan01_16h.txt"),  # Jan 1, 2024 16:00:00 UTC (target)
            (1704038460, "file3_dec31_16h01m.txt"),  # Dec 31, 2023 16:01:00 UTC
            (1703952000, "file4_dec30_16h.txt"),  # Dec 30, 2023 16:00:00 UTC
        ]

        for timestamp, filename in test_cases:
            write_file_partitioned(
                path=os.path.join(temp_dir, filename),
                data=f"Content of {filename}",
                partition_value=timestamp,
                partition_transform=epoch_timestamp_partition_transform,
            )

        # List files prior to Jan 1, 2024 16:00:00 UTC (1704124800)
        _, filesystem = resolve_path_and_filesystem(temp_dir)
        prior_files = list_directory_partitioned(
            path=temp_dir,
            filesystem=filesystem,
            partition_value=1704124800,  # Jan 1, 2024 16:00:00 UTC
            partition_transform=epoch_timestamp_partition_transform,
            partition_file_parser=epoch_timestamp_file_parser,
            limit=10,
            partition_dir_parser=simple_parser,
        )

        # Should find files from prior partitions only
        assert len(prior_files) > 0, "Should have found files from prior partitions"

        file_paths = [file_path for file_path, _ in prior_files]

        # Verify files are from Dec 31, Dec 30, and Jan 1 partitions (including target)
        dec31_found = any(
            "2023/12/31" in path for path in file_paths
        )  # Dec 31 partition
        dec30_found = any(
            "2023/12/30" in path for path in file_paths
        )  # Dec 30 partition
        jan1_found = any(
            "2024/01/01" in path for path in file_paths
        )  # Jan 1 partition (should be found with <= logic)

        assert (
            dec31_found or dec30_found
        ), "Should find files from Dec 31 or Dec 30 partitions"
        assert (
            jan1_found
        ), "Should find files from Jan 1 partition (target partition with <= logic)"

    def test_list_partitioned_directory_cross_hour_boundary(self, temp_dir):
        """Test partitioning across hour boundaries."""

        def simple_parser(partition_dirs):
            return parse_epoch_timestamp_partitions(partition_dirs)

        # Create files at hour boundaries
        timestamps = [
            1704067200
            - 1,  # Just before Jan 1, 2024 00:00:00 UTC (Dec 31, 2023 23:59:59 UTC)
            1704067200,  # Exactly at Jan 1, 2024 00:00:00 UTC
            1704067200
            + 1,  # Just after Jan 1, 2024 00:00:00 UTC (Jan 1, 2024 00:00:01 UTC)
        ]

        for i, timestamp in enumerate(timestamps):
            filename = f"boundary_file{i}.txt"
            write_file_partitioned(
                path=os.path.join(temp_dir, filename),
                data=f"Boundary content {i}",
                partition_value=timestamp,
                partition_transform=epoch_timestamp_partition_transform,
            )

        # Query for files before the next hour
        _, filesystem = resolve_path_and_filesystem(temp_dir)
        boundary_files = list_directory_partitioned(
            path=temp_dir,
            filesystem=filesystem,
            partition_value=1704067200 + 3600,  # Jan 1, 2024 01:00:00 UTC
            partition_transform=epoch_timestamp_partition_transform,
            partition_file_parser=epoch_timestamp_file_parser,
            limit=10,
            partition_dir_parser=simple_parser,
        )

        # Should find all files from the previous hour
        assert len(boundary_files) == 3, "Should find all files from the previous hour"

    def test_list_partitioned_directory_result_ordering(self, temp_dir):
        """Test that results are ordered from closest partition to furthest partition."""

        def simple_parser(partition_dirs):
            return parse_epoch_timestamp_partitions(partition_dirs)

        # Target timestamp: Jan 1, 2024 12:00:00 (1704115200)
        target_timestamp = 1704115200

        # Create files with timestamps that will create partitions at different distances
        test_cases = [
            # Same day, previous hour (closest)
            (1704111600, "file_same_day_prev_hour.txt"),  # Jan 1, 2024 11:00:00
            # Previous day, same hour (next closest)
            (1704028800, "file_prev_day_same_hour.txt"),  # Dec 31, 2023 12:00:00
            # Same day, two hours back (further)
            (1704108000, "file_same_day_two_hours_back.txt"),  # Jan 1, 2024 10:00:00
            # Two days back, same hour (furthest)
            (1703942400, "file_two_days_back_same_hour.txt"),  # Dec 30, 2023 12:00:00
            # Same day, three hours back (even further)
            (1704104400, "file_same_day_three_hours_back.txt"),  # Jan 1, 2024 09:00:00
        ]

        # Write files using partitioned writer
        for timestamp, filename in test_cases:
            write_file_partitioned(
                path=os.path.join(temp_dir, filename),
                data=f"Content for {filename}",
                partition_value=timestamp,
                partition_transform=epoch_timestamp_partition_transform,
            )

        # Query for files prior to target timestamp
        _, filesystem = resolve_path_and_filesystem(temp_dir)
        ordered_files = list_directory_partitioned(
            path=temp_dir,
            filesystem=filesystem,
            partition_value=target_timestamp,
            partition_transform=epoch_timestamp_partition_transform,
            partition_file_parser=epoch_timestamp_file_parser,
            limit=10,
            partition_dir_parser=simple_parser,
        )

        # Should find files from all test partitions
        assert len(ordered_files) == 5, f"Expected 5 files, got {len(ordered_files)}"

        # Expected order by partition proximity (closest to furthest):
        # 1. Jan 1, 2024 11:00:00 (same day, previous hour)
        # 2. Jan 1, 2024 10:00:00 (same day, two hours back)
        # 3. Jan 1, 2024 09:00:00 (same day, three hours back)
        # 4. Dec 31, 2023 12:00:00 (previous day, same hour)
        # 5. Dec 30, 2023 12:00:00 (two days back, same hour)

        # The partition directories will be based on the epoch timestamp transform
        # Jan 1, 2024 11:00:00 -> partitions: [2024, 01, 01, 11, 00] -> dirs with timestamps ending at those boundaries
        # Dec 31, 2023 12:00:00 -> partitions: [2023, 12, 31, 12, 00] -> different year/month/day

        # Verify the expected ordering based on actual results
        expected_order = [
            "file_same_day_prev_hour.txt",  # Jan 1 11:00 (closest - same day, 1 hour back)
            "file_same_day_two_hours_back.txt",  # Jan 1 10:00 (next closest - same day, 2 hours back)
            "file_same_day_three_hours_back.txt",  # Jan 1 09:00 (next closest - same day, 3 hours back)
            "file_prev_day_same_hour.txt",  # Dec 31 12:00 (further - previous day, same hour)
            "file_two_days_back_same_hour.txt",  # Dec 30 12:00 (furthest - two days back, same hour)
        ]

        # Check that we have the expected files
        actual_filenames = [os.path.basename(path) for path, _ in ordered_files]
        assert len(actual_filenames) == len(
            expected_order
        ), f"Expected {len(expected_order)} files, got {len(actual_filenames)}"

        # Check that the ordering matches exactly
        assert (
            actual_filenames == expected_order
        ), f"File ordering incorrect.\nExpected: {expected_order}\nActual: {actual_filenames}"

        # Additional validation: ensure same-day files come before cross-day files
        same_day_indices = [
            i for i, filename in enumerate(actual_filenames) if "same_day" in filename
        ]
        cross_day_indices = [
            i
            for i, filename in enumerate(actual_filenames)
            if "prev_day" in filename or "two_days" in filename
        ]

        if same_day_indices and cross_day_indices:
            max_same_day = max(same_day_indices)
            min_cross_day = min(cross_day_indices)
            assert (
                max_same_day < min_cross_day
            ), "Same-day files should appear before cross-day files"

    def test_list_partitioned_directory_ordering_with_limit(self, temp_dir):
        """Test that ordering is maintained when limit is applied."""

        def simple_parser(partition_dirs):
            return parse_epoch_timestamp_partitions(partition_dirs)

        # Create files with timestamps that will create different partition distances
        test_cases = [
            (1704111600, "file_12_20pm.txt"),  # Jan 1, 2024 12:20:00 UTC
            (1704028800, "file_dec31_13_20pm.txt"),  # Dec 31, 2023 13:20:00 UTC
            (1704108000, "file_11_20am.txt"),  # Jan 1, 2024 11:20:00 UTC
            (1703942400, "file_dec30_13_20pm.txt"),  # Dec 30, 2023 13:20:00 UTC
        ]

        for timestamp, filename in test_cases:
            write_file_partitioned(
                path=os.path.join(temp_dir, filename),
                data=f"Content for {filename}",
                partition_value=timestamp,
                partition_transform=epoch_timestamp_partition_transform,
            )

        # Query with limit of 2 (should get files from the 2 closest partitions)
        _, filesystem = resolve_path_and_filesystem(temp_dir)
        limited_files = list_directory_partitioned(
            path=temp_dir,
            filesystem=filesystem,
            partition_value=1704115200,  # Jan 1, 2024 13:20:00 UTC (target)
            partition_transform=epoch_timestamp_partition_transform,
            partition_file_parser=epoch_timestamp_file_parser,
            limit=2,
            partition_dir_parser=simple_parser,
        )

        # Should get at most 2 files
        assert (
            len(limited_files) <= 2
        ), f"Limit should restrict to 2 files, got {len(limited_files)}"
        assert len(limited_files) > 0, "Should get at least some files"

        # The key test: when we apply a limit, we should get files from the closest partitions
        # This validates that ordering is respected even with limits
        file_paths = [file_path for file_path, _ in limited_files]

        # Extract the partition levels from the file paths to understand ordering
        # The paths will have partition directories like: /year/month/day/hour/minute/filename
        partition_levels = []
        for path in file_paths:
            # Split path and find the partition directories (should be numeric timestamps)
            parts = path.split("/")
            # Find numeric parts that look like timestamps (10-13 digits)
            timestamp_parts = [p for p in parts if p.isdigit() and 10 <= len(p) <= 13]
            if timestamp_parts:
                partition_levels.append(timestamp_parts)

        # With limit=2, we should have at most 2 different partition combinations
        unique_partitions = set()
        for parts in partition_levels:
            unique_partitions.add(tuple(parts))

        # The number of unique partition combinations should be <= 2 (our limit)
        assert (
            len(unique_partitions) <= 2
        ), f"Limit should restrict to 2 partitions, got {len(unique_partitions)} unique partitions"

    def test_list_partitioned_directory_comprehensive_ordering(self, temp_dir):
        """Test ordering across all partition depths: years, months, days, hours, minutes."""

        def simple_parser(partition_dirs):
            return parse_epoch_timestamp_partitions(partition_dirs)

        # Create test files spanning different time scales
        # Target: Jan 15, 2024 12:50:00 UTC (1705323000)
        target_timestamp = 1705323000

        test_cases = [
            # Same year, same month, same day, same hour - different minutes
            (
                1705322400,
                "same_year_month_day_hour_min-1.txt",
            ),  # Jan 15, 2024 12:40:00 UTC (-10 min)
            (
                1705322700,
                "same_year_month_day_hour_min-2.txt",
            ),  # Jan 15, 2024 12:45:00 UTC (-5 min)
            # Same year, same month, same day - different hours
            (
                1705319400,
                "same_year_month_day_hour-1.txt",
            ),  # Jan 15, 2024 11:50:00 UTC (-1 hour)
            (
                1705315800,
                "same_year_month_day_hour-2.txt",
            ),  # Jan 15, 2024 10:50:00 UTC (-2 hours)
            # Same year, same month - different days
            (
                1705236600,
                "same_year_month_day-1.txt",
            ),  # Jan 14, 2024 12:50:00 UTC (-1 day)
            (
                1705150200,
                "same_year_month_day-2.txt",
            ),  # Jan 13, 2024 12:50:00 UTC (-2 days)
            # Same year - different months
            (
                1702644600,
                "same_year_month-1.txt",
            ),  # Dec 15, 2023 12:50:00 UTC (-1 month)
            (
                1699966200,
                "same_year_month-2.txt",
            ),  # Nov 14, 2023 12:50:00 UTC (-2 months)
            # Different years
            (1673796600, "different_year-1.txt"),  # Jan 15, 2023 15:30:00 UTC (-1 year)
            (
                1642258200,
                "different_year-2.txt",
            ),  # Jan 15, 2022 14:50:00 UTC (-2 years)
        ]

        # Write files
        for timestamp, filename in test_cases:
            write_file_partitioned(
                path=os.path.join(temp_dir, filename),
                data=f"Content for {filename}",
                partition_value=timestamp,
                partition_transform=epoch_timestamp_partition_transform,
            )

        # Query for files prior to target
        _, filesystem = resolve_path_and_filesystem(temp_dir)
        ordered_files = list_directory_partitioned(
            path=temp_dir,
            filesystem=filesystem,
            partition_value=target_timestamp,
            partition_transform=epoch_timestamp_partition_transform,
            partition_file_parser=epoch_timestamp_file_parser,
            limit=20,  # Get all files
            partition_dir_parser=simple_parser,
        )

        # Should find all test files
        assert len(ordered_files) == len(
            test_cases
        ), f"Expected {len(test_cases)} files, got {len(ordered_files)}"

        # Extract filenames for analysis
        actual_filenames = [os.path.basename(path) for path, _ in ordered_files]

        # Expected ordering by temporal proximity (closest to furthest)
        expected_filenames = [
            "same_year_month_day_hour_min-2.txt",  # -5 min (closest)
            "same_year_month_day_hour_min-1.txt",  # -10 min
            "same_year_month_day_hour-1.txt",  # -1 hour
            "same_year_month_day_hour-2.txt",  # -2 hours
            "same_year_month_day-1.txt",  # -1 day
            "same_year_month_day-2.txt",  # -2 days
            "same_year_month-1.txt",  # -1 month
            "same_year_month-2.txt",  # -2 months
            "different_year-1.txt",  # -1 year
            "different_year-2.txt",  # -2 years (furthest)
        ]

        # Verify exact ordering
        assert (
            actual_filenames == expected_filenames
        ), f"Ordering incorrect.\nExpected: {expected_filenames}\nActual: {actual_filenames}"

        # Additional validation: ensure ordering follows temporal hierarchy
        # Same minute/hour/day/month/year should come before different ones at that level

        # Find indices for different temporal levels
        minute_indices = [i for i, name in enumerate(actual_filenames) if "min" in name]
        hour_indices = [
            i
            for i, name in enumerate(actual_filenames)
            if "hour" in name and "min" not in name
        ]
        day_indices = [
            i
            for i, name in enumerate(actual_filenames)
            if "day" in name and "hour" not in name and "min" not in name
        ]
        month_indices = [
            i
            for i, name in enumerate(actual_filenames)
            if "month" in name
            and "day" not in name
            and "hour" not in name
            and "min" not in name
        ]
        year_indices = [
            i
            for i, name in enumerate(actual_filenames)
            if "year" in name
            and "month" not in name
            and "day" not in name
            and "hour" not in name
            and "min" not in name
        ]

        # Validate hierarchical ordering: same-level groups should come before different-level groups
        if minute_indices and hour_indices:
            assert max(minute_indices) < min(
                hour_indices
            ), "Same-minute files should come before different-hour files"

        if hour_indices and day_indices:
            assert max(hour_indices) < min(
                day_indices
            ), "Same-hour files should come before different-day files"

        if day_indices and month_indices:
            assert max(day_indices) < min(
                month_indices
            ), "Same-day files should come before different-month files"

        if month_indices and year_indices:
            assert max(month_indices) < min(
                year_indices
            ), "Same-month files should come before different-year files"

    def test_list_partitioned_directory_unlimited(self, temp_dir):
        """Test listing partitioned directory with unlimited results (limit=None)."""

        def simple_parser(partition_dirs):
            return parse_epoch_timestamp_partitions(partition_dirs)

        # Create multiple partitioned files
        timestamps = [
            1704038400,  # Dec 31, 2023 16:00:00 UTC
            1703952000,  # Dec 30, 2023 16:00:00 UTC
            1703865600,  # Dec 29, 2023 16:00:00 UTC
            1703779200,  # Dec 28, 2023 16:00:00 UTC
            1704124800,  # Jan 1, 2024 16:00:00 UTC (target - should not be included)
        ]

        for i, timestamp in enumerate(timestamps):
            filename = f"file{i}.txt"
            content = f"content {i}"

            write_file_partitioned(
                path=os.path.join(temp_dir, filename),
                data=content,
                partition_value=timestamp,
                partition_transform=epoch_timestamp_partition_transform,
            )

        # List files with unlimited results (limit=None)
        _, filesystem = resolve_path_and_filesystem(temp_dir)
        unlimited_files = list_directory_partitioned(
            path=temp_dir,
            filesystem=filesystem,
            partition_value=1704124800,  # Jan 1, 2024
            partition_transform=epoch_timestamp_partition_transform,
            partition_file_parser=epoch_timestamp_file_parser,
            limit=None,  # Unlimited
            partition_dir_parser=simple_parser,
        )

        # Should find all 5 files from prior partitions (including the target partition)
        assert len(unlimited_files) == 5

        # Compare with limited results to ensure unlimited returns more
        limited_files = list_directory_partitioned(
            path=temp_dir,
            filesystem=filesystem,
            partition_value=1704124800,
            partition_transform=epoch_timestamp_partition_transform,
            partition_file_parser=epoch_timestamp_file_parser,
            limit=2,  # Limited to 2
            partition_dir_parser=simple_parser,
        )

        # Unlimited should return more files than limited
        assert len(unlimited_files) > len(limited_files)
        assert len(limited_files) == 2  # Limited to 2 as expected

    def test_return_unpartitioned_functionality(self, temp_dir):
        """Test the return_unpartitioned parameter for backwards compatibility."""
        import posixpath
        from typing import Optional, List

        # Create both partitioned and unpartitioned files
        partitioned_files = [
            (f"{temp_dir}/2023/01/file1.json", 1000, 20230101),
            (f"{temp_dir}/2023/02/file2.json", 2000, 20230201),
            (f"{temp_dir}/2024/01/file3.json", 1500, 20240101),
        ]

        # Create unpartitioned files (files in base directory from before partitioning)
        unpartitioned_files = [
            (f"{temp_dir}/legacy_file1.json", 500, 20221201),  # Old file
            (f"{temp_dir}/legacy_file2.json", 800, 20230115),  # Another old file
            (
                f"{temp_dir}/recent_file.json",
                1200,
                20240115,
            ),  # Recent unpartitioned file
            (
                f"{temp_dir}/future_file.json",
                900,
                20250101,
            ),  # Future file (should be excluded)
        ]

        # Create all partitioned files
        for file_path, file_size, _ in partitioned_files:
            write_file(file_path, "x" * file_size, filesystem=LocalFileSystem())

        # Create all unpartitioned files
        for file_path, file_size, _ in unpartitioned_files:
            write_file(file_path, "x" * file_size, filesystem=LocalFileSystem())

        # Create a simple parser that extracts date from filename
        def parse_date_from_filename(file_path: str) -> Optional[int]:
            basename = posixpath.basename(file_path)
            if basename.startswith("legacy_file1"):
                return 20221201
            elif basename.startswith("legacy_file2"):
                return 20230115
            elif basename.startswith("recent_file"):
                return 20240115
            elif basename.startswith("future_file"):
                return 20250101
            elif basename.startswith("file1"):
                return 20230101
            elif basename.startswith("file2"):
                return 20230201
            elif basename.startswith("file3"):
                return 20240101
            return None

        def simple_transform(value: int) -> List[str]:
            # Transform YYYYMMDD to ["YYYY", "MM"]
            year = str(value)[:4]
            month = str(value)[4:6]
            return [year, month]

        def simple_parser(dirs: List[str]) -> Optional[int]:
            # Handle partial directory paths more gracefully
            if len(dirs) >= 1:
                try:
                    if len(dirs) == 1:
                        # Just year provided
                        year = int(dirs[0])
                        return year * 10000 + 1 * 100 + 1  # Jan 1st of that year
                    elif len(dirs) >= 2:
                        # Year and month provided
                        year = int(dirs[0])
                        month = int(dirs[1])
                        return year * 10000 + month * 100 + 1  # First day of month
                except (ValueError, IndexError):
                    return None
            return None

        target_date = 20240101  # Target: January 1, 2024

        # Test with return_unpartitioned=False (default behavior)
        partitioned_only = list_directory_partitioned(
            path=temp_dir,
            filesystem=LocalFileSystem(),
            partition_value=target_date,
            partition_transform=simple_transform,
            partition_file_parser=parse_date_from_filename,
            partition_dir_parser=simple_parser,
            return_unpartitioned=False,
        )

        # Should only return partitioned files <= target date
        partitioned_only_paths = [path for path, _ in partitioned_only]
        assert f"{temp_dir}/2023/01/file1.json" in partitioned_only_paths
        assert f"{temp_dir}/2023/02/file2.json" in partitioned_only_paths
        assert f"{temp_dir}/2024/01/file3.json" in partitioned_only_paths
        # Should NOT include unpartitioned files
        assert f"{temp_dir}/legacy_file1.json" not in partitioned_only_paths
        assert f"{temp_dir}/legacy_file2.json" not in partitioned_only_paths
        assert f"{temp_dir}/recent_file.json" not in partitioned_only_paths

        # Test with return_unpartitioned=True
        all_files = list_directory_partitioned(
            path=temp_dir,
            filesystem=LocalFileSystem(),
            partition_value=target_date,
            partition_transform=simple_transform,
            partition_file_parser=parse_date_from_filename,
            partition_dir_parser=simple_parser,
            return_unpartitioned=True,
        )

        # Should include both partitioned and valid unpartitioned files
        all_files_paths = [path for path, _ in all_files]

        # Partitioned files <= target date
        assert f"{temp_dir}/2023/01/file1.json" in all_files_paths
        assert f"{temp_dir}/2023/02/file2.json" in all_files_paths
        assert f"{temp_dir}/2024/01/file3.json" in all_files_paths

        # Unpartitioned files <= target date
        assert (
            f"{temp_dir}/legacy_file1.json" in all_files_paths
        )  # 20221201 <= 20240101
        assert (
            f"{temp_dir}/legacy_file2.json" in all_files_paths
        )  # 20230115 <= 20240101

        # Should NOT include future unpartitioned files
        assert (
            f"{temp_dir}/future_file.json" not in all_files_paths
        )  # 20250101 > 20240101
        assert (
            f"{temp_dir}/recent_file.json" not in all_files_paths
        )  # 20240115 > 20240101

        # Verify file count increase
        assert len(all_files) > len(partitioned_only)
        assert (
            len(all_files) == len(partitioned_only) + 2
        )  # Added 2 valid unpartitioned files

    def test_return_unpartitioned_with_limit(self, temp_dir):
        """Test return_unpartitioned respects the limit parameter."""
        import posixpath
        from typing import Optional, List

        # Create partitioned and unpartitioned files
        write_file(
            f"{temp_dir}/2023/01/file1.json", "content1", filesystem=LocalFileSystem()
        )
        write_file(f"{temp_dir}/legacy1.json", "content2", filesystem=LocalFileSystem())
        write_file(f"{temp_dir}/legacy2.json", "content3", filesystem=LocalFileSystem())

        def parse_filename(file_path: str) -> Optional[int]:
            basename = posixpath.basename(file_path)
            if "file1" in basename:
                return 20230101
            elif "legacy1" in basename:
                return 20221201
            elif "legacy2" in basename:
                return 20221202
            return None

        def transform(value: int) -> List[str]:
            year = str(value)[:4]
            month = str(value)[4:6]
            return [year, month]

        def parser(dirs: List[str]) -> Optional[int]:
            if len(dirs) >= 1:
                try:
                    if len(dirs) == 1:
                        year = int(dirs[0])
                        return year * 10000 + 1 * 100 + 1
                    elif len(dirs) >= 2:
                        year = int(dirs[0])
                        month = int(dirs[1])
                        return year * 10000 + month * 100 + 1
                except (ValueError, IndexError):
                    return None
            return None

        # Test with limit=2 and return_unpartitioned=True
        limited_files = list_directory_partitioned(
            path=temp_dir,
            filesystem=LocalFileSystem(),
            partition_value=20230101,
            partition_transform=transform,
            partition_file_parser=parse_filename,
            partition_dir_parser=parser,
            limit=2,
            return_unpartitioned=True,
        )

        # Should respect the limit
        assert len(limited_files) == 2

    def test_return_unpartitioned_parser_failure(self, temp_dir):
        """Test return_unpartitioned handles files that can't be parsed."""
        import posixpath
        from typing import Optional, List

        # Create files that will fail parsing
        write_file(
            f"{temp_dir}/valid_file.json", "content1", filesystem=LocalFileSystem()
        )
        write_file(
            f"{temp_dir}/invalid_file.json", "content2", filesystem=LocalFileSystem()
        )
        write_file(
            f"{temp_dir}/2023/01/partitioned.json",
            "content3",
            filesystem=LocalFileSystem(),
        )

        def parse_filename(file_path: str) -> Optional[int]:
            basename = posixpath.basename(file_path)
            if "invalid_file" in basename:
                return None  # Parsing fails
            elif "valid_file" in basename:
                return 20230101
            elif "partitioned" in basename:
                return 20230101
            return None

        def transform(value: int) -> List[str]:
            return [str(value)[:4], str(value)[4:6]]

        def parser(dirs: List[str]) -> Optional[int]:
            if len(dirs) >= 1:
                try:
                    if len(dirs) == 1:
                        year = int(dirs[0])
                        return year * 10000 + 1 * 100 + 1
                    elif len(dirs) >= 2:
                        year = int(dirs[0])
                        month = int(dirs[1])
                        return year * 10000 + month * 100 + 1
                except (ValueError, IndexError):
                    return None
            return None

        files = list_directory_partitioned(
            path=temp_dir,
            filesystem=LocalFileSystem(),
            partition_value=20230101,
            partition_transform=transform,
            partition_file_parser=parse_filename,
            partition_dir_parser=parser,
            return_unpartitioned=True,
        )

        files_paths = [path for path, _ in files]

        # Should include parseable files
        assert f"{temp_dir}/valid_file.json" in files_paths
        assert f"{temp_dir}/2023/01/partitioned.json" in files_paths

        # Should exclude unparseable files
        assert f"{temp_dir}/invalid_file.json" not in files_paths

    def test_return_unpartitioned_empty_base_directory(self, temp_dir):
        """Test return_unpartitioned with empty base directory."""
        from typing import Optional, List

        # Only create partitioned files
        write_file(
            f"{temp_dir}/2023/01/file1.json", "content", filesystem=LocalFileSystem()
        )

        def parse_filename(file_path: str) -> Optional[int]:
            return 20230101

        def transform(value: int) -> List[str]:
            return [str(value)[:4], str(value)[4:6]]

        def parser(dirs: List[str]) -> Optional[int]:
            if len(dirs) >= 1:
                try:
                    if len(dirs) == 1:
                        year = int(dirs[0])
                        return year * 10000 + 1 * 100 + 1
                    elif len(dirs) >= 2:
                        year = int(dirs[0])
                        month = int(dirs[1])
                        return year * 10000 + month * 100 + 1
                except (ValueError, IndexError):
                    return None
            return None

        files = list_directory_partitioned(
            path=temp_dir,
            filesystem=LocalFileSystem(),
            partition_value=20230101,
            partition_transform=transform,
            partition_file_parser=parse_filename,
            partition_dir_parser=parser,
            return_unpartitioned=True,
        )

        # Should still work with no unpartitioned files
        assert len(files) == 1
        assert files[0][0] == f"{temp_dir}/2023/01/file1.json"

    def test_list_partitioned_directory_with_start_value(self, temp_dir):
        """Test listing partitioned directory with partition_start_value."""

        def simple_parser(partition_dirs):
            return parse_epoch_timestamp_partitions(partition_dirs)

        # Create files with timestamps spanning multiple days
        timestamps = [
            1704038400,  # Dec 31, 2023 16:00:00 UTC (before range)
            1704124800,  # Jan 1, 2024 16:00:00 UTC (in range - lower bound)
            1704146400,  # Jan 1, 2024 22:00:00 UTC (in range)
            1704211200,  # Jan 2, 2024 16:00:00 UTC (in range - upper bound)
            1704297600,  # Jan 3, 2024 16:00:00 UTC (after range)
        ]

        for i, timestamp in enumerate(timestamps):
            filename = f"file{i}.txt"
            content = f"content {i}"

            write_file_partitioned(
                path=os.path.join(temp_dir, filename),
                data=content,
                partition_value=timestamp,
                partition_transform=epoch_timestamp_partition_transform,
            )

        # List files from Jan 1, 2024 to Jan 2, 2024 (inclusive)
        _, filesystem = resolve_path_and_filesystem(temp_dir)
        files = list_directory_partitioned(
            path=temp_dir,
            filesystem=filesystem,
            partition_value=1704211200,  # Jan 2, 2024
            partition_transform=epoch_timestamp_partition_transform,
            partition_file_parser=epoch_timestamp_file_parser,
            limit=10,
            partition_dir_parser=simple_parser,
            partition_start_value=1704124800,  # Jan 1, 2024
        )

        # Should find 3 files (Jan 1 and Jan 2 files, but not Dec 31 or Jan 3)
        assert len(files) == 3

        # Verify the files are from the correct timestamps
        file_timestamps = []
        for file_path, _ in files:
            timestamp = epoch_timestamp_file_parser(file_path)
            file_timestamps.append(timestamp)

        assert 1704124800 in file_timestamps  # Jan 1, 2024
        assert 1704146400 in file_timestamps  # Jan 1, 2024 (later)
        assert 1704211200 in file_timestamps  # Jan 2, 2024
        assert 1704038400 not in file_timestamps  # Dec 31, 2023 (before range)
        assert 1704297600 not in file_timestamps  # Jan 3, 2024 (after range)

    def test_list_partitioned_directory_start_value_none(self, temp_dir):
        """Test that partition_start_value=None behaves like before (no lower bound)."""

        def simple_parser(partition_dirs):
            return parse_epoch_timestamp_partitions(partition_dirs)

        timestamps = [
            1704038400,  # Dec 31, 2023
            1704124800,  # Jan 1, 2024
            1704211200,  # Jan 2, 2024
        ]

        for i, timestamp in enumerate(timestamps):
            filename = f"file{i}.txt"
            content = f"content {i}"

            write_file_partitioned(
                path=os.path.join(temp_dir, filename),
                data=content,
                partition_value=timestamp,
                partition_transform=epoch_timestamp_partition_transform,
            )

        # Test with partition_start_value=None (should include all files <= partition_value)
        _, filesystem = resolve_path_and_filesystem(temp_dir)
        files_with_none = list_directory_partitioned(
            path=temp_dir,
            filesystem=filesystem,
            partition_value=1704211200,  # Jan 2, 2024
            partition_transform=epoch_timestamp_partition_transform,
            partition_file_parser=epoch_timestamp_file_parser,
            limit=10,
            partition_dir_parser=simple_parser,
            partition_start_value=None,
        )

        # Test without partition_start_value (should be equivalent)
        files_without = list_directory_partitioned(
            path=temp_dir,
            filesystem=filesystem,
            partition_value=1704211200,  # Jan 2, 2024
            partition_transform=epoch_timestamp_partition_transform,
            partition_file_parser=epoch_timestamp_file_parser,
            limit=10,
            partition_dir_parser=simple_parser,
        )

        # Both should return the same results
        assert len(files_with_none) == len(files_without) == 3

    def test_list_partitioned_directory_invalid_start_value(self, temp_dir):
        """Test that ValueError is raised when partition_start_value > partition_value."""

        def simple_parser(partition_dirs):
            return parse_epoch_timestamp_partitions(partition_dirs)

        _, filesystem = resolve_path_and_filesystem(temp_dir)

        with pytest.raises(
            ValueError,
            match="Start value \\(1704211200\\) must be less than or equal to end value \\(1704124800\\)",
        ):
            list_directory_partitioned(
                path=temp_dir,
                filesystem=filesystem,
                partition_value=1704124800,  # Jan 1, 2024
                partition_transform=epoch_timestamp_partition_transform,
                partition_file_parser=epoch_timestamp_file_parser,
                limit=10,
                partition_dir_parser=simple_parser,
                partition_start_value=1704211200,  # Jan 2, 2024 (greater than partition_value)
            )

    def test_list_partitioned_directory_start_value_equal_to_end_value(self, temp_dir):
        """Test that partition_start_value == partition_value works correctly."""

        def simple_parser(partition_dirs):
            return parse_epoch_timestamp_partitions(partition_dirs)

        timestamps = [
            1704038400,  # Dec 31, 2023 (before range)
            1704124800,  # Jan 1, 2024 (in range)
            1704211200,  # Jan 2, 2024 (after range)
        ]

        for i, timestamp in enumerate(timestamps):
            filename = f"file{i}.txt"
            content = f"content {i}"

            write_file_partitioned(
                path=os.path.join(temp_dir, filename),
                data=content,
                partition_value=timestamp,
                partition_transform=epoch_timestamp_partition_transform,
            )

        # List files where start_value == end_value
        _, filesystem = resolve_path_and_filesystem(temp_dir)
        files = list_directory_partitioned(
            path=temp_dir,
            filesystem=filesystem,
            partition_value=1704124800,  # Jan 1, 2024
            partition_transform=epoch_timestamp_partition_transform,
            partition_file_parser=epoch_timestamp_file_parser,
            limit=10,
            partition_dir_parser=simple_parser,
            partition_start_value=1704124800,  # Jan 1, 2024 (same as partition_value)
        )

        # Should find only 1 file (the Jan 1 file)
        assert len(files) == 1

        file_path, _ = files[0]
        timestamp = epoch_timestamp_file_parser(file_path)
        assert timestamp == 1704124800  # Jan 1, 2024

    def test_list_partitioned_directory_start_value_with_unpartitioned_files(
        self, temp_dir
    ):
        """Test partition_start_value filtering with return_unpartitioned=True."""
        import posixpath

        def simple_parser(partition_dirs):
            return parse_epoch_timestamp_partitions(partition_dirs)

        def custom_file_parser(file_path: str):
            """Custom parser that can handle both partitioned and unpartitioned files."""
            # First try the standard parser for partitioned files
            result = epoch_timestamp_file_parser(file_path)
            if result is not None:
                return result

            # For unpartitioned files, parse based on filename
            basename = posixpath.basename(file_path)
            if basename == "unpartitioned_old.txt":
                return 1704038400  # Dec 31, 2023
            elif basename == "unpartitioned_new.txt":
                return 1704124800  # Jan 1, 2024
            return None

        # Create some partitioned files
        timestamps = [
            1704038400,  # Dec 31, 2023 (before range)
            1704124800,  # Jan 1, 2024 (in range)
        ]

        for i, timestamp in enumerate(timestamps):
            filename = f"partitioned_file{i}.txt"
            content = f"partitioned content {i}"

            write_file_partitioned(
                path=os.path.join(temp_dir, filename),
                data=content,
                partition_value=timestamp,
                partition_transform=epoch_timestamp_partition_transform,
            )

        # Create unpartitioned files directly in the base directory
        unpartitioned_files = [
            ("unpartitioned_old.txt", 1704038400),  # Dec 31, 2023 (before range)
            ("unpartitioned_new.txt", 1704124800),  # Jan 1, 2024 (in range)
        ]

        for filename, timestamp in unpartitioned_files:
            file_path = os.path.join(temp_dir, filename)
            content = f"unpartitioned content for {timestamp}"
            with open(file_path, "w") as f:
                f.write(content)

        # List with return_unpartitioned=True and partition_start_value
        files = list_directory_partitioned(
            path=temp_dir,
            filesystem=LocalFileSystem(),
            partition_value=1704124800,  # Jan 1, 2024
            partition_transform=epoch_timestamp_partition_transform,
            partition_file_parser=custom_file_parser,
            limit=10,
            partition_dir_parser=simple_parser,
            partition_start_value=1704124800,  # Jan 1, 2024
            return_unpartitioned=True,
        )

        # Should find 2 files: 1 partitioned + 1 unpartitioned (both from Jan 1)
        assert len(files) == 2

        # Verify both files are from the correct timestamp
        for file_path, _ in files:
            timestamp = custom_file_parser(file_path)
            assert timestamp == 1704124800  # Jan 1, 2024


class TestListDirectoryPartitionedWithExponentialTransform:
    """Test list_directory_partitioned function with exponential transform."""

    def test_exponential_partitioned_ordering_basic(self, temp_dir):
        """Test basic ordering with exponential partitioning."""
        # Create files with different revision numbers that will create different partitions
        # Using zero-padded filenames for deterministic ordering within ties
        test_cases = [
            (1500, "0000001500_file.txt"),  # -> ["1000000", "2000"] -> sum: 1002000
            (2500, "0000002500_file.txt"),  # -> ["1000000", "3000"] -> sum: 1003000
            (500000, "0000500000_file.txt"),  # -> ["1000000", "500000"] -> sum: 1500000
            (999, "0000000999_file.txt"),  # -> ["1000000", "1000"] -> sum: 1001000
        ]

        # Write files using exponential partitioning
        for revision, filename in test_cases:
            write_file_partitioned(
                path=os.path.join(temp_dir, filename),
                data=f"Content for {filename}",
                partition_value=revision,
                partition_transform=exponential_partition_transform,
            )

        # Test ordering with target value 2000000 (should find all files with partitions < 2000000)
        # Target: 2000000 -> ["2000000", "1000000"] -> sum: 3000000
        # All our files are in ["1000000", ...] which is < "2000000", so they should all be found
        _, filesystem = resolve_path_and_filesystem(temp_dir)
        ordered_files = list_directory_partitioned(
            path=temp_dir,
            filesystem=filesystem,
            partition_value=2000000,  # Target value - larger than all our test files
            partition_transform=exponential_partition_transform,
            partition_file_parser=exponential_file_parser,
            limit=10,
            partition_dir_parser=partial(
                parse_exponential_partitions, base=1000, levels=2
            ),
        )

        # Should find all files, ordered by proximity to partition value
        assert len(ordered_files) == 4

        # Extract filenames for analysis
        filenames = [os.path.basename(path) for path, _ in ordered_files]

        # Verify correct ordering based on distance from target sum (3000000)
        # Expected order by distance from 3000000:
        # 1. 0000500000_file.txt (sum: 1500000, distance: |3000000 - 1500000| = 1500000)
        # 2. 0000002500_file.txt (sum: 1003000, distance: |3000000 - 1003000| = 1997000)
        # 3. 0000001500_file.txt (sum: 1002000, distance: |3000000 - 1002000| = 1998000)
        # 4. 0000000999_file.txt (sum: 1001000, distance: |3000000 - 1001000| = 1999000)
        expected_order = [
            "0000500000_file.txt",
            "0000002500_file.txt",
            "0000001500_file.txt",
            "0000000999_file.txt",
        ]
        assert filenames == expected_order

    def test_exponential_partitioned_ordering_detailed(self, temp_dir):
        """Test detailed ordering behavior with exponential partitioning."""
        # Create files with carefully chosen revision numbers
        # Using zero-padded filenames for deterministic ordering within ties
        test_cases = [
            (1000, "0000001000_file.txt"),  # -> ["1000000", "1000"] -> sum: 1001000
            (1500, "0000001500_file.txt"),  # -> ["1000000", "2000"] -> sum: 1002000
            (2000, "0000002000_file.txt"),  # -> ["1000000", "2000"] -> sum: 1002000
            (3000, "0000003000_file.txt"),  # -> ["1000000", "3000"] -> sum: 1003000
            (500000, "0000500000_file.txt"),  # -> ["1000000", "500000"] -> sum: 1500000
        ]

        # Write files using exponential partitioning
        for revision, filename in test_cases:
            write_file_partitioned(
                path=os.path.join(temp_dir, filename),
                data=f"Content for revision {revision}",
                partition_value=revision,
                partition_transform=exponential_partition_transform,
            )

        # Test with target value 2000000 (finds all files with partitions < 2000000)
        # Target: 2000000 -> ["2000000", "1000000"] -> sum: 3000000
        _, filesystem = resolve_path_and_filesystem(temp_dir)
        ordered_files = list_directory_partitioned(
            path=temp_dir,
            filesystem=filesystem,
            partition_value=2000000,  # Target larger than all test files
            partition_transform=exponential_partition_transform,
            partition_file_parser=exponential_file_parser,
            limit=10,
            partition_dir_parser=partial(
                parse_exponential_partitions, base=1000, levels=2
            ),
        )

        assert len(ordered_files) == 5
        filenames = [os.path.basename(path) for path, _ in ordered_files]

        # Verify correct ordering based on distance from target sum (3000000)
        # Expected order by distance from 3000000:
        # 1. 0000500000_file.txt (sum: 1500000, distance: |3000000 - 1500000| = 1500000)
        # 2. 0000003000_file.txt (sum: 1003000, distance: |3000000 - 1003000| = 1997000)
        # 3. 0000002000_file.txt (sum: 1002000, distance: |3000000 - 1002000| = 1998000) - larger number first in tie
        # 4. 0000001500_file.txt (sum: 1002000, distance: |3000000 - 1002000| = 1998000) - smaller number second in tie
        # 5. 0000001000_file.txt (sum: 1001000, distance: |3000000 - 1001000| = 1999000)
        expected_order = [
            "0000500000_file.txt",
            "0000003000_file.txt",
            "0000002000_file.txt",
            "0000001500_file.txt",
            "0000001000_file.txt",
        ]
        assert filenames == expected_order

    def test_exponential_partitioned_limit_respects_ordering(self, temp_dir):
        """Test that limit parameter respects the ordering."""
        # Create many files with different partitions
        # Using zero-padded filenames for deterministic ordering within ties
        test_cases = [
            (500, "0000000500_file.txt"),  # -> ["1000000", "1000"] -> sum: 1001000
            (1000, "0000001000_file.txt"),  # -> ["1000000", "1000"] -> sum: 1001000
            (1500, "0000001500_file.txt"),  # -> ["1000000", "2000"] -> sum: 1002000
            (2000, "0000002000_file.txt"),  # -> ["1000000", "2000"] -> sum: 1002000
            (3000, "0000003000_file.txt"),  # -> ["1000000", "3000"] -> sum: 1003000
            (500000, "0000500000_file.txt"),  # -> ["1000000", "500000"] -> sum: 1500000
        ]

        for revision, filename in test_cases:
            write_file_partitioned(
                path=os.path.join(temp_dir, filename),
                data=f"Content for revision {revision}",
                partition_value=revision,
                partition_transform=exponential_partition_transform,
            )

        _, filesystem = resolve_path_and_filesystem(temp_dir)

        # Get all files first - use target 2000000 to find all files < 2000000
        # Target: 2000000 -> ["2000000", "1000000"] -> sum: 3000000
        all_files = list_directory_partitioned(
            path=temp_dir,
            filesystem=filesystem,
            partition_value=2000000,  # Target larger than all test files
            partition_transform=exponential_partition_transform,
            partition_file_parser=exponential_file_parser,
            limit=None,  # No limit
            partition_dir_parser=partial(
                parse_exponential_partitions, base=1000, levels=2
            ),
        )

        # Get limited files
        limited_files = list_directory_partitioned(
            path=temp_dir,
            filesystem=filesystem,
            partition_value=2000000,  # Same target
            partition_transform=exponential_partition_transform,
            partition_file_parser=exponential_file_parser,
            limit=3,  # Limit to 3
            partition_dir_parser=partial(
                parse_exponential_partitions, base=1000, levels=2
            ),
        )

        # Verify limit is respected
        assert len(limited_files) == 3
        assert len(all_files) == 6  # Should find all 6 files

        # Verify that limited files are the first 3 from the unlimited results
        all_filenames = [os.path.basename(path) for path, _ in all_files]
        limited_filenames = [os.path.basename(path) for path, _ in limited_files]

        # The limited files should be the first 3 files from the ordered list
        assert limited_filenames == all_filenames[:3]

        # Verify the expected ordering for all files based on distance from target sum (3000000)
        # Expected order by distance from 3000000:
        # 1. 0000500000_file.txt (sum: 1500000, distance: 1500000)
        # 2. 0000003000_file.txt (sum: 1003000, distance: 1997000)
        # 3. 0000002000_file.txt (sum: 1002000, distance: 1998000) - larger number first in tie
        # 4. 0000001500_file.txt (sum: 1002000, distance: 1998000) - smaller number second in tie
        # 5. 0000001000_file.txt (sum: 1001000, distance: 1999000) - larger number first in tie
        # 6. 0000000500_file.txt (sum: 1001000, distance: 1999000) - smaller number second in tie
        expected_all_order = [
            "0000500000_file.txt",
            "0000003000_file.txt",
            "0000002000_file.txt",
            "0000001500_file.txt",
            "0000001000_file.txt",
            "0000000500_file.txt",
        ]
        assert all_filenames == expected_all_order

        # Verify the limited files are the first 3 from the expected order
        expected_limited_order = expected_all_order[:3]
        assert limited_filenames == expected_limited_order

    def test_exponential_partitioned_proximity_ordering(self, temp_dir):
        """Test that files are ordered by proximity to the target partition value."""
        # Create files with values that will test proximity ordering across different partitions
        # Using zero-padded filenames for deterministic ordering within ties
        test_cases = [
            (1200, "0000001200_file.txt"),  # -> ["1000000", "2000"] -> sum: 1002000
            (1800, "0000001800_file.txt"),  # -> ["1000000", "2000"] -> sum: 1002000
            (4000, "0000004000_file.txt"),  # -> ["1000000", "4000"] -> sum: 1004000
            (10000, "0000010000_file.txt"),  # -> ["1000000", "10000"] -> sum: 1010000
            (
                1000000,
                "0001000000_file.txt",
            ),  # -> ["2000000", "1000000"] -> sum: 3000000
        ]

        for revision, filename in test_cases:
            write_file_partitioned(
                path=os.path.join(temp_dir, filename),
                data=f"Content for revision {revision}",
                partition_value=revision,
                partition_transform=exponential_partition_transform,
            )

        _, filesystem = resolve_path_and_filesystem(temp_dir)

        # Target value: 3000000 (should find files from both 1000000 and 2000000 partitions)
        # Target: 3000000 -> ["3000000", "1000000"] -> sum: 4000000
        # Files in 1000000 partition: first 4 files -> ["1000000", ...] (< "3000000")
        # Files in 2000000 partition: 0001000000_file.txt -> ["2000000", "1000000"] (< "3000000")
        ordered_files = list_directory_partitioned(
            path=temp_dir,
            filesystem=filesystem,
            partition_value=3000000,  # Target larger than all partitions
            partition_transform=exponential_partition_transform,
            partition_file_parser=exponential_file_parser,
            limit=10,
            partition_dir_parser=partial(
                parse_exponential_partitions, base=1000, levels=2
            ),
        )

        assert len(ordered_files) == 5
        filenames = [os.path.basename(path) for path, _ in ordered_files]

        # Verify correct ordering based on distance from target sum (4000000)
        # Expected order by distance from 4000000:
        # 1. 0001000000_file.txt (sum: 3000000, distance: |4000000 - 3000000| = 1000000)
        # 2. 0000010000_file.txt (sum: 1010000, distance: |4000000 - 1010000| = 2990000)
        # 3. 0000004000_file.txt (sum: 1004000, distance: |4000000 - 1004000| = 2996000)
        # 4. 0000001800_file.txt (sum: 1002000, distance: |4000000 - 1002000| = 2998000) - larger number first in tie
        # 5. 0000001200_file.txt (sum: 1002000, distance: |4000000 - 1002000| = 2998000) - smaller number second in tie
        expected_order = [
            "0001000000_file.txt",
            "0000010000_file.txt",
            "0000004000_file.txt",
            "0000001800_file.txt",
            "0000001200_file.txt",
        ]
        assert filenames == expected_order

    def test_exponential_partitioned_empty_result(self, temp_dir):
        """Test behavior when no files match the partition criteria."""
        # Don't create any files
        _, filesystem = resolve_path_and_filesystem(temp_dir)

        result = list_directory_partitioned(
            path=temp_dir,
            filesystem=filesystem,
            partition_value=1500,
            partition_transform=exponential_partition_transform,
            partition_file_parser=exponential_file_parser,
            limit=10,
            partition_dir_parser=partial(
                parse_exponential_partitions, base=1000, levels=2
            ),
        )

        assert result == []

    def test_exponential_partitioned_single_file(self, temp_dir):
        """Test behavior with a single partitioned file."""
        write_file_partitioned(
            path=os.path.join(temp_dir, "0000001500_single_file.txt"),
            data="Single file content",
            partition_value=1500,  # -> ["1000000", "2000"]
            partition_transform=exponential_partition_transform,
        )

        _, filesystem = resolve_path_and_filesystem(temp_dir)

        # Use target 2000000 to find the file in 1000000 partition
        result = list_directory_partitioned(
            path=temp_dir,
            filesystem=filesystem,
            partition_value=2000000,  # Target larger than the file's partition
            partition_transform=exponential_partition_transform,
            partition_file_parser=exponential_file_parser,
            limit=10,
            partition_dir_parser=partial(
                parse_exponential_partitions, base=1000, levels=2
            ),
        )

        assert len(result) == 1
        filename = os.path.basename(result[0][0])
        assert filename == "0000001500_single_file.txt"

    def test_exponential_partitioned_small_values_same_partition(self, temp_dir):
        """Test ordering of small values that map to the same exponential partition."""
        # Small values (1, 2, 10, etc.) all map to the same partition with default settings
        # This tests that our reverse sorting works correctly for files in the same partition
        test_cases = [
            (1, "0000000001_file.txt"),  # -> ["1000000", "1000"] -> sum: 1001000
            (2, "0000000002_file.txt"),  # -> ["1000000", "1000"] -> sum: 1001000
        ]

        # Write files using exponential partitioning
        for value, filename in test_cases:
            write_file_partitioned(
                path=os.path.join(temp_dir, filename),
                data=f"Content for value {value}",
                partition_value=value,
                partition_transform=exponential_partition_transform,
            )

        _, filesystem = resolve_path_and_filesystem(temp_dir)

        # Use target 1000000 to find files in the ["1000000", "1000"] partition
        # Target: 1000000 -> ["1000000", "1000000"] -> sum: 2000000
        # Files are in ["1000000", "1000"] which is < ["1000000", "1000000"], so they should be found
        ordered_files = list_directory_partitioned(
            path=temp_dir,
            filesystem=filesystem,
            partition_value=1000000,  # Target larger than the files' partition
            partition_transform=exponential_partition_transform,
            partition_file_parser=exponential_file_parser,
            limit=10,
            partition_dir_parser=partial(
                parse_exponential_partitions, base=1000, levels=2
            ),
        )

        assert len(ordered_files) == 2
        filenames = [os.path.basename(path) for path, _ in ordered_files]

        # Both files have the same distance (same partition sum), so order should be by filename in reverse
        # Expected: larger number first (0000000002_file.txt before 0000000001_file.txt)
        expected_order = ["0000000002_file.txt", "0000000001_file.txt"]
        assert filenames == expected_order

        # Verify both files have the same distance from target
        target_sum = parse_exponential_partitions(
            exponential_partition_transform(1000000)
        )  # 2000000
        file_sum = parse_exponential_partitions(
            exponential_partition_transform(1)
        )  # 1001000 (same for both files)
        expected_distance = abs(target_sum - file_sum)  # |2000000 - 1001000| = 999000

        for file_path, _ in ordered_files:
            filename = os.path.basename(file_path)
            original_value = int(filename.split("_")[0])
            actual_sum = parse_exponential_partitions(
                exponential_partition_transform(original_value)
            )
            actual_distance = abs(target_sum - actual_sum)
            assert (
                actual_distance == expected_distance
            ), f"File {filename} should have distance {expected_distance}, got {actual_distance}"

    def test_exponential_partitioned_consecutive_ordering_base2(self, temp_dir):
        """Test comprehensive ordering with consecutive values 1-32 using base=2, levels=3."""
        # Create files for consecutive values 1-32 with base=2, levels=3
        # This tests the full range of a 3-level exponential partition system
        test_values = list(range(1, 33))  # 1-32 inclusive

        # Create partitioned files for all test values
        for value in test_values:
            filename = f"{value:08d}_file.txt"  # Zero-padded to 8 digits for consistent ordering
            write_file_partitioned(
                path=os.path.join(temp_dir, filename),
                data=f"Content for value {value}",
                partition_value=value,
                partition_transform=partial(
                    exponential_partition_transform, base=2, levels=3
                ),
            )

        _, filesystem = resolve_path_and_filesystem(temp_dir)

        # Test list_directory_partitioned for each consecutive value
        # For each target value, verify that all values <= target are returned in correct order
        for target_value in range(1, 33):  # Start from 1 since we want values <= target
            ordered_files = list_directory_partitioned(
                path=temp_dir,
                filesystem=filesystem,
                partition_value=target_value,
                partition_transform=partial(
                    exponential_partition_transform, base=2, levels=3
                ),
                partition_file_parser=exponential_file_parser,
                partition_dir_parser=partial(
                    parse_exponential_partitions, base=2, levels=3
                ),
            )

            # Extract the original values from filenames
            found_values = []
            for file_path, _ in ordered_files:
                filename = os.path.basename(file_path)
                original_value = int(filename.split("_")[0])
                found_values.append(original_value)

            # Expected values: all values from 1 to target_value (inclusive)
            expected_values = list(range(1, target_value + 1))

            # Verify we found all expected values
            assert set(found_values) == set(expected_values), (
                f"Target {target_value}: Expected values {expected_values}, "
                f"but found {found_values}. Missing: {set(expected_values) - set(found_values)}, "
                f"Extra: {set(found_values) - set(expected_values)}"
            )

            # Verify ordering: values should be ordered by distance from target
            target_parsed = parse_exponential_partitions(
                exponential_partition_transform(target_value, base=2, levels=3),
                base=2,
                levels=3,
            )

            # Calculate expected ordering by distance
            value_distances = []
            for value in expected_values:
                value_parsed = parse_exponential_partitions(
                    exponential_partition_transform(value, base=2, levels=3),
                    base=2,
                    levels=3,
                )
                distance = abs(target_parsed - value_parsed)
                value_distances.append((distance, value))

            # Sort by distance (ascending), then by value (descending) for ties
            value_distances.sort(key=lambda x: (x[0], -x[1]))
            expected_order = [value for _, value in value_distances]

            assert found_values == expected_order, (
                f"Target {target_value}: Incorrect ordering. "
                f"Expected: {expected_order}, Got: {found_values}"
            )

            # Additional verification: ensure distances are non-decreasing
            prev_distance = -1
            for i, value in enumerate(found_values):
                value_parsed = parse_exponential_partitions(
                    exponential_partition_transform(value, base=2, levels=3),
                    base=2,
                    levels=3,
                )
                distance = abs(target_parsed - value_parsed)

                assert distance >= prev_distance, (
                    f"Target {target_value}: Distance should be non-decreasing. "
                    f"Value {value} at position {i} has distance {distance}, "
                    f"but previous distance was {prev_distance}"
                )
                prev_distance = distance

    def test_exponential_partitioned_metafile_revision_parsing(self, temp_dir):
        """Test exponential partitioning with realistic metafile revision names using MetafileRevisionInfo.parse."""
        # Create files with realistic metafile revision names
        test_files = [
            (
                1,
                "00000000000000000001_create_1756565041547264000_99e2789d-aa09-4026-8fd4-3a3eee992265.mpk",
            ),
            (
                2,
                "00000000000000000002_update_1756565041547264000_99e2789d-aa09-4026-8fd4-3a3eee992265.mpk",
            ),
            (
                5,
                "00000000000000000005_delete_1756565041547264000_99e2789d-aa09-4026-8fd4-3a3eee992265.mpk",
            ),
            (
                10,
                "00000000000000000010_create_1756565041547264000_99e2789d-aa09-4026-8fd4-3a3eee992265.mpk",
            ),
        ]

        for revision, filename in test_files:
            write_file_partitioned(
                path=os.path.join(temp_dir, filename),
                data=f"Metafile content for revision {revision}",
                partition_value=revision,
                partition_transform=exponential_partition_transform,
            )

        _, filesystem = resolve_path_and_filesystem(temp_dir)

        # Test that we can find revisions <= 5
        result = list_directory_partitioned(
            path=temp_dir,
            filesystem=filesystem,
            partition_value=5,
            partition_transform=exponential_partition_transform,
            partition_file_parser=MetafileRevisionInfo.parse_revision,
            partition_dir_parser=partial(
                parse_exponential_partitions, base=1000, levels=2
            ),
        )

        # Should find revisions 1, 2, and 5 (but not 10)
        found_revisions = []
        for file_path, _ in result:
            revision = MetafileRevisionInfo.parse_revision(file_path)
            found_revisions.append(revision)

        expected_revisions = [5, 2, 1]  # Ordered by proximity to target value 5
        assert (
            found_revisions == expected_revisions
        ), f"Expected {expected_revisions}, got {found_revisions}"

        # Verify the filenames are correct
        expected_filenames = [
            "00000000000000000005_delete_1756565041547264000_99e2789d-aa09-4026-8fd4-3a3eee992265.mpk",
            "00000000000000000002_update_1756565041547264000_99e2789d-aa09-4026-8fd4-3a3eee992265.mpk",
            "00000000000000000001_create_1756565041547264000_99e2789d-aa09-4026-8fd4-3a3eee992265.mpk",
        ]
        actual_filenames = [os.path.basename(file_path) for file_path, _ in result]
        assert actual_filenames == expected_filenames


class TestExponentialPartitionParser:
    """Test exponential partition parser function."""

    def test_parse_valid_partitions(self):
        """Test parsing valid exponential partition directories."""
        # Test with 2-level partitions - should return actual capacity contribution
        result = parse_exponential_partitions(["1000000", "2000"], base=1000, levels=2)
        assert result == 2000  # (1-1)*1000000 + 2*1000 = 0 + 2000 = 2000

        result = parse_exponential_partitions(
            ["2000000", "500000"], base=1000, levels=2
        )
        assert (
            result == 1500000
        )  # (2-1)*1000000 + 500*1000 = 1000000 + 500000 = 1500000

        # Test with 3-level partitions
        result = parse_exponential_partitions(
            ["1000000000", "1000000", "3000"], base=1000, levels=3
        )
        assert (
            result == 3000
        )  # (1-1)*1000000000 + (1-1)*1000000 + 3*1000 = 0 + 0 + 3000 = 3000

        # Test with single-level partition
        result = parse_exponential_partitions(["3000"], base=1000, levels=1)
        assert result == 3000  # Final level: 3*1000 = 3000

    def test_parse_empty_list(self):
        """Test parsing an empty partition list."""
        result = parse_exponential_partitions([])
        assert result is None

    def test_parse_invalid_partitions(self):
        """Test parsing invalid partition directories."""
        # Non-numeric first partition
        result = parse_exponential_partitions(
            ["not_a_number", "2000"], base=1000, levels=2
        )
        assert result is None

        # Mixed valid/invalid partitions (should fail completely)
        result = parse_exponential_partitions(
            ["1000000", "invalid"], base=1000, levels=2
        )
        assert result is None

        # Empty string in first partition
        result = parse_exponential_partitions(["", "2000"], base=1000, levels=2)
        assert result is None

    def test_parse_ordering_consistency(self):
        """Test that parser maintains ordering consistency."""
        # Test various partition combinations to ensure ordering is preserved
        test_cases = [
            (["1000000", "1000"], 1000),  # (1-1)*1000000 + 1*1000 = 1000
            (["1000000", "2000"], 2000),  # (1-1)*1000000 + 2*1000 = 2000
            (["2000000", "500000"], 1500000),  # (2-1)*1000000 + 500*1000 = 1500000
            (["3000000", "100000"], 2100000),  # (3-1)*1000000 + 100*1000 = 2100000
        ]

        results = []
        for partition_dirs, expected in test_cases:
            result = parse_exponential_partitions(partition_dirs, base=1000, levels=2)
            assert result == expected
            results.append(result)

        # Verify ordering is maintained
        assert results == sorted(results)

    def test_parse_real_exponential_partition_transform_outputs(self):
        """Test parser with actual outputs from exponential transform."""
        # Test with various values and their exponential transform outputs
        test_values = [
            (1500, ["1000000", "2000"], 2000),  # (1-1)*1000000 + 2*1000 = 2000
            (
                1500000,
                ["2000000", "500000"],
                1500000,
            ),  # (2-1)*1000000 + 500*1000 = 1500000
            (2500, ["1000000", "3000"], 3000),  # (1-1)*1000000 + 3*1000 = 3000
            (999, ["1000000", "1000"], 1000),  # (1-1)*1000000 + 1*1000 = 1000
        ]

        for original_value, transform_output, expected_contribution in test_values:
            # Verify the transform produces the expected output
            actual_transform = exponential_partition_transform(
                original_value, base=1000, levels=2
            )
            assert actual_transform == transform_output

            # Test that our parser works with this output
            parsed_value = parse_exponential_partitions(
                transform_output, base=1000, levels=2
            )
            assert parsed_value == expected_contribution

    def test_parse_different_bases_and_levels(self):
        """Test parser with different base values and levels."""
        # Base 10, 2 levels
        transform_output = exponential_partition_transform(123, base=10, levels=2)
        expected = ["200", "30"]
        assert transform_output == expected

        parsed_value = parse_exponential_partitions(transform_output, base=10, levels=2)
        assert parsed_value == 130  # (2-1)*100 + 3*10 = 100 + 30 = 130

        # Base 2, 3 levels
        transform_output = exponential_partition_transform(13, base=2, levels=3)
        expected = ["16", "8", "2"]
        assert transform_output == expected

        parsed_value = parse_exponential_partitions(transform_output, base=2, levels=3)
        assert (
            parsed_value == 14
        )  # multipliers=[2,2,1]: (2-1)*8 + (2-1)*4 + 1*2 = 8 + 4 + 2 = 14

        # Test additional cases with base=2, levels=3 for values 14, 15, 16
        # Value 14: exponential_partition_transform(14) = ["16", "8", "2"]
        transform_output = exponential_partition_transform(14, base=2, levels=3)
        expected = ["16", "8", "2"]
        assert transform_output == expected
        parsed_value = parse_exponential_partitions(transform_output, base=2, levels=3)
        assert (
            parsed_value == 14
        )  # multipliers=[2,2,1]: (2-1)*8 + (2-1)*4 + 1*2 = 8 + 4 + 2 = 14

        # Value 15: exponential_partition_transform(15) = ["16", "8", "4"]
        transform_output = exponential_partition_transform(15, base=2, levels=3)
        expected = ["16", "8", "4"]
        assert transform_output == expected
        parsed_value = parse_exponential_partitions(transform_output, base=2, levels=3)
        assert (
            parsed_value == 16
        )  # multipliers=[2,2,2]: (2-1)*8 + (2-1)*4 + 2*2 = 8 + 4 + 4 = 16

        # Value 16: exponential_partition_transform(16) = ["16", "8", "4"]
        transform_output = exponential_partition_transform(16, base=2, levels=3)
        expected = ["16", "8", "4"]
        assert transform_output == expected
        parsed_value = parse_exponential_partitions(transform_output, base=2, levels=3)
        assert (
            parsed_value == 16
        )  # multipliers=[2,2,2]: (2-1)*8 + (2-1)*4 + 2*2 = 8 + 4 + 4 = 16

    def test_parse_edge_cases(self):
        """Test parser with edge cases."""
        # Very large numbers (must be exact multiples)
        result = parse_exponential_partitions(
            ["999999000000", "123000"], base=1000, levels=2
        )
        assert result == 999998123000  # (999999-1)*1000000 + 123*1000

        # Zero - should return None
        result = parse_exponential_partitions(["0", "1000"], base=1000, levels=2)
        assert result is None

        # Negative values - should return None (caught by isdigit check)
        result = parse_exponential_partitions(["-5", "1000"], base=1000, levels=2)
        assert result is None

        # Single character less than base - should return None (invalid partition value)
        result = parse_exponential_partitions(["5"], base=1000, levels=1)
        assert result is None  # 5 < 1000, so multiplier is 0, which is invalid

        # Leading zeros (should still parse correctly)
        result = parse_exponential_partitions(
            ["0001000000", "2000"], base=1000, levels=2
        )
        assert result == 2000  # (1-1)*1000000 + 2*1000 = 2000

    def test_parse_zero_negative_at_any_position(self):
        """Test that zero or negative values at ANY position in hierarchy return None."""
        # Test zero at different positions
        test_cases = [
            (["0", "1000"], "Zero in first position"),
            (["1000000", "0"], "Zero in second position"),
            (["0", "0"], "Zero in both positions"),
            (["1000000", "1000", "0"], "Zero in third position (3-level)"),
            (["0", "1000000", "1000"], "Zero in first of 3-level"),
            (["1000000", "0", "1000"], "Zero in middle of 3-level"),
        ]

        for partition_dirs, description in test_cases:
            result = parse_exponential_partitions(
                partition_dirs, base=1000, levels=len(partition_dirs)
            )
            assert result is None, f"{description} should return None, got {result}"

        # Test negative values (caught by isdigit check)
        negative_cases = [
            (["-5", "1000"], "Negative in first position"),
            (["1000000", "-5"], "Negative in second position"),
            (["-1", "-2"], "Negative in both positions"),
        ]

        for partition_dirs, description in negative_cases:
            result = parse_exponential_partitions(
                partition_dirs, base=1000, levels=len(partition_dirs)
            )
            assert result is None, f"{description} should return None, got {result}"

    def test_parse_exact_multiple_validation(self):
        """Test that parser rejects partition values that are not exact multiples of expected divisors."""
        # exponential_partition_transform ALWAYS generates exact multiples, so non-multiples indicate corrupted data

        # Test base=1000, levels=2 cases
        invalid_cases = [
            (["1500000", "1000"], "First partition not multiple of 1000000"),
            (["1000000", "1500"], "Second partition not multiple of 1000"),
            (["2500000", "2500"], "Both partitions not exact multiples"),
            (["999000", "1000"], "First partition less than minimum divisor"),
            (["1000000", "999"], "Second partition less than minimum divisor"),
        ]

        for partition_dirs, description in invalid_cases:
            result = parse_exponential_partitions(partition_dirs, base=1000, levels=2)
            assert result is None, f"{description} should return None, got {result}"

        # Test base=2, levels=3 cases
        base2_invalid_cases = [
            (["9", "4", "2"], "First partition not multiple of 8"),
            (["8", "5", "2"], "Second partition not multiple of 4"),
            (["8", "4", "3"], "Third partition not multiple of 2"),
            (["7", "4", "2"], "First partition less than minimum divisor"),
            (["8", "3", "2"], "Second partition less than minimum divisor"),
            (["8", "4", "1"], "Third partition less than minimum divisor"),
        ]

        for partition_dirs, description in base2_invalid_cases:
            result = parse_exponential_partitions(partition_dirs, base=2, levels=3)
            assert result is None, f"{description} should return None, got {result}"

        # Test valid cases to ensure we don't reject legitimate values
        valid_cases = [
            (["1000000", "1000"], 1000, 1000, 2),
            (["2000000", "3000"], 1003000, 1000, 2),
            (["8", "4", "2"], 2, 2, 3),
            (["16", "8", "4"], 16, 2, 3),
        ]

        for partition_dirs, expected_result, base, levels in valid_cases:
            result = parse_exponential_partitions(
                partition_dirs, base=base, levels=levels
            )
            assert (
                result == expected_result
            ), f"Valid case {partition_dirs} should return {expected_result}, got {result}"

    def test_uint64_max_validation(self):
        """Test that exponential partition parser validates results don't exceed UNSIGNED_INT64_MAX_VALUE."""
        # Test case with valid large values (should work)
        # Using large but reasonable partition directories
        large_but_valid = ["1000000000000000", "1000000000000"]  # 10^15 and 10^12
        result = parse_exponential_partitions(large_but_valid, base=1000, levels=2)
        assert result is not None
        assert result <= UNSIGNED_INT64_MAX_VALUE

        # Test case that would cause overflow in calculation (should return None)
        # Create partition directories that would result in a value > UNSIGNED_INT64_MAX_VALUE
        # For base=10, levels=2: result = (mult1-1)*10 + mult2*1
        # If mult1 = UNSIGNED_INT64_MAX_VALUE and mult2 = 1, this would overflow
        max_str = str(UNSIGNED_INT64_MAX_VALUE)
        overflow_case = [max_str + "0", "1"]  # This would cause overflow in the parser
        result = parse_exponential_partitions(overflow_case, base=10, levels=2)
        assert (
            result is None
        ), "Parser should return None for values that would cause overflow"

        # Test edge case - result exactly equal to UNSIGNED_INT64_MAX_VALUE (should work)
        # We'll use a simpler case we can control
        # For base=2, levels=1: result = multiplier * 2
        # If multiplier = UNSIGNED_INT64_MAX_VALUE // 2, result should be valid
        max_even = (UNSIGNED_INT64_MAX_VALUE // 2) * 2  # Largest even number <= max
        valid_edge_case = [str(max_even)]
        result = parse_exponential_partitions(valid_edge_case, base=2, levels=1)
        assert result is not None
        assert result <= UNSIGNED_INT64_MAX_VALUE

        # Test case just over the limit (should return None)
        # Create a case that would result in max + 1
        if UNSIGNED_INT64_MAX_VALUE % 2 == 1:  # If max is odd
            over_limit_case = [str(UNSIGNED_INT64_MAX_VALUE + 1)]
            result = parse_exponential_partitions(over_limit_case, base=1, levels=1)
            assert (
                result is None
            ), "Parser should return None for values > UNSIGNED_INT64_MAX_VALUE"


class TestExponentialPartitionRemover:
    """Test remove_exponential_partitions function."""

    def test_remove_valid_exponential_partitions(self):
        """Test removing valid exponential partitions from paths."""
        # Test basic 2-level removal
        result = remove_exponential_partitions(
            "/data/1000000/2000/file.json", base=1000, levels=2
        )
        assert result == "/data/file.json"

        # Test with deeper base path
        result = remove_exponential_partitions(
            "/base/path/1000000/2000/file.json", base=1000, levels=2
        )
        assert result == "/base/path/file.json"

        # Test with different partition values
        result = remove_exponential_partitions(
            "/data/2000000/500000/file.json", base=1000, levels=2
        )
        assert result == "/data/file.json"

        # Test 3-level removal
        result = remove_exponential_partitions(
            "/data/1000000000/1000000/3000/file.json", base=1000, levels=3
        )
        assert result == "/data/file.json"

        # Test single-level removal
        result = remove_exponential_partitions(
            "/data/3000/file.json", base=1000, levels=1
        )
        assert result == "/data/file.json"

    def test_no_partitions_to_remove(self):
        """Test paths that don't contain exponential partitions."""
        # Path with no partitions
        result = remove_exponential_partitions("/data/file.json", base=1000, levels=2)
        assert result == "/data/file.json"

        # Path with insufficient directories
        result = remove_exponential_partitions("/data/file.json", base=1000, levels=3)
        assert result == "/data/file.json"

        # Path with invalid partition values
        result = remove_exponential_partitions(
            "/data/999999/2000/file.json", base=1000, levels=2
        )
        assert result == "/data/999999/2000/file.json"

        # Path with non-numeric partition values
        result = remove_exponential_partitions(
            "/data/abc/2000/file.json", base=1000, levels=2
        )
        assert result == "/data/abc/2000/file.json"

        # Path with valid exponential partitions not at the end (should not remove)
        result = remove_exponential_partitions(
            "/data/1000000/2000/subdir/file.json", base=1000, levels=2
        )
        assert result == "/data/1000000/2000/subdir/file.json"

    def test_edge_cases(self):
        """Test edge cases for exponential partition removal."""
        # Root level file
        result = remove_exponential_partitions("file.json", base=1000, levels=2)
        assert result == "file.json"

        # File in current directory with partitions
        result = remove_exponential_partitions(
            "1000000/2000/file.json", base=1000, levels=2
        )
        assert result == "file.json"

        # Empty base directory after removal
        result = remove_exponential_partitions(
            "1000000/2000/file.json", base=1000, levels=2
        )
        assert result == "file.json"

    def test_different_base_values(self):
        """Test with different base values."""
        # Base 10
        result = remove_exponential_partitions(
            "/data/100/20/file.json", base=10, levels=2
        )
        assert result == "/data/file.json"

        # Base 2
        result = remove_exponential_partitions("/data/8/4/file.json", base=2, levels=2)
        assert result == "/data/file.json"

    def test_parameter_validation(self):
        """Test parameter validation."""
        # Invalid path type
        with pytest.raises(TypeError, match="path must be a string"):
            remove_exponential_partitions(123, base=1000, levels=2)

        # Invalid base type
        with pytest.raises(TypeError, match="base must be an integer"):
            remove_exponential_partitions("/data/file.json", base="1000", levels=2)

        # Invalid levels type
        with pytest.raises(TypeError, match="levels must be an integer"):
            remove_exponential_partitions("/data/file.json", base=1000, levels="2")

        # Invalid base value
        with pytest.raises(ValueError, match="base must be greater than 1"):
            remove_exponential_partitions("/data/file.json", base=1, levels=2)

        with pytest.raises(ValueError, match="base must be greater than 1"):
            remove_exponential_partitions("/data/file.json", base=0, levels=2)

        # Invalid levels value
        with pytest.raises(ValueError, match="levels must be positive"):
            remove_exponential_partitions("/data/file.json", base=1000, levels=0)

        with pytest.raises(ValueError, match="levels must be positive"):
            remove_exponential_partitions("/data/file.json", base=1000, levels=-1)

    def test_complex_paths(self):
        """Test with complex path structures."""
        # Path with many directories
        result = remove_exponential_partitions(
            "/a/b/c/d/e/1000000/2000/file.json", base=1000, levels=2
        )
        assert result == "/a/b/c/d/e/file.json"

        # Path with only partition directories
        result = remove_exponential_partitions(
            "1000000/2000/file.json", base=1000, levels=2
        )
        assert result == "file.json"

        # Path with special characters in filename
        result = remove_exponential_partitions(
            "/data/1000000/2000/file-name_with.special.chars.json", base=1000, levels=2
        )
        assert result == "/data/file-name_with.special.chars.json"

    def test_no_filename_component(self):
        """Test paths without filename component."""
        # Directory path only - partitions should be removed since they're valid
        result = remove_exponential_partitions(
            "/data/1000000/2000/", base=1000, levels=2
        )
        assert result == "/data"

        # Empty string
        result = remove_exponential_partitions("", base=1000, levels=2)
        assert result == ""

    def test_is_directory_path_parameter(self):
        """Test the is_directory_path parameter functionality."""
        # Test is_directory_path=False (default behavior - file paths)
        result = remove_exponential_partitions(
            "/data/1000000/2000/file.json", base=1000, levels=2, is_directory_path=False
        )
        assert result == "/data/file.json"

        # Test is_directory_path=True (directory paths only)
        result = remove_exponential_partitions(
            "/data/1000000/2000", base=1000, levels=2, is_directory_path=True
        )
        assert result == "/data"

        # Test with deeper base path (directory path)
        result = remove_exponential_partitions(
            "/base/path/1000000/2000", base=1000, levels=2, is_directory_path=True
        )
        assert result == "/base/path"

        # Test 3-level removal with is_directory_path=True (directory path)
        result = remove_exponential_partitions(
            "/data/1000000000/1000000/3000", base=1000, levels=3, is_directory_path=True
        )
        assert result == "/data"

        # Test single-level removal with is_directory_path=True (directory path)
        result = remove_exponential_partitions(
            "/data/3000", base=1000, levels=1, is_directory_path=True
        )
        assert result == "/data"

        # Test no partitions to remove with is_directory_path=True (should remain unchanged)
        result = remove_exponential_partitions(
            "/data/not_partition", base=1000, levels=2, is_directory_path=True
        )
        assert (
            result == "/data/not_partition"
        )  # Should remain unchanged since no partitions found

        # Test invalid partitions with is_directory_path=True
        result = remove_exponential_partitions(
            "/data/999999/2000", base=1000, levels=2, is_directory_path=True
        )
        assert result == "/data/999999/2000"  # Should remain unchanged

        # Test root level paths with is_directory_path=True
        result = remove_exponential_partitions(
            "1000000/2000", base=1000, levels=2, is_directory_path=True
        )
        assert result == ""

        # Test paths that end with directory (trailing slash) with is_directory_path=True
        result = remove_exponential_partitions(
            "/data/1000000/2000/", base=1000, levels=2, is_directory_path=True
        )
        assert (
            result == "/data/1000000/2000/"
        )  # Trailing slash creates empty component, no valid partitions found

        # Test empty path with is_directory_path=True
        result = remove_exponential_partitions(
            "", base=1000, levels=2, is_directory_path=True
        )
        assert result == ""

        # Test complex nested paths with is_directory_path=True (directory path)
        result = remove_exponential_partitions(
            "/complex/nested/path/1000000/2000",
            base=1000,
            levels=2,
            is_directory_path=True,
        )
        assert result == "/complex/nested/path"

        # Test different base and levels with is_directory_path=True (directory path)
        result = remove_exponential_partitions(
            "/data/100/20", base=10, levels=2, is_directory_path=True
        )
        assert result == "/data"

        # Test the key difference: file path with is_directory_path=True should NOT remove partitions
        # because the filename is treated as a directory component and won't match partition patterns
        result = remove_exponential_partitions(
            "/data/1000000/2000/file.json", base=1000, levels=2, is_directory_path=True
        )
        assert (
            result == "/data/1000000/2000/file.json"
        )  # No partitions found in directory components

    def test_is_directory_path_comprehensive_scenarios(self):
        """Comprehensive test scenarios for is_directory_path parameter to protect against regressions."""

        # Standard file path processing (is_directory_path=False, default)
        result = remove_exponential_partitions(
            "/catalog/1000000/2000/metafile.json", base=1000, levels=2
        )
        assert result == "/catalog/metafile.json"

        # Standard directory path processing (is_directory_path=True)
        result = remove_exponential_partitions(
            "/catalog/1000000/2000", base=1000, levels=2, is_directory_path=True
        )
        assert result == "/catalog"

        # Multi-level partitioning with file paths
        result = remove_exponential_partitions(
            "/base/1000000000/1000000/3000/data.parquet",
            base=1000,
            levels=3,
            is_directory_path=False,
        )
        assert result == "/base/data.parquet"

        # Multi-level partitioning with directory paths
        result = remove_exponential_partitions(
            "/base/1000000000/1000000/3000", base=1000, levels=3, is_directory_path=True
        )
        assert result == "/base"

        # Empty directory path
        result = remove_exponential_partitions(
            "", base=1000, levels=2, is_directory_path=True
        )
        assert result == ""

        # Root-level partitions with file path
        result = remove_exponential_partitions(
            "1000000/2000/file.txt", base=1000, levels=2, is_directory_path=False
        )
        assert result == "file.txt"

        # Root-level partitions with directory path
        result = remove_exponential_partitions(
            "1000000/2000", base=1000, levels=2, is_directory_path=True
        )
        assert result == ""

        # Single partition level with file path
        result = remove_exponential_partitions(
            "/data/5000/document.pdf", base=1000, levels=1, is_directory_path=False
        )
        assert result == "/data/document.pdf"

        # Single partition level with directory path
        result = remove_exponential_partitions(
            "/data/5000", base=1000, levels=1, is_directory_path=True
        )
        assert result == "/data"

        # Path with trailing slash (file path interpretation)
        result = remove_exponential_partitions(
            "/data/1000000/2000/", base=1000, levels=2, is_directory_path=False
        )
        assert (
            result == "/data"
        )  # Empty basename after trailing slash, partitions removed from dirname

        # Path with trailing slash (directory path interpretation)
        result = remove_exponential_partitions(
            "/data/1000000/2000/", base=1000, levels=2, is_directory_path=True
        )
        assert (
            result == "/data/1000000/2000/"
        )  # Trailing slash creates empty component, no valid partitions

        # Very deep nesting with file path
        result = remove_exponential_partitions(
            "/a/b/c/d/e/1000000/2000/file.json",
            base=1000,
            levels=2,
            is_directory_path=False,
        )
        assert result == "/a/b/c/d/e/file.json"

        # Very deep nesting with directory path
        result = remove_exponential_partitions(
            "/a/b/c/d/e/1000000/2000", base=1000, levels=2, is_directory_path=True
        )
        assert result == "/a/b/c/d/e"

        # Non-exponential values with file path
        result = remove_exponential_partitions(
            "/data/999999/1999/file.json", base=1000, levels=2, is_directory_path=False
        )
        assert result == "/data/999999/1999/file.json"  # Should remain unchanged

        # Non-exponential values with directory path
        result = remove_exponential_partitions(
            "/data/999999/1999", base=1000, levels=2, is_directory_path=True
        )
        assert result == "/data/999999/1999"  # Should remain unchanged

        # Mixed valid/invalid partitions with file path
        result = remove_exponential_partitions(
            "/data/1000000/1999/file.json", base=1000, levels=2, is_directory_path=False
        )
        assert (
            result == "/data/1000000/1999/file.json"
        )  # Should remain unchanged (second partition invalid)

        # Mixed valid/invalid partitions with directory path
        result = remove_exponential_partitions(
            "/data/1000000/1999", base=1000, levels=2, is_directory_path=True
        )
        assert (
            result == "/data/1000000/1999"
        )  # Should remain unchanged (second partition invalid)

        # Base 10 with file path
        result = remove_exponential_partitions(
            "/data/100/20/file.csv", base=10, levels=2, is_directory_path=False
        )
        assert result == "/data/file.csv"

        # Base 10 with directory path
        result = remove_exponential_partitions(
            "/data/100/20", base=10, levels=2, is_directory_path=True
        )
        assert result == "/data"

        # Single level with base 100
        result = remove_exponential_partitions(
            "/data/10000/file.txt", base=100, levels=1, is_directory_path=False
        )
        assert result == "/data/file.txt"

        # Ensure file path with is_directory_path=True treats filename as directory component
        result = remove_exponential_partitions(
            "/metastore/1000000/2000/namespace.json",
            base=1000,
            levels=2,
            is_directory_path=True,
        )
        assert (
            result == "/metastore/1000000/2000/namespace.json"
        )  # No partitions found because filename not numeric

        # Ensure directory path with is_directory_path=False extracts directory properly
        result = remove_exponential_partitions(
            "/metastore/1000000/2000", base=1000, levels=2, is_directory_path=False
        )
        assert (
            result == "/metastore/1000000/2000"
        )  # dirname "/metastore/1000000" lacks 2 partition levels, no change

        # Directory path that would work with file mode
        result = remove_exponential_partitions(
            "/base/data/1000000/2000/finaldir",
            base=1000,
            levels=2,
            is_directory_path=False,
        )
        assert (
            result == "/base/data/finaldir"
        )  # dirname "/base/data/1000000/2000" has valid partitions, basename "finaldir" preserved

        # Complex filename that could be mistaken for partition
        result = remove_exponential_partitions(
            "/data/1000000/2000/3000.json", base=1000, levels=2, is_directory_path=False
        )
        assert result == "/data/3000.json"  # Only directory partitions removed

        # Complex filename that could be mistaken for partition with is_directory_path=True
        result = remove_exponential_partitions(
            "/data/1000000/2000/3000.json", base=1000, levels=2, is_directory_path=True
        )
        assert (
            result == "/data/1000000/2000/3000.json"
        )  # No valid partitions found (3000.json not numeric)

        # Simulate metafile revision directory path (directory only, no filename)
        revision_dir_path = "/catalog/namespace_id/table_id/rev/1000000/2000"
        result = remove_exponential_partitions(
            revision_dir_path, is_directory_path=True
        )
        assert result == "/catalog/namespace_id/table_id/rev"

        # Simulate deeper revision directory path
        revision_dir_path = "/catalog/ns/tb/rev/1000000000/1000000/3000"
        result = remove_exponential_partitions(
            revision_dir_path, base=1000, levels=3, is_directory_path=True
        )
        assert result == "/catalog/ns/tb/rev"

        # Test that metafile usage doesn't accidentally remove non-partition directories
        revision_dir_path = "/catalog/namespace_id/table_id/rev/invaliddir/anotherdir"
        result = remove_exponential_partitions(
            revision_dir_path, is_directory_path=True
        )
        assert (
            result == "/catalog/namespace_id/table_id/rev/invaliddir/anotherdir"
        )  # No valid partitions

    def test_is_directory_path_parameter_validation(self):
        """Test parameter validation and error handling for is_directory_path parameter."""

        # Test that is_directory_path parameter accepts boolean values correctly
        result = remove_exponential_partitions(
            "/data/1000000/2000", is_directory_path=True
        )
        assert result == "/data"

        result = remove_exponential_partitions(
            "/data/1000000/2000/file.json", is_directory_path=False
        )
        assert result == "/data/file.json"

        # Test that function works with explicit False (ensuring default behavior)
        result = remove_exponential_partitions(
            "/data/1000000/2000/file.json", is_directory_path=False
        )
        assert result == "/data/file.json"

        # Test that default parameter behavior is preserved (is_directory_path=False by default)
        result_default = remove_exponential_partitions("/data/1000000/2000/file.json")
        result_explicit = remove_exponential_partitions(
            "/data/1000000/2000/file.json", is_directory_path=False
        )
        assert result_default == result_explicit

        # Test parameter combination with other parameters
        result = remove_exponential_partitions(
            "/data/100/20", base=10, levels=2, is_directory_path=True
        )
        assert result == "/data"

        result = remove_exponential_partitions(
            "/data/100/20/file.txt", base=10, levels=2, is_directory_path=False
        )
        assert result == "/data/file.txt"

        # Test that keyword argument works correctly
        result = remove_exponential_partitions(
            path="/data/1000000/2000", is_directory_path=True
        )
        assert result == "/data"

        # Ensure all existing behavior still works with explicit is_directory_path=False
        test_cases = [
            ("/data/1000000/2000/file.json", "/data/file.json"),
            ("/base/path/1000000/2000/document.pdf", "/base/path/document.pdf"),
            ("1000000/2000/file.txt", "file.txt"),
            ("/data/file.json", "/data/file.json"),  # No partitions to remove
            ("", ""),  # Empty string
        ]

        for input_path, expected_output in test_cases:
            result_default = remove_exponential_partitions(input_path)
            result_explicit = remove_exponential_partitions(
                input_path, is_directory_path=False
            )
            assert (
                result_default == expected_output
            ), f"Default behavior failed for {input_path}"
            assert (
                result_explicit == expected_output
            ), f"Explicit is_directory_path=False failed for {input_path}"
            assert (
                result_default == result_explicit
            ), f"Default vs explicit mismatch for {input_path}"
