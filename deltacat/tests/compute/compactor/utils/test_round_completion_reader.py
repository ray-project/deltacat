from unittest.mock import Mock
from deltacat.compute.compactor.utils.round_completion_reader import (
    read_round_completion_info,
)
from deltacat.tests.compute.test_util_common import get_test_partition_locator
from deltacat.compute.compactor import RoundCompletionInfo
from deltacat.compute.compactor.model.pyarrow_write_result import PyArrowWriteResult
from deltacat.storage.model.partition import Partition


class TestRoundCompletionInfoInPartition:
    def test_read_round_completion_info_from_partition_with_matching_source(self):
        """
        Test reading RoundCompletionInfo from partition metafile with matching source partition locator.
        """
        source_locator = get_test_partition_locator("source")
        destination_locator = get_test_partition_locator("destination")

        # Create a test RoundCompletionInfo with prev_source_partition_locator
        pawr = PyArrowWriteResult.of(
            file_count=1, pyarrow_bytes=1000, file_bytes=1000, record_count=100
        )

        expected_rci = RoundCompletionInfo.of(
            high_watermark=122,
            compacted_delta_locator=None,
            compacted_pyarrow_write_result=pawr,
            sort_keys_bit_width=12,
            prev_source_partition_locator=source_locator,
        )

        # Create a partition with RoundCompletionInfo
        partition = Partition.of(
            locator=destination_locator,
            content_types=None,
            compaction_round_completion_info=expected_rci,
        )

        # Mock the storage
        mock_storage = Mock()

        # Test reading with partition provided (no storage call needed)
        rci = read_round_completion_info(
            source_partition_locator=source_locator,
            destination_partition_locator=destination_locator,
            deltacat_storage=mock_storage,
            deltacat_storage_kwargs={},
            destination_partition=partition,
        )

        assert rci is not None
        assert rci == expected_rci
        assert rci.high_watermark == 122
        assert rci.sort_keys_bit_width == 12
        assert (
            rci.prev_source_partition_locator.partition_id
            == source_locator.partition_id
        )

        # Verify storage was not called since partition was provided
        mock_storage.get_partition.assert_not_called()

    def test_read_round_completion_info_from_partition_with_mismatched_source(self):
        """
        Test reading RoundCompletionInfo from partition metafile with mismatched source partition locator.
        Should return None and log a warning.
        """
        source_locator = get_test_partition_locator("source")
        different_source_locator = get_test_partition_locator("different_source")
        destination_locator = get_test_partition_locator("destination")

        # Create a test RoundCompletionInfo with different prev_source_partition_locator
        pawr = PyArrowWriteResult.of(
            file_count=1, pyarrow_bytes=1000, file_bytes=1000, record_count=100
        )

        expected_rci = RoundCompletionInfo.of(
            high_watermark=122,
            compacted_delta_locator=None,
            compacted_pyarrow_write_result=pawr,
            sort_keys_bit_width=12,
            prev_source_partition_locator=different_source_locator,  # Different from source_locator
        )

        # Create a partition with RoundCompletionInfo
        partition = Partition.of(
            locator=destination_locator,
            content_types=None,
            compaction_round_completion_info=expected_rci,
        )

        # Mock the storage
        mock_storage = Mock()

        # Test reading with mismatched source locator
        rci = read_round_completion_info(
            source_partition_locator=source_locator,  # Different from the one in RoundCompletionInfo
            destination_partition_locator=destination_locator,
            deltacat_storage=mock_storage,
            deltacat_storage_kwargs={},
            destination_partition=partition,
        )

        # Should return None due to mismatch
        assert rci is None

        # Verify storage was not called since partition was provided
        mock_storage.get_partition.assert_not_called()

    def test_read_round_completion_info_from_storage_when_partition_not_provided(self):
        """
        Test reading RoundCompletionInfo from storage when partition is not provided.
        """
        source_locator = get_test_partition_locator("source")
        destination_locator = get_test_partition_locator("destination")

        # Create a test RoundCompletionInfo
        pawr = PyArrowWriteResult.of(
            file_count=1, pyarrow_bytes=1000, file_bytes=1000, record_count=100
        )

        expected_rci = RoundCompletionInfo.of(
            high_watermark=122,
            compacted_delta_locator=None,
            compacted_pyarrow_write_result=pawr,
            sort_keys_bit_width=12,
            prev_source_partition_locator=source_locator,
        )

        # Create a partition with RoundCompletionInfo
        partition = Partition.of(
            locator=destination_locator,
            content_types=None,
            compaction_round_completion_info=expected_rci,
        )

        # Mock the storage to return the partition
        mock_storage = Mock()
        mock_storage.get_partition.return_value = partition

        # Test reading without partition provided (storage call needed)
        rci = read_round_completion_info(
            source_partition_locator=source_locator,
            destination_partition_locator=destination_locator,
            deltacat_storage=mock_storage,
            deltacat_storage_kwargs={"test_arg": "test_value"},
        )

        assert rci is not None
        assert rci == expected_rci
        assert rci.high_watermark == 122

        # Verify storage was called with correct parameters
        mock_storage.get_partition.assert_called_once_with(
            destination_locator.stream_locator,
            destination_locator.partition_values,
            test_arg="test_value",
        )

    def test_read_round_completion_info_when_partition_not_found(self):
        """
        Test reading RoundCompletionInfo when partition is not found in storage.
        """
        source_locator = get_test_partition_locator("source")
        destination_locator = get_test_partition_locator("destination")

        # Mock the storage to return None (partition not found)
        mock_storage = Mock()
        mock_storage.get_partition.return_value = None

        # Test reading when partition not found
        rci = read_round_completion_info(
            source_partition_locator=source_locator,
            destination_partition_locator=destination_locator,
            deltacat_storage=mock_storage,
            deltacat_storage_kwargs={},
        )

        # Should return None when partition not found
        assert rci is None

        # Verify storage was called
        mock_storage.get_partition.assert_called_once()

    def test_read_round_completion_info_when_no_completion_info_in_partition(self):
        """
        Test reading RoundCompletionInfo when partition exists but has no completion info.
        """
        source_locator = get_test_partition_locator("source")
        destination_locator = get_test_partition_locator("destination")

        # Create a partition without RoundCompletionInfo
        partition = Partition.of(
            locator=destination_locator,
            content_types=None,
            compaction_round_completion_info=None,
        )

        # Mock the storage to return the partition
        mock_storage = Mock()
        mock_storage.get_partition.return_value = partition

        # Test reading when no completion info in partition
        rci = read_round_completion_info(
            source_partition_locator=source_locator,
            destination_partition_locator=destination_locator,
            deltacat_storage=mock_storage,
            deltacat_storage_kwargs={},
        )

        # Should return None when no completion info
        assert rci is None

    def test_read_with_missing_prev_source_partition_locator_returns_none(self):
        """
        Test that reading with missing prev_source_partition_locator returns None.
        """
        source_locator = get_test_partition_locator("source")
        destination_locator = get_test_partition_locator("destination")

        # Create RoundCompletionInfo without prev_source_partition_locator
        pawr = PyArrowWriteResult.of(
            file_count=1, pyarrow_bytes=1000, file_bytes=1000, record_count=100
        )

        rcf = RoundCompletionInfo.of(
            high_watermark=122,
            compacted_delta_locator=None,
            compacted_pyarrow_write_result=pawr,
            sort_keys_bit_width=12,
            prev_source_partition_locator=None,  # Missing
        )

        # Create a partition with RoundCompletionInfo
        partition = Partition.of(
            locator=destination_locator,
            content_types=None,
            compaction_round_completion_info=rcf,
        )

        # Mock the storage
        mock_storage = Mock()

        # Test reading should return None due to missing prev_source_partition_locator
        result = read_round_completion_info(
            source_partition_locator=source_locator,
            destination_partition_locator=destination_locator,
            deltacat_storage=mock_storage,
            deltacat_storage_kwargs={},
            destination_partition=partition,
        )

        # Should return None when prev_source_partition_locator is missing or mismatched
        assert result is None
