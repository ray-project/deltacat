import pytest
import tempfile
import shutil
from deltacat.catalog import CatalogProperties
from deltacat.compute.compactor.utils.round_completion_file import (
    read_round_completion_file,
    write_round_completion_file,
)
from deltacat.tests.compute.test_util_common import get_test_partition_locator
from deltacat.compute.compactor import RoundCompletionInfo


@pytest.fixture(scope="function")
def temp_catalog():
    """Create a temporary catalog for testing."""
    tmpdir = tempfile.mkdtemp()
    catalog = CatalogProperties(root=tmpdir)
    yield catalog
    shutil.rmtree(tmpdir)


class TestReadWriteRoundCompletionFile:
    def test_read_when_rcf_written_without_destination(self, temp_catalog):
        """
        This test case tests the backward compatibility by successfully
        reading the previously written rcf.
        """

        source_locator = get_test_partition_locator("source")
        destination_locator = get_test_partition_locator("destination")
        compaction_artifact_path = temp_catalog.root + "/compute/compactor"

        expected_rcf = RoundCompletionInfo.of(
            high_watermark=122,
            compacted_delta_locator={},
            compacted_pyarrow_write_result={},
            sort_keys_bit_width=12,
        )

        rcf_url = write_round_completion_file(
            compaction_artifact_path, source_locator, None, expected_rcf
        )

        rcf = read_round_completion_file(
            compaction_artifact_path, source_locator, destination_locator
        )

        assert rcf_url.endswith("f9829af39770d904dbb811bd8f4e886dd307f507.json")
        assert rcf == expected_rcf

    def test_read_when_rcf_written_with_destination(self, temp_catalog):
        """
        This test case tests the backward compatibility by successfully
        reading the previously written rcf.
        """

        source_locator = get_test_partition_locator("source")
        destination_locator = get_test_partition_locator("destination")
        compaction_artifact_path = temp_catalog.root + "/compute/compactor"

        expected_rcf = RoundCompletionInfo.of(
            high_watermark=122,
            compacted_delta_locator={},
            compacted_pyarrow_write_result={},
            sort_keys_bit_width=12,
        )

        rcf_url = write_round_completion_file(
            compaction_artifact_path, source_locator, destination_locator, expected_rcf
        )

        rcf = read_round_completion_file(
            compaction_artifact_path, source_locator, destination_locator
        )

        assert rcf_url.endswith(
            "f9829af39770d904dbb811bd8f4e886dd307f507/e9939deadc091b3289a2eb0ca56b1ba86b9892f4.json"
        )
        assert rcf == expected_rcf

    def test_read_without_destination_when_rcf_written_with_destination(
        self, temp_catalog
    ):
        """
        This test case tests the backward compatibility by successfully
        reading the previously written rcf.
        """

        source_locator = get_test_partition_locator("source")
        destination_locator = get_test_partition_locator("destination")
        compaction_artifact_path = temp_catalog.root + "/compute/compactor"

        expected_rcf = RoundCompletionInfo.of(
            high_watermark=122,
            compacted_delta_locator={},
            compacted_pyarrow_write_result={},
            sort_keys_bit_width=12,
        )

        write_round_completion_file(
            compaction_artifact_path, source_locator, destination_locator, expected_rcf
        )

        rcf = read_round_completion_file(compaction_artifact_path, source_locator, None)

        assert rcf is None

    def test_read_without_destination_when_rcf_written_without_destination(
        self, temp_catalog
    ):
        """
        This test case tests the backward compatibility by successfully
        reading the previously written rcf.
        """

        source_locator = get_test_partition_locator("source")
        compaction_artifact_path = temp_catalog.root + "/compute/compactor"

        expected_rcf = RoundCompletionInfo.of(
            high_watermark=122,
            compacted_delta_locator={},
            compacted_pyarrow_write_result={},
            sort_keys_bit_width=12,
        )

        write_round_completion_file(
            compaction_artifact_path, source_locator, None, expected_rcf
        )

        rcf = read_round_completion_file(compaction_artifact_path, source_locator, None)

        assert rcf == expected_rcf

    def test_read_when_rcf_written_both_with_and_without_destination(
        self, temp_catalog
    ):
        """
        This test case tests the backward compatibility by successfully
        reading the previously written rcf.
        """

        source_locator = get_test_partition_locator("source")
        destination_locator = get_test_partition_locator("destination")
        compaction_artifact_path = temp_catalog.root + "/compute/compactor"

        expected_rcf = RoundCompletionInfo.of(
            high_watermark=122,
            compacted_delta_locator={},
            compacted_pyarrow_write_result={},
            sort_keys_bit_width=12,
        )

        expected_rcf_2 = RoundCompletionInfo.of(
            high_watermark=1223,
            compacted_delta_locator={},
            compacted_pyarrow_write_result={},
            sort_keys_bit_width=1233,
        )

        write_round_completion_file(
            compaction_artifact_path, source_locator, None, expected_rcf
        )

        write_round_completion_file(
            compaction_artifact_path,
            source_locator,
            destination_locator,
            expected_rcf_2,
        )

        rcf = read_round_completion_file(
            compaction_artifact_path, source_locator, destination_locator
        )

        assert rcf == expected_rcf_2

    def test_read_when_none_destination_partition_id(self, temp_catalog):

        source_locator = get_test_partition_locator("source")
        destination_locator = get_test_partition_locator(None)
        compaction_artifact_path = temp_catalog.root + "/compute/compactor"

        expected_rcf = RoundCompletionInfo.of(
            high_watermark=122,
            compacted_delta_locator={},
            compacted_pyarrow_write_result={},
            sort_keys_bit_width=12,
        )

        write_round_completion_file(
            compaction_artifact_path, source_locator, destination_locator, expected_rcf
        )

        rcf = read_round_completion_file(
            compaction_artifact_path, source_locator, destination_locator
        )

        assert rcf == expected_rcf

    def test_write_when_custom_url_is_passed(self, temp_catalog):
        """
        This test case tests the backward compatibility by successfully
        reading the previously written rcf.
        """

        source_locator = get_test_partition_locator("source")
        compaction_artifact_path = temp_catalog.root + "/compute/compactor"

        expected_rcf = RoundCompletionInfo.of(
            high_watermark=122,
            compacted_delta_locator={},
            compacted_pyarrow_write_result={},
            sort_keys_bit_width=12,
        )

        completion_file_path = temp_catalog.root + "/test.json"
        rcf_url = write_round_completion_file(
            None,
            None,
            None,
            expected_rcf,
            completion_file_path=completion_file_path,
        )

        rcf = read_round_completion_file(compaction_artifact_path, source_locator, None)

        assert rcf_url == completion_file_path
        assert rcf is None
