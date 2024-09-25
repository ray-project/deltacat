import pytest
from deltacat.storage import ManifestEntry, ManifestMeta
import pyarrow.parquet as papq
from deltacat.types.partial_download import PartialParquetParameters
from deltacat.compute.resource_requirements.utils import (
    estimate_manifest_entry_column_size_bytes,
    estimate_manifest_entry_num_rows,
    estimate_manifest_entry_size_bytes,
)

PARQUET_FILE_PATH_NO_STATS = (
    "deltacat/tests/compute/resource_requirements/data/sample_no_stats.parquet"
)
PARQUET_FILE_PATH_WITH_STATS = (
    "deltacat/tests/compute/resource_requirements/data/sample_with_stats.parquet"
)


@pytest.fixture(scope="module")
def sample_no_stats_entry():
    manifest_meta = ManifestMeta.of(
        content_length=113629,
        record_count=0,
        content_type="application/parquet",
        content_encoding="identity",
        content_type_parameters=[
            PartialParquetParameters.of(
                pq_metadata=papq.ParquetFile(PARQUET_FILE_PATH_NO_STATS).metadata
            )
        ],
    )
    return ManifestEntry.of(
        url=PARQUET_FILE_PATH_NO_STATS,
        uri=PARQUET_FILE_PATH_NO_STATS,
        mandatory=True,
        uuid="test",
        meta=manifest_meta,
    )


@pytest.fixture(scope="module")
def sample_with_no_type_params():
    manifest_meta = ManifestMeta.of(
        content_length=113629,
        record_count=0,
        content_type="application/parquet",
        content_encoding="identity",
        content_type_parameters=[],
    )
    return ManifestEntry.of(
        url=PARQUET_FILE_PATH_NO_STATS,
        uri=PARQUET_FILE_PATH_NO_STATS,
        mandatory=True,
        uuid="test",
        meta=manifest_meta,
    )


@pytest.fixture(scope="module")
def sample_with_stats_entry():
    manifest_meta = ManifestMeta.of(
        content_length=113629,
        record_count=0,
        content_type="application/parquet",
        content_encoding="identity",
        content_type_parameters=[
            PartialParquetParameters.of(
                pq_metadata=papq.ParquetFile(PARQUET_FILE_PATH_WITH_STATS).metadata
            )
        ],
    )
    return ManifestEntry.of(
        url=PARQUET_FILE_PATH_WITH_STATS,
        uri=PARQUET_FILE_PATH_WITH_STATS,
        mandatory=True,
        uuid="test",
        meta=manifest_meta,
    )


class TestEstimateManifestEntryColumnSizeBytes:
    def test_when_no_columns_passed_sanity(
        self, sample_no_stats_entry, sample_with_stats_entry
    ):
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_no_stats_entry, 1, False
                )
            )
            == 0
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_with_stats_entry, 1, False
                )
            )
            == 0
        )

    def test_when_no_columns_passed_with_intelligent_estimation(
        self, sample_no_stats_entry, sample_with_stats_entry
    ):
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_no_stats_entry, 1, True
                )
            )
            == 0
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_with_stats_entry, 1, True
                )
            )
            == 0
        )

    def test_when_one_string_column_passed(
        self, sample_no_stats_entry, sample_with_stats_entry
    ):
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_no_stats_entry, 1, False, ["first_name"]
                )
            )
            == 2988
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_with_stats_entry, 1, False, ["first_name"]
                )
            )
            == 2989
        )

    def test_when_multiple_columns_passed(
        self, sample_no_stats_entry, sample_with_stats_entry
    ):
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_no_stats_entry, 1, False, ["first_name", "id"]
                )
            )
            == 7031
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_with_stats_entry, 1, False, ["first_name", "id"]
                )
            )
            == 8314
        )

    def test_when_timestamp_column_passed(
        self, sample_no_stats_entry, sample_with_stats_entry
    ):
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_no_stats_entry, 2, False, ["registration_dttm"]
                )
            )
            == 26540
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_with_stats_entry, 2, False, ["registration_dttm"]
                )
            )
            == 18602
        )

    def test_when_intelligent_estimation_enabled_single_column(
        self, sample_no_stats_entry, sample_with_stats_entry
    ):
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_no_stats_entry, 1, True, ["first_name"]
                )
            )
            == 2988
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_with_stats_entry, 1, True, ["first_name"]
                )
            )
            == 7000
        )

    def test_when_intelligent_estimation_enabled_timestamp_column(
        self, sample_no_stats_entry, sample_with_stats_entry
    ):
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_no_stats_entry, 1, True, ["registration_dttm"]
                )
            )
            == 12000
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_with_stats_entry, 1, True, ["registration_dttm"]
                )
            )
            == 8000
        )

    def test_when_intelligent_estimation_enabled_int_column(
        self, sample_no_stats_entry, sample_with_stats_entry
    ):
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_no_stats_entry, 1, True, ["id"]
                )
            )
            == 4000
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_with_stats_entry, 1, True, ["id"]
                )
            )
            == 4000
        )

    def test_when_intelligent_estimation_enabled_double_column(
        self, sample_no_stats_entry, sample_with_stats_entry
    ):
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_no_stats_entry, 1, True, ["salary"]
                )
            )
            == 8000
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_with_stats_entry, 1, True, ["salary"]
                )
            )
            == 8000
        )

    def test_when_intelligent_estimation_enabled_multiple_columns(
        self, sample_no_stats_entry, sample_with_stats_entry
    ):
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_no_stats_entry, 1, True, ["first_name", "id"]
                )
            )
            == 6988
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_with_stats_entry, 1, True, ["first_name", "id"]
                )
            )
            == 11000
        )

    def test_when_intelligent_estimation_enabled_with_no_type_params(
        self, sample_with_no_type_params
    ):
        assert (
            estimate_manifest_entry_column_size_bytes(
                sample_with_no_type_params, 1, True, ["first_name"]
            )
            is None
        )


class TestEstimateManifestEntryNumRows:
    def test_sanity(self, sample_no_stats_entry):
        assert (
            estimate_manifest_entry_num_rows(sample_no_stats_entry, 1000, 7, 1, False)
            == 1000
        )

    def test_when_previous_inflation_forced(self, sample_no_stats_entry):
        assert (
            estimate_manifest_entry_num_rows(sample_no_stats_entry, 1000, 7, 1, True)
            == 795
        )

    def test_when_type_params_absent(self, sample_with_no_type_params):
        assert (
            estimate_manifest_entry_num_rows(
                sample_with_no_type_params, 1000, 7, 1, True
            )
            == 795
        )


class TestEstimateManifestEntrySizeBytes:
    def test_sanity(self, sample_no_stats_entry):
        assert (
            int(
                estimate_manifest_entry_size_bytes(
                    sample_no_stats_entry, 7, 2, False, False
                )
            )
            == 224984
        )

    def test_when_previous_inflation_forced(self, sample_no_stats_entry):
        # using previous inflation has turned out to be inaccurate
        assert (
            int(
                estimate_manifest_entry_size_bytes(
                    sample_no_stats_entry, 7, 2, True, False
                )
            )
            == 795403
        )

    def test_when_type_params_absent(self, sample_with_no_type_params):
        assert (
            int(
                estimate_manifest_entry_size_bytes(
                    sample_with_no_type_params, 7, 2, False, True
                )
            )
            == 795403
        )

    def test_when_intelligent_estimation_is_enabled(self, sample_no_stats_entry):
        assert (
            int(
                estimate_manifest_entry_size_bytes(
                    sample_no_stats_entry, 7, 2, False, True
                )
            )
            == 223096
        )

    def test_when_intelligent_estimation_is_enabled_with_stats(
        self, sample_with_stats_entry
    ):
        assert (
            int(
                estimate_manifest_entry_size_bytes(
                    sample_with_stats_entry, 7, 2, False, True
                )
            )
            == 290222
        )
