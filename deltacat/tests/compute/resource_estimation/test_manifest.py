import pytest
from deltacat.storage import ManifestEntry, ManifestMeta
import pyarrow.parquet as papq
from deltacat.types.partial_download import PartialParquetParameters
from deltacat.compute.resource_estimation.manifest import (
    estimate_manifest_entry_column_size_bytes,
    estimate_manifest_entry_num_rows,
    estimate_manifest_entry_size_bytes,
)
from deltacat.compute.resource_estimation.model import (
    OperationType,
    EstimateResourcesParams,
    ResourceEstimationMethod,
)

PARQUET_FILE_PATH_NO_STATS = (
    "deltacat/tests/compute/resource_estimation/data/sample_no_stats.parquet"
)
PARQUET_FILE_PATH_WITH_STATS = (
    "deltacat/tests/compute/resource_estimation/data/sample_with_stats.parquet"
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
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=1
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_no_stats_entry,
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 0
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_with_stats_entry,
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 0
        )

    def test_when_no_columns_passed_with_intelligent_estimation(
        self, sample_no_stats_entry, sample_with_stats_entry
    ):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=1,
            resource_estimation_method=ResourceEstimationMethod.INTELLIGENT_ESTIMATION,
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_no_stats_entry,
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 0
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_with_stats_entry,
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 0
        )

    def test_when_one_string_column_passed(
        self, sample_no_stats_entry, sample_with_stats_entry
    ):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=1
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_no_stats_entry,
                    columns=["first_name"],
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 2988
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_with_stats_entry,
                    columns=["first_name"],
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 2989
        )

    def test_when_invalid_column_passed_assumes_null(self, sample_no_stats_entry):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=1
        )

        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_no_stats_entry,
                    columns=["invalid_column"],
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 4000
        )

    def test_when_multiple_columns_passed(
        self, sample_no_stats_entry, sample_with_stats_entry
    ):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=1
        )

        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_no_stats_entry,
                    columns=["first_name", "id"],
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 7031
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_with_stats_entry,
                    columns=["first_name", "id"],
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 8314
        )

    def test_when_timestamp_column_passed(
        self, sample_no_stats_entry, sample_with_stats_entry
    ):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=2
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_no_stats_entry,
                    columns=["registration_dttm"],
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 26540
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_with_stats_entry,
                    columns=["registration_dttm"],
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 18602
        )

    def test_when_intelligent_estimation_enabled_single_column(
        self, sample_no_stats_entry, sample_with_stats_entry
    ):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=1,
            resource_estimation_method=ResourceEstimationMethod.INTELLIGENT_ESTIMATION,
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_no_stats_entry,
                    columns=["first_name"],
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 2988
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_with_stats_entry,
                    columns=["first_name"],
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 7000
        )

    def test_when_intelligent_estimation_enabled_timestamp_column(
        self, sample_no_stats_entry, sample_with_stats_entry
    ):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=1,
            resource_estimation_method=ResourceEstimationMethod.INTELLIGENT_ESTIMATION,
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_no_stats_entry,
                    columns=["registration_dttm"],
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 12000
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_with_stats_entry,
                    columns=["registration_dttm"],
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 8000
        )

    def test_when_intelligent_estimation_enabled_int_column(
        self, sample_no_stats_entry, sample_with_stats_entry
    ):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=1,
            resource_estimation_method=ResourceEstimationMethod.INTELLIGENT_ESTIMATION,
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_no_stats_entry,
                    columns=["id"],
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                ),
            )
            == 4000
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_with_stats_entry,
                    columns=["id"],
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 4000
        )

    def test_when_intelligent_estimation_enabled_double_column(
        self, sample_no_stats_entry, sample_with_stats_entry
    ):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=1,
            resource_estimation_method=ResourceEstimationMethod.INTELLIGENT_ESTIMATION,
        )

        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_no_stats_entry,
                    columns=["salary"],
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 8000
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_with_stats_entry,
                    columns=["salary"],
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 8000
        )

    def test_when_intelligent_estimation_enabled_multiple_columns(
        self, sample_no_stats_entry, sample_with_stats_entry
    ):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=1,
            resource_estimation_method=ResourceEstimationMethod.INTELLIGENT_ESTIMATION,
        )

        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_no_stats_entry,
                    columns=["first_name", "id"],
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 6988
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_with_stats_entry,
                    columns=["first_name", "id"],
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 11000
        )

    def test_when_default_v2_enabled_multiple_columns(
        self, sample_no_stats_entry, sample_with_stats_entry
    ):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=1,
            resource_estimation_method=ResourceEstimationMethod.DEFAULT_V2,
        )

        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_no_stats_entry,
                    columns=["first_name", "id"],
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 6988
        )
        assert (
            int(
                estimate_manifest_entry_column_size_bytes(
                    sample_with_stats_entry,
                    columns=["first_name", "id"],
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 11000
        )

    def test_when_default_v2_enabled_multiple_columns_and_inflation_not_passed(
        self, sample_no_stats_entry
    ):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=None,
            resource_estimation_method=ResourceEstimationMethod.DEFAULT_V2,
        )

        assert (
            estimate_manifest_entry_column_size_bytes(
                sample_no_stats_entry,
                columns=["first_name", "id"],
                operation_type=OperationType.PYARROW_DOWNLOAD,
                estimate_resources_params=estimate_resources_params,
            )
            is None
        )

    def test_when_intelligent_estimation_enabled_with_no_type_params(
        self, sample_with_no_type_params
    ):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=1,
            resource_estimation_method=ResourceEstimationMethod.INTELLIGENT_ESTIMATION,
        )

        assert (
            estimate_manifest_entry_column_size_bytes(
                sample_with_no_type_params,
                columns=["first_name"],
                operation_type=OperationType.PYARROW_DOWNLOAD,
                estimate_resources_params=estimate_resources_params,
            )
            is None
        )

    def test_when_previous_inflation_method_with_no_type_params(
        self, sample_with_no_type_params
    ):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=1,
            resource_estimation_method=ResourceEstimationMethod.PREVIOUS_INFLATION,
        )

        assert (
            estimate_manifest_entry_column_size_bytes(
                sample_with_no_type_params,
                columns=["first_name"],
                operation_type=OperationType.PYARROW_DOWNLOAD,
                estimate_resources_params=estimate_resources_params,
            )
            is None
        )

    def test_when_default_v2_method_with_no_type_params(
        self, sample_with_no_type_params
    ):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=1,
            resource_estimation_method=ResourceEstimationMethod.DEFAULT_V2,
        )

        assert (
            estimate_manifest_entry_column_size_bytes(
                sample_with_no_type_params,
                columns=["first_name"],
                operation_type=OperationType.PYARROW_DOWNLOAD,
                estimate_resources_params=estimate_resources_params,
            )
            is None
        )


class TestEstimateManifestEntryNumRows:
    def test_sanity(self, sample_no_stats_entry):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=1,
            previous_inflation=7,
            average_record_size_bytes=1000,
            resource_estimation_method=ResourceEstimationMethod.DEFAULT,
        )

        assert (
            estimate_manifest_entry_num_rows(
                sample_no_stats_entry,
                operation_type=OperationType.PYARROW_DOWNLOAD,
                estimate_resources_params=estimate_resources_params,
            )
            == 1000
        )

    def test_when_previous_inflation_forced(self, sample_no_stats_entry):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=1,
            previous_inflation=7,
            average_record_size_bytes=1000,
            resource_estimation_method=ResourceEstimationMethod.PREVIOUS_INFLATION,
        )
        assert (
            estimate_manifest_entry_num_rows(
                sample_no_stats_entry,
                operation_type=OperationType.PYARROW_DOWNLOAD,
                estimate_resources_params=estimate_resources_params,
            )
            == 795
        )

    def test_when_type_params_absent_default_method(self, sample_with_no_type_params):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=1,
            previous_inflation=7,
            average_record_size_bytes=1000,
            resource_estimation_method=ResourceEstimationMethod.DEFAULT,
        )

        assert (
            estimate_manifest_entry_num_rows(
                sample_with_no_type_params,
                operation_type=OperationType.PYARROW_DOWNLOAD,
                estimate_resources_params=estimate_resources_params,
            )
            == 795
        )

    def test_when_type_params_absent_intelligent_estimation(
        self, sample_with_no_type_params
    ):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=1,
            previous_inflation=7,
            average_record_size_bytes=1000,
            resource_estimation_method=ResourceEstimationMethod.INTELLIGENT_ESTIMATION,
        )

        assert (
            estimate_manifest_entry_num_rows(
                sample_with_no_type_params,
                operation_type=OperationType.PYARROW_DOWNLOAD,
                estimate_resources_params=estimate_resources_params,
            )
            is None
        )

    def test_when_type_params_absent_content_type_meta(
        self, sample_with_no_type_params
    ):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=1,
            previous_inflation=7,
            average_record_size_bytes=1000,
            resource_estimation_method=ResourceEstimationMethod.CONTENT_TYPE_META,
        )

        assert (
            estimate_manifest_entry_num_rows(
                sample_with_no_type_params,
                operation_type=OperationType.PYARROW_DOWNLOAD,
                estimate_resources_params=estimate_resources_params,
            )
            is None
        )

    def test_when_type_params_absent_previous_inflation(
        self, sample_with_no_type_params
    ):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=1,
            previous_inflation=7,
            average_record_size_bytes=1000,
            resource_estimation_method=ResourceEstimationMethod.PREVIOUS_INFLATION,
        )

        assert (
            estimate_manifest_entry_num_rows(
                sample_with_no_type_params,
                operation_type=OperationType.PYARROW_DOWNLOAD,
                estimate_resources_params=estimate_resources_params,
            )
            == 795
        )

    def test_when_type_params_absent_default_v2(self, sample_with_no_type_params):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=1,
            previous_inflation=7,
            average_record_size_bytes=1000,
            resource_estimation_method=ResourceEstimationMethod.DEFAULT_V2,
        )

        assert (
            estimate_manifest_entry_num_rows(
                sample_with_no_type_params,
                operation_type=OperationType.PYARROW_DOWNLOAD,
                estimate_resources_params=estimate_resources_params,
            )
            == 795  # same as previous inflation
        )

    def test_when_type_params_no_stats_with_default_v2(self, sample_no_stats_entry):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=1,
            previous_inflation=7,
            average_record_size_bytes=1000,
            resource_estimation_method=ResourceEstimationMethod.DEFAULT_V2,
        )
        assert (
            estimate_manifest_entry_num_rows(
                sample_no_stats_entry,
                operation_type=OperationType.PYARROW_DOWNLOAD,
                estimate_resources_params=estimate_resources_params,
            )
            == 1000
        )

    def test_when_type_params_parquet_inflation_absent_with_default_v2(
        self, sample_no_stats_entry
    ):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=None,
            previous_inflation=7,
            average_record_size_bytes=1000,
            resource_estimation_method=ResourceEstimationMethod.DEFAULT_V2,
        )
        assert (
            estimate_manifest_entry_num_rows(
                sample_no_stats_entry,
                operation_type=OperationType.PYARROW_DOWNLOAD,
                estimate_resources_params=estimate_resources_params,
            )
            == 1000
        )

    def test_when_type_params_with_default_v2(self, sample_with_stats_entry):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=1,
            previous_inflation=7,
            average_record_size_bytes=1000,
            resource_estimation_method=ResourceEstimationMethod.DEFAULT_V2,
        )
        assert (
            estimate_manifest_entry_num_rows(
                sample_with_stats_entry,
                operation_type=OperationType.PYARROW_DOWNLOAD,
                estimate_resources_params=estimate_resources_params,
            )
            == 1000
        )


class TestEstimateManifestEntrySizeBytes:
    def test_sanity(self, sample_no_stats_entry):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=2,
            previous_inflation=7,
            resource_estimation_method=ResourceEstimationMethod.DEFAULT,
        )

        assert (
            int(
                estimate_manifest_entry_size_bytes(
                    sample_no_stats_entry,
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 224984
        )

    def test_when_previous_inflation_forced(self, sample_no_stats_entry):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=2,
            previous_inflation=7,
            resource_estimation_method=ResourceEstimationMethod.PREVIOUS_INFLATION,
        )
        assert (
            int(
                estimate_manifest_entry_size_bytes(
                    sample_no_stats_entry,
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 795403
        )

    def test_when_type_params_absent_default(self, sample_with_no_type_params):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=2,
            previous_inflation=7,
            resource_estimation_method=ResourceEstimationMethod.DEFAULT,
        )
        assert (
            int(
                estimate_manifest_entry_size_bytes(
                    sample_with_no_type_params,
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 795403
        )

    def test_when_type_params_absent_intelligent_estimation(
        self, sample_with_no_type_params
    ):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=2,
            previous_inflation=7,
            resource_estimation_method=ResourceEstimationMethod.INTELLIGENT_ESTIMATION,
        )
        assert (
            estimate_manifest_entry_size_bytes(
                sample_with_no_type_params,
                operation_type=OperationType.PYARROW_DOWNLOAD,
                estimate_resources_params=estimate_resources_params,
            )
            is None
        )

    def test_when_type_params_absent_content_meta(self, sample_with_no_type_params):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=2,
            previous_inflation=7,
            resource_estimation_method=ResourceEstimationMethod.CONTENT_TYPE_META,
        )
        assert (
            estimate_manifest_entry_size_bytes(
                sample_with_no_type_params,
                operation_type=OperationType.PYARROW_DOWNLOAD,
                estimate_resources_params=estimate_resources_params,
            )
            is None
        )

    def test_when_type_params_absent_previous_inflation(
        self, sample_with_no_type_params
    ):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=2,
            previous_inflation=7,
            resource_estimation_method=ResourceEstimationMethod.PREVIOUS_INFLATION,
        )
        assert (
            estimate_manifest_entry_size_bytes(
                sample_with_no_type_params,
                operation_type=OperationType.PYARROW_DOWNLOAD,
                estimate_resources_params=estimate_resources_params,
            )
            == 795403
        )

    def test_when_intelligent_estimation_sanity(self, sample_no_stats_entry):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=2,
            previous_inflation=7,
            resource_estimation_method=ResourceEstimationMethod.INTELLIGENT_ESTIMATION,
        )
        assert (
            int(
                estimate_manifest_entry_size_bytes(
                    sample_no_stats_entry,
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 223096
        )

    def test_when_type_params_with_stats_default_method(self, sample_with_stats_entry):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=2,
            previous_inflation=7,
            resource_estimation_method=ResourceEstimationMethod.DEFAULT,
        )
        assert (
            int(
                estimate_manifest_entry_size_bytes(
                    sample_with_stats_entry,
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 227794
        )

    def test_when_type_params_with_stats_intelligent_method(
        self, sample_with_stats_entry
    ):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=2,
            previous_inflation=7,
            resource_estimation_method=ResourceEstimationMethod.INTELLIGENT_ESTIMATION,
        )
        assert (
            int(
                estimate_manifest_entry_size_bytes(
                    sample_with_stats_entry,
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 290222
        )

    def test_when_type_params_with_content_type_meta_method(
        self, sample_with_stats_entry
    ):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=2,
            previous_inflation=7,
            resource_estimation_method=ResourceEstimationMethod.CONTENT_TYPE_META,
        )
        assert (
            int(
                estimate_manifest_entry_size_bytes(
                    sample_with_stats_entry,
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 227794
        )

    def test_when_type_params_with_stats_default_v2_method(
        self, sample_with_stats_entry
    ):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=2,
            previous_inflation=7,
            resource_estimation_method=ResourceEstimationMethod.DEFAULT_V2,
        )
        assert (
            int(
                estimate_manifest_entry_size_bytes(
                    sample_with_stats_entry,
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 290222  # same result as intelligent estimation
        )

    def test_when_type_params_without_stats_default_v2_method(
        self, sample_no_stats_entry
    ):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=2,
            previous_inflation=7,
            resource_estimation_method=ResourceEstimationMethod.DEFAULT_V2,
        )
        assert (
            int(
                estimate_manifest_entry_size_bytes(
                    sample_no_stats_entry,
                    operation_type=OperationType.PYARROW_DOWNLOAD,
                    estimate_resources_params=estimate_resources_params,
                )
            )
            == 223096
        )

    def test_when_no_type_params_default_v2_method(self, sample_with_no_type_params):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=2,
            previous_inflation=7,
            resource_estimation_method=ResourceEstimationMethod.DEFAULT_V2,
        )
        assert (
            estimate_manifest_entry_size_bytes(
                sample_with_no_type_params,
                operation_type=OperationType.PYARROW_DOWNLOAD,
                estimate_resources_params=estimate_resources_params,
            )
            == 795403  # same as previous inflation
        )

    def test_when_type_params_but_inflation_absent_default_v2_method(
        self, sample_with_stats_entry
    ):
        estimate_resources_params = EstimateResourcesParams.of(
            parquet_to_pyarrow_inflation=None,
            previous_inflation=7,
            resource_estimation_method=ResourceEstimationMethod.DEFAULT_V2,
        )
        assert (
            estimate_manifest_entry_size_bytes(
                sample_with_stats_entry,
                operation_type=OperationType.PYARROW_DOWNLOAD,
                estimate_resources_params=estimate_resources_params,
            )
            == 795403  # same as previous inflation
        )
