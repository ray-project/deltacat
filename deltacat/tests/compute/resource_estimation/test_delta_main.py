from deltacat.storage import metastore
from deltacat.types.media import ContentType
import pytest
import tempfile
from deltacat.storage import Delta
from deltacat.compute.resource_estimation.delta import (
    estimate_resources_required_to_process_delta,
)
from deltacat.compute.resource_estimation.model import (
    OperationType,
    EstimateResourcesParams,
    ResourceEstimationMethod,
)

DELTA_CSV_FILE_PATH = (
    "deltacat/tests/compute/resource_estimation/data/date_pk_table.csv"
)

"""
Function scoped fixtures
"""


@pytest.fixture(scope="function")
def main_deltacat_storage_kwargs():
    # Create a temporary directory for main storage
    temp_dir = tempfile.mkdtemp()
    from deltacat.catalog import CatalogProperties
    catalog_properties = CatalogProperties(root=temp_dir)
    storage_kwargs = {"catalog": catalog_properties}
    yield storage_kwargs
    # Clean up temporary directory
    import shutil
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture(scope="function")
def parquet_delta_with_manifest(main_deltacat_storage_kwargs):
    """
    These fixtures are function scoped as functions can modify the delta.
    """
    from deltacat.tests.test_utils.pyarrow_main import create_delta_from_csv_file

    result = create_delta_from_csv_file(
        "test_namespace_main",
        file_paths=[DELTA_CSV_FILE_PATH],
        content_type=ContentType.PARQUET,
        **main_deltacat_storage_kwargs
    )

    result.meta["source_content_length"] = 0
    result.meta["record_count"] = 0
    for entry in result.manifest.entries:
        entry.meta["source_content_length"] = 0
        entry.meta["record_count"] = 0

    return result


@pytest.fixture(scope="function")
def utsv_delta_with_manifest(main_deltacat_storage_kwargs):
    from deltacat.tests.test_utils.pyarrow_main import create_delta_from_csv_file

    result = create_delta_from_csv_file(
        "test_namespace_main",
        file_paths=[DELTA_CSV_FILE_PATH],
        content_type=ContentType.UNESCAPED_TSV,
        **main_deltacat_storage_kwargs
    )

    result.meta["source_content_length"] = 0
    result.meta["record_count"] = 0
    for entry in result.manifest.entries:
        entry.meta["source_content_length"] = 0
        entry.meta["record_count"] = 0

    return result


@pytest.fixture(scope="function")
def delta_without_manifest(main_deltacat_storage_kwargs):
    from deltacat.tests.test_utils.pyarrow_main import create_delta_from_csv_file

    delta = create_delta_from_csv_file(
        "test_namespace_main",
        file_paths=[DELTA_CSV_FILE_PATH],
        content_type=ContentType.PARQUET,
        **main_deltacat_storage_kwargs
    )

    # now we intentionally remove manifest
    delta.manifest = None
    delta.meta["source_content_length"] = 0
    delta.meta["record_count"] = 0

    return delta


@pytest.fixture(scope="function")
def delta_with_populated_meta(main_deltacat_storage_kwargs):
    from deltacat.tests.test_utils.pyarrow_main import create_delta_from_csv_file

    delta = create_delta_from_csv_file(
        "test_namespace_main",
        file_paths=[DELTA_CSV_FILE_PATH],
        content_type=ContentType.PARQUET,
        **main_deltacat_storage_kwargs
    )

    return delta


class TestEstimateResourcesRequiredToProcessDeltaMain:
    def test_delta_with_prepopulated_meta_returns_directly(
        self, main_deltacat_storage_kwargs, delta_with_populated_meta: Delta
    ):

        result = estimate_resources_required_to_process_delta(
            delta=delta_with_populated_meta,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=metastore,
            deltacat_storage_kwargs=main_deltacat_storage_kwargs,
        )

        assert (
            result.memory_bytes == delta_with_populated_meta.meta.source_content_length
        )
        assert (
            result.statistics.in_memory_size_bytes
            == delta_with_populated_meta.meta.source_content_length
        )
        assert (
            result.statistics.on_disk_size_bytes
            == delta_with_populated_meta.meta.content_length
        )
        assert (
            result.statistics.record_count
            == delta_with_populated_meta.meta.record_count
        )

    def test_delta_manifest_empty_when_default_method(
        self, main_deltacat_storage_kwargs, delta_without_manifest: Delta
    ):
        params = EstimateResourcesParams.of(
            resource_estimation_method=ResourceEstimationMethod.DEFAULT,
            previous_inflation=7,
            average_record_size_bytes=1000,
        )

        result = estimate_resources_required_to_process_delta(
            delta=delta_without_manifest,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=metastore,
            deltacat_storage_kwargs=main_deltacat_storage_kwargs,
            estimate_resources_params=params,
        )

        assert result.memory_bytes != delta_without_manifest.meta.source_content_length
        assert (
            result.memory_bytes
            == delta_without_manifest.meta.content_length * params.previous_inflation
        )
        assert result.statistics.in_memory_size_bytes == result.memory_bytes
        assert (
            result.statistics.on_disk_size_bytes
            == delta_without_manifest.meta.content_length
        )
        assert result.statistics.record_count == int(
            result.memory_bytes / params.average_record_size_bytes
        )

    def test_delta_manifest_exists_when_default_method(
        self, main_deltacat_storage_kwargs, parquet_delta_with_manifest: Delta
    ):
        params = EstimateResourcesParams.of(
            resource_estimation_method=ResourceEstimationMethod.DEFAULT,
            previous_inflation=7,
            average_record_size_bytes=1000,
        )

        result = estimate_resources_required_to_process_delta(
            delta=parquet_delta_with_manifest,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=metastore,
            deltacat_storage_kwargs=main_deltacat_storage_kwargs,
            estimate_resources_params=params,
        )

        assert (
            result.memory_bytes
            != parquet_delta_with_manifest.meta.source_content_length
        )
        assert (
            result.memory_bytes
            == parquet_delta_with_manifest.meta.content_length
            * params.previous_inflation
        )
        assert result.statistics.in_memory_size_bytes == result.memory_bytes
        assert (
            result.statistics.on_disk_size_bytes
            == parquet_delta_with_manifest.meta.content_length
        )
        assert result.statistics.record_count == int(
            result.memory_bytes / params.average_record_size_bytes
        )

    def test_estimate_resources_params_not_passed_assumes_default(
        self, main_deltacat_storage_kwargs, parquet_delta_with_manifest: Delta
    ):
        result = estimate_resources_required_to_process_delta(
            delta=parquet_delta_with_manifest,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=metastore,
            deltacat_storage_kwargs=main_deltacat_storage_kwargs,
        )

        assert (
            result.memory_bytes
            != parquet_delta_with_manifest.meta.source_content_length
        )
        assert (
            result.memory_bytes
            == parquet_delta_with_manifest.meta.content_length
            * EstimateResourcesParams.DEFAULT_PREVIOUS_INFLATION
        )
        assert result.statistics.in_memory_size_bytes == result.memory_bytes
        assert (
            result.statistics.on_disk_size_bytes
            == parquet_delta_with_manifest.meta.content_length
        )
        assert result.statistics.record_count == int(
            result.memory_bytes
            / EstimateResourcesParams.DEFAULT_AVERAGE_RECORD_SIZE_BYTES
        )

    def test_delta_manifest_exists_when_intelligent_estimation(
        self, main_deltacat_storage_kwargs, parquet_delta_with_manifest: Delta
    ):
        params = EstimateResourcesParams.of(
            resource_estimation_method=ResourceEstimationMethod.INTELLIGENT_ESTIMATION,
            parquet_to_pyarrow_inflation=2,
        )

        result = estimate_resources_required_to_process_delta(
            delta=parquet_delta_with_manifest,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=metastore,
            deltacat_storage_kwargs=main_deltacat_storage_kwargs,
            estimate_resources_params=params,
        )

        assert parquet_delta_with_manifest.manifest is not None
        assert result is not None
        assert result.memory_bytes is not None
        assert result.statistics.in_memory_size_bytes == result.memory_bytes
        assert (
            result.statistics.on_disk_size_bytes
            == parquet_delta_with_manifest.meta.content_length
        )
        assert result.statistics.record_count >= 0 