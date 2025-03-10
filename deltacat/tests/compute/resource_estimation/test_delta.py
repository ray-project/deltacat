import deltacat.tests.local_deltacat_storage as ds
from deltacat.types.media import ContentType
import os
import pytest
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
def local_deltacat_storage_kwargs():
    DATABASE_FILE_PATH_KEY, DATABASE_FILE_PATH_VALUE = (
        "db_file_path",
        "deltacat/tests/local_deltacat_storage/db_test.sqlite",
    )
    # see deltacat/tests/local_deltacat_storage/README.md for documentation
    kwargs_for_local_deltacat_storage = {
        DATABASE_FILE_PATH_KEY: DATABASE_FILE_PATH_VALUE,
    }
    yield kwargs_for_local_deltacat_storage
    if os.path.exists(DATABASE_FILE_PATH_VALUE):
        os.remove(DATABASE_FILE_PATH_VALUE)


@pytest.fixture(scope="function")
def parquet_delta_with_manifest(local_deltacat_storage_kwargs):
    """
    These fixtures are function scoped as functions can modify the delta.
    """
    from deltacat.tests.test_utils.pyarrow import create_delta_from_csv_file

    result = create_delta_from_csv_file(
        "test_namespace",
        file_paths=[DELTA_CSV_FILE_PATH],
        content_type=ContentType.PARQUET,
        **local_deltacat_storage_kwargs
    )

    result.meta["source_content_length"] = 0
    result.meta["record_count"] = 0
    for entry in result.manifest.entries:
        entry.meta["source_content_length"] = 0
        entry.meta["record_count"] = 0

    return result


@pytest.fixture(scope="function")
def utsv_delta_with_manifest(local_deltacat_storage_kwargs):
    from deltacat.tests.test_utils.pyarrow import create_delta_from_csv_file

    result = create_delta_from_csv_file(
        "test_namespace",
        file_paths=[DELTA_CSV_FILE_PATH],
        content_type=ContentType.UNESCAPED_TSV,
        **local_deltacat_storage_kwargs
    )

    result.meta["source_content_length"] = 0
    result.meta["record_count"] = 0
    for entry in result.manifest.entries:
        entry.meta["source_content_length"] = 0
        entry.meta["record_count"] = 0

    return result


@pytest.fixture(scope="function")
def delta_without_manifest(local_deltacat_storage_kwargs):
    from deltacat.tests.test_utils.pyarrow import create_delta_from_csv_file

    delta = create_delta_from_csv_file(
        "test_namespace",
        file_paths=[DELTA_CSV_FILE_PATH],
        content_type=ContentType.PARQUET,
        **local_deltacat_storage_kwargs
    )

    # now we intentionally remove manifest
    delta.manifest = None
    delta.meta["source_content_length"] = 0
    delta.meta["record_count"] = 0

    return delta


@pytest.fixture(scope="function")
def delta_with_populated_meta(local_deltacat_storage_kwargs):
    from deltacat.tests.test_utils.pyarrow import create_delta_from_csv_file

    delta = create_delta_from_csv_file(
        "test_namespace",
        file_paths=[DELTA_CSV_FILE_PATH],
        content_type=ContentType.PARQUET,
        **local_deltacat_storage_kwargs
    )

    return delta


class TestEstimateResourcesRequiredToProcessDelta:
    def test_delta_with_prepopulated_meta_returns_directly(
        self, local_deltacat_storage_kwargs, delta_with_populated_meta: Delta
    ):

        result = estimate_resources_required_to_process_delta(
            delta=delta_with_populated_meta,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=ds,
            deltacat_storage_kwargs=local_deltacat_storage_kwargs,
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
        self, local_deltacat_storage_kwargs, delta_without_manifest: Delta
    ):
        params = EstimateResourcesParams.of(
            resource_estimation_method=ResourceEstimationMethod.DEFAULT,
            previous_inflation=7,
            average_record_size_bytes=1000,
        )

        result = estimate_resources_required_to_process_delta(
            delta=delta_without_manifest,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=ds,
            deltacat_storage_kwargs=local_deltacat_storage_kwargs,
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
        self, local_deltacat_storage_kwargs, parquet_delta_with_manifest: Delta
    ):
        params = EstimateResourcesParams.of(
            resource_estimation_method=ResourceEstimationMethod.DEFAULT,
            previous_inflation=7,
            average_record_size_bytes=1000,
        )

        result = estimate_resources_required_to_process_delta(
            delta=parquet_delta_with_manifest,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=ds,
            deltacat_storage_kwargs=local_deltacat_storage_kwargs,
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

    def test_previous_inflation_arg_not_passed_when_default_method(
        self, local_deltacat_storage_kwargs, parquet_delta_with_manifest: Delta
    ):
        with pytest.raises(AssertionError):
            params = EstimateResourcesParams.of(
                resource_estimation_method=ResourceEstimationMethod.DEFAULT,
                average_record_size_bytes=1000,
            )

            estimate_resources_required_to_process_delta(
                delta=parquet_delta_with_manifest,
                operation_type=OperationType.PYARROW_DOWNLOAD,
                deltacat_storage=ds,
                deltacat_storage_kwargs=local_deltacat_storage_kwargs,
                estimate_resources_params=params,
            )

    def test_estimate_resources_params_not_passed_assumes_default(
        self, local_deltacat_storage_kwargs, parquet_delta_with_manifest: Delta
    ):
        params = EstimateResourcesParams.of(
            previous_inflation=7,
            average_record_size_bytes=1000,
        )

        result = estimate_resources_required_to_process_delta(
            delta=parquet_delta_with_manifest,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=ds,
            deltacat_storage_kwargs=local_deltacat_storage_kwargs,
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

    def test_delta_manifest_empty_when_content_type_meta(
        self, local_deltacat_storage_kwargs, delta_without_manifest: Delta
    ):
        params = EstimateResourcesParams.of(
            resource_estimation_method=ResourceEstimationMethod.CONTENT_TYPE_META,
            parquet_to_pyarrow_inflation=2,
        )

        result = estimate_resources_required_to_process_delta(
            delta=delta_without_manifest,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=ds,
            deltacat_storage_kwargs=local_deltacat_storage_kwargs,
            estimate_resources_params=params,
        )

        assert delta_without_manifest.manifest is not None
        assert int(result.memory_bytes) == 84
        assert int(result.statistics.in_memory_size_bytes) == 84
        assert (
            result.statistics.on_disk_size_bytes
            == delta_without_manifest.meta.content_length
        )
        assert result.statistics.record_count == 7

    def test_delta_manifest_exists_when_content_type_meta(
        self, local_deltacat_storage_kwargs, parquet_delta_with_manifest: Delta
    ):
        params = EstimateResourcesParams.of(
            resource_estimation_method=ResourceEstimationMethod.CONTENT_TYPE_META,
            parquet_to_pyarrow_inflation=2,
        )

        result = estimate_resources_required_to_process_delta(
            delta=parquet_delta_with_manifest,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=ds,
            deltacat_storage_kwargs=local_deltacat_storage_kwargs,
            estimate_resources_params=params,
        )

        assert parquet_delta_with_manifest.manifest is not None
        assert int(result.memory_bytes) == 464
        assert int(result.statistics.in_memory_size_bytes) == int(result.memory_bytes)
        assert (
            result.statistics.on_disk_size_bytes
            == parquet_delta_with_manifest.meta.content_length
        )
        assert result.statistics.record_count == 7

    def test_delta_manifest_empty_when_intelligent_estimation(
        self, local_deltacat_storage_kwargs, delta_without_manifest: Delta
    ):
        params = EstimateResourcesParams.of(
            resource_estimation_method=ResourceEstimationMethod.INTELLIGENT_ESTIMATION,
            parquet_to_pyarrow_inflation=2,
        )

        result = estimate_resources_required_to_process_delta(
            delta=delta_without_manifest,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=ds,
            deltacat_storage_kwargs=local_deltacat_storage_kwargs,
            estimate_resources_params=params,
        )

        assert delta_without_manifest.manifest is not None
        assert int(result.memory_bytes) == 84
        assert int(result.statistics.in_memory_size_bytes) == 84
        assert (
            result.statistics.on_disk_size_bytes
            == delta_without_manifest.meta.content_length
        )
        assert result.statistics.record_count == 7

    def test_delta_manifest_exists_when_intelligent_estimation(
        self, local_deltacat_storage_kwargs, parquet_delta_with_manifest: Delta
    ):
        params = EstimateResourcesParams.of(
            resource_estimation_method=ResourceEstimationMethod.INTELLIGENT_ESTIMATION,
            parquet_to_pyarrow_inflation=2,
        )

        result = estimate_resources_required_to_process_delta(
            delta=parquet_delta_with_manifest,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=ds,
            deltacat_storage_kwargs=local_deltacat_storage_kwargs,
            estimate_resources_params=params,
        )

        assert parquet_delta_with_manifest.manifest is not None
        assert int(result.memory_bytes) == 168
        assert int(result.statistics.in_memory_size_bytes) == int(result.memory_bytes)
        assert (
            result.statistics.on_disk_size_bytes
            == parquet_delta_with_manifest.meta.content_length
        )
        assert result.statistics.record_count == 7

    def test_delta_manifest_exists_inflation_absent_when_intelligent_estimation(
        self, local_deltacat_storage_kwargs, parquet_delta_with_manifest: Delta
    ):
        params = EstimateResourcesParams.of(
            resource_estimation_method=ResourceEstimationMethod.INTELLIGENT_ESTIMATION,
            parquet_to_pyarrow_inflation=None,
        )

        result = estimate_resources_required_to_process_delta(
            delta=parquet_delta_with_manifest,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=ds,
            deltacat_storage_kwargs=local_deltacat_storage_kwargs,
            estimate_resources_params=params,
        )

        assert result is None

    def test_delta_utsv_data_when_intelligent_estimation(
        self, local_deltacat_storage_kwargs, utsv_delta_with_manifest: Delta
    ):
        params = EstimateResourcesParams.of(
            resource_estimation_method=ResourceEstimationMethod.INTELLIGENT_ESTIMATION,
            parquet_to_pyarrow_inflation=2,
        )

        result = estimate_resources_required_to_process_delta(
            delta=utsv_delta_with_manifest,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=ds,
            deltacat_storage_kwargs=local_deltacat_storage_kwargs,
            estimate_resources_params=params,
        )

        assert result is None

    def test_empty_delta_sampled_when_file_sampling(
        self, local_deltacat_storage_kwargs, delta_without_manifest: Delta
    ):
        params = EstimateResourcesParams.of(
            resource_estimation_method=ResourceEstimationMethod.FILE_SAMPLING,
            max_files_to_sample=2,
        )

        result = estimate_resources_required_to_process_delta(
            delta=delta_without_manifest,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=ds,
            deltacat_storage_kwargs=local_deltacat_storage_kwargs,
            estimate_resources_params=params,
        )

        assert delta_without_manifest.manifest is not None
        assert result.memory_bytes is not None
        assert (
            result.statistics.on_disk_size_bytes
            == delta_without_manifest.meta.content_length
        )

    def test_delta_manifest_parquet_when_file_sampling(
        self, local_deltacat_storage_kwargs, parquet_delta_with_manifest: Delta
    ):
        params = EstimateResourcesParams.of(
            resource_estimation_method=ResourceEstimationMethod.FILE_SAMPLING,
            max_files_to_sample=2,
        )

        result = estimate_resources_required_to_process_delta(
            delta=parquet_delta_with_manifest,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=ds,
            deltacat_storage_kwargs=local_deltacat_storage_kwargs,
            estimate_resources_params=params,
        )
        assert result.memory_bytes is not None
        assert (
            result.statistics.on_disk_size_bytes
            == parquet_delta_with_manifest.meta.content_length
        )

    def test_parquet_delta_when_file_sampling_and_arrow_size_zero(
        self,
        local_deltacat_storage_kwargs,
        parquet_delta_with_manifest: Delta,
        monkeypatch,
    ):
        params = EstimateResourcesParams.of(
            resource_estimation_method=ResourceEstimationMethod.FILE_SAMPLING,
            max_files_to_sample=2,
        )

        def mock_func(*args, **kwargs):
            class MockedValue:
                nbytes = 0

                def __len__(self):
                    return 0

            return MockedValue()

        monkeypatch.setattr(ds, "download_delta_manifest_entry", mock_func)

        result = estimate_resources_required_to_process_delta(
            delta=parquet_delta_with_manifest,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=ds,
            deltacat_storage_kwargs=local_deltacat_storage_kwargs,
            estimate_resources_params=params,
        )

        assert parquet_delta_with_manifest.manifest is not None
        assert result.memory_bytes == 0
        assert (
            result.statistics.on_disk_size_bytes
            == parquet_delta_with_manifest.meta.content_length
        )

    def test_delta_manifest_utsv_when_file_sampling(
        self, local_deltacat_storage_kwargs, utsv_delta_with_manifest: Delta
    ):
        params = EstimateResourcesParams.of(
            resource_estimation_method=ResourceEstimationMethod.FILE_SAMPLING,
            max_files_to_sample=2,
        )

        result = estimate_resources_required_to_process_delta(
            delta=utsv_delta_with_manifest,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=ds,
            deltacat_storage_kwargs=local_deltacat_storage_kwargs,
            estimate_resources_params=params,
        )
        assert result.memory_bytes is not None
        assert (
            result.statistics.on_disk_size_bytes
            == utsv_delta_with_manifest.meta.content_length
        )

    def test_delta_manifest_utsv_when_file_sampling_zero_files_to_sample(
        self, local_deltacat_storage_kwargs, utsv_delta_with_manifest: Delta
    ):
        params = EstimateResourcesParams.of(
            resource_estimation_method=ResourceEstimationMethod.FILE_SAMPLING,
            max_files_to_sample=None,
        )

        result = estimate_resources_required_to_process_delta(
            delta=utsv_delta_with_manifest,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=ds,
            deltacat_storage_kwargs=local_deltacat_storage_kwargs,
            estimate_resources_params=params,
        )
        assert result is None

    def test_empty_delta_when_default_v2(
        self, local_deltacat_storage_kwargs, delta_without_manifest: Delta
    ):
        params = EstimateResourcesParams.of(
            resource_estimation_method=ResourceEstimationMethod.DEFAULT_V2,
            max_files_to_sample=2,
            previous_inflation=7,
            average_record_size_bytes=1000,
        )

        result = estimate_resources_required_to_process_delta(
            delta=delta_without_manifest,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=ds,
            deltacat_storage_kwargs=local_deltacat_storage_kwargs,
            estimate_resources_params=params,
        )

        assert delta_without_manifest.manifest is not None
        assert result.memory_bytes is not None
        assert (
            result.statistics.on_disk_size_bytes
            == delta_without_manifest.meta.content_length
        )

    def test_parquet_delta_when_default_v2(
        self, local_deltacat_storage_kwargs, parquet_delta_with_manifest: Delta
    ):
        params = EstimateResourcesParams.of(
            resource_estimation_method=ResourceEstimationMethod.DEFAULT_V2,
            max_files_to_sample=2,
            previous_inflation=7,
            average_record_size_bytes=1000,
            parquet_to_pyarrow_inflation=1,
        )

        result = estimate_resources_required_to_process_delta(
            delta=parquet_delta_with_manifest,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=ds,
            deltacat_storage_kwargs=local_deltacat_storage_kwargs,
            estimate_resources_params=params,
        )

        assert parquet_delta_with_manifest.manifest is not None
        assert result.memory_bytes is not None
        assert (
            result.statistics.on_disk_size_bytes
            == parquet_delta_with_manifest.meta.content_length
        )

    def test_parquet_delta_when_default_v2_without_avg_record_size_and_sampling(
        self, local_deltacat_storage_kwargs, parquet_delta_with_manifest: Delta
    ):
        params = EstimateResourcesParams.of(
            resource_estimation_method=ResourceEstimationMethod.DEFAULT_V2,
            previous_inflation=7,
            parquet_to_pyarrow_inflation=1,
        )

        result = estimate_resources_required_to_process_delta(
            delta=parquet_delta_with_manifest,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=ds,
            deltacat_storage_kwargs=local_deltacat_storage_kwargs,
            estimate_resources_params=params,
        )

        assert parquet_delta_with_manifest.manifest is not None
        assert result.memory_bytes is not None
        assert (
            result.statistics.on_disk_size_bytes
            == parquet_delta_with_manifest.meta.content_length
        )

    def test_parquet_delta_when_default_v2_and_files_to_sample_zero(
        self, local_deltacat_storage_kwargs, parquet_delta_with_manifest: Delta
    ):
        params = EstimateResourcesParams.of(
            resource_estimation_method=ResourceEstimationMethod.DEFAULT_V2,
            max_files_to_sample=0,
            previous_inflation=7,
            average_record_size_bytes=1000,
            parquet_to_pyarrow_inflation=1,
        )

        result = estimate_resources_required_to_process_delta(
            delta=parquet_delta_with_manifest,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=ds,
            deltacat_storage_kwargs=local_deltacat_storage_kwargs,
            estimate_resources_params=params,
        )

        assert parquet_delta_with_manifest.manifest is not None
        assert result.memory_bytes is not None
        assert (
            result.statistics.on_disk_size_bytes
            == parquet_delta_with_manifest.meta.content_length
        )

    def test_utsv_delta_when_default_v2(
        self, local_deltacat_storage_kwargs, utsv_delta_with_manifest: Delta
    ):
        params = EstimateResourcesParams.of(
            resource_estimation_method=ResourceEstimationMethod.DEFAULT_V2,
            max_files_to_sample=2,
            previous_inflation=7,
            average_record_size_bytes=1000,
            parquet_to_pyarrow_inflation=1,
        )

        result = estimate_resources_required_to_process_delta(
            delta=utsv_delta_with_manifest,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=ds,
            deltacat_storage_kwargs=local_deltacat_storage_kwargs,
            estimate_resources_params=params,
        )

        assert utsv_delta_with_manifest.manifest is not None
        assert result.memory_bytes is not None
        assert (
            result.statistics.on_disk_size_bytes
            == utsv_delta_with_manifest.meta.content_length
        )

    def test_utsv_delta_when_default_v2_without_avg_record_size(
        self, local_deltacat_storage_kwargs, utsv_delta_with_manifest: Delta
    ):
        params = EstimateResourcesParams.of(
            resource_estimation_method=ResourceEstimationMethod.DEFAULT_V2,
            previous_inflation=7,
            average_record_size_bytes=None,  # note
            parquet_to_pyarrow_inflation=1,
        )

        result = estimate_resources_required_to_process_delta(
            delta=utsv_delta_with_manifest,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=ds,
            deltacat_storage_kwargs=local_deltacat_storage_kwargs,
            estimate_resources_params=params,
        )

        assert utsv_delta_with_manifest.manifest is not None
        assert result.memory_bytes is not None
        assert result.statistics.record_count == 0
        assert (
            result.statistics.on_disk_size_bytes
            == utsv_delta_with_manifest.meta.content_length
        )

    def test_parquet_delta_without_inflation_when_default_v2(
        self, local_deltacat_storage_kwargs, parquet_delta_with_manifest: Delta
    ):
        params = EstimateResourcesParams.of(
            resource_estimation_method=ResourceEstimationMethod.DEFAULT_V2,
            max_files_to_sample=2,
            previous_inflation=7,
            average_record_size_bytes=1000,
            parquet_to_pyarrow_inflation=None,  # inflation is None
        )

        result = estimate_resources_required_to_process_delta(
            delta=parquet_delta_with_manifest,
            operation_type=OperationType.PYARROW_DOWNLOAD,
            deltacat_storage=ds,
            deltacat_storage_kwargs=local_deltacat_storage_kwargs,
            estimate_resources_params=params,
        )

        assert parquet_delta_with_manifest.manifest is not None
        assert result.memory_bytes is not None
        assert (
            result.statistics.on_disk_size_bytes
            == parquet_delta_with_manifest.meta.content_length
        )
