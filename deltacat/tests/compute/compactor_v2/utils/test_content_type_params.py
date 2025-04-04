import ray
from deltacat.compute.compactor_v2.constants import (
    TASK_MAX_PARALLELISM,
    MAX_PARQUET_METADATA_SIZE,
)
from deltacat.utils.common import ReadKwargsProvider
from deltacat.utils.ray_utils.concurrency import invoke_parallel
from deltacat import logs
from deltacat.storage import (
    Delta,
    ManifestEntry,
    interface as unimplemented_deltacat_storage,
)
from typing import Dict, Any
from deltacat.types.media import TableType
from deltacat.types.media import ContentType
from deltacat.types.partial_download import PartialParquetParameters
from deltacat.exceptions import RetryableError

import pytest
import deltacat.tests.local_deltacat_storage as ds
import os

DATABASE_FILE_PATH_KEY, DATABASE_FILE_PATH_VALUE = (
    "db_file_path",
    "deltacat/tests/local_deltacat_storage/db_test.sqlite",
)


class TestContentTypeParams:
    @pytest.fixture(scope="module", autouse=True)
    def setup_ray_cluster(self):
        ray.init(local_mode=True, ignore_reinit_error=True)
        yield
        ray.shutdown()

    @pytest.fixture(scope="function")
    def local_deltacat_storage_kwargs(self, request: pytest.FixtureRequest):
        # see deltacat/tests/local_deltacat_storage/README.md for documentation
        kwargs_for_local_deltacat_storage: Dict[str, Any] = {
            DATABASE_FILE_PATH_KEY: DATABASE_FILE_PATH_VALUE,
        }
        yield kwargs_for_local_deltacat_storage
        if os.path.exists(DATABASE_FILE_PATH_VALUE):
            os.remove(DATABASE_FILE_PATH_VALUE)

    def test_foo(self):
        assert True
