import pytest

from deltacat.storage import (
    DeltaType,
    Delta,
)
from deltacat.storage import (
    Partition,
    PartitionLocator,
    Stream,
)
from deltacat.tests.compute.test_util_constant import (
    TEST_S3_RCF_BUCKET_NAME,
)
from deltacat.tests.compute.test_util_common import (
    create_src_table,
    create_destination_table,
)

from dataclasses import dataclass, fields
import ray
import os
from typing import Any, Dict, List, Optional, Tuple
import deltacat.tests.local_deltacat_storage as ds
from deltacat.compute.compactor.model.compact_partition_params import (
    CompactPartitionParams,
)
import pyarrow as pa
from deltacat.storage import DeleteParameters


DATABASE_FILE_PATH_KEY, DATABASE_FILE_PATH_VALUE = (
    "db_file_path",
    "deltacat/tests/local_deltacat_storage/db_test.sqlite",
)


@dataclass(frozen=True)
class PrepareDeleteTestCaseParams:
    """
    A pytest parameterized test case for the `prepare_deletes` function.
    """

    deltas_to_compact: List[Tuple[pa.Table, DeltaType, Optional[DeleteParameters]]]
    expected_delta_file_envelopes_len: int
    expected_delete_table: List[pa.Table]
    expected_non_delete_deltas_length: int
    throws_error_type: BaseException

    # makes TestCaseParams iterable which is required to build the list of pytest.param values to pass to pytest.mark.parametrize
    def __iter__(self):
        return (getattr(self, field.name) for field in fields(self))


@pytest.fixture(scope="function")
def local_deltacat_storage_kwargs(request: pytest.FixtureRequest):
    # see deltacat/tests/local_deltacat_storage/README.md for documentation
    kwargs_for_local_deltacat_storage: Dict[str, Any] = {
        DATABASE_FILE_PATH_KEY: DATABASE_FILE_PATH_VALUE,
    }
    yield kwargs_for_local_deltacat_storage
    if os.path.exists(DATABASE_FILE_PATH_VALUE):
        os.remove(DATABASE_FILE_PATH_VALUE)


class TestEqualityDeleteStrategy:
    def test_apply_deletes(self):
        pass

    def test_apply_all_deletes(self):
        pass
