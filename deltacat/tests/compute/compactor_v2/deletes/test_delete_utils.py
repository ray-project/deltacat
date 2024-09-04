import pytest

from deltacat.storage import (
    DeltaType,
    Delta, EntryParams,
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

DATABASE_FILE_PATH_KEY, DATABASE_FILE_PATH_VALUE = (
    "db_file_path",
    "deltacat/tests/local_deltacat_storage/db_test.sqlite",
)


@dataclass(frozen=True)
class PrepareDeleteTestCaseParams:
    """
    A pytest parameterized test case for the `prepare_deletes` function.
    """

    deltas_to_compact: List[Tuple[pa.Table, DeltaType, Optional[EntryParams]]]
    expected_delta_file_envelopes_len: int
    expected_delete_table: List[pa.Table]
    expected_non_delete_deltas_length: int
    expected_exception: BaseException

    # makes TestCaseParams iterable which is required to build the list of pytest.param values to pass to pytest.mark.parametrize
    def __iter__(self):
        return (getattr(self, field.name) for field in fields(self))


@pytest.fixture(scope="module", autouse=True)
def setup_ray_cluster():
    ray.init(local_mode=True, ignore_reinit_error=True)
    yield
    ray.shutdown()


@pytest.fixture(scope="function")
def local_deltacat_storage_kwargs(request: pytest.FixtureRequest):
    # see deltacat/tests/local_deltacat_storage/README.md for documentation
    kwargs_for_local_deltacat_storage: Dict[str, Any] = {
        DATABASE_FILE_PATH_KEY: DATABASE_FILE_PATH_VALUE,
    }
    yield kwargs_for_local_deltacat_storage
    if os.path.exists(DATABASE_FILE_PATH_VALUE):
        os.remove(DATABASE_FILE_PATH_VALUE)


TEST_CASES_PREPARE_DELETE = {
    "1-test-single-upsert": PrepareDeleteTestCaseParams(
        [
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10)]),
                        pa.array([i for i in range(20, 30)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(40, 50)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                EntryParams.of(["col_1"]),
            ),
        ],
        0,
        None,
        1,
        None,
    ),
    "2-test-single-delete": PrepareDeleteTestCaseParams(
        [
            (
                pa.Table.from_arrays(
                    [
                        pa.array([i / 10 for i in range(40, 50)]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                EntryParams.of(["col_1"]),
            ),
        ],
        1,
        [
            pa.Table.from_arrays(
                [
                    pa.array([4.0, 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 4.9]),
                ],
                names=["col_1"],
            )
        ],
        0,
        None,
    ),
    "3-test-single-upsert-then-delete": PrepareDeleteTestCaseParams(
        [
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10)]),
                        pa.array([i for i in range(20, 30)]),
                        pa.array(["foo"] * 10),
                        pa.array([i for i in range(40, 50)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([40]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                EntryParams.of(["col_1"]),
            ),
        ],
        1,
        [
            pa.Table.from_arrays(
                [
                    pa.array([40]),
                ],
                names=["col_1"],
            )
        ],
        1,
        None,
    ),
    "4-test-upsert-delete-upsert": PrepareDeleteTestCaseParams(
        [
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10)]),
                        pa.array([i for i in range(40, 50)]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([40]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                EntryParams.of(["col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10)]),
                        pa.array([i for i in range(70, 80)]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
        ],
        1,
        [
            pa.Table.from_arrays(
                [
                    pa.array([40]),
                ],
                names=["col_1"],
            )
        ],
        2,
        None,
    ),
    "5-test-upsert-delete-upsert-delete": PrepareDeleteTestCaseParams(
        [
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10)]),
                        pa.array([i for i in range(40, 50)]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array(["0"]),
                    ],
                    names=["pk_col_1"],
                ),
                DeltaType.DELETE,
                EntryParams.of(["pk_col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array(["1"]),
                    ],
                    names=["pk_col_1"],
                ),
                DeltaType.DELETE,
                EntryParams.of(["pk_col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10)]),
                        pa.array([i for i in range(70, 80)]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([72]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                EntryParams.of(["col_1"]),
            ),
        ],
        2,
        [
            pa.Table.from_arrays(
                [
                    pa.array(["0", "1"]),
                ],
                names=["pk_col_1"],
            ),
            pa.Table.from_arrays(
                [
                    pa.array([72]),
                ],
                names=["col_1"],
            ),
        ],
        2,
        None,
    ),
    "6-test-upsert-deletesequence-different-delete-params-upsert-delete": PrepareDeleteTestCaseParams(
        [
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10)]),
                        pa.array([i for i in range(40, 50)]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([40]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                EntryParams.of(["col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array(["1"]),
                        pa.array([41]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.DELETE,
                EntryParams.of(["pk_col_1", "col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array(["9"]),
                    ],
                    names=["pk_col_1"],
                ),
                DeltaType.DELETE,
                EntryParams.of(["pk_col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10)]),
                        pa.array([i for i in range(70, 80)]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([72]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                EntryParams.of(["col_1"]),
            ),
        ],
        4,
        [
            pa.Table.from_arrays(
                [
                    pa.array([40]),
                ],
                names=["col_1"],
            ),
            pa.Table.from_arrays(
                [
                    pa.array(["1"]),
                    pa.array([41]),
                ],
                names=["pk_col_1", "col_1"],
            ),
            pa.Table.from_arrays(
                [
                    pa.array(["9"]),
                ],
                names=["pk_col_1"],
            ),
            pa.Table.from_arrays(
                [
                    pa.array([72]),
                ],
                names=["col_1"],
            ),
        ],
        2,
        None,
    ),
    "7-test-exception-thrown-if-properties-not-defined": PrepareDeleteTestCaseParams(
        [
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10)]),
                        pa.array([i for i in range(20, 30)]),
                        pa.array(["foo"] * 10),
                        pa.array([i for i in range(40, 50)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([40]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                None,
            ),
        ],
        0,
        None,
        None,
        AssertionError,
    ),
    "8-test-only-deletes": PrepareDeleteTestCaseParams(
        [
            (
                pa.Table.from_arrays(
                    [
                        pa.array([i for i in range(40, 50)]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                EntryParams.of(["col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([40]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                EntryParams.of(["col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([55]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                EntryParams.of(["col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([72]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                EntryParams.of(["col_1"]),
            ),
        ],
        1,
        [
            pa.Table.from_arrays(
                [
                    pa.array([40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 40, 55, 72]),
                ],
                names=["col_1"],
            ),
        ],
        0,
        None,
    ),
}


class TestPrepareDeletes:
    TEST_PRIMARY_KEYS = ["pk_col_1"]

    @pytest.mark.parametrize(
        [
            "test_name",
            "deltas_to_compact",
            "expected_delta_file_envelopes_len",
            "expected_delete_tables",
            "expected_non_delete_deltas_length",
            "expected_exception",
        ],
        [
            (
                test_name,
                deltas_to_compact,
                expected_delta_file_envelopes_len,
                expected_delete_tables,
                expected_non_delete_deltas_length,
                expected_exception,
            )
            for test_name, (
                deltas_to_compact,
                expected_delta_file_envelopes_len,
                expected_delete_tables,
                expected_non_delete_deltas_length,
                expected_exception,
            ) in TEST_CASES_PREPARE_DELETE.items()
        ],
        ids=[test_name for test_name in TEST_CASES_PREPARE_DELETE],
    )
    def test_prepare_deletes_with_deletes(
        self,
        local_deltacat_storage_kwargs: Dict[str, Any],
        test_name: str,
        deltas_to_compact: List[Tuple[pa.Table, DeltaType, Optional[EntryParams]]],
        expected_delta_file_envelopes_len: int,
        expected_delete_tables,
        expected_non_delete_deltas_length,
        expected_exception,
    ):
        from deltacat.compute.compactor_v2.deletes.utils import (
            prepare_deletes,
        )

        source_namespace, source_table_name, source_table_version = create_src_table(
            set(self.TEST_PRIMARY_KEYS),
            None,
            None,
            local_deltacat_storage_kwargs,
        )
        source_table_stream: Stream = ds.get_stream(
            namespace=source_namespace,
            table_name=source_table_name,
            table_version=source_table_version,
            **local_deltacat_storage_kwargs,
        )
        staged_partition: Partition = ds.stage_partition(
            source_table_stream, None, **local_deltacat_storage_kwargs
        )
        input_deltas: List[Delta] = []
        for (incremental_delta, delta_type, delete_parameters) in deltas_to_compact:
            input_deltas.append(
                ds.commit_delta(
                    ds.stage_delta(
                        incremental_delta,
                        staged_partition,
                        delta_type,
                        entry_params=delete_parameters,
                        **local_deltacat_storage_kwargs,
                    ),
                    **local_deltacat_storage_kwargs,
                )
            )
        ds.commit_partition(staged_partition, **local_deltacat_storage_kwargs)
        src_table_stream_after_committed_delta: Stream = ds.get_stream(
            source_namespace,
            source_table_name,
            source_table_version,
            **local_deltacat_storage_kwargs,
        )
        src_partition_after_committed_delta: Partition = ds.get_partition(
            src_table_stream_after_committed_delta.locator,
            None,
            **local_deltacat_storage_kwargs,
        )
        (
            destination_table_namespace,
            destination_table_name,
            destination_table_version,
        ) = create_destination_table(
            set(self.TEST_PRIMARY_KEYS),
            None,
            None,
            local_deltacat_storage_kwargs,
        )
        destination_table_stream: Stream = ds.get_stream(
            namespace=destination_table_namespace,
            table_name=destination_table_name,
            table_version=destination_table_version,
            **local_deltacat_storage_kwargs,
        )
        params = CompactPartitionParams.of(
            {
                "compaction_artifact_s3_bucket": TEST_S3_RCF_BUCKET_NAME,
                "deltacat_storage": ds,
                "deltacat_storage_kwargs": local_deltacat_storage_kwargs,
                "destination_partition_locator": PartitionLocator.of(
                    destination_table_stream, None, None
                ),
                "last_stream_position_to_compact": staged_partition.stream_position,
                "list_deltas_kwargs": {
                    **local_deltacat_storage_kwargs,
                    **{"equivalent_table_types": []},
                },
                "read_kwargs_provider": None,
                "source_partition_locator": src_partition_after_committed_delta.locator,
            }
        )
        # action
        if expected_exception:
            with pytest.raises(expected_exception):
                prepare_deletes(
                    params,
                    input_deltas,
                )
            return
        actual_prepare_delete_result = prepare_deletes(
            params,
            input_deltas,
        )
        actual_non_delete_deltas = actual_prepare_delete_result.non_delete_deltas
        actual_delete_file_envelopes = (
            actual_prepare_delete_result.delete_file_envelopes
        )
        actual_delete_tables = [
            delete_file_envelope.table
            for delete_file_envelope in actual_delete_file_envelopes
        ]
        # verify
        assert len(actual_non_delete_deltas) is expected_non_delete_deltas_length
        actual_dictionary_length = len(actual_delete_file_envelopes)
        assert (
            expected_delta_file_envelopes_len == actual_dictionary_length
        ), f"{expected_delta_file_envelopes_len} does not match {actual_dictionary_length}"
        if expected_delta_file_envelopes_len > 0:
            for i, (actual_delete_table, expected_delete_tables) in enumerate(
                zip(actual_delete_tables, expected_delete_tables)
            ):
                actual_table = actual_delete_table.combine_chunks()
                expected_delete_table = expected_delete_tables.combine_chunks()
                assert actual_table.equals(expected_delete_table)
        return
