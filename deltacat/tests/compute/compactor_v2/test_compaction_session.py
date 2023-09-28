import unittest
import sqlite3
import ray
import os
from unittest.mock import patch
import deltacat.tests.local_deltacat_storage as ds
from deltacat.types.media import ContentType
from deltacat.compute.compactor_v2.compaction_session import compact_partition
from deltacat.compute.compactor.model.compact_partition_params import (
    CompactPartitionParams,
)
from deltacat.utils.common import current_time_ms
from deltacat.tests.test_utils.pyarrow import stage_partition_from_file_paths


class TestCompactionSession(unittest.TestCase):
    """
    This class adds specific tests that aren't part of the parametrized test suite.
    """

    DB_FILE_PATH = f"{current_time_ms()}.db"
    NAMESPACE = "compact_partition_v2_namespace"

    @classmethod
    def setUpClass(cls):
        ray.init(local_mode=True, ignore_reinit_error=True)

        con = sqlite3.connect(cls.DB_FILE_PATH)
        cur = con.cursor()
        cls.kwargs = {ds.SQLITE_CON_ARG: con, ds.SQLITE_CUR_ARG: cur}
        cls.deltacat_storage_kwargs = {ds.DB_FILE_PATH_ARG: cls.DB_FILE_PATH}

        super().setUpClass()

    @classmethod
    def doClassCleanups(cls) -> None:
        os.remove(cls.DB_FILE_PATH)

    @patch("deltacat.compute.compactor_v2.compaction_session.rcf")
    @patch("deltacat.compute.compactor_v2.compaction_session.s3_utils")
    def test_compact_partition_when_no_input_deltas_to_compact(self, s3_utils, rcf_url):
        # setup
        rcf_url.read_round_completion_file.return_value = None
        staged_source = stage_partition_from_file_paths(
            self.NAMESPACE, ["test"], **self.deltacat_storage_kwargs
        )
        source_partition = ds.commit_partition(
            staged_source, **self.deltacat_storage_kwargs
        )

        staged_dest = stage_partition_from_file_paths(
            self.NAMESPACE, ["destination"], **self.deltacat_storage_kwargs
        )
        dest_partition = ds.commit_partition(
            staged_dest, **self.deltacat_storage_kwargs
        )

        # action
        rcf_url = compact_partition(
            CompactPartitionParams.of(
                {
                    "compaction_artifact_s3_bucket": "test_bucket",
                    "compacted_file_content_type": ContentType.PARQUET,
                    "dd_max_parallelism_ratio": 1.0,
                    "deltacat_storage": ds,
                    "deltacat_storage_kwargs": self.deltacat_storage_kwargs,
                    "destination_partition_locator": dest_partition.locator,
                    "drop_duplicates": True,
                    "hash_bucket_count": 1,
                    "last_stream_position_to_compact": source_partition.stream_position,
                    "list_deltas_kwargs": {
                        **self.deltacat_storage_kwargs,
                        **{"equivalent_table_types": []},
                    },
                    "primary_keys": [],
                    "rebase_source_partition_locator": None,
                    "rebase_source_partition_high_watermark": None,
                    "records_per_compacted_file": 4000,
                    "s3_client_kwargs": {},
                    "source_partition_locator": source_partition.locator,
                }
            )
        )

        # verify that no RCF is written
        self.assertIsNone(rcf_url)
