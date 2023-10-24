import unittest
import sqlite3
import ray
import os
from collections import defaultdict
from deltacat.compute.compactor import DeltaAnnotated
import deltacat.tests.local_deltacat_storage as ds
from deltacat.io.ray_plasma_object_store import RayPlasmaObjectStore
from deltacat.compute.compactor_v2.model.hash_bucket_input import HashBucketInput
from deltacat.compute.compactor_v2.model.hash_bucket_result import HashBucketResult
from deltacat.compute.compactor_v2.steps.hash_bucket import hash_bucket
from deltacat.utils.common import current_time_ms
from deltacat.tests.test_utils.pyarrow import create_delta_from_csv_file


class TestHashBucket(unittest.TestCase):
    HASH_BUCKET_NAMESPACE = "test_hash_bucket"
    DB_FILE_PATH = f"{current_time_ms()}.db"
    STRING_PK_FILE_PATH = (
        "deltacat/tests/compute/compactor_v2/steps/data/string_pk_table.csv"
    )
    DATE_PK_FILE_PATH = (
        "deltacat/tests/compute/compactor_v2/steps/data/date_pk_table.csv"
    )
    MULTIPLE_PK_FILE_PATH = (
        "deltacat/tests/compute/compactor_v2/steps/data/multiple_pk_table.csv"
    )
    NO_PK_FILE_PATH = "deltacat/tests/compute/compactor_v2/steps/data/no_pk_table.csv"

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

    def test_single_string_pk_correctly_hashes(self):
        # setup
        delta = create_delta_from_csv_file(
            self.HASH_BUCKET_NAMESPACE, [self.STRING_PK_FILE_PATH], **self.kwargs
        )

        annotated_delta = DeltaAnnotated.of(delta)
        object_store = RayPlasmaObjectStore()
        hb_input = HashBucketInput.of(
            annotated_delta=annotated_delta,
            primary_keys=["pk"],
            num_hash_buckets=3,
            num_hash_groups=2,
            deltacat_storage=ds,
            deltacat_storage_kwargs=self.deltacat_storage_kwargs,
            object_store=object_store,
        )

        # action
        hb_result_promise = hash_bucket.remote(hb_input)
        hb_result: HashBucketResult = ray.get(hb_result_promise)

        # assert
        # PK hash column is also persisted.
        self._validate_hash_bucket_result(
            hb_result,
            record_count=6,
            num_hash_buckets=3,
            num_columns=2,
            object_store=object_store,
        )

    def test_single_date_pk_correctly_hashes(self):
        # setup
        delta = create_delta_from_csv_file(
            self.HASH_BUCKET_NAMESPACE, [self.DATE_PK_FILE_PATH], **self.kwargs
        )

        annotated_delta = DeltaAnnotated.of(delta)
        object_store = RayPlasmaObjectStore()
        hb_input = HashBucketInput.of(
            annotated_delta=annotated_delta,
            primary_keys=["pk"],
            num_hash_buckets=2,
            num_hash_groups=1,
            deltacat_storage=ds,
            deltacat_storage_kwargs=self.deltacat_storage_kwargs,
            object_store=object_store,
        )

        # action
        hb_result_promise = hash_bucket.remote(hb_input)
        hb_result: HashBucketResult = ray.get(hb_result_promise)

        # assert
        self._validate_hash_bucket_result(
            hb_result,
            record_count=7,
            num_hash_buckets=2,
            num_columns=2,
            object_store=object_store,
        )

    def test_no_pk_does_not_hash(self):
        # setup
        delta = create_delta_from_csv_file(
            self.HASH_BUCKET_NAMESPACE, [self.NO_PK_FILE_PATH], **self.kwargs
        )

        annotated_delta = DeltaAnnotated.of(delta)
        object_store = RayPlasmaObjectStore()
        hb_input = HashBucketInput.of(
            annotated_delta=annotated_delta,
            primary_keys=[],
            num_hash_buckets=2,
            num_hash_groups=1,
            deltacat_storage=ds,
            deltacat_storage_kwargs=self.deltacat_storage_kwargs,
            object_store=object_store,
        )

        # action
        hb_result_promise = hash_bucket.remote(hb_input)
        hb_result: HashBucketResult = ray.get(hb_result_promise)

        # assert
        self._validate_hash_bucket_result(
            hb_result,
            record_count=6,
            num_hash_buckets=2,
            num_columns=2,
            object_store=object_store,
        )

    def test_multiple_pk_correctly_hashes(self):
        # setup
        delta = create_delta_from_csv_file(
            self.HASH_BUCKET_NAMESPACE, [self.MULTIPLE_PK_FILE_PATH], **self.kwargs
        )

        annotated_delta = DeltaAnnotated.of(delta)
        object_store = RayPlasmaObjectStore()
        hb_input = HashBucketInput.of(
            annotated_delta=annotated_delta,
            primary_keys=["pk1", "pk2"],
            num_hash_buckets=2,
            num_hash_groups=1,
            deltacat_storage=ds,
            deltacat_storage_kwargs=self.deltacat_storage_kwargs,
            object_store=object_store,
        )

        # action
        hb_result_promise = hash_bucket.remote(hb_input)
        hb_result: HashBucketResult = ray.get(hb_result_promise)

        # assert
        self._validate_hash_bucket_result(
            hb_result,
            record_count=6,
            num_hash_buckets=2,
            num_columns=3,
            object_store=object_store,
        )

    def _validate_hash_bucket_result(
        self,
        hb_result: HashBucketResult,
        record_count: int,
        num_hash_buckets: int,
        num_columns: int,
        object_store,
    ):

        self.assertEqual(hb_result.hb_record_count, record_count)
        self.assertIsNotNone(hb_result)
        self.assertIsNotNone(hb_result.peak_memory_usage_bytes)
        self.assertIsNotNone(hb_result.task_completed_at)

        hb_index_to_dfes = defaultdict(list)
        total_records_in_result = 0
        for _, object_id in enumerate(hb_result.hash_bucket_group_to_obj_id_tuple):
            if object_id:
                obj = object_store.get(object_id[0])
                for hb_idx, dfes in enumerate(obj):
                    if dfes is not None:
                        hb_index_to_dfes[hb_idx].extend(dfes)
                        for dfe in dfes:
                            self.assertIsNotNone(dfe)
                            total_records_in_result += len(dfe.table)
                            self.assertEqual(num_columns, len(dfe.table.column_names))

        self.assertTrue(len(hb_index_to_dfes) <= num_hash_buckets)
        self.assertEqual(total_records_in_result, record_count)
