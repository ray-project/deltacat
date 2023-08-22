import unittest
import sqlite3
import ray
import os
from typing import List
from collections import defaultdict
from deltacat.compute.compactor import DeltaAnnotated
import deltacat.tests.local_deltacat_storage as ds
from deltacat.io.ray_plasma_object_store import RayPlasmaObjectStore
from deltacat.compute.compactor_v2.model.hash_bucket_input import HashBucketInput
from deltacat.compute.compactor_v2.model.hash_bucket_result import HashBucketResult
from deltacat.compute.compactor_v2.steps.hash_bucket import hash_bucket
from deltacat.compute.compactor_v2.steps.merge import _dedupe_incremental, merge_tables
from deltacat.utils.common import current_time_ms

from deltacat.tests.test_utils.pyarrow import (
    create_delta_from_csv_file,
    stage_partition_from_csv_file,
    commit_delta_to_staged_partition,
)
from deltacat.compute.compactor_v2.utils.primary_key_index import (
    generate_pk_hash_column,
)


class TestMerge(unittest.TestCase):
    MERGE_NAMESPACE = "test_merge"
    DB_FILE_PATH = f"{current_time_ms()}.db"
    DEDUPE_BASE_COMPACTED_TABLE_STRING_PK = "deltacat/tests/compute/compactor_v2/steps/data/dedupe_base_compacted_table_string_pk.csv"
    DEDUPE_NO_DUPLICATION_STRING_PK = "deltacat/tests/compute/compactor_v2/steps/data/dedupe_table_no_duplication_string_pk.csv"
    DEDUPE_WITH_DUPLICATION_STRING_PK = "deltacat/tests/compute/compactor_v2/steps/data/dedupe_table_with_duplication_string_pk.csv"
    DEDUPE_BASE_COMPACTED_TABLE_DATE_PK = "deltacat/tests/compute/compactor_v2/steps/data/dedupe_base_compacted_table_date_pk.csv"
    DEDUPE_NO_DUPLICATION_DATE_PK = "deltacat/tests/compute/compactor_v2/steps/data/dedupe_table_no_duplication_date_pk.csv"
    DEDUPE_WITH_DUPLICATION_DATE_PK = "deltacat/tests/compute/compactor_v2/steps/data/dedupe_table_with_duplication_date_pk.csv"
    DEDUPE_BASE_COMPACTED_TABLE_MULTIPLE_PK = "deltacat/tests/compute/compactor_v2/steps/data/dedupe_base_compacted_table_multiple_pk.csv"
    DEDUPE_NO_DUPLICATION_MULTIPLE_PK = "deltacat/tests/compute/compactor_v2/steps/data/dedupe_table_no_duplication_multiple_pk.csv"
    DEDUPE_WITH_DUPLICATION_MULTIPLE_PK = "deltacat/tests/compute/compactor_v2/steps/data/dedupe_table_with_duplication_multiple_pk.csv"

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

    def test_dedupe_incremental_success(self):
        new_delta = create_delta_from_csv_file(
            self.MERGE_NAMESPACE,
            [self.DEDUPE_WITH_DUPLICATION_STRING_PK],
            **self.kwargs,
        )
        annotated_delta = DeltaAnnotated.of(new_delta)
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
        hb_result_promise = hash_bucket.remote(hb_input)
        hb_results: List[HashBucketResult] = [ray.get(hb_result_promise)]
        print(f"hb_result{type(hb_results[0].hash_bucket_group_to_obj_id_tuple)}")

        all_hash_group_idx_to_obj_id = defaultdict(list)
        for hb_group in range(2):
            all_hash_group_idx_to_obj_id[hb_group] = []

        for hb_result in hb_results:
            for hash_group_index, object_id_size_tuple in enumerate(
                hb_result.hash_bucket_group_to_obj_id_tuple
            ):
                if object_id_size_tuple:
                    all_hash_group_idx_to_obj_id[hash_group_index].append(
                        object_id_size_tuple[0]
                    )

        # dedupe incremental test case
        length_of_table = 0
        input_list = defaultdict(list)
        for (
            hg_index,
            delta_file_envelope_groups,
        ) in all_hash_group_idx_to_obj_id.items():
            hb_index_to_delta_file_envelopes_list = object_store.get_many(
                delta_file_envelope_groups
            )
            for (
                hb_index_to_delta_file_envelopes
            ) in hb_index_to_delta_file_envelopes_list:
                for hb_idx, dfes in enumerate(hb_index_to_delta_file_envelopes):
                    if dfes is not None:
                        input_list[hb_idx].append(dfes)

        for hb_idx, dfe_list in input_list.items():
            length_of_table += len(_dedupe_incremental(hb_idx, dfe_list))
        # 4 unique records in test table
        assert length_of_table == 4

    def test_merge_table_string_pk(self):
        # setup
        partition = stage_partition_from_csv_file(
            self.MERGE_NAMESPACE,
            [self.DEDUPE_BASE_COMPACTED_TABLE_STRING_PK],
            **self.kwargs,
        )
        old_delta = commit_delta_to_staged_partition(
            partition, [self.DEDUPE_BASE_COMPACTED_TABLE_STRING_PK], **self.kwargs
        )
        new_delta = create_delta_from_csv_file(
            self.MERGE_NAMESPACE, [self.DEDUPE_NO_DUPLICATION_STRING_PK], **self.kwargs
        )

        new_tables = ds.download_delta(new_delta, **self.kwargs)
        old_tables = ds.download_delta(old_delta, **self.kwargs)
        hashed_new_table = generate_pk_hash_column(new_tables[0], primary_keys=["pk"])
        hashed_old_table = generate_pk_hash_column(old_tables[0], primary_keys=["pk"])
        merged_table = merge_tables(hashed_new_table, hashed_old_table)
        # 8 unique primary keys in old delta, 6 primary keys in new delta
        # with only 1 additional unique primary key
        assert len(merged_table) == 9

    def test_merge_table_date_pk(self):
        # setup
        partition = stage_partition_from_csv_file(
            self.MERGE_NAMESPACE,
            [self.DEDUPE_BASE_COMPACTED_TABLE_DATE_PK],
            **self.kwargs,
        )
        old_delta = commit_delta_to_staged_partition(
            partition, [self.DEDUPE_BASE_COMPACTED_TABLE_DATE_PK], **self.kwargs
        )
        new_delta = create_delta_from_csv_file(
            self.MERGE_NAMESPACE, [self.DEDUPE_NO_DUPLICATION_DATE_PK], **self.kwargs
        )

        new_tables = ds.download_delta(new_delta, **self.kwargs)
        old_tables = ds.download_delta(old_delta, **self.kwargs)
        hashed_new_table = generate_pk_hash_column(new_tables[0], primary_keys=["pk"])
        hashed_old_table = generate_pk_hash_column(old_tables[0], primary_keys=["pk"])
        merged_table = merge_tables(hashed_new_table, hashed_old_table)
        # 7 unique primary keys in old delta, 9 primary keys in new delta
        # with only 2 additional unique primary key
        assert len(merged_table) == 9

    def test_merge_table_multiple_pk(self):
        # setup
        partition = stage_partition_from_csv_file(
            self.MERGE_NAMESPACE,
            [self.DEDUPE_BASE_COMPACTED_TABLE_MULTIPLE_PK],
            **self.kwargs,
        )
        old_delta = commit_delta_to_staged_partition(
            partition, [self.DEDUPE_BASE_COMPACTED_TABLE_MULTIPLE_PK], **self.kwargs
        )
        new_delta = create_delta_from_csv_file(
            self.MERGE_NAMESPACE,
            [self.DEDUPE_NO_DUPLICATION_MULTIPLE_PK],
            **self.kwargs,
        )

        new_tables = ds.download_delta(new_delta, **self.kwargs)
        old_tables = ds.download_delta(old_delta, **self.kwargs)
        hashed_new_table = generate_pk_hash_column(
            new_tables[0], primary_keys=["pk1", "pk2"]
        )
        hashed_old_table = generate_pk_hash_column(
            old_tables[0], primary_keys=["pk1", "pk2"]
        )
        merged_table = merge_tables(hashed_new_table, hashed_old_table)
        # 8 unique primary keys in old delta, 8 primary keys in new delta
        # with only 3 additional unique primary key
        assert len(merged_table) == 11
