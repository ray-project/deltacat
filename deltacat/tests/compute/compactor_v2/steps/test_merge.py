import unittest
import sqlite3
import ray
import os
import deltacat.tests.local_deltacat_storage as ds
from deltacat.compute.compactor_v2.steps.merge import _merge_tables
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
        merged_table = _merge_tables(hashed_new_table, hashed_old_table)
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
        merged_table = _merge_tables(hashed_new_table, hashed_old_table)
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
        merged_table = _merge_tables(hashed_new_table, hashed_old_table)
        # 8 unique primary keys in old delta, 8 primary keys in new delta
        # with only 3 additional unique primary key
        assert len(merged_table) == 11
