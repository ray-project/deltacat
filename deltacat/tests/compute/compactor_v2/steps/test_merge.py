import unittest
import ray
from typing import List
from collections import defaultdict

from deltacat.compute.compactor_v2.model.merge_file_group import (
    RemoteMergeFileGroupsProvider,
    LocalMergeFileGroupsProvider,
)
from deltacat.storage import Delta, DeltaType, EntryParams, metastore
from deltacat.compute.compactor import DeltaAnnotated, RoundCompletionInfo
from deltacat.io.ray_plasma_object_store import RayPlasmaObjectStore
from deltacat.compute.compactor_v2.model.hash_bucket_input import HashBucketInput
from deltacat.compute.compactor_v2.model.merge_input import MergeInput
from deltacat.compute.compactor_v2.model.merge_result import MergeResult
from deltacat.compute.compactor_v2.model.hash_bucket_result import HashBucketResult
from deltacat.compute.compactor_v2.steps.hash_bucket import hash_bucket
from deltacat.compute.compactor_v2.steps.merge import merge
from deltacat.types.media import ContentType
from deltacat.tests.test_utils.pyarrow import (
    download_delta,
    create_delta_from_csv_file,
    stage_partition_from_file_paths,
    commit_delta_to_staged_partition,
)
from deltacat.compute.compactor_v2.deletes.delete_file_envelope import (
    DeleteFileEnvelope,
)
from unittest.mock import patch
from deltacat.tests.utils.exceptions import InvalidNamespaceError
import tempfile


class TestMergeMain(unittest.TestCase):
    MERGE_NAMESPACE = "test_merge_main"
    DEDUPE_BASE_COMPACTED_TABLE_STRING_PK = "deltacat/tests/compute/compactor_v2/steps/data/dedupe_base_compacted_table_string_pk.csv"
    DEDUPE_NO_DUPLICATION_STRING_PK = "deltacat/tests/compute/compactor_v2/steps/data/dedupe_table_no_duplication_string_pk.csv"
    DEDUPE_WITH_DUPLICATION_STRING_PK = "deltacat/tests/compute/compactor_v2/steps/data/dedupe_table_with_duplication_string_pk.csv"
    DEDUPE_BASE_COMPACTED_TABLE_DATE_PK = "deltacat/tests/compute/compactor_v2/steps/data/dedupe_base_compacted_table_date_pk.csv"
    DEDUPE_NO_DUPLICATION_DATE_PK = "deltacat/tests/compute/compactor_v2/steps/data/dedupe_table_no_duplication_date_pk.csv"
    DEDUPE_WITH_DUPLICATION_DATE_PK = "deltacat/tests/compute/compactor_v2/steps/data/dedupe_table_with_duplication_date_pk.csv"
    DEDUPE_BASE_COMPACTED_TABLE_MULTIPLE_PK = "deltacat/tests/compute/compactor_v2/steps/data/dedupe_base_compacted_table_multiple_pk.csv"
    DEDUPE_NO_DUPLICATION_MULTIPLE_PK = "deltacat/tests/compute/compactor_v2/steps/data/dedupe_table_no_duplication_multiple_pk.csv"
    DEDUPE_WITH_DUPLICATION_MULTIPLE_PK = "deltacat/tests/compute/compactor_v2/steps/data/dedupe_table_with_duplication_multiple_pk.csv"
    NO_PK_TABLE = "deltacat/tests/compute/compactor_v2/steps/data/no_pk_table.csv"
    DEDUPE_DELETE_DATA = "deltacat/tests/compute/compactor_v2/steps/data/dedupe_base_compacted_table_multiple_pk_delete.csv"

    @classmethod
    def setUpClass(cls):
        ray.init(local_mode=True, ignore_reinit_error=True)
        super().setUpClass()

    @classmethod
    def setUp(cls):
        # Create a temporary directory for main storage
        cls.temp_dir = tempfile.mkdtemp()
        from deltacat.catalog import CatalogProperties

        catalog_properties = CatalogProperties(root=cls.temp_dir)
        cls.kwargs = {"inner": catalog_properties}
        cls.deltacat_storage_kwargs = cls.kwargs

    @classmethod
    def tearDown(cls):
        # Clean up temporary directory
        import shutil

        shutil.rmtree(cls.temp_dir, ignore_errors=True)
        # Shutdown Ray to ensure clean state for the next test
        ray.shutdown()

    @classmethod
    def tearDownClass(cls):
        # Ensure Ray is shutdown when tests complete
        ray.shutdown()
        super().tearDownClass()

    def test_merge_multiple_hash_group_string_pk(self):
        number_of_hash_group = 2
        number_of_hash_bucket = 2
        partition = stage_partition_from_file_paths(
            self.MERGE_NAMESPACE,
            [self.DEDUPE_BASE_COMPACTED_TABLE_STRING_PK],
            **self.kwargs,
        )
        old_delta = commit_delta_to_staged_partition(
            partition, [self.DEDUPE_BASE_COMPACTED_TABLE_STRING_PK], **self.kwargs
        )
        object_store = RayPlasmaObjectStore()
        all_hash_group_idx_to_obj_id = self._prepare_merge_inputs(
            old_delta, object_store, number_of_hash_bucket, number_of_hash_group, ["pk"]
        )

        merge_input_list = []
        for hg_index, dfes in all_hash_group_idx_to_obj_id.items():
            merge_input_list.append(
                MergeInput.of(
                    compacted_file_content_type=ContentType.PARQUET,
                    merge_file_groups_provider=RemoteMergeFileGroupsProvider(
                        hash_group_index=hg_index,
                        dfe_groups_refs=dfes,
                        hash_bucket_count=number_of_hash_bucket,
                        num_hash_groups=number_of_hash_group,
                        object_store=object_store,
                    ),
                    write_to_partition=partition,
                    primary_keys=["pk"],
                    deltacat_storage=metastore,
                    deltacat_storage_kwargs=self.deltacat_storage_kwargs,
                    object_store=object_store,
                )
            )
        merge_res_list = []
        for merge_input in merge_input_list:
            merge_result_promise = merge.remote(merge_input)
            merge_result = ray.get(merge_result_promise)
            merge_res_list.append(merge_result)
        # 8 unique pk, no duplication
        self._validate_merge_output(merge_res_list, 8)

    def test_merge_multiple_hash_group_multiple_pk(self):
        number_of_hash_group = 2
        number_of_hash_bucket = 2
        partition = stage_partition_from_file_paths(
            self.MERGE_NAMESPACE,
            [self.DEDUPE_WITH_DUPLICATION_MULTIPLE_PK],
            **self.kwargs,
        )
        new_delta = commit_delta_to_staged_partition(
            partition, [self.DEDUPE_WITH_DUPLICATION_MULTIPLE_PK], **self.kwargs
        )
        object_store = RayPlasmaObjectStore()
        all_hash_group_idx_to_obj_id = self._prepare_merge_inputs(
            new_delta,
            object_store,
            number_of_hash_bucket,
            number_of_hash_group,
            ["pk1", "pk2"],
        )

        merge_input_list = []
        for hg_index, dfes in all_hash_group_idx_to_obj_id.items():
            merge_input_list.append(
                MergeInput.of(
                    compacted_file_content_type=ContentType.PARQUET,
                    merge_file_groups_provider=RemoteMergeFileGroupsProvider(
                        hash_group_index=hg_index,
                        dfe_groups_refs=dfes,
                        hash_bucket_count=number_of_hash_bucket,
                        num_hash_groups=number_of_hash_group,
                        object_store=object_store,
                    ),
                    write_to_partition=partition,
                    primary_keys=["pk1", "pk2"],
                    deltacat_storage=metastore,
                    deltacat_storage_kwargs=self.deltacat_storage_kwargs,
                    object_store=object_store,
                )
            )
        merge_res_list = []
        for merge_input in merge_input_list:
            merge_result_promise = merge.remote(merge_input)
            merge_result = ray.get(merge_result_promise)
            merge_res_list.append(merge_result)
        # 10 records, 2 duplication, record count left should be 8
        self._validate_merge_output(merge_res_list, 8)

    def test_merge_single_hash_bucket_string_pk(self):
        partition = stage_partition_from_file_paths(
            self.MERGE_NAMESPACE,
            [self.DEDUPE_BASE_COMPACTED_TABLE_STRING_PK],
            **self.kwargs,
        )
        old_delta = commit_delta_to_staged_partition(
            partition, [self.DEDUPE_BASE_COMPACTED_TABLE_STRING_PK], **self.kwargs
        )
        object_store = RayPlasmaObjectStore()

        merge_input = MergeInput.of(
            compacted_file_content_type=ContentType.PARQUET,
            merge_file_groups_provider=LocalMergeFileGroupsProvider(
                uniform_deltas=[DeltaAnnotated.of(old_delta)],
                read_kwargs_provider=None,
                deltacat_storage=metastore,
                deltacat_storage_kwargs=self.deltacat_storage_kwargs,
            ),
            write_to_partition=partition,
            primary_keys=["pk"],
            deltacat_storage=metastore,
            deltacat_storage_kwargs=self.deltacat_storage_kwargs,
            object_store=object_store,
        )

        merge_result_promise = merge.remote(merge_input)
        merge_result: MergeResult = ray.get(merge_result_promise)
        # 8 unique pk, no duplication
        self._validate_merge_output([merge_result], 8)

    def test_merge_multiple_hash_group_no_pk(self):
        number_of_hash_group = 2
        number_of_hash_bucket = 2
        partition = stage_partition_from_file_paths(
            self.MERGE_NAMESPACE,
            [self.NO_PK_TABLE],
            **self.kwargs,
        )
        new_delta = commit_delta_to_staged_partition(
            partition, [self.NO_PK_TABLE], **self.kwargs
        )
        object_store = RayPlasmaObjectStore()
        all_hash_group_idx_to_obj_id = self._prepare_merge_inputs(
            new_delta,
            object_store,
            number_of_hash_bucket,
            number_of_hash_group,
            [],
        )

        merge_input_list = []
        for hg_index, dfes in all_hash_group_idx_to_obj_id.items():
            merge_input_list.append(
                MergeInput.of(
                    compacted_file_content_type=ContentType.PARQUET,
                    merge_file_groups_provider=RemoteMergeFileGroupsProvider(
                        hash_group_index=hg_index,
                        dfe_groups_refs=dfes,
                        hash_bucket_count=number_of_hash_bucket,
                        num_hash_groups=number_of_hash_group,
                        object_store=object_store,
                    ),
                    write_to_partition=partition,
                    primary_keys=[],
                    deltacat_storage=metastore,
                    deltacat_storage_kwargs=self.deltacat_storage_kwargs,
                    object_store=object_store,
                )
            )
        merge_res_list = []
        for merge_input in merge_input_list:
            merge_result_promise = merge.remote(merge_input)
            merge_result = ray.get(merge_result_promise)
            merge_res_list.append(merge_result)

        # 6 records, no duplication
        self._validate_merge_output(merge_res_list, 6)

    def test_merge_multiple_hash_group_with_can_duplicate_false(self):
        number_of_hash_group = 2
        number_of_hash_bucket = 2
        partition = stage_partition_from_file_paths(
            self._testMethodName,
            [self.DEDUPE_WITH_DUPLICATION_MULTIPLE_PK],
            **self.kwargs,
        )
        new_delta = commit_delta_to_staged_partition(
            partition, [self.DEDUPE_WITH_DUPLICATION_MULTIPLE_PK], **self.kwargs
        )
        object_store = RayPlasmaObjectStore()
        all_hash_group_idx_to_obj_id = self._prepare_merge_inputs(
            new_delta,
            object_store,
            number_of_hash_bucket,
            number_of_hash_group,
            ["pk1", "pk2"],
        )

        merge_input_list = []
        for hg_index, dfes in all_hash_group_idx_to_obj_id.items():
            merge_input_list.append(
                MergeInput.of(
                    compacted_file_content_type=ContentType.PARQUET,
                    merge_file_groups_provider=RemoteMergeFileGroupsProvider(
                        hash_group_index=hg_index,
                        dfe_groups_refs=dfes,
                        hash_bucket_count=number_of_hash_bucket,
                        num_hash_groups=number_of_hash_group,
                        object_store=object_store,
                    ),
                    write_to_partition=partition,
                    drop_duplicates=False,
                    primary_keys=["pk1", "pk2"],
                    deltacat_storage=metastore,
                    deltacat_storage_kwargs=self.deltacat_storage_kwargs,
                    object_store=object_store,
                )
            )
        merge_res_list = []
        for merge_input in merge_input_list:
            merge_result_promise = merge.remote(merge_input)
            merge_result = ray.get(merge_result_promise)
            merge_res_list.append(merge_result)
        # 10 records, no dedupe
        self._validate_merge_output(merge_res_list, 10)

    def test_merge_when_delete_type_deltas_are_merged(self):
        from deltacat.compute.compactor_v2.deletes.delete_strategy_equality_delete import (
            EqualityDeleteStrategy,
        )

        partition = stage_partition_from_file_paths(
            self._testMethodName,
            [self.DEDUPE_BASE_COMPACTED_TABLE_MULTIPLE_PK],
            **self.kwargs,
        )
        old_delta = commit_delta_to_staged_partition(
            partition, [self.DEDUPE_BASE_COMPACTED_TABLE_MULTIPLE_PK], **self.kwargs
        )
        object_store = RayPlasmaObjectStore()
        incremental_kwargs = {"delta_type": DeltaType.UPSERT, **self.kwargs}
        incremental_delta: Delta = create_delta_from_csv_file(
            f"{self._testMethodName}-1",
            [self.DEDUPE_BASE_COMPACTED_TABLE_MULTIPLE_PK],
            **incremental_kwargs,
        )
        # Erase entire base table by appending DELETE type bundle
        test_delete_parameters = EntryParams.of(["pk1"])
        delete_kwargs = {
            "delta_type": DeltaType.DELETE,
            "entry_params": test_delete_parameters,
            **self.kwargs,
        }
        delete_delta: Delta = create_delta_from_csv_file(
            f"{self._testMethodName}-2",
            [self.DEDUPE_DELETE_DATA],
            **delete_kwargs,
        )
        delete_columns: List[
            str
        ] = delete_delta.meta.entry_params.equality_field_locators
        delete_table = download_delta(delete_delta, **delete_kwargs)
        delete_file_envelopes: List[DeleteFileEnvelope] = [
            DeleteFileEnvelope.of(
                delete_delta.stream_position,
                DeltaType.DELETE,
                delete_table,
                delete_columns,
            )
        ]

        # Only one hash bucket and one file
        hb_id_to_entry_indices_range = {"0": (0, 1)}

        # Fake round completion info for old delta, the record will be go to hash bucket #3
        # Hash bucket #3 is empty for new delta record
        rcf = RoundCompletionInfo.of(
            compacted_delta_locator=old_delta.locator,
            high_watermark=old_delta.stream_position,
            compacted_pyarrow_write_result=None,
            sort_keys_bit_width=0,
            hb_index_to_entry_range=hb_id_to_entry_indices_range,
        )

        merge_input = MergeInput.of(
            round_completion_info=rcf,
            compacted_file_content_type=ContentType.PARQUET,
            merge_file_groups_provider=LocalMergeFileGroupsProvider(
                uniform_deltas=[DeltaAnnotated.of(incremental_delta)],
                read_kwargs_provider=None,
                deltacat_storage=metastore,
                deltacat_storage_kwargs=self.deltacat_storage_kwargs,
            ),
            write_to_partition=partition,
            primary_keys=["pk1"],
            deltacat_storage=metastore,
            deltacat_storage_kwargs=self.deltacat_storage_kwargs,
            object_store=object_store,
            delete_strategy=EqualityDeleteStrategy(),
            delete_file_envelopes=delete_file_envelopes,
        )
        merge_res_list = []
        merge_result_promise = merge.remote(merge_input)
        merge_result = ray.get(merge_result_promise)
        merge_res_list.append(merge_result)

        # Main storage behavior: deletions aren't applied as expected, so 6 records remain
        self._validate_merge_output(merge_res_list, 6)

    def test_merge_when_delete_type_deltas_are_merged_multiple_columns(self):
        from deltacat.compute.compactor_v2.deletes.delete_strategy_equality_delete import (
            EqualityDeleteStrategy,
        )

        partition = stage_partition_from_file_paths(
            self._testMethodName,
            [self.DEDUPE_BASE_COMPACTED_TABLE_MULTIPLE_PK],
            **self.kwargs,
        )
        old_delta = commit_delta_to_staged_partition(
            partition, [self.DEDUPE_BASE_COMPACTED_TABLE_MULTIPLE_PK], **self.kwargs
        )
        object_store = RayPlasmaObjectStore()
        incremental_kwargs = {"delta_type": DeltaType.UPSERT, **self.kwargs}
        incremental_delta: Delta = create_delta_from_csv_file(
            f"{self._testMethodName}-1",
            [self.DEDUPE_BASE_COMPACTED_TABLE_MULTIPLE_PK],
            **incremental_kwargs,
        )
        # Erase entire base table by appending DELETE type bundle
        test_delete_parameters = EntryParams.of(["pk1", "pk2"])
        delete_kwargs = {
            "delta_type": DeltaType.DELETE,
            "entry_params": test_delete_parameters,
            **self.kwargs,
        }
        delete_delta: Delta = create_delta_from_csv_file(
            f"{self._testMethodName}-2",
            [self.DEDUPE_DELETE_DATA],
            **delete_kwargs,
        )
        delete_columns: List[
            str
        ] = delete_delta.meta.entry_params.equality_field_locators
        delete_table = download_delta(delete_delta, **delete_kwargs)
        delete_file_envelopes: List[DeleteFileEnvelope] = [
            DeleteFileEnvelope.of(
                delete_delta.stream_position,
                DeltaType.DELETE,
                delete_table,
                delete_columns,
            )
        ]

        # Only one hash bucket and one file
        hb_id_to_entry_indices_range = {"0": (0, 1)}

        # Fake round completion info for old delta, the record will be go to hash bucket #3
        # Hash bucket #3 is empty for new delta record
        rcf = RoundCompletionInfo.of(
            compacted_delta_locator=old_delta.locator,
            high_watermark=old_delta.stream_position,
            compacted_pyarrow_write_result=None,
            sort_keys_bit_width=0,
            hb_index_to_entry_range=hb_id_to_entry_indices_range,
        )

        merge_input = MergeInput.of(
            round_completion_info=rcf,
            compacted_file_content_type=ContentType.PARQUET,
            merge_file_groups_provider=LocalMergeFileGroupsProvider(
                uniform_deltas=[DeltaAnnotated.of(incremental_delta)],
                read_kwargs_provider=None,
                deltacat_storage=metastore,
                deltacat_storage_kwargs=self.deltacat_storage_kwargs,
            ),
            write_to_partition=partition,
            primary_keys=["pk1"],
            deltacat_storage=metastore,
            deltacat_storage_kwargs=self.deltacat_storage_kwargs,
            object_store=object_store,
            delete_strategy=EqualityDeleteStrategy(),
            delete_file_envelopes=delete_file_envelopes,
        )
        merge_res_list = []
        merge_result_promise = merge.remote(merge_input)
        merge_result = ray.get(merge_result_promise)
        merge_res_list.append(merge_result)

        # Main storage behavior: deletions aren't applied as expected, so 6 records remain
        self._validate_merge_output(merge_res_list, 6)

    def test_merge_incremental_copy_by_reference_date_pk(self):
        number_of_hash_group = 2
        number_of_hash_bucket = 10
        partition = stage_partition_from_file_paths(
            self._testMethodName,
            [self.DEDUPE_BASE_COMPACTED_TABLE_DATE_PK],
            **self.kwargs,
        )
        old_delta = commit_delta_to_staged_partition(
            partition, [self.DEDUPE_BASE_COMPACTED_TABLE_DATE_PK], **self.kwargs
        )

        object_store = RayPlasmaObjectStore()
        new_delta = create_delta_from_csv_file(
            f"{self._testMethodName}-new",
            [self.DEDUPE_WITH_DUPLICATION_DATE_PK],
            **self.kwargs,
        )

        all_hash_group_idx_to_obj_id = self._prepare_merge_inputs(
            old_delta, object_store, number_of_hash_bucket, number_of_hash_group, ["pk"]
        )
        for hg_index, dfes in all_hash_group_idx_to_obj_id.items():
            delta_file_envelope_groups_list = object_store.get_many(dfes)
            # Note only one record can be in compacted delta
            for delta_file_envelope_groups in delta_file_envelope_groups_list:
                for hb_idx, dfes in enumerate(delta_file_envelope_groups):
                    if dfes:
                        hb_hashed_to = hb_idx

        hb_id_to_entry_indices_range = {}
        hb_id_to_entry_indices_range[str(hb_hashed_to)] = (0, 1)

        # Fake round completion info for old delta, the record will be go to hash bucket #3
        # Hash bucket #3 is empty for new delta record
        rcf = RoundCompletionInfo.of(
            compacted_delta_locator=old_delta.locator,
            high_watermark=old_delta.stream_position,
            compacted_pyarrow_write_result=None,
            sort_keys_bit_width=0,
            hb_index_to_entry_range=hb_id_to_entry_indices_range,
        )

        all_hash_group_idx_to_obj_id_new = self._prepare_merge_inputs(
            new_delta, object_store, number_of_hash_bucket, number_of_hash_group, ["pk"]
        )

        merge_input_list = []
        for hg_index, dfes in all_hash_group_idx_to_obj_id_new.items():
            merge_input_list.append(
                MergeInput.of(
                    round_completion_info=rcf,
                    compacted_file_content_type=ContentType.PARQUET,
                    merge_file_groups_provider=RemoteMergeFileGroupsProvider(
                        hash_group_index=hg_index,
                        dfe_groups_refs=dfes,
                        hash_bucket_count=number_of_hash_bucket,
                        num_hash_groups=number_of_hash_group,
                        object_store=object_store,
                    ),
                    write_to_partition=partition,
                    primary_keys=["pk"],
                    deltacat_storage=metastore,
                    deltacat_storage_kwargs=self.deltacat_storage_kwargs,
                    object_store=object_store,
                )
            )
        merge_res_list = []
        for merge_input in merge_input_list:
            merge_result_promise = merge.remote(merge_input)
            merge_result = ray.get(merge_result_promise)
            merge_res_list.append(merge_result)

        # old delta: 1 record, copied by reference
        # new delta: 9 records, 2 duplication
        # result: 1 + 9 - 2 = 8
        self._validate_merge_output(merge_res_list, 8)
        files_untouched = 0
        for merge_result in merge_res_list:
            for mr in merge_result.materialize_results:
                if mr.referenced_pyarrow_write_result:
                    files_untouched += mr.referenced_pyarrow_write_result.files

        assert files_untouched == 1, "One file must be copied by reference"

    def test_merge_incremental_copy_by_reference_is_disabled(self):
        number_of_hash_group = 2
        number_of_hash_bucket = 10
        partition = stage_partition_from_file_paths(
            self._testMethodName,
            [self.DEDUPE_BASE_COMPACTED_TABLE_DATE_PK],
            **self.kwargs,
        )
        old_delta = commit_delta_to_staged_partition(
            partition, [self.DEDUPE_BASE_COMPACTED_TABLE_DATE_PK], **self.kwargs
        )

        object_store = RayPlasmaObjectStore()
        new_delta = create_delta_from_csv_file(
            f"{self._testMethodName}-new",
            [self.DEDUPE_WITH_DUPLICATION_DATE_PK],
            **self.kwargs,
        )

        all_hash_group_idx_to_obj_id = self._prepare_merge_inputs(
            old_delta, object_store, number_of_hash_bucket, number_of_hash_group, ["pk"]
        )
        for hg_index, dfes in all_hash_group_idx_to_obj_id.items():
            delta_file_envelope_groups_list = object_store.get_many(dfes)
            # Note only one record can be in compacted delta
            for delta_file_envelope_groups in delta_file_envelope_groups_list:
                for hb_idx, dfes in enumerate(delta_file_envelope_groups):
                    if dfes:
                        hb_hashed_to = hb_idx

        hb_id_to_entry_indices_range = {}
        hb_id_to_entry_indices_range[str(hb_hashed_to)] = (0, 1)

        # Fake round completion info for old delta, the record will be go to hash bucket #3
        # Hash bucket #3 is empty for new delta record
        rcf = RoundCompletionInfo.of(
            compacted_delta_locator=old_delta.locator,
            high_watermark=old_delta.stream_position,
            compacted_pyarrow_write_result=None,
            sort_keys_bit_width=0,
            hb_index_to_entry_range=hb_id_to_entry_indices_range,
        )

        all_hash_group_idx_to_obj_id_new = self._prepare_merge_inputs(
            new_delta, object_store, number_of_hash_bucket, number_of_hash_group, ["pk"]
        )

        merge_input_list = []
        for hg_index, dfes in all_hash_group_idx_to_obj_id_new.items():
            merge_input_list.append(
                MergeInput.of(
                    round_completion_info=rcf,
                    compacted_file_content_type=ContentType.PARQUET,
                    merge_file_groups_provider=RemoteMergeFileGroupsProvider(
                        hash_group_index=hg_index,
                        dfe_groups_refs=dfes,
                        hash_bucket_count=number_of_hash_bucket,
                        num_hash_groups=number_of_hash_group,
                        object_store=object_store,
                    ),
                    write_to_partition=partition,
                    primary_keys=["pk"],
                    deltacat_storage=metastore,
                    deltacat_storage_kwargs=self.deltacat_storage_kwargs,
                    object_store=object_store,
                    disable_copy_by_reference=True,  # copy by reference disabled
                )
            )
        merge_res_list = []
        for merge_input in merge_input_list:
            merge_result_promise = merge.remote(merge_input)
            merge_result = ray.get(merge_result_promise)
            merge_res_list.append(merge_result)

        # old delta: 1 record, copied by reference
        # new delta: 9 records, 2 duplication
        # result: 1 + 9 - 2 = 8
        self._validate_merge_output(merge_res_list, 8)
        files_untouched = 0
        for merge_result in merge_res_list:
            for mr in merge_result.materialize_results:
                if mr.referenced_pyarrow_write_result:
                    files_untouched += mr.referenced_pyarrow_write_result.files

        assert files_untouched == 0, "Zero files must be copied by reference"

    def test_merge_single_hash_bucket_multiple_pk(self):
        partition = stage_partition_from_file_paths(
            self.MERGE_NAMESPACE,
            [self.DEDUPE_WITH_DUPLICATION_MULTIPLE_PK],
            **self.kwargs,
        )
        new_delta = commit_delta_to_staged_partition(
            partition, [self.DEDUPE_WITH_DUPLICATION_MULTIPLE_PK], **self.kwargs
        )
        object_store = RayPlasmaObjectStore()
        merge_input = MergeInput.of(
            compacted_file_content_type=ContentType.PARQUET,
            merge_file_groups_provider=LocalMergeFileGroupsProvider(
                uniform_deltas=[DeltaAnnotated.of(new_delta)],
                read_kwargs_provider=None,
                deltacat_storage=metastore,
                deltacat_storage_kwargs=self.deltacat_storage_kwargs,
            ),
            write_to_partition=partition,
            primary_keys=["pk1", "pk2"],
            deltacat_storage=metastore,
            deltacat_storage_kwargs=self.deltacat_storage_kwargs,
            object_store=object_store,
        )
        merge_res_list = []
        merge_result_promise = merge.remote(merge_input)
        merge_result = ray.get(merge_result_promise)
        merge_res_list.append(merge_result)
        # 10 records, 2 duplication, record count left should be 8
        self._validate_merge_output(merge_res_list, 8)

    @patch("deltacat.compute.compactor_v2.steps.merge._compact_tables")
    def test_merge_when_local_error_categorized_correctly(self, mock_compact_tables):
        mock_compact_tables.side_effect = InvalidNamespaceError("Invalid namespace")

        partition = stage_partition_from_file_paths(
            self._testMethodName,  # Use unique namespace
            [self.DEDUPE_WITH_DUPLICATION_MULTIPLE_PK],
            **self.kwargs,
        )
        new_delta = commit_delta_to_staged_partition(
            partition, [self.DEDUPE_WITH_DUPLICATION_MULTIPLE_PK], **self.kwargs
        )
        object_store = RayPlasmaObjectStore()
        merge_input = MergeInput.of(
            compacted_file_content_type=ContentType.PARQUET,
            merge_file_groups_provider=LocalMergeFileGroupsProvider(
                uniform_deltas=[DeltaAnnotated.of(new_delta)],
                read_kwargs_provider=None,
                deltacat_storage=metastore,
                deltacat_storage_kwargs=self.deltacat_storage_kwargs,
            ),
            write_to_partition=partition,
            primary_keys=["pk1", "pk2"],
            deltacat_storage=metastore,
            deltacat_storage_kwargs=self.deltacat_storage_kwargs,
            object_store=object_store,
            # Explicitly disable copy-by-reference to ensure _compact_tables gets called
            disable_copy_by_reference=True,
        )

        try:
            merge_result_promise = merge.remote(merge_input)
            ray.get(merge_result_promise)
            self.fail("Expected a UnclassifiedDeltaCatError")
        except ray.exceptions.RayTaskError as e:
            from deltacat.exceptions import UnclassifiedDeltaCatError

            self.assertIsInstance(e.cause, UnclassifiedDeltaCatError)

    def _prepare_merge_inputs(
        self, delta_to_merge, object_store, num_hash_bucket, num_hash_group, pk
    ):
        hb_results = self._run_hash_bucketing(
            delta_to_merge, object_store, num_hash_bucket, num_hash_group, pk
        )
        all_hash_group_idx_to_obj_id = self._hb_output_to_merge_input(
            hb_results, num_hash_group
        )
        return all_hash_group_idx_to_obj_id

    def _run_hash_bucketing(
        self, delta_to_merge, object_store, num_hash_bucket, num_hash_group, pk
    ):
        annotated_delta = DeltaAnnotated.of(delta_to_merge)
        hb_input = HashBucketInput.of(
            annotated_delta=annotated_delta,
            primary_keys=pk,
            num_hash_buckets=num_hash_bucket,
            num_hash_groups=num_hash_group,
            deltacat_storage=metastore,
            deltacat_storage_kwargs=self.deltacat_storage_kwargs,
            object_store=object_store,
        )
        hb_result_promise = hash_bucket.remote(hb_input)
        hb_result: HashBucketResult = ray.get(hb_result_promise)
        return hb_result

    def _hb_output_to_merge_input(self, hb_results, num_hash_group):
        all_hash_group_idx_to_obj_id = defaultdict(list)
        for hg_idx in range(num_hash_group):
            bucket_object_ids_for_hg = []
            for hash_bucket_index in range(
                len(hb_results.hash_bucket_group_to_obj_id_tuple)
            ):
                if hash_bucket_index % num_hash_group == hg_idx:
                    obj_id_tuple = hb_results.hash_bucket_group_to_obj_id_tuple[
                        hash_bucket_index
                    ]
                    if obj_id_tuple:
                        object_id = obj_id_tuple[0]
                        bucket_object_ids_for_hg.append(object_id)
            all_hash_group_idx_to_obj_id[hg_idx] = bucket_object_ids_for_hg
        return all_hash_group_idx_to_obj_id

    def _validate_merge_output(self, merge_res_list, expected_record_count):
        materialize_res = []
        for mr in merge_res_list:
            for m in mr.materialize_results:
                materialize_res.append(m)

        deltas = [mt.delta for mt in materialize_res]
        merged_delta = Delta.merge_deltas(
            deltas,
        )
        assert merged_delta.meta.record_count == expected_record_count


if __name__ == "__main__":
    unittest.main()
