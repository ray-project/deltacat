import unittest
from unittest import mock

from deltacat.tests.compute.conftest import (
    create_local_deltacat_storage_file,
    clean_up_local_deltacat_storage_file,
)
from deltacat.tests.test_utils.constants import TEST_UPSERT_DELTA


class TestFitInputDeltas(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module_patcher = mock.patch.dict("sys.modules", {"ray": mock.MagicMock()})
        cls.module_patcher.start()

        from deltacat.compute.compactor.model.compaction_session_audit_info import (
            CompactionSessionAuditInfo,
        )

        cls.kwargs_for_local_deltacat_storage = create_local_deltacat_storage_file()

        cls.COMPACTION_AUDIT = CompactionSessionAuditInfo("1.0", "2.3", "test")

        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.module_patcher.stop()
        clean_up_local_deltacat_storage_file(cls.kwargs_for_local_deltacat_storage)

    def test_sanity(self):
        from deltacat.compute.compactor.utils import io
        import deltacat.tests.local_deltacat_storage as ds

        (
            delta_list,
            hash_bucket_count,
            high_watermark,
            require_multiple_rounds,
        ) = io.fit_input_deltas(
            [TEST_UPSERT_DELTA],
            {"CPU": 1, "memory": 20000000},
            self.COMPACTION_AUDIT,
            None,
            ds,
            self.kwargs_for_local_deltacat_storage,
        )

        self.assertIsNotNone(hash_bucket_count)
        self.assertTrue(1, len(delta_list))
        self.assertIsNotNone(high_watermark)
        self.assertFalse(require_multiple_rounds)
        self.assertIsNotNone(hash_bucket_count, self.COMPACTION_AUDIT.hash_bucket_count)
        self.assertIsNotNone(self.COMPACTION_AUDIT.input_file_count)
        self.assertIsNotNone(self.COMPACTION_AUDIT.input_size_bytes)
        self.assertIsNotNone(self.COMPACTION_AUDIT.total_cluster_memory_bytes)

    def test_when_hash_bucket_count_overridden(self):
        from deltacat.compute.compactor.utils import io
        import deltacat.tests.local_deltacat_storage as ds

        (
            delta_list,
            hash_bucket_count,
            high_watermark,
            require_multiple_rounds,
        ) = io.fit_input_deltas(
            [TEST_UPSERT_DELTA],
            {"CPU": 1, "memory": 20000000},
            self.COMPACTION_AUDIT,
            20,
            ds,
            self.kwargs_for_local_deltacat_storage,
        )

        self.assertEqual(20, hash_bucket_count)
        self.assertEqual(1, len(delta_list))
        self.assertIsNotNone(high_watermark)
        self.assertFalse(require_multiple_rounds)

    def test_when_not_enough_memory_splits_manifest_entries(self):
        from deltacat.compute.compactor.utils import io
        import deltacat.tests.local_deltacat_storage as ds

        (
            delta_list,
            hash_bucket_count,
            high_watermark,
            require_multiple_rounds,
        ) = io.fit_input_deltas(
            [TEST_UPSERT_DELTA],
            {"CPU": 2, "memory": 10},
            self.COMPACTION_AUDIT,
            20,
            ds,
            self.kwargs_for_local_deltacat_storage,
        )

        self.assertIsNotNone(hash_bucket_count)
        self.assertTrue(2, len(delta_list))
        self.assertIsNotNone(high_watermark)
        self.assertFalse(require_multiple_rounds)

    def test_when_no_input_deltas(self):
        from deltacat.compute.compactor.utils import io
        import deltacat.tests.local_deltacat_storage as ds

        with self.assertRaises(AssertionError):
            io.fit_input_deltas(
                [],
                {"CPU": 100, "memory": 20000.0},
                self.COMPACTION_AUDIT,
                None,
                ds,
                self.kwargs_for_local_deltacat_storage,
            )

    def test_when_cpu_resources_is_not_passed(self):
        from deltacat.compute.compactor.utils import io
        import deltacat.tests.local_deltacat_storage as ds

        with self.assertRaises(KeyError):
            io.fit_input_deltas(
                [],
                {},
                self.COMPACTION_AUDIT,
                None,
                ds,
                self.kwargs_for_local_deltacat_storage,
            )
