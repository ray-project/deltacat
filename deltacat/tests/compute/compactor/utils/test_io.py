import pytest
from unittest import mock

from deltacat.tests.test_utils.constants import TEST_UPSERT_DELTA


@pytest.fixture(scope="module", autouse=True)
def mock_ray():
    """Mock ray module for all tests in this module"""
    module_patcher = mock.patch.dict("sys.modules", {"ray": mock.MagicMock()})
    module_patcher.start()
    yield
    module_patcher.stop()


@pytest.fixture
def compaction_audit():
    """Fixture for CompactionSessionAuditInfo"""
    from deltacat.compute.compactor.model.compaction_session_audit_info import (
        CompactionSessionAuditInfo,
    )

    return CompactionSessionAuditInfo("1.0", "2.3", "test")


def test_sanity(main_deltacat_storage_kwargs, compaction_audit):
    from deltacat.compute.compactor.utils import io
    from deltacat.storage import metastore

    (
        delta_list,
        hash_bucket_count,
        high_watermark,
        require_multiple_rounds,
    ) = io.fit_input_deltas(
        [TEST_UPSERT_DELTA],
        {"CPU": 1, "memory": 20000000},
        compaction_audit,
        None,
        metastore,
        main_deltacat_storage_kwargs,
    )

    assert hash_bucket_count is not None
    assert len(delta_list) == 1
    assert high_watermark is not None
    assert require_multiple_rounds is False
    assert compaction_audit.hash_bucket_count is not None
    assert compaction_audit.input_file_count is not None
    assert compaction_audit.input_size_bytes is not None
    assert compaction_audit.total_cluster_memory_bytes is not None


def test_when_hash_bucket_count_overridden(
    main_deltacat_storage_kwargs, compaction_audit
):
    from deltacat.compute.compactor.utils import io
    from deltacat.storage import metastore

    (
        delta_list,
        hash_bucket_count,
        high_watermark,
        require_multiple_rounds,
    ) = io.fit_input_deltas(
        [TEST_UPSERT_DELTA],
        {"CPU": 1, "memory": 20000000},
        compaction_audit,
        20,
        metastore,
        main_deltacat_storage_kwargs,
    )

    assert hash_bucket_count == 20
    assert len(delta_list) == 1
    assert high_watermark is not None
    assert require_multiple_rounds is False


def test_when_not_enough_memory_splits_manifest_entries(
    main_deltacat_storage_kwargs, compaction_audit
):
    from deltacat.compute.compactor.utils import io
    from deltacat.storage import metastore

    (
        delta_list,
        hash_bucket_count,
        high_watermark,
        require_multiple_rounds,
    ) = io.fit_input_deltas(
        [TEST_UPSERT_DELTA],
        {"CPU": 2, "memory": 10},
        compaction_audit,
        20,
        metastore,
        main_deltacat_storage_kwargs,
    )

    assert hash_bucket_count is not None
    assert len(delta_list) == 2
    assert high_watermark is not None
    assert require_multiple_rounds is False


def test_when_no_input_deltas(main_deltacat_storage_kwargs, compaction_audit):
    from deltacat.compute.compactor.utils import io
    from deltacat.storage import metastore

    with pytest.raises(AssertionError):
        io.fit_input_deltas(
            [],
            {"CPU": 100, "memory": 20000.0},
            compaction_audit,
            None,
            metastore,
            main_deltacat_storage_kwargs,
        )


def test_when_cpu_resources_is_not_passed(
    main_deltacat_storage_kwargs, compaction_audit
):
    from deltacat.compute.compactor.utils import io
    from deltacat.storage import metastore

    with pytest.raises(KeyError):
        io.fit_input_deltas(
            [],
            {},
            compaction_audit,
            None,
            metastore,
            main_deltacat_storage_kwargs,
        )
