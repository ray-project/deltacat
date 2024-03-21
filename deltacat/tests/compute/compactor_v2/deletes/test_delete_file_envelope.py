import pytest

import pyarrow as pa

from deltacat.storage import (
    DeltaType,
)
from deltacat.compute.compactor_v2.deletes.delete_file_envelope import (
    DeleteFileEnvelope,
)
from deltacat.compute.compactor.model.table_object_store import (
    LocalTableNOOPStorageStrategy,
    LocalTableRayObjectStoreReferenceStorageStrategy,
)


class TestDeleteFileEnvelope:
    def test_of_missing_required_args(self):
        with pytest.raises(ValueError):
            DeleteFileEnvelope.of(
                stream_position=None,
                delta_type=DeltaType.DELETE,
                table=None,
                delete_columns=["col1", "col2"],
            )

        with pytest.raises(ValueError):
            DeleteFileEnvelope.of(
                stream_position=1,
                delta_type=None,
                table=pa.table({"col1": [1, 2, 3]}),
                delete_columns=["col1", "col2"],
            )

        with pytest.raises(ValueError):
            DeleteFileEnvelope.of(
                stream_position=1,
                delta_type=DeltaType.DELETE,
                table=None,
                delete_columns=["col1", "col2"],
            )

    def test_of_valid_args(self):
        stream_position = 1
        delta_type = DeltaType.DELETE
        table = pa.table({"col1": [1, 2, 3]})
        delete_columns = ["col1", "col2"]
        file_index = 2
        is_src_delta = True
        file_record_count = 3
        delete_file_envelope = DeleteFileEnvelope.of(
            stream_position=stream_position,
            delta_type=delta_type,
            table=table,
            delete_columns=delete_columns,
            file_index=file_index,
            is_src_delta=is_src_delta,
            file_record_count=file_record_count,
        )

        assert delete_file_envelope.stream_position == stream_position
        assert delete_file_envelope.delta_type == delta_type
        assert delete_file_envelope.table.combine_chunks().equals(table)
        assert delete_file_envelope.delete_columns == delete_columns
        assert delete_file_envelope.file_index == file_index
        assert delete_file_envelope.is_src_delta == is_src_delta
        assert delete_file_envelope.file_record_count == file_record_count
        assert isinstance(
            delete_file_envelope.table_storage_strategy,
            LocalTableRayObjectStoreReferenceStorageStrategy,
        )

    def test_delete_columns_property(self):
        delete_columns = ["col1", "col2"]
        delete_file_envelope = DeleteFileEnvelope.of(
            stream_position=1,
            delta_type=DeltaType.DELETE,
            table=pa.table({"col1": [1, 2, 3]}),
            delete_columns=delete_columns,
        )
        assert delete_file_envelope.delete_columns == delete_columns

    def test_delete_columns_minimum_length(self):
        with pytest.raises(ValueError):
            DeleteFileEnvelope.of(
                stream_position=1,
                delta_type=None,
                table=pa.table({"col1": [1, 2, 3]}),
                delete_columns=[],
            )

    def test_table_storage_strategy_default(self):
        delete_file_envelope = DeleteFileEnvelope.of(
            stream_position=1,
            delta_type=DeltaType.DELETE,
            table=pa.table({"col1": [1, 2, 3]}),
            delete_columns=["col1", "col2"],
        )
        assert isinstance(
            delete_file_envelope.table_storage_strategy,
            LocalTableRayObjectStoreReferenceStorageStrategy,
        )

    def test_table_storage_strategy_override(self):
        table_storage_strategy = LocalTableNOOPStorageStrategy()
        delete_file_envelope = DeleteFileEnvelope.of(
            stream_position=1,
            delta_type=DeltaType.DELETE,
            table=pa.table({"col1": [1, 2, 3]}),
            delete_columns=["col1", "col2"],
            table_storage_strategy=table_storage_strategy,
        )
        assert isinstance(
            delete_file_envelope.table_storage_strategy, LocalTableNOOPStorageStrategy
        )
