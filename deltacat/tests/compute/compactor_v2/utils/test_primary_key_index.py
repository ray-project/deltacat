import pyarrow as pa
from deltacat.compute.compactor_v2.utils.primary_key_index import (
    group_by_pk_hash_bucket,
)


class TestGroupByPkHashBucket:
    def test_sanity(self):
        record = pa.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
        pk = pa.array(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"])
        record_batch = pa.RecordBatch.from_arrays([record, pk], names=["record", "pk"])
        table = pa.Table.from_batches([record_batch])
        grouped_array = group_by_pk_hash_bucket(table, 3, ["pk"])

        assert len(grouped_array) == 3
        total_records = 0
        for arr in grouped_array:
            if arr is not None:
                total_records += len(arr[1])

        assert total_records == len(table)

    def test_when_record_batches_exceed_int_max_size(self):
        record = pa.array(["12bytestring" * 90_000_000])
        record_batch = pa.RecordBatch.from_arrays([record], names=["pk"])
        table = pa.Table.from_batches([record_batch, record_batch])

        grouped_array = group_by_pk_hash_bucket(table, 3, ["pk"])

        assert len(grouped_array) == 3
        # two record batches are preserved as combining them
        # would exceed 2GB.
        assert len(grouped_array[2].to_batches()) == 2

    def test_when_record_batches_less_than_int_max_size(self):
        record = pa.array(["12bytestring" * 90_000])
        record_batch = pa.RecordBatch.from_arrays([record], names=["pk"])
        table = pa.Table.from_batches([record_batch, record_batch])

        grouped_array = group_by_pk_hash_bucket(table, 3, ["pk"])

        assert len(grouped_array) == 3
        # Combined the arrays into one record batch as the size
        # would not exceed 2GB.
        assert len(grouped_array[1].to_batches()) == 1
