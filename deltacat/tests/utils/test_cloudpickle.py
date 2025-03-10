from unittest import TestCase
from ray import cloudpickle
from deltacat.utils.cloudpickle import dump_into_chunks
import pyarrow as pa
import numpy as np


class TestDumpIntoChunks(TestCase):
    def test_dump_into_chunks_sanity(self):
        table = self._get_test_table()
        chunks = dump_into_chunks(
            table, table.nbytes // 100
        )  # divides into atleast 100 chunks
        serialized = cloudpickle.dumps(table)
        concatenated_serialized = self._concat_bytes_chunks(chunks)

        self.assertEqual(serialized, concatenated_serialized)
        self.assertGreater(len(chunks), 100)

    def test_dump_into_chunks_when_max_size_zero(self):
        table = self._get_test_table()
        self.assertRaises(AssertionError, lambda: dump_into_chunks(table, 0))

    def test_dump_into_chunks_when_max_size_greater_or_equal_table_size(self):
        table = self._get_test_table()
        chunks = dump_into_chunks(table, table.nbytes)
        serialized = cloudpickle.dumps(table)
        concatenated_serialized = self._concat_bytes_chunks(chunks)

        self.assertEqual(serialized, concatenated_serialized)
        self.assertLess(len(chunks), 3)

    def _get_test_table(self):
        col1 = np.random.randint(100000, size=100000)
        return pa.table({"pk_hash": col1, "some_col": col1, "some_col2": col1})

    def _concat_bytes_chunks(self, chunks):
        result = bytearray()
        for chunk in chunks:
            result.extend(chunk)

        return result
