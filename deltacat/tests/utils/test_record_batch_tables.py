import unittest

import pyarrow as pa

from deltacat.utils.pyarrow import RecordBatchTables


class TestRecordBatchTables(unittest.TestCase):
    def setUp(self) -> None:
        self.column_names = ["pk", "sk"]

    def test_single_table_with_batches_and_remainder(self):
        min_records_batch = 8
        bt = RecordBatchTables(min_records_batch)
        col1 = pa.array([i for i in range(10)])
        col2 = pa.array(["test"] * 10)
        test_table = pa.Table.from_arrays([col1, col2], names=self.column_names)
        bt.append(test_table)
        self.assertTrue(bt.has_batches())
        self.assertEqual(bt.batched_record_count, 8)
        self.assertTrue(_is_gte_batch_size_and_divisible(bt, min_records_batch))
        self.assertTrue(bt.has_remaining())
        self.assertEqual(bt.remaining_record_count, 2)
        self.assertTrue(_is_sorted(bt, self.column_names[0]))

    def test_single_table_with_no_remainder(self):
        min_records_batch = 5
        bt = RecordBatchTables(min_records_batch)
        col1 = pa.array([i for i in range(min_records_batch)])
        col2 = pa.array(["test"] * min_records_batch)
        test_table = pa.Table.from_arrays([col1, col2], names=self.column_names)
        bt.append(test_table)
        self.assertFalse(bt.has_remaining())
        self.assertTrue(_is_sorted(bt, self.column_names[0]))

    def test_single_table_with_only_batches(self):
        min_records_batch = 10
        bt = RecordBatchTables(min_records_batch)
        col1 = pa.array([i for i in range(min_records_batch)])
        col2 = pa.array(["test"] * min_records_batch)
        test_table = pa.Table.from_arrays([col1, col2], names=self.column_names)
        bt.append(test_table)
        self.assertTrue(bt.has_batches())
        self.assertTrue(_is_gte_batch_size_and_divisible(bt, min_records_batch))
        self.assertFalse(bt.has_remaining())
        self.assertEqual(bt.batched_record_count, 10)
        self.assertEqual(bt.remaining_record_count, 0)
        self.assertTrue(_is_sorted(bt, self.column_names[0]))

    def test_single_table_with_only_remainder(self):
        min_records_batch = 11
        bt = RecordBatchTables(min_records_batch)
        col1 = pa.array([i for i in range(10)])
        col2 = pa.array(["test"] * 10)
        test_table = pa.Table.from_arrays([col1, col2], names=self.column_names)
        bt.append(test_table)
        self.assertFalse(bt.has_batches())
        self.assertTrue(bt.has_remaining())
        self.assertEqual(bt.batched_record_count, 0)
        self.assertEqual(bt.remaining_record_count, 10)
        self.assertTrue(_is_sorted(bt, self.column_names[0]))

    def test_grouped_tables_with_only_remainder(self):
        min_records_batch = 600
        test_table_num_records = 100
        grouped_tables = [
            pa.Table.from_arrays(
                [
                    pa.array(
                        [
                            i
                            for i in range(
                                i * test_table_num_records,
                                (i + 1) * test_table_num_records,
                            )
                        ]
                    ),
                    pa.array(["foo"] * test_table_num_records),
                ],
                names=self.column_names,
            )
            for i in range(5)
        ]

        bt = RecordBatchTables(min_records_batch)
        for table in grouped_tables:
            bt.append(table)
        self.assertFalse(bt.has_batches())
        self.assertTrue(bt.has_remaining())
        self.assertEqual(bt.remaining_record_count, 500)
        self.assertLess(bt.remaining_record_count, min_records_batch)
        self.assertTrue(_is_sorted(bt, self.column_names[0]))

    def test_grouped_tables_with_batches_and_remainder(self):
        min_records_batch = 450
        test_table_num_records = 100
        grouped_tables = [
            pa.Table.from_arrays(
                [
                    pa.array(
                        [
                            i
                            for i in range(
                                i * test_table_num_records,
                                (i + 1) * test_table_num_records,
                            )
                        ]
                    ),
                    pa.array(["foo"] * 100),
                ],
                names=self.column_names,
            )
            for i in range(5)
        ]

        bt = RecordBatchTables(min_records_batch)
        for table in grouped_tables:
            bt.append(table)
        self.assertTrue(bt.has_batches())
        self.assertTrue(_is_gte_batch_size_and_divisible(bt, min_records_batch))
        self.assertTrue(bt.has_remaining())
        self.assertEqual(bt.batched_record_count, 450)
        self.assertEqual(bt.remaining_record_count, 50)
        self.assertTrue(bt.batched_record_count % min_records_batch == 0)
        self.assertLess(bt.remaining_record_count, min_records_batch)
        self.assertTrue(_is_sorted(bt, self.column_names[0]))

    def test_grouped_tables_with_smaller_batch_size_than_table_records(self):
        min_records_batch = 5
        test_table_num_records = 39
        grouped_tables = [
            pa.Table.from_arrays(
                [
                    pa.array(
                        [
                            i
                            for i in range(
                                i * test_table_num_records,
                                (i + 1) * test_table_num_records,
                            )
                        ]
                    ),
                    pa.array(["foo"] * test_table_num_records),
                ],
                names=self.column_names,
            )
            for i in range(3)
        ]

        bt = RecordBatchTables(min_records_batch)
        for table in grouped_tables:
            bt.append(table)
            self.assertTrue(_is_sorted(bt, self.column_names[0]))

        self.assertTrue(bt.has_batches())
        self.assertTrue(_is_gte_batch_size_and_divisible(bt, min_records_batch))
        self.assertEqual(bt.batched_record_count, 115)
        self.assertTrue(bt.batched_record_count % min_records_batch == 0)
        self.assertTrue(bt.has_remaining())
        self.assertEqual(bt.remaining_record_count, 2)
        self.assertLess(bt.remaining_record_count, min_records_batch)
        self.assertTrue(_is_sorted(bt, self.column_names[0]))

    def test_batched_tables_factory_from_input_tables(self):
        min_records_batch = 5
        test_table_num_records = 39
        grouped_tables = [
            pa.Table.from_arrays(
                [
                    pa.array(
                        [
                            i
                            for i in range(
                                i * test_table_num_records,
                                (i + 1) * test_table_num_records,
                            )
                        ]
                    ),
                    pa.array(["foo"] * test_table_num_records),
                ],
                names=self.column_names,
            )
            for i in range(3)
        ]
        bt = RecordBatchTables.from_tables(grouped_tables, min_records_batch)
        self.assertTrue(type(bt), RecordBatchTables)
        self.assertTrue(bt.has_batches())
        self.assertTrue(_is_gte_batch_size_and_divisible(bt, min_records_batch))
        self.assertEqual(bt.batched_record_count, 115)
        self.assertTrue(bt.batched_record_count % min_records_batch == 0)
        self.assertTrue(bt.has_remaining())
        self.assertEqual(bt.remaining_record_count, 2)
        self.assertLess(bt.remaining_record_count, min_records_batch)
        self.assertTrue(_is_sorted(bt, self.column_names[0]))

    def test_clear(self):
        min_records_batch = 8
        bt = RecordBatchTables(min_records_batch)
        col1 = pa.array([i for i in range(10)])
        col2 = pa.array(["test"] * 10)
        test_table = pa.Table.from_arrays([col1, col2], names=self.column_names)
        bt.append(test_table)
        self.assertTrue(bt.has_batches())
        self.assertTrue(_is_gte_batch_size_and_divisible(bt, min_records_batch))
        self.assertEqual(bt.batched_record_count, 8)

        bt.clear_batches()
        self.assertFalse(bt.has_batches())
        self.assertEqual(bt.batched_record_count, 0)

    def test_append_after_clear(self):
        min_records_batch = 8
        bt = RecordBatchTables(min_records_batch)
        col1 = pa.array([i for i in range(10)])
        col2 = pa.array(["test"] * 10)
        test_table = pa.Table.from_arrays([col1, col2], names=self.column_names)
        bt.append(test_table)
        self.assertTrue(bt.has_batches())
        self.assertTrue(_is_gte_batch_size_and_divisible(bt, min_records_batch))
        self.assertEqual(bt.batched_record_count, 8)
        prev_remainder_records = bt.remaining_record_count
        self.assertEqual(bt.remaining_record_count, 2)

        bt.clear_batches()
        self.assertFalse(bt.has_batches())
        self.assertEqual(bt.batched_record_count, 0)

        col1 = pa.array([i for i in range(10, 20)])
        col2 = pa.array(["test"] * 10)
        test_table = pa.Table.from_arrays([col1, col2], names=self.column_names)
        bt.append(test_table)

        self.assertEqual(bt.batched_record_count, 8)
        self.assertEqual(bt.remaining_record_count, 4)
        self.assertNotEqual(prev_remainder_records, bt.remaining_record_count)
        self.assertTrue(_is_sorted(bt, self.column_names[0]))

        bt.clear_remaining()
        self.assertFalse(bt.has_remaining())
        self.assertTrue(bt.remaining_record_count == 0)

    def test_evict(self):
        min_records_batch = 8
        bt = RecordBatchTables(min_records_batch)
        col1 = pa.array([i for i in range(10)])
        col2 = pa.array(["test"] * 10)
        test_table = pa.Table.from_arrays([col1, col2], names=self.column_names)
        bt.append(test_table)
        self.assertTrue(bt.has_batches())
        self.assertTrue(_is_gte_batch_size_and_divisible(bt, min_records_batch))
        self.assertTrue(bt.has_remaining())
        self.assertEqual(bt.batched_record_count, 8)
        self.assertEqual(bt.remaining_record_count, 2)
        prev_batched_records = bt.batched_record_count

        evicted_tables = bt.evict()
        self.assertFalse(bt.has_batches())
        self.assertTrue(bt.has_remaining())
        self.assertEqual(bt.batched_record_count, 0)
        self.assertEqual(bt.remaining_record_count, 2)
        self.assertEqual(sum([len(t) for t in evicted_tables]), prev_batched_records)


def _is_sorted(batched_tables: RecordBatchTables, sort_key: str):
    merged_table = pa.concat_tables(
        [*batched_tables.batched, *batched_tables.remaining]
    )
    explicitly_sorted_merged_table = merged_table.sort_by([(sort_key, "ascending")])
    return explicitly_sorted_merged_table == merged_table


def _is_gte_batch_size_and_divisible(
    batched_tables: RecordBatchTables, min_records_batch: int
):
    return all(
        [
            len(table) // min_records_batch > 0 and len(table) % min_records_batch == 0
            for table in batched_tables.batched
        ]
    )


if __name__ == "__main__":
    unittest.main()
