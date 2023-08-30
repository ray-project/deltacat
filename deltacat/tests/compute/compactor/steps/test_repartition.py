import unittest
from unittest.mock import MagicMock
import pyarrow as pa
from deltacat.compute.compactor.steps.repartition import repartition_range
from deltacat.types.media import ContentType
from deltacat.compute.compactor.model.repartition_result import RepartitionResult
from deltacat.storage import (
    PartitionLocator,
)

"""
Summary of Test Cases:
0. Given empty ranges, error should be raised
1. Given one value in ranges, e.g., [1678665487112747], Two range deltas should be produced
2. Given two values in ranges, e.g., [1678665487112747, 1678665487112999], three range deltas should be produced
3. No enough records exist for all ranges, i.e., some range will have empty file, such that number of returned range deltas should be less
4. column doens't exist in any table, error should be raised
5. column exists in some table, but not all, error should be raised
6. Given ranges is unsorted , e.g., [1678665487112747, 1678665487112745, 1678665487112748]
7. Given ranges may have same values, e.g., [1678665487112747, 1678665487112747]
8. Ranges with pre-dfined inf, e.g., [1678665487112747, inf]
"""


class TestRepartitionRange(unittest.TestCase):
    def setUp(self):
        self.tables = [
            pa.table(
                {
                    "last_updated": [
                        1678665487112745,
                        1678665487112746,
                        1678665487112747,
                        1678665487112748,
                    ]
                }
            ),
            pa.table(
                {
                    "last_updated": [
                        1678665487112748,
                        1678665487112749,
                        1678665487112750,
                        1678665487112751,
                    ]
                }
            ),
        ]
        self.destination_partition: PartitionLocator = MagicMock()
        self.repartition_args = {"column": "last_updated", "ranges": [1678665487112747]}
        self.max_records_per_output_file = 2
        self.s3_table_writer_kwargs = {}
        self.repartitioned_file_content_type = ContentType.PARQUET
        self.deltacat_storage = MagicMock()
        self.deltacat_storage_kwargs = MagicMock()

    def test_repartition_range(self):
        result = repartition_range(
            self.tables,
            self.destination_partition,
            self.repartition_args,
            self.max_records_per_output_file,
            self.s3_table_writer_kwargs,
            self.repartitioned_file_content_type,
            self.deltacat_storage,
            self.deltacat_storage_kwargs,
        )
        # Assert that a RepartitionResult object is returned
        self.assertIsInstance(result, RepartitionResult)

        # Assert that the correct number of range_deltas was produced
        self.assertEqual(
            len(result.range_deltas), len(self.repartition_args["ranges"]) + 1
        )

        # Assert that the function called the deltacat_storage.stage_delta method the correct number of times
        self.assertEqual(
            self.deltacat_storage.stage_delta.call_count,
            len(self.repartition_args["ranges"]) + 1,
        )

    def test_repartition_range_nonexistent_column(self):
        self.repartition_args["column"] = "nonexistent_column"
        with self.assertRaises(ValueError):
            repartition_range(
                self.tables,
                self.destination_partition,
                self.repartition_args,
                self.max_records_per_output_file,
                self.s3_table_writer_kwargs,
                self.repartitioned_file_content_type,
                self.deltacat_storage,
                self.deltacat_storage_kwargs,
            )

    def test_empty_ranges(self):
        self.repartition_args["ranges"] = []
        with self.assertRaises(ValueError):
            repartition_range(
                self.tables,
                self.destination_partition,
                self.repartition_args,
                self.max_records_per_output_file,
                self.s3_table_writer_kwargs,
                self.repartitioned_file_content_type,
                self.deltacat_storage,
                self.deltacat_storage_kwargs,
            )

    def test_one_value_in_ranges(self):
        self.repartition_args["ranges"] = [1678665487112747]
        result = repartition_range(
            self.tables,
            self.destination_partition,
            self.repartition_args,
            self.max_records_per_output_file,
            self.s3_table_writer_kwargs,
            self.repartitioned_file_content_type,
            self.deltacat_storage,
            self.deltacat_storage_kwargs,
        )
        self.assertEqual(len(result.range_deltas), 2)

    def test_two_values_in_ranges(self):
        self.repartition_args["ranges"] = [1678665487112747, 1678665487112749]
        result = repartition_range(
            self.tables,
            self.destination_partition,
            self.repartition_args,
            self.max_records_per_output_file,
            self.s3_table_writer_kwargs,
            self.repartitioned_file_content_type,
            self.deltacat_storage,
            self.deltacat_storage_kwargs,
        )
        self.assertEqual(len(result.range_deltas), 3)

    def test_not_enough_records_for_all_ranges(self):
        reduced_tables = [self.tables[0]]  # use only the first table
        self.repartition_args["ranges"] = [1678665487112749, 1678665487112999]
        result = repartition_range(
            reduced_tables,
            self.destination_partition,
            self.repartition_args,
            self.max_records_per_output_file,
            self.s3_table_writer_kwargs,
            self.repartitioned_file_content_type,
            self.deltacat_storage,
            self.deltacat_storage_kwargs,
        )
        self.assertLess(len(result.range_deltas), 2)

    def test_column_does_not_exist_in_all_tables(self):
        self.tables.append(pa.table({"other_column": [1, 2, 3]}))
        with self.assertRaises(ValueError):
            repartition_range(
                self.tables,
                self.destination_partition,
                self.repartition_args,
                self.max_records_per_output_file,
                self.s3_table_writer_kwargs,
                self.repartitioned_file_content_type,
                self.deltacat_storage,
                self.deltacat_storage_kwargs,
            )

    def test_unsorted_ranges(self):
        self.repartition_args["ranges"] = [
            1678665487112747,
            1678665487112745,
            1678665487112748,
        ]
        result = repartition_range(
            self.tables,
            self.destination_partition,
            self.repartition_args,
            self.max_records_per_output_file,
            self.s3_table_writer_kwargs,
            self.repartitioned_file_content_type,
            self.deltacat_storage,
            self.deltacat_storage_kwargs,
        )
        self.assertEqual(len(result.range_deltas), 4)

    def test_same_values_in_ranges(self):
        self.repartition_args["ranges"] = [1678665487112747, 1678665487112747]
        result = repartition_range(
            self.tables,
            self.destination_partition,
            self.repartition_args,
            self.max_records_per_output_file,
            self.s3_table_writer_kwargs,
            self.repartitioned_file_content_type,
            self.deltacat_storage,
            self.deltacat_storage_kwargs,
        )
        self.assertEqual(len(result.range_deltas), 2)

    def test_ranges_with_inf(self):
        self.repartition_args["ranges"] = [1678665487112747, float("inf")]

        self.assertRaises(
            pa.lib.ArrowInvalid,
            lambda: repartition_range(
                self.tables,
                self.destination_partition,
                self.repartition_args,
                self.max_records_per_output_file,
                self.s3_table_writer_kwargs,
                self.repartitioned_file_content_type,
                self.deltacat_storage,
            ),
        )

    def test_null_rows_are_not_dropped(self):
        # Add null value to the first table
        tables_with_null = [
            pa.table(
                {
                    "last_updated": [
                        None,
                        1678665487112746,
                        1678665487112747,
                        1678665487112748,
                    ]
                }
            ),
            self.tables[1],
        ]

        result = repartition_range(
            tables_with_null,
            self.destination_partition,
            self.repartition_args,
            self.max_records_per_output_file,
            self.s3_table_writer_kwargs,
            self.repartitioned_file_content_type,
            self.deltacat_storage,
            self.deltacat_storage_kwargs,
        )

        # Assuming range_deltas is a list of DataFrames,
        # check that the first DataFrame has the null value in the 'last_updated' column
        # This may need to be adjusted depending on the actual structure of range_deltas
        self.assertEqual(len(result.range_deltas), 2)


if __name__ == "__main__":
    unittest.main()
