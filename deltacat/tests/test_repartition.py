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
What test cases to cover:
0. Given zero values in ranges, check if error is raised
1. Given one value in ranges, check if two range deltas are produced
2. Given two values in ranges, check if three range deltas are produced
3. No enough records exist for all ranges, i.e., some range will have empty file, returned range deltas should be less
4. Test column doens't exist in any table, error should be raised
5. Test column exist in some table, but not all, error should be raised
6. Test unsorted ranges, e.g., -1, 3, -2, 4,
7. Test ranges that may have same values, e.g., 1,1,2,2
8. Test
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
        self.repartitioned_file_content_type = ContentType.PARQUET
        self.deltacat_storage = MagicMock()

    def test_repartition_range(self):
        result = repartition_range(
            self.tables,
            self.destination_partition,
            self.repartition_args,
            self.max_records_per_output_file,
            self.repartitioned_file_content_type,
            self.deltacat_storage,
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
                self.repartitioned_file_content_type,
                self.deltacat_storage,
            )


if __name__ == "__main__":
    unittest.main()
