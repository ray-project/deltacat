import unittest
from typing import Tuple

from deltacat.compute.stats.utils.intervals import merge_intervals, DeltaRange


class TestMergeIntervals(unittest.TestCase):
    def test_unbounded_start_range(self):
        intervals = sorted(merge_intervals(
            {(3, 9), (None, 15), (13, 30)}
        ))
        interval: Tuple[DeltaRange, DeltaRange] = intervals[0]
        self.assertEqual(interval[0], None)
        self.assertEqual(interval[1], 30)

    def test_unbounded_end_range(self):
        intervals = sorted(merge_intervals(
            {(3, 9), (2, None), (13, 30)}
        ))
        interval: Tuple[DeltaRange, DeltaRange] = intervals[0]
        self.assertEqual(interval[0], 2)
        self.assertEqual(interval[1], None)

    def test_unbounded_start_end_range(self):
        intervals = sorted(merge_intervals(
            {(None, None)}
        ))
        interval: Tuple[DeltaRange, DeltaRange] = intervals[0]
        self.assertEqual(interval[0], None)
        self.assertEqual(interval[1], None)

    def test_no_overlap_range(self):
        intervals = sorted(merge_intervals(
            {(3, 9), (11, 14), (19, 30)}
        ))
        interval1: Tuple[DeltaRange, DeltaRange] = intervals[0]
        interval2: Tuple[DeltaRange, DeltaRange] = intervals[1]
        interval3: Tuple[DeltaRange, DeltaRange] = intervals[2]
        self.assertEqual(interval1, (3, 9))
        self.assertEqual(interval2, (11, 14))
        self.assertEqual(interval3, (19, 30))

    def test_overlap_range(self):
        intervals = sorted(merge_intervals(
            {(3, 9), (9, 14), (14, 30)}
        ))
        interval1: Tuple[DeltaRange, DeltaRange] = intervals[0]
        self.assertEqual(interval1, (3, 30))

    def test_invalid_range(self):
        self.assertRaises(ValueError, merge_intervals,
            {(3, 9), (9, 3)}
        )

    def test_invalid_type(self):
        self.assertRaises(ValueError, merge_intervals,
                          {(3, 9), (1.2, 3)})
        self.assertRaises(ValueError, merge_intervals,
                          {(3, 9), ("1", 3)})

if __name__ == '__main__':
    unittest.main()
