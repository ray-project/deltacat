import unittest

from deltacat.storage.rivulet.reader.query_expression import QueryExpression

class TestQueryExpression(unittest.TestCase):

    def test_with_key(self):
        query = QueryExpression[int]()
        query.with_key(5)
        self.assertEqual(query.min_key, 5)
        self.assertEqual(query.max_key, 5)

        with self.assertRaises(ValueError):
            query.with_key(10)

    def test_with_range(self):
        query = QueryExpression[int]()
        query.with_range(10, 5)
        self.assertEqual(query.min_key, 5)
        self.assertEqual(query.max_key, 10)

        with self.assertRaises(ValueError):
            query.with_range(20, 25)

    def test_matches_query(self):
        query = QueryExpression[int]()
        self.assertTrue(query.matches_query(5))  # No range set, should match everything

        query.with_range(10, 20)
        self.assertTrue(query.matches_query(15))
        self.assertFalse(query.matches_query(25))
        self.assertFalse(query.matches_query(5))

    def test_below_query_range(self):
        query = QueryExpression[int]()
        self.assertFalse(query.below_query_range(5))  # No range set, should always return False

        query.with_range(10, 20)
        self.assertTrue(query.below_query_range(5))
        self.assertFalse(query.below_query_range(15))
        self.assertFalse(query.below_query_range(25))

    def test_intersect_with(self):
        query1 = QueryExpression[int]().with_range(10, 20)
        query2 = QueryExpression[int]().with_range(15, 25)

        intersection = query1.intersect_with(query2)
        self.assertEqual(intersection.min_key, 15)
        self.assertEqual(intersection.max_key, 20)

        query3 = QueryExpression[int]().with_range(30, 40)
        empty_intersection = query1.intersect_with(query3)
        self.assertIsNone(empty_intersection.min_key)
        self.assertIsNone(empty_intersection.max_key)

        query4 = QueryExpression[int]()  # No range set
        self.assertEqual(query1.intersect_with(query4).min_key, 10)
        self.assertEqual(query1.intersect_with(query4).max_key, 20)

    def test_intersect_with_empty_query(self):
        query1 = QueryExpression[int]().with_range(10, 20)
        query2 = QueryExpression[int]()  # Empty query

        result = query1.intersect_with(query2)
        self.assertEqual(result.min_key, 10)
        self.assertEqual(result.max_key, 20)
