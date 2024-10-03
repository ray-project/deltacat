import unittest
from unittest import mock


class TestRayPlasmaObjectStore(unittest.TestCase):

    TEST_VALUE = "test-value"

    @classmethod
    def setUpClass(cls):
        from deltacat.io.ray_plasma_object_store import RayPlasmaObjectStore

        cls.object_store = RayPlasmaObjectStore()

        super().setUpClass()

    @mock.patch("deltacat.io.ray_plasma_object_store.ray")
    @mock.patch("deltacat.io.ray_plasma_object_store.cloudpickle")
    def test_put_many_sanity(self, mock_cloudpickle, mock_ray):
        mock_ray.put.return_value = "c"
        mock_cloudpickle.dumps.return_value = self.TEST_VALUE
        result = self.object_store.put_many(["a", "b"])

        self.assertEqual(2, len(result))

    @mock.patch("deltacat.io.ray_plasma_object_store.ray")
    @mock.patch("deltacat.io.ray_plasma_object_store.cloudpickle")
    def test_get_many_sanity(self, mock_cloudpickle, mock_ray):
        mock_ray.get.return_value = ["a", "b"]
        mock_cloudpickle.loads.return_value = self.TEST_VALUE

        result = self.object_store.get_many(["test", "test"])

        self.assertEqual(2, len(result))

    @mock.patch("deltacat.io.ray_plasma_object_store.ray")
    @mock.patch("deltacat.io.ray_plasma_object_store.cloudpickle")
    def test_get_sanity(self, mock_cloudpickle, mock_ray):
        mock_ray.get.return_value = [self.TEST_VALUE]
        mock_cloudpickle.loads.return_value = self.TEST_VALUE

        result = self.object_store.get("test")

        self.assertEqual(self.TEST_VALUE, result)

    @mock.patch("deltacat.io.ray_plasma_object_store.ray")
    @mock.patch("deltacat.io.ray_plasma_object_store.cloudpickle")
    def test_put_sanity(self, mock_cloudpickle, mock_ray):
        mock_ray.put.return_value = "c"
        mock_cloudpickle.dumps.return_value = self.TEST_VALUE

        result = self.object_store.put("test")

        self.assertEqual(self.TEST_VALUE, result)
