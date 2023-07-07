import unittest
from unittest import mock
import sys


class TestGetCurrentClusterUtilization(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ray_mock = mock.MagicMock()
        cls.ray_mock.cluster_resources.return_value = {
            "CPU": 10,
            "memory": 10,
            "object_store_memory": 5,
        }
        cls.ray_mock.available_resources.return_value = {
            "CPU": 6,
            "memory": 4,
            "object_store_memory": 5,
        }

        cls.module_patcher = mock.patch.dict("sys.modules", {"ray": cls.ray_mock})
        cls.module_patcher.start()

        # delete reference to reload from mocked ray
        if "deltacat.utils.resources" in sys.modules:
            del sys.modules["deltacat.utils.resources"]

        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.module_patcher.stop()

    def test_sanity(self):
        from deltacat.utils.resources import ClusterUtilization

        result = ClusterUtilization.get_current_cluster_utilization()

        self.assertEqual(10, result.total_cpu)
        self.assertEqual(4, result.used_cpu)
        self.assertEqual(10, result.total_memory_bytes)
        self.assertEqual(5, result.total_object_store_memory_bytes)
        self.assertEqual(0, result.used_object_store_memory_bytes)
        self.assertEqual(6, result.used_memory_bytes)
        self.assertIsNotNone(result.used_resources)
