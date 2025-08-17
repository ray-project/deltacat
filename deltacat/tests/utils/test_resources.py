import unittest
from unittest import mock
import time


class TestGetCurrentClusterUtilization(unittest.TestCase):
    @mock.patch("deltacat.utils.resources.ray")
    def test_sanity(self, ray_mock):
        ray_mock.cluster_resources.return_value = {
            "CPU": 10,
            "memory": 10,
            "object_store_memory": 5,
        }
        ray_mock.available_resources.return_value = {
            "CPU": 6,
            "memory": 4,
            "object_store_memory": 5,
        }

        from deltacat.utils.resources import ClusterUtilization

        result = ClusterUtilization.get_current_cluster_utilization()

        self.assertEqual(10, result.total_cpu)
        self.assertEqual(4, result.used_cpu)
        self.assertEqual(10, result.total_memory_bytes)
        self.assertEqual(5, result.total_object_store_memory_bytes)
        self.assertEqual(0, result.used_object_store_memory_bytes)
        self.assertEqual(6, result.used_memory_bytes)
        self.assertIsNotNone(result.used_resources)


class TestClusterUtilizationOverTimeRange(unittest.TestCase):
    @mock.patch("deltacat.utils.resources.ray")
    def test_sanity(self, ray_mock):
        from deltacat.utils.resources import ClusterUtilizationOverTimeRange

        ray_mock.cluster_resources.side_effect = [{"CPU": 32} for _ in range(5)]
        ray_mock.available_resources.side_effect = [
            {"CPU": 2 ** (i + 1)} for i in range(5)
        ]

        with ClusterUtilizationOverTimeRange() as cu:
            time.sleep(3)
            self.assertTrue(cu.used_vcpu_seconds <= 82)  # 30 + 28 + 24
            self.assertTrue(
                cu.total_vcpu_seconds >= cu.used_vcpu_seconds
            )  # total is greater than used
            self.assertIsNotNone(cu.total_memory_gb_seconds)
            self.assertIsNotNone(cu.used_memory_gb_seconds)
            self.assertIsNotNone(cu.max_cpu)


class TestProcessUtilizationOverTimeRange(unittest.TestCase):
    def test_sanity(self):
        from deltacat.utils.resources import ProcessUtilizationOverTimeRange

        with ProcessUtilizationOverTimeRange() as nu:
            time.sleep(3)
            self.assertIsNotNone(nu.max_memory)

    def test_callback(self):
        from deltacat.utils.resources import ProcessUtilizationOverTimeRange

        with ProcessUtilizationOverTimeRange() as nu:

            def test_callback():
                nu.test_field_set = True

            nu.schedule_callback(test_callback, 1)
            time.sleep(3)
            self.assertTrue(nu.test_field_set)
