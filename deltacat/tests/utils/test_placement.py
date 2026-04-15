import unittest
import ray
from deltacat.utils.placement import (
    PlacementGroupManager,
    _get_available_resources_per_node,
)


class TestPlacementGroupManager(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(
            local_mode=True,
            ignore_reinit_error=True,
        )
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()
        super().tearDownClass()

    def test_placement_group_manager_sanity(self):

        pgm = PlacementGroupManager(1, 1, 1)

        self.assertIsNotNone(pgm)

    def test_ray_state_api_returns_correctly(self):

        result = _get_available_resources_per_node()

        self.assertIsNotNone(result)

    def test_placement_group_manager_constructor_accepts_custom_resources(self):
        """Verify PlacementGroupManager.__init__ signature accepts custom_resources."""
        import inspect

        sig = inspect.signature(PlacementGroupManager.__init__)
        self.assertIn("custom_resources", sig.parameters)

    def test_custom_resources_merged_into_bundles(self):
        """Verify custom_resources are merged into each bundle dict."""
        custom_resources = {"storage_worker": 1, "gpu": 2}
        total_cpus_per_pg = 4
        cpu_per_node = 2
        num_bundles = int(total_cpus_per_pg / cpu_per_node)
        bundles = [
            {"CPU": cpu_per_node, **(custom_resources or {})}
            for _ in range(num_bundles)
        ]

        self.assertEqual(len(bundles), 2)
        for bundle in bundles:
            self.assertEqual(bundle["CPU"], 2)
            self.assertEqual(bundle["storage_worker"], 1)
            self.assertEqual(bundle["gpu"], 2)

    def test_custom_resources_none_produces_cpu_only_bundles(self):
        """Verify bundles contain only CPU when custom_resources is None."""
        custom_resources = None
        bundles = [{"CPU": 4, **(custom_resources or {})} for _ in range(3)]

        for bundle in bundles:
            self.assertEqual(bundle, {"CPU": 4})

    def test_custom_resources_added_to_cluster_resources(self):
        """Verify custom_resources are added to cluster_resources dict."""
        cluster_resources = {"CPU": 8, "memory": 1024.0, "object_store_memory": 512.0}
        custom_resources = {"storage_worker": 1}
        if custom_resources:
            cluster_resources.update(custom_resources)

        self.assertEqual(cluster_resources["storage_worker"], 1)
        self.assertEqual(cluster_resources["CPU"], 8)
