import unittest
import ray
from deltacat.utils.placement import (
    PlacementGroupManager,
    PlacementGroupConfig,
    _get_available_resources_per_node,
)


class TestPlacementGroupManager(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Note: Do not use local_mode=True as PlacementGroupManager requires
        # the Ray Dashboard (State API) which is not available in local_mode
        ray.init(ignore_reinit_error=True)
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()
        super().tearDownClass()

    def test_placement_group_manager_sanity(self):
        pgm = PlacementGroupManager(1, 1, 1)
        self.assertIsNotNone(pgm)

    def test_placement_group_manager_returns_pg_configs(self):
        """Verify pgs property returns list of PlacementGroupConfig."""
        pgm = PlacementGroupManager(1, 1, 1)
        self.assertEqual(len(pgm.pgs), 1)
        self.assertIsInstance(pgm.pgs[0], PlacementGroupConfig)

    def test_placement_group_manager_with_custom_resources(self):
        """Verify custom_resources are included in cluster_resources.

        This supports targeting specific node types (e.g., storage_worker: 1)
        for placement groups as part of RayTeam-1931.
        """
        pgm = PlacementGroupManager(1, 1, 1, custom_resources={"storage_worker": 1})
        pg_config = pgm.pgs[0]
        self.assertIn("storage_worker", pg_config.resource)
        self.assertEqual(pg_config.resource["storage_worker"], 1)

    def test_ray_state_api_returns_correctly(self):
        result = _get_available_resources_per_node()
        self.assertIsNotNone(result)


class TestPlacementGroupConfig(unittest.TestCase):
    def test_placement_group_config_stores_attributes(self):
        opts = {"scheduling_strategy": "test"}
        resource = {"CPU": 15, "memory": 1024.0, "storage_worker": 1}
        node_ips = ["192.168.1.1"]

        config = PlacementGroupConfig(opts=opts, resource=resource, node_ips=node_ips)

        self.assertEqual(config.opts, opts)
        self.assertEqual(config.resource, resource)
        self.assertEqual(config.node_ips, node_ips)
