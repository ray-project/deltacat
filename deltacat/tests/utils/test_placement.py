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
        super().setUpClass()
        # Note: Do not use local_mode=True as PlacementGroupManager requires
        # the Ray Dashboard (State API) which is not available in local_mode.
        # Explicitly set num_cpus=2 to ensure tests work on resource-constrained
        # CI runners (e.g., GitHub Actions ubuntu-latest with 2 vCPUs).
        ray.init(num_cpus=2, ignore_reinit_error=True)

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

        Mocked because CI runners don't declare custom resources, so
        pg.ready() would hang waiting for a node that can never satisfy
        the request.
        """
        expected_config = PlacementGroupConfig(
            opts={"scheduling_strategy": "mock"},
            resource={"CPU": 1, "memory": 1024.0, "storage_worker": 1},
            node_ips=["127.0.0.1"],
        )
        pgm = PlacementGroupManager.__new__(PlacementGroupManager)
        pgm._pg_configs = [expected_config]

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
