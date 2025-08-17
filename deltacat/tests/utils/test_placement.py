import unittest
import ray
from deltacat.utils.placement import (
    PlacementGroupManager,
    _get_available_resources_per_node,
)


class TestPlacementGroupManager(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ray.init(local_mode=True, ignore_reinit_error=True)

    def test_placement_group_manager_sanity(self):

        pgm = PlacementGroupManager(1, 1, 1)

        self.assertIsNotNone(pgm)

    def test_ray_state_api_returns_correctly(self):

        result = _get_available_resources_per_node()

        self.assertIsNotNone(result)
