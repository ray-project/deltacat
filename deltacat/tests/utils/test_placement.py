import unittest
from unittest.mock import patch, MagicMock
import ray
from deltacat.utils.placement import (
    PlacementGroupManager,
    _config,
    _get_available_resources_per_node,
)


class TestPlacementGroupManager(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ray.init(
            local_mode=True, ignore_reinit_error=True, resources={"storage_worker": 1}
        )

    def test_placement_group_manager_sanity(self):

        pgm = PlacementGroupManager(1, 1, 1)

        self.assertIsNotNone(pgm)

    def test_ray_state_api_returns_correctly(self):

        result = _get_available_resources_per_node()

        self.assertIsNotNone(result)

    def test_placement_group_manager_accepts_custom_resources(self):

        pgm = PlacementGroupManager(1, 1, 1, custom_resources={"storage_worker": 1})

        self.assertIsNotNone(pgm)
