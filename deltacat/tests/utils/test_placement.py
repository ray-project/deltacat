import unittest
import unittest.mock
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

    @unittest.mock.patch("deltacat.utils.placement._config")
    def test_placement_group_manager_accepts_custom_resources(self, mock_config):
        mock_pg_config = unittest.mock.MagicMock()
        mock_config.options.return_value.remote.return_value = mock_pg_config

        original_ray_get = ray.get
        ray.get = unittest.mock.MagicMock(return_value=[mock_pg_config])
        try:
            pgm = PlacementGroupManager(1, 1, 1, custom_resources={"storage_worker": 1})
        finally:
            ray.get = original_ray_get

        self.assertIsNotNone(pgm)
        call_kwargs = mock_config.options.return_value.remote.call_args
        self.assertEqual(call_kwargs.kwargs["custom_resources"], {"storage_worker": 1})
