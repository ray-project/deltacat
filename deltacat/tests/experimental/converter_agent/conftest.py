import pytest
import ray


@pytest.fixture(autouse=True, scope="module")
def setup_ray_cluster():
    """Set up Ray cluster for table monitor tests."""
    ray.init(local_mode=True, ignore_reinit_error=True)
    yield
    ray.shutdown()
