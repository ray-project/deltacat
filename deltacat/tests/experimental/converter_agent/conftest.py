import pytest
import ray
import daft


@pytest.fixture(autouse=True, scope="module")
def setup_ray_cluster():
    """Set up Ray cluster for table monitor tests."""
    ray.init(
        local_mode=True,
        ignore_reinit_error=True,
        resources={
            "convert_task": 10
        },  # Provide convert_task resource for converter session
    )
    yield
    ray.shutdown()


@pytest.fixture(scope="session")
def daft_native_runner():
    """
    Session-scoped fixture to set Daft to use native runner for table monitor tests.
    This is set once per test session and cannot be changed (Daft limitation).
    Tests that need the native runner should explicitly request this fixture.
    """
    # Set to native runner only when explicitly requested
    # Note: Daft only allows setting runner once per session
    try:
        daft.context.set_runner_native()
    except Exception as e:
        # If runner is already set, that's okay - just log it
        print(f"Note: Daft runner already set, continuing with existing runner: {e}")

    yield

    # No teardown needed - Daft doesn't allow changing runner after it's set
