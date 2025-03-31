"""Fixtures for catalog tests."""

import uuid
import pytest
import ray


@pytest.fixture(scope="function")
def isolated_ray_env(request):
    """
    Setup and teardown an isolated Ray environment for tests.

    In order to use this fixture, Ray must be re-initialized providing the namespace and ignoring re-initialization errors.

    Re-initialize ray like:
    init(catalog,
        ray_init_args={"namespace": namespace, "ignore_reinit_error": True},
        **{"force_reinitialize": True})
    """
    namespace = f"test_catalogs_{uuid.uuid4().hex}"

    # Reset the global catalog registry state for this namespace
    from deltacat.catalog.model.catalog import catalog_registry
    if namespace in catalog_registry:
        del catalog_registry[namespace]

    yield namespace

    # Clean up the actor in this namespace if it exists
    if namespace in catalog_registry:
        try:
            ray.kill(catalog_registry[namespace])
            # We don't fully shut down ray as it might be used by other tests
        except Exception:
            pass

    # Clean up the registry
    if namespace in catalog_registry:
        del catalog_registry[namespace]