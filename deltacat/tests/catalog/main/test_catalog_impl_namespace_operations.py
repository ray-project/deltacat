import shutil

from deltacat.catalog import get_catalog_properties
from deltacat.exceptions import NamespaceAlreadyExistsError
import pytest
import tempfile
import deltacat.catalog.main.impl as catalog


class TestCatalogNamespaceOperations:
    temp_dir = None
    property_catalog = None
    catalog = None

    @classmethod
    def setup_class(cls):
        cls.temp_dir = tempfile.mkdtemp()
        cls.catalog_properties = get_catalog_properties(root=cls.temp_dir)

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.temp_dir)

    def test_create_namespace(self):
        """Test creating a namespace with properties"""
        namespace = "test_create_namespace"
        properties = {"description": "Test Namespace", "owner": "test-user"}

        # Create namespace
        catalog.create_namespace(
            namespace=namespace, properties=properties, inner=self.catalog_properties
        )

        # Verify namespace exists
        assert catalog.namespace_exists(namespace, inner=self.catalog_properties)

        # Get namespace and verify properties
        namespace = catalog.get_namespace(namespace, inner=self.catalog_properties)
        assert namespace.namespace == "test_create_namespace"
        assert namespace.properties["description"] == "Test Namespace"

    def test_get_namespace(self):
        """Test getting namespace properties"""
        namespace = "test_get_namespace"
        properties = {"description": "foo", "created_by": "bar"}

        # Create namespace
        catalog.create_namespace(
            namespace=namespace, properties=properties, inner=self.catalog_properties
        )

        # Get namespace properties
        namespace = catalog.get_namespace(namespace, inner=self.catalog_properties)

        # Verify properties
        assert namespace.namespace == "test_get_namespace"
        assert namespace.properties["created_by"] == "bar"

    def test_namespace_exists(self):
        """Test checking if a namespace exists"""
        existing_namespace = "test_namespace_exists"
        non_existing_namespace = "non_existing_namespace"

        # Create namespace
        catalog.create_namespace(
            namespace=existing_namespace, properties={}, inner=self.catalog_properties
        )

        # Check existing namespace
        assert catalog.namespace_exists(
            existing_namespace, inner=self.catalog_properties
        )

        # Check non-existing namespace
        assert not catalog.namespace_exists(
            non_existing_namespace, inner=self.catalog_properties
        )

    def test_create_namespace_already_exists(self):
        """Test creating a namespace that already exists should fail"""
        namespace = "test_create_namespace_already_exists"
        properties = {"description": "Test namespace", "owner": "test-user"}

        # Create namespace first time
        catalog.create_namespace(
            namespace=namespace,
            properties=properties,
            inner=self.catalog_properties,
        )

        # Verify namespace exists
        assert catalog.namespace_exists(namespace, inner=self.catalog_properties)

        # Try to create the same namespace again, should raise ValueError
        with pytest.raises(NamespaceAlreadyExistsError, match=namespace):
            catalog.create_namespace(
                namespace=namespace,
                properties=properties,
                inner=self.catalog_properties,
            )

    def test_drop_namespace(self):
        """Test dropping a namespace"""
        namespace = "test_drop_namespace"
        properties = {"description": "Test Namespace", "owner": "test-user"}

        # Create namespace
        catalog.create_namespace(
            namespace=namespace,
            properties=properties,
            inner=self.catalog_properties,
        )

        # Verify namespace exists
        assert catalog.namespace_exists(
            namespace,
            inner=self.catalog_properties,
        )

        # Drop namespace
        catalog.drop_namespace(
            namespace,
            inner=self.catalog_properties,
        )

        # Verify namespace does not exist
        assert not catalog.namespace_exists(
            namespace,
            inner=self.catalog_properties,
        )
