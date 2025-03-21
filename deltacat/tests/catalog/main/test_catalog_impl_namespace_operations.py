import shutil
from deltacat.exceptions import NamespaceAlreadyExistsError
import pytest
import tempfile
import deltacat.catalog.v2.catalog_impl as catalog
from deltacat.catalog.catalog_properties import initialize_properties


class TestCatalogNamespaceOperations:
    temp_dir = None
    property_catalog = None
    catalog = None

    @classmethod
    def setup_class(cls):
        cls.temp_dir = tempfile.mkdtemp()
        cls.catalog_properties = initialize_properties(root=cls.temp_dir)

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.temp_dir)

    def test_create_namespace(self):
        """Test creating a namespace with properties"""
        namespace = "test_create_namespace"
        properties = {"description": "Test Namespace", "owner": "test-user"}

        # Create namespace
        catalog.create_namespace(
            namespace=namespace, properties=properties, catalog=catalog
        )

        # Verify namespace exists
        assert catalog.namespace_exists(namespace, catalog=catalog)

        # Get namespace and verify properties
        namespace = catalog.get_namespace(
            namespace, catalog_properties=self.catalog_properties
        )
        assert namespace.namespace == "test_create_namespace"
        assert namespace.properties["description"] == "Test Namespace"

    def test_get_namespace(self):
        """Test getting namespace properties"""
        namespace = "test_get_namespace"
        properties = {"description": "foo", "created_by": "bar"}

        # Create namespace
        catalog.create_namespace(
            namespace=namespace, properties=properties, catalog=catalog
        )

        # Get namespace properties
        namespace = catalog.get_namespace(namespace)

        # Verify properties
        assert namespace.namespace == "test_get_namespace"
        assert namespace.properties["created_by"] == "bar"

    def test_namespace_exists(self):
        """Test checking if a namespace exists"""
        existing_namespace = "test_namespace_exists"
        non_existing_namespace = "non_existing_namespace"

        # Create namespace
        catalog.create_namespace(
            namespace=existing_namespace, properties={}, catalog=catalog
        )

        # Check existing namespace
        assert catalog.namespace_exists(existing_namespace, catalog=catalog)

        # Check non-existing namespace
        assert not catalog.namespace_exists(non_existing_namespace, catalog=catalog)

    def test_create_namespace_already_exists(self):
        """Test creating a namespace that already exists should fail"""
        namespace = "test_create_namespace_already_exists"
        properties = {"description": "Test namespace", "owner": "test-user"}

        # Create namespace first time
        catalog.create_namespace(
            namespace=namespace,
            properties=properties,
            catalog_properties=self.catalog_properties,
        )

        # Verify namespace exists
        assert catalog.namespace_exists(namespace, catalog=catalog)

        # Try to create the same namespace again, should raise ValueError
        with pytest.raises(NamespaceAlreadyExistsError, match=namespace):
            catalog.create_namespace(
                namespace=namespace, properties=properties, catalog=catalog
            )

    def test_drop_namespace(self):
        """Test dropping a namespace"""
        namespace = "test_drop_namespace"
        properties = {"description": "Test Namespace", "owner": "test-user"}

        # Create namespace
        catalog.create_namespace(
            namespace=namespace, properties=properties, catalog=catalog
        )

        # Verify namespace exists
        assert catalog.namespace_exists(namespace, catalog=catalog)

        # Drop namespace
        catalog.drop_namespace(namespace, catalog=catalog)

        # Verify namespace does not exist
        assert not catalog.namespace_exists(namespace, catalog=catalog)
