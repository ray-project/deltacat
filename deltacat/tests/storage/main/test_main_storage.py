from deltacat.storage import (
    metastore,
    Namespace,
    NamespaceLocator,
)


class TestMainStorage:
    def test_list_namespaces(self, temp_catalog):
        # given a namespace to create
        namespace_name = "test_namespace"

        # when the namespace is created
        created_namespace = metastore.create_namespace(
            namespace=namespace_name,
            catalog=temp_catalog,
        )

        # expect the namespace returned to match the input namespace to create
        namespace_locator = NamespaceLocator.of(namespace=namespace_name)
        expected_namespace = Namespace.of(locator=namespace_locator)
        assert expected_namespace.equivalent_to(created_namespace)

        # expect the namespace to exist
        assert metastore.namespace_exists(
            namespace=namespace_name,
            catalog=temp_catalog,
        )

        # expect the namespace to also be returned when listing namespaces
        list_result = metastore.list_namespaces(catalog=temp_catalog)
        namespaces = list_result.all_items()
        assert len(namespaces) == 1
        assert namespaces[0].equivalent_to(expected_namespace)

        # expect the namespace to also be returned when explicitly retrieved
        read_namespace = metastore.get_namespace(
            namespace=namespace_name,
            catalog=temp_catalog,
        )
        assert read_namespace and read_namespace.equivalent_to(expected_namespace)
