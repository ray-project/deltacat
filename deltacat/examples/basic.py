from deltacat.storage import interface as unimplemented_deltacat_storage


def run_all(dc_storage=unimplemented_deltacat_storage):
    """Run all examples."""

    """
    Example list_namespaces() result containing a single namespace:
    {
        'items': [
            {
                'namespace': 'TestNamespace',
            }
        ],
        'pagination_key': 'dmVyc2lvbmVkVGFibGVOYW1l'
    }
    """
    namespaces = []
    namespaces_list_result = dc_storage.list_namespaces()
    while namespaces_list_result:
        namespaces_list_result = namespaces_list_result.next_page()
        namespaces.extend(namespaces_list_result.read_page())

    print(f"All Namespaces: {namespaces}")

    """
    Example list_tables() result containing a single table:
    {
        'items': [
            {
                'id': {
                    'namespace': 'TestNamespace',
                    'tableName': 'TestTable'
                },
                'description': 'Test table description.',
                'properties': {
                    'testPropertyOne': 'testValueOne',
                    'testPropertyTwo': 'testValueTwo'
                }
            }
        ],
       'pagination_key': 'dmVyc2lvbmVkVGFibGVOYW1l'
    }
    """
    test_tables = []
    tables_list_result = dc_storage.list_tables("TestNamespace")
    while tables_list_result:
        tables_list_result = tables_list_result.next_page()
        test_tables.extend(tables_list_result.read_page())

    print(f"All 'TestNamespace' Tables: {test_tables}")

    """
    Example list_partitions() result containing a single partition:
    {
        'items': [
            {
                'partitionKeyValues': ['1', '2017-08-31T00:00:00.000Z']
            }
        ],
        'pagination_key': 'dmVyc2lvbmVkVGFibGVOYW1l'
    }
    """
    # Partitions will automatically be returned for the latest active version of
    # the specified table.
    table_partitions = []
    partitions_list_result = dc_storage.list_partitions(
        "TestNamespace",
        "TestTable",
    )
    while partitions_list_result:
        partitions_list_result = partitions_list_result.next_page()
        table_partitions.extend(partitions_list_result.read_page())
    print(f"All Table Partitions: {table_partitions}")

    """
    Example list_deltas result containing a single delta:
    {
        'items': [
            {
                'type': 'upsert',
                'locator": {
                    'streamPosition': 1551898425276,
                    'partitionLocator': {
                        'partitionId': 'de75623a-7adf-4cf0-b982-7b514502be82'
                        'partitionValues': ['1', '2018-03-06T00:00:00.000Z'],
                        'streamLocator': {
                            'namespace': 'TestNamespace',
                            'tableName': 'TestTable',
                            'tableVersion': '1',
                            'streamId': 'dbcbbf56-4bcb-4b94-8cf2-1c6d57ccfe74',
                            'storageType': 'AwsGlueCatalog'
                        }
                    }
                },
                'properties': {
                    'parent_stream_position': '1551898423165'
                },
                'meta': {
                    'contentLength': 9423157342,
                    'fileCount': 117,
                    'recordCount': 188463146,
                    'sourceContentLength': 37692629368,
                }
            }
        ],
        'paginationKey': 'enQzd3mqcnNkQIFkaHQ1ZW2m'
    }
    """
    # Deltas will automatically be returned for the latest active version of the
    # specified table.
    deltas_list_result = dc_storage.list_deltas(
        "TestNamespace",
        "TestTable",
        ["1", "2018-03-06T00:00:00.000Z"],
    )
    all_partition_deltas = deltas_list_result.all_items()
    print(f"All Partition Deltas: {all_partition_deltas}")


if __name__ == "__main__":
    run_all()
