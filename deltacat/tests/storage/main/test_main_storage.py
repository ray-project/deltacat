import os
from typing import List, Tuple


import pyarrow as pa

from deltacat.catalog.main.impl import PropertyCatalog
from deltacat import (
    Schema,
    Field,
    PartitionScheme,
    PartitionKey,
    ContentEncoding,
    ContentType,
    SortScheme,
    SortKey,
    SortOrder,
    NullOrder,
    LifecycleState,
)
from deltacat.storage import (
    BucketTransform,
    BucketTransformParameters,
    BucketingStrategy,
    CommitState,
    DeltaLocator,
    Delta,
    DeltaType,
    EntryParams,
    EntryType,
    Manifest,
    ManifestAuthor,
    ManifestEntry,
    ManifestMeta,
    metastore,
    Namespace,
    NamespaceLocator,
    PartitionLocator,
    Partition,
    StreamLocator,
    StreamFormat,
    Stream,
    Table,
    TableLocator,
    TableVersionLocator,
    TableVersion,
    Transaction,
    TransactionType,
    TransactionOperation,
    TransactionOperationType,
    TruncateTransform,
    TruncateTransformParameters,
)
from deltacat.storage.model.metafile import (
    TXN_DIR_NAME,
    Metafile,
    _filesystem,
    MetafileRevisionInfo,
)


def _commit_single_delta_table(temp_dir: str) -> List[Tuple[Metafile, Metafile, str]]:
    # namespace
    namespace_locator = NamespaceLocator.of(namespace="test_namespace")
    namespace = Namespace.of(locator=namespace_locator)

    # table
    table_locator = TableLocator.at(
        namespace="test_namespace",
        table_name="test_table",
    )
    table = Table.of(
        locator=table_locator,
        description="test table description",
    )

    # stream
    table_version_locator = TableVersionLocator.at(
        namespace="test_namespace",
        table_name="test_table",
        table_version="test_table_version",
    )
    schema = Schema.of(
        [
            Field.of(
                field=pa.field("some_string", pa.string(), nullable=False),
                field_id=1,
                is_merge_key=True,
            ),
            Field.of(
                field=pa.field("some_int32", pa.int32(), nullable=False),
                field_id=2,
                is_merge_key=True,
            ),
            Field.of(
                field=pa.field("some_float64", pa.float64()),
                field_id=3,
                is_merge_key=False,
            ),
        ]
    )
    bucket_transform = BucketTransform.of(
        BucketTransformParameters.of(
            num_buckets=2,
            bucketing_strategy=BucketingStrategy.DEFAULT,
        )
    )
    partition_keys = [
        PartitionKey.of(
            key=["some_string", "some_int32"],
            name="test_partition_key",
            field_id="test_field_id",
            transform=bucket_transform,
        )
    ]
    partition_scheme = PartitionScheme.of(
        keys=partition_keys,
        name="test_partition_scheme",
        scheme_id="test_partition_scheme_id",
    )
    sort_keys = [
        SortKey.of(
            key=["some_int32"],
            sort_order=SortOrder.DESCENDING,
            null_order=NullOrder.AT_START,
            transform=TruncateTransform.of(
                TruncateTransformParameters.of(width=3),
            ),
        )
    ]
    sort_scheme = SortScheme.of(
        keys=sort_keys,
        name="test_sort_scheme",
        scheme_id="test_sort_scheme_id",
    )
    table_version = TableVersion.of(
        locator=table_version_locator,
        schema=schema,
        partition_scheme=partition_scheme,
        description="test table version description",
        properties={"test_property_key": "test_property_value"},
        content_types=[ContentType.PARQUET],
        sort_scheme=sort_scheme,
        watermark=1,
        lifecycle_state=LifecycleState.CREATED,
        schemas=[schema, schema, schema],
        partition_schemes=[partition_scheme, partition_scheme],
        sort_schemes=[sort_scheme, sort_scheme],
    )

    stream_locator = StreamLocator.at(
        namespace="test_namespace",
        table_name="test_table",
        table_version="test_table_version",
        stream_id="test_stream_id",
        stream_format=StreamFormat.DELTACAT,
    )
    bucket_transform = BucketTransform.of(
        BucketTransformParameters.of(
            num_buckets=2,
            bucketing_strategy=BucketingStrategy.DEFAULT,
        )
    )
    partition_keys = [
        PartitionKey.of(
            key=["some_string", "some_int32"],
            name="test_partition_key",
            field_id="test_field_id",
            transform=bucket_transform,
        )
    ]
    partition_scheme = PartitionScheme.of(
        keys=partition_keys,
        name="test_partition_scheme",
        scheme_id="test_partition_scheme_id",
    )
    stream = Stream.of(
        locator=stream_locator,
        partition_scheme=partition_scheme,
        state=CommitState.STAGED,
        previous_stream_id="test_previous_stream_id",
        watermark=1,
    )

    # partition
    partition_locator = PartitionLocator.at(
        namespace="test_namespace",
        table_name="test_table",
        table_version="test_table_version",
        stream_id="test_stream_id",
        stream_format=StreamFormat.DELTACAT,
        partition_values=["a", 1],
        partition_id="test_partition_id",
    )
    schema = Schema.of(
        [
            Field.of(
                field=pa.field("some_string", pa.string(), nullable=False),
                field_id=1,
                is_merge_key=True,
            ),
            Field.of(
                field=pa.field("some_int32", pa.int32(), nullable=False),
                field_id=2,
                is_merge_key=True,
            ),
            Field.of(
                field=pa.field("some_float64", pa.float64()),
                field_id=3,
                is_merge_key=False,
            ),
        ]
    )
    partition = Partition.of(
        locator=partition_locator,
        schema=schema,
        content_types=[ContentType.PARQUET],
        state=CommitState.STAGED,
        previous_stream_position=0,
        previous_partition_id="test_previous_partition_id",
        stream_position=1,
        partition_scheme_id="test_partition_scheme_id",
    )

    # delta
    delta_locator = DeltaLocator.at(
        namespace="test_namespace",
        table_name="test_table",
        table_version="test_table_version",
        stream_id="test_stream_id",
        stream_format=StreamFormat.DELTACAT,
        partition_values=["a", 1],
        partition_id="test_partition_id",
        stream_position=1,
    )
    manifest_entry_params = EntryParams.of(
        equality_field_locators=["some_string", "some_int32"],
    )
    manifest_meta = ManifestMeta.of(
        record_count=1,
        content_length=10,
        content_type=ContentType.PARQUET.value,
        content_encoding=ContentEncoding.IDENTITY.value,
        source_content_length=100,
        credentials={"foo": "bar"},
        content_type_parameters=[{"param1": "value1"}],
        entry_type=EntryType.EQUALITY_DELETE,
        entry_params=manifest_entry_params,
    )
    manifest = Manifest.of(
        entries=[
            ManifestEntry.of(
                url="s3://test/url",
                meta=manifest_meta,
            )
        ],
        author=ManifestAuthor.of(
            name="deltacat",
            version="2.0",
        ),
        entry_type=EntryType.EQUALITY_DELETE,
        entry_params=manifest_entry_params,
    )
    delta = Delta.of(
        locator=delta_locator,
        delta_type=DeltaType.APPEND,
        meta=manifest_meta,
        properties={"property1": "value1"},
        manifest=manifest,
        previous_stream_position=0,
    )
    meta_to_create = [
        namespace,
        table,
        table_version,
        stream,
        partition,
        delta,
    ]
    txn_operations = [
        TransactionOperation.of(
            operation_type=TransactionOperationType.CREATE,
            dest_metafile=meta,
        )
        for meta in meta_to_create
    ]
    transaction = Transaction.of(
        txn_type=TransactionType.APPEND,
        txn_operations=txn_operations,
    )
    write_paths, txn_log_path = transaction.commit(temp_dir)
    write_paths_copy = write_paths.copy()
    assert os.path.exists(txn_log_path)
    metafiles_created = [
        Delta.read(write_paths.pop()),
        Partition.read(write_paths.pop()),
        Stream.read(write_paths.pop()),
        TableVersion.read(write_paths.pop()),
        Table.read(write_paths.pop()),
        Namespace.read(write_paths.pop()),
    ]
    metafiles_created.reverse()
    return list(zip(meta_to_create, metafiles_created, write_paths_copy))


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
