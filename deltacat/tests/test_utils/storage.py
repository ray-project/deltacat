from typing import Optional, Dict

import pyarrow as pa

from deltacat import (
    ContentEncoding,
    ContentType,
)
from deltacat.storage import (
    BucketTransform,
    BucketTransformParameters,
    BucketingStrategy,
    CommitState,
    Delta,
    DeltaLocator,
    DeltaType,
    EntryParams,
    EntryType,
    Field,
    LifecycleState,
    ManifestAuthor,
    ManifestEntry,
    Namespace,
    NamespaceLocator,
    NullOrder,
    Partition,
    PartitionKey,
    PartitionLocator,
    PartitionScheme,
    Schema,
    SchemaList,
    SortScheme,
    SortKey,
    SortOrder,
    StreamLocator,
    StreamFormat,
    Stream,
    Table,
    TableLocator,
    TableVersionLocator,
    TableVersion,
    TruncateTransform,
    TruncateTransformParameters,
)

from deltacat.storage.model.manifest import (
    Manifest,
    ManifestMeta,
    ManifestEntryList,
)
from deltacat.utils.common import current_time_ms


def create_empty_delta(
    partition: Partition,
    delta_type: DeltaType,
    author: Optional[str],
    properties: Optional[Dict[str, str]] = None,
    manifest_entry_id: Optional[str] = None,
) -> Delta:
    stream_position = current_time_ms()
    delta_locator = DeltaLocator.of(partition.locator, stream_position=stream_position)

    if manifest_entry_id:
        manifest = Manifest.of(
            entries=ManifestEntryList.of([]),
            author=author,
            uuid=manifest_entry_id,
        )
    else:
        manifest = None

    return Delta.of(
        delta_locator,
        delta_type=delta_type,
        meta=ManifestMeta(),
        properties=properties,
        manifest=manifest,
        previous_stream_position=partition.stream_position,
    )


def create_test_namespace():
    namespace_locator = NamespaceLocator.of(namespace="test_namespace")
    return Namespace.of(locator=namespace_locator)


def create_test_table():
    table_locator = TableLocator.at(
        namespace="test_namespace",
        table_name="test_table",
    )
    return Table.of(
        locator=table_locator,
        description="test table description",
    )


def create_test_table_version():
    table_version_locator = TableVersionLocator.at(
        namespace="test_namespace",
        table_name="test_table",
        table_version="v.1",
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
    return TableVersion.of(
        locator=table_version_locator,
        schema=schema,
        partition_scheme=partition_scheme,
        description="test table version description",
        properties={"test_property_key": "test_property_value"},
        content_types=[ContentType.PARQUET],
        sort_scheme=sort_scheme,
        watermark=None,
        lifecycle_state=LifecycleState.CREATED,
        schemas=SchemaList.of([schema]),
        partition_schemes=[partition_scheme],
        sort_schemes=[sort_scheme],
    )


def create_test_stream():
    stream_locator = StreamLocator.at(
        namespace="test_namespace",
        table_name="test_table",
        table_version="v.1",
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
    return Stream.of(
        locator=stream_locator,
        partition_scheme=partition_scheme,
        state=CommitState.STAGED,
        previous_stream_id="test_previous_stream_id",
        watermark=1,
    )


def create_test_partition():
    partition_locator = PartitionLocator.at(
        namespace="test_namespace",
        table_name="test_table",
        table_version="v.1",
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
    return Partition.of(
        locator=partition_locator,
        schema=schema,
        content_types=[ContentType.PARQUET],
        state=CommitState.STAGED,
        previous_stream_position=0,
        previous_partition_id="test_previous_partition_id",
        stream_position=1,
        partition_scheme_id="test_partition_scheme_id",
    )


def create_test_delta():
    delta_locator = DeltaLocator.at(
        namespace="test_namespace",
        table_name="test_table",
        table_version="v.1",
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
    return Delta.of(
        locator=delta_locator,
        delta_type=DeltaType.APPEND,
        meta=manifest_meta,
        properties={"property1": "value1"},
        manifest=manifest,
        previous_stream_position=0,
    )
