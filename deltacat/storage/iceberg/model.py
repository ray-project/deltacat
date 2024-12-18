from typing import Optional, Dict, List, Union

import pyarrow as pa
from pyiceberg.catalog.rest import NAMESPACE_SEPARATOR

from pyiceberg.io import load_file_io
from pyiceberg.io.pyarrow import pyarrow_to_schema, schema_to_pyarrow
from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import (
    INITIAL_SCHEMA_ID,
    NestedField,
    Schema as IcebergSchema,
)
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import (
    Table as IcebergTable,
    Namespace as IcebergNamespace,
    TableIdentifier,
)
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.table.snapshots import MetadataLogEntry, Snapshot
from pyiceberg.table.sorting import (
    SortField,
    SortDirection,
    NullOrder as IcebergNullOrder,
    SortOrder as IcebergSortOrder,
)
from pyiceberg.transforms import (
    BucketTransform as IcebergBucketTransform,
    HourTransform as IcebergHourTransform,
    DayTransform as IcebergDayTransform,
    MonthTransform as IcebergMonthTransform,
    YearTransform as IcebergYearTransform,
    IdentityTransform as IcebergIdentityTransform,
    TruncateTransform as IcebergTruncateTransform,
    VoidTransform as IcebergIcebergVoidTransform,
    UnknownTransform as IcebergUnknownTransform,
    Transform as IcebergTransform,
)
from pyiceberg.typedef import Identifier, EMPTY_DICT

from deltacat.exceptions import (
    NamespaceNotFoundError,
    TableVersionNotFoundError,
    StreamNotFoundError,
    TableNotFoundError,
)
from deltacat.storage import (
    BucketingStrategy,
    BucketTransform,
    BucketTransformParameters,
    DayTransform,
    Field,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    Namespace,
    NamespaceLocator,
    Schema,
    StreamLocator,
    Stream,
    Table,
    TableLocator,
    TableVersion,
    TableVersionLocator,
    Transform,
    TransformName,
    TruncateTransform,
    TruncateTransformParameters,
    UnknownTransform,
    VoidTransform,
    YearTransform,
    SortOrder,
    NullOrder,
)
from deltacat.storage.model.interop import ModelMapper, OneWayModelMapper
from deltacat.storage.model.partition import PartitionKey, PartitionScheme
from deltacat.storage.model.sort_key import (
    SortKey,
    SortScheme,
)
from deltacat.storage.model.types import StreamFormat, CommitState


def _get_snapshot_for_meta(
    meta: TableMetadata,
    snapshot_id: int,
) -> Snapshot:
    try:
        return next(s for s in meta.snapshots if s.snapshot_id == snapshot_id)
    except StopIteration as e:
        err_msg = f"No table snapshot with ID: {snapshot_id}"
        raise ValueError(err_msg) from e


def _resolve_stream_snapshot(
    meta: TableMetadata,
    snapshot_id: Optional[int],
) -> Snapshot:
    sid = snapshot_id if snapshot_id else meta.current_snapshot_id
    try:
        return _get_snapshot_for_meta(meta, sid)
    except ValueError as e:
        err_msg = f"No snapshot with timestamp: {sid}.\nTable Metadata: {meta}"
        raise StreamNotFoundError(err_msg) from e


def _get_metadata_for_timestamp(
    timestamp: int,
    meta_log: List[MetadataLogEntry],
    catalog_properties: Dict[str, str] = EMPTY_DICT,
) -> TableMetadata:
    try:
        meta_log_entry = next(
            entry for entry in meta_log if entry.timestamp_ms == timestamp
        )
    except StopIteration as e:
        err_msg = f"No table metadata log with timestamp: {timestamp}"
        raise ValueError(err_msg) from e
    io = load_file_io(
        properties=catalog_properties,
        location=meta_log_entry.metadata_file,
    )
    file = io.new_input(meta_log_entry.metadata_file)
    return FromInputFile.table_metadata(file)


def _resolve_table_version_metadata(
    table: Optional[IcebergTable],
    timestamp: Optional[int] = None,
    catalog_properties: Dict[str, str] = EMPTY_DICT,
) -> TableMetadata:
    try:
        latest = table.metadata
        return (
            _get_metadata_for_timestamp(
                timestamp,
                table.metadata.metadata_log,
                catalog_properties,
            )
            if timestamp is not None and timestamp != latest.last_updated_ms
            else latest
        )
    except ValueError as e:
        raise TableVersionNotFoundError(
            f"Table version `{timestamp}` not found."
        ) from e


def _resolve_table_version(
    meta: TableMetadata,
    timestamp: Optional[int] = None,
) -> int:
    try:
        return (
            next(
                entry.timestamp_ms
                for entry in meta.metadata_log
                if entry.timestamp_ms == timestamp
            )
            if timestamp
            else meta.last_updated_ms
        )
    except StopIteration as e:
        err_msg = f"Table version `{timestamp}` not found."
        raise TableVersionNotFoundError(err_msg) from e


def _get_current_schema_for_meta(meta: TableMetadata) -> IcebergSchema:
    schema_id = meta.current_schema_id
    try:
        return next(schema for schema in meta.schemas if schema.schema_id == schema_id)
    except StopIteration as e:
        err_msg = f"No table schema with ID: {schema_id}"
        raise ValueError(err_msg) from e


def _get_current_spec_for_meta(meta: TableMetadata) -> PartitionSpec:
    spec_id = meta.default_spec_id
    try:
        return next(spec for spec in meta.partition_specs if spec.spec_id == spec_id)
    except StopIteration as e:
        err_msg = f"No table partition spec with ID: {spec_id}"
        raise ValueError(err_msg) from e


def _get_current_sort_order_for_meta(meta: TableMetadata) -> SortOrder:
    sort_order_id = meta.default_sort_order_id
    try:
        return next(
            sort_order
            for sort_order in meta.sort_orders
            if sort_order.order_id == sort_order_id
        )
    except StopIteration as e:
        err_msg = f"No table sort order with ID: {sort_order_id}"
        raise ValueError(err_msg) from e


class TransformMapper(ModelMapper[IcebergTransform, Transform]):
    @staticmethod
    def map(
        obj: Optional[IcebergTransform],
        **kwargs,
    ) -> Optional[Transform]:
        if obj is None:
            return None
        if isinstance(obj, IcebergIdentityTransform):
            return IdentityTransform.of()
        if isinstance(obj, IcebergHourTransform):
            return HourTransform.of()
        if isinstance(obj, IcebergDayTransform):
            return DayTransform.of()
        if isinstance(obj, IcebergMonthTransform):
            return MonthTransform.of()
        if isinstance(obj, IcebergYearTransform):
            return YearTransform.of()
        if isinstance(obj, IcebergIcebergVoidTransform):
            return VoidTransform.of()
        if isinstance(obj, IcebergBucketTransform):
            return BucketTransform.of(
                BucketTransformParameters.of(
                    num_buckets=obj.num_buckets,
                    bucketing_strategy=BucketingStrategy.ICEBERG,
                ),
            )
        if isinstance(obj, IcebergTruncateTransform):
            return TruncateTransform.of(
                TruncateTransformParameters.of(width=obj.width),
            )
        return UnknownTransform.of()

    @staticmethod
    def unmap(
        obj: Optional[Transform],
        **kwargs,
    ) -> Optional[IcebergTransform]:
        if obj is None:
            return None
        if obj.name == TransformName.IDENTITY:
            return IcebergIdentityTransform()
        if obj.name == TransformName.HOUR:
            return IcebergHourTransform()
        if obj.name == TransformName.DAY:
            return IcebergDayTransform()
        if obj.name == TransformName.MONTH:
            return IcebergMonthTransform()
        if obj.name == TransformName.YEAR:
            return IcebergYearTransform()
        if obj.name == TransformName.VOID:
            return IcebergIcebergVoidTransform()
        if obj.name == TransformName.BUCKET:
            parameters = BucketTransformParameters(obj.parameters)
            strategy = parameters.bucketing_strategy
            if strategy == BucketingStrategy.ICEBERG:
                return IcebergBucketTransform(parameters.num_buckets)
            else:
                err_msg = f"Unsupported Iceberg Bucketing Strategy: {strategy}."
                raise ValueError(err_msg)
        if obj.name == TransformName.TRUNCATE:
            parameters = TruncateTransformParameters(obj.parameters)
            return IcebergTruncateTransform(parameters.width)
        return IcebergUnknownTransform(obj.name)


class PartitionKeyMapper(ModelMapper[PartitionField, PartitionKey]):
    @staticmethod
    def map(
        obj: Optional[PartitionField],
        schema: IcebergSchema = IcebergSchema(),
        **kwargs,
    ) -> Optional[PartitionKey]:
        if obj is None:
            return None
        if not schema:
            err_msg = "Schema is required for Partition Field conversion."
            raise ValueError(err_msg)
        field = schema.find_field(name_or_id=obj.source_id)
        return PartitionKey.of(
            key=[field.name],
            key_types=[str(field.field_type)],
            name=obj.name,
            field_id=obj.field_id,
            transform=TransformMapper.map(obj.transform),
            native_object=obj,
        )

    @staticmethod
    def unmap(
        obj: Optional[PartitionKey],
        schema: IcebergSchema = IcebergSchema(),
        case_sensitive: bool = True,
    ) -> Optional[PartitionField]:
        if obj is None:
            return None
        if not schema:
            err_msg = "Schema is required for Partition Key conversion."
            raise ValueError(err_msg)
        if len(obj.key) > 1:
            err_msg = f"Iceberg only supports transforming 1 partition field."
            raise ValueError(err_msg)
        field = schema.find_field(
            name_or_id=obj.key[0],
            case_sensitive=case_sensitive,
        )
        return PartitionField(
            source_id=field.field_id,
            field_id=obj.id if obj.id else None,
            transform=TransformMapper.unmap(obj.transform),
            name=obj.name,
        )


class PartitionSchemeMapper(ModelMapper[PartitionSpec, PartitionScheme]):
    @staticmethod
    def map(
        obj: Optional[PartitionSpec],
        schema: IcebergSchema = IcebergSchema(),
        name: Optional[str] = None,
    ) -> Optional[PartitionScheme]:
        if obj is None:
            return None
        elif not schema:
            err_msg = "Schema is required for Partition Spec conversion."
            raise ValueError(err_msg)
        keys = [PartitionKeyMapper.map(field, schema) for field in obj.fields]
        return PartitionScheme.of(
            keys=keys,
            name=name,
            scheme_id=str(obj.spec_id),
            native_object=obj,
        )

    @staticmethod
    def unmap(
        obj: Optional[PartitionScheme],
        schema: IcebergSchema = IcebergSchema(),
        case_sensitive: bool = True,
    ) -> Optional[PartitionSpec]:
        if obj is None:
            return None
        if not schema:
            err_msg = "Schema is required for Partition Scheme conversion."
            raise ValueError(err_msg)
        fields = [PartitionKeyMapper.unmap(key, schema, case_sensitive) for key in obj]
        return PartitionSpec(
            fields=fields,
            spec_id=int(obj.id),
        )


class SortKeyMapper(ModelMapper[SortField, SortKey]):
    @staticmethod
    def unmap(
        obj: Optional[SortKey],
        schema: IcebergSchema = IcebergSchema(),
        case_sensitive: bool = True,
    ) -> Optional[SortField]:
        if obj is None:
            return None
        if not schema:
            err_msg = "Schema is required for Sort Key conversion."
            raise ValueError(err_msg)
        if len(obj.key) > 1:
            err_msg = f"Iceberg only supports transforming 1 sort field."
            raise ValueError(err_msg)
        field = schema.find_field(
            name_or_id=obj.key[0],
            case_sensitive=case_sensitive,
        )
        direction = (
            SortDirection.ASC
            if obj.sort_order is SortOrder.ASCENDING
            else SortDirection.DESC
            if obj.sort_order is SortOrder.DESCENDING
            else None
        )
        null_order = (
            IcebergNullOrder.NULLS_FIRST
            if obj.null_order is NullOrder.AT_START
            else IcebergNullOrder.NULLS_LAST
            if obj.null_order is NullOrder.AT_END
            else None
        )
        return SortField(
            source_id=field.field_id,
            transform=TransformMapper.unmap(obj.transform),
            direction=direction,
            null_order=null_order,
        )

    @staticmethod
    def map(
        obj: Optional[SortField],
        schema: IcebergSchema = IcebergSchema(),
        **kwargs,
    ) -> Optional[SortKey]:
        if obj is None:
            return None
        if not schema:
            err_msg = "Schema is required for Sort Field conversion."
            raise ValueError(err_msg)
        field = schema.find_field(name_or_id=obj.source_id)
        return SortKey.of(
            key=[field.name],
            sort_order=SortOrder(obj.direction.value or "ascending"),
            null_order=NullOrder(obj.null_order.value or "first"),
            transform=TransformMapper.map(obj.transform),
            native_object=obj,
        )


class SortSchemeMapper(ModelMapper[IcebergSortOrder, SortScheme]):
    @staticmethod
    def map(
        obj: Optional[IcebergSortOrder],
        schema: IcebergSchema = IcebergSchema(),
        name: Optional[str] = None,
        id: Optional[str] = None,
    ) -> Optional[SortScheme]:
        if obj is None:
            return None
        elif not schema:
            err_msg = "Schema is required for Sort Order conversion."
            raise ValueError(err_msg)
        keys = [SortKeyMapper.map(field, schema) for field in obj.fields]
        return SortScheme.of(
            keys=keys,
            name=name,
            scheme_id=id,
            native_object=obj,
        )

    @staticmethod
    def unmap(
        obj: Optional[SortScheme],
        schema: IcebergSchema = IcebergSchema(),
        case_sensitive: bool = True,
    ) -> Optional[IcebergSortOrder]:
        if obj is None:
            return None
        if not schema:
            err_msg = "Schema is required for Sort Scheme conversion."
            raise ValueError(err_msg)
        fields = [SortKeyMapper.unmap(key, schema, case_sensitive) for key in obj]
        return IcebergSortOrder(fields=fields)


class SchemaMapper(ModelMapper[IcebergSchema, Schema]):
    @staticmethod
    def map(
        obj: Optional[IcebergSchema],
        stream_locator: Optional[StreamLocator] = None,
        **kwargs,
    ) -> Optional[Schema]:
        if obj is None:
            return None
        schema: pa.Schema = schema_to_pyarrow(obj)
        # use DeltaCAT fields to extract field IDs from PyArrow schema metadata
        fields = [Field.of(field) for field in schema]
        final_fields = []
        for field in fields:
            iceberg_field = obj.find_field(field.id)
            final_field = Field.of(
                field=field.arrow,
                field_id=field.id,
                is_merge_key=field.id in obj.identifier_field_ids,
                doc=iceberg_field.doc,
                past_default=iceberg_field.initial_default,
                future_default=iceberg_field.write_default,
                native_object=iceberg_field,
            )
            final_fields.append(final_field)
        # TODO(pdames): Traverse DeltaCAT schemas to find one already related
        #  to this Iceberg schema.
        return Schema.of(
            schema=final_fields,
            metadata=schema.metadata,
            native_object=obj,
        )

    @staticmethod
    def unmap(
        obj: Optional[Schema], stream_locator: Optional[StreamLocator] = None, **kwargs
    ) -> Optional[IcebergSchema]:
        if obj is None:
            return None
        if isinstance(obj.arrow, pa.Schema):
            schema = pyarrow_to_schema(obj)
            final_fields = []
            for field in obj.field_ids_to_fields.values():
                iceberg_field = schema.find_field(field.id)
                final_field = NestedField(
                    field_id=iceberg_field.field_id,
                    name=iceberg_field.name,
                    field_type=iceberg_field.field_type,
                    required=iceberg_field.required,
                    doc=field.doc,
                    initial_default=field.past_default,
                    write_default=field.future_default,
                )
                final_fields.append(final_field)
            iceberg_schema = IcebergSchema(
                fields=final_fields,
                schema_id=obj.linked_schema_ids.get(stream_locator, INITIAL_SCHEMA_ID)
                if obj.linked_schema_ids is not None
                else INITIAL_SCHEMA_ID,
                identifier_field_ids=obj.merge_keys,
            )
        else:
            err_msg = (
                f"unsupported schema type: `{type(obj.arrow)}`. "
                f"expected schema type: {pa.Schema}"
            )
            raise TypeError(err_msg)
        return iceberg_schema


class NamespaceLocatorMapper(
    ModelMapper[Union[Identifier, IcebergNamespace], NamespaceLocator]
):
    @staticmethod
    def map(
        obj: Optional[Union[Identifier, IcebergNamespace]], **kwargs
    ) -> Optional[NamespaceLocator]:
        if obj is None:
            return None
        namespace = (
            NAMESPACE_SEPARATOR.join(obj.namespace.root[1:])
            if isinstance(obj, IcebergNamespace)
            else ".".join(Catalog.namespace_from(obj))
        )
        if not namespace:
            err_msg = f"No namespace in identifier: {obj}"
            raise NamespaceNotFoundError(err_msg)
        return NamespaceLocator.of(namespace)

    @staticmethod
    def unmap(obj: Optional[NamespaceLocator], **kwargs) -> Optional[Identifier]:
        if obj is None:
            return None
        return tuple(obj.namespace.split("."))


class NamespaceMapper(ModelMapper[Union[Identifier, IcebergNamespace], Namespace]):
    @staticmethod
    def map(
        obj: Optional[Union[Identifier, IcebergNamespace]], **kwargs
    ) -> Optional[Namespace]:
        if obj is None:
            return None
        locator = NamespaceLocatorMapper.map(obj)
        return Namespace.of(locator=locator, properties=None)

    @staticmethod
    def unmap(
        obj: Optional[Namespace],
        **kwargs,
    ) -> Optional[Identifier]:
        if obj is None:
            return None
        return NamespaceLocatorMapper.unmap(obj.locator)


class TableLocatorMapper(ModelMapper[Union[Identifier, TableIdentifier], TableLocator]):
    @staticmethod
    def map(
        obj: Optional[Union[Identifier, TableIdentifier]], **kwargs
    ) -> Optional[TableLocator]:
        if obj is None:
            return None
        namespace_locator = NamespaceLocatorMapper.map(obj)
        table_name = (
            obj.name
            if isinstance(obj, TableIdentifier)
            else Catalog.table_name_from(obj)
        )
        if not table_name:
            raise TableNotFoundError(f"No table name in identifier: {obj}")
        return TableLocator.of(namespace_locator, table_name)

    @staticmethod
    def unmap(
        obj: Optional[TableLocator], catalog_name: Optional[str] = None, **kwargs
    ) -> Optional[Union[Identifier, TableIdentifier]]:
        if obj is None:
            return None
        identifier = tuple(obj.namespace.split(".")) + (obj.table_name,)
        return identifier


class TableMapper(OneWayModelMapper[IcebergTable, Table]):
    @staticmethod
    def map(
        obj: Optional[IcebergTable],
        **kwargs,
    ) -> Optional[Table]:
        if obj is None:
            return None
        locator = TableLocatorMapper.map(obj.identifier)
        return Table.of(
            locator=locator,
            description=None,
            properties=None,
            native_object=obj,
        )


class TableVersionLocatorMapper(OneWayModelMapper[IcebergTable, TableVersionLocator]):
    @staticmethod
    def map(
        obj: Optional[IcebergTable], timestamp: Optional[int] = None, **kwargs
    ) -> Optional[TableVersionLocator]:
        if obj is None:
            return None
        table_version = _resolve_table_version(obj.metadata, timestamp)
        return TableVersionLocator.of(
            table_locator=TableLocatorMapper.map(obj.identifier),
            table_version=str(table_version),
        )


class TableVersionMapper(OneWayModelMapper[IcebergTable, TableVersion]):
    @staticmethod
    def map(
        obj: Optional[IcebergTable],
        timestamp: Optional[int] = None,
        catalog_properties: Dict[str, str] = EMPTY_DICT,
        **kwargs,
    ) -> Optional[TableVersion]:
        if obj is None:
            return None
        metadata = _resolve_table_version_metadata(obj, timestamp, catalog_properties)
        schema = _get_current_schema_for_meta(metadata)
        partition_spec = _get_current_spec_for_meta(metadata)
        sort_order = _get_current_sort_order_for_meta(metadata)
        return TableVersion.of(
            locator=TableVersionLocatorMapper.map(obj, timestamp),
            schema=SchemaMapper.map(schema),
            partition_scheme=PartitionSchemeMapper.map(partition_spec, schema),
            description=None,
            properties=obj.properties,
            content_types=None,
            sort_scheme=SortSchemeMapper.map(sort_order, schema),
            native_object=metadata,
        )


class StreamLocatorMapper(OneWayModelMapper[IcebergTable, StreamLocator]):
    @staticmethod
    def map(
        obj: Optional[IcebergTable],
        metadata_timestamp: Optional[int] = None,
        snapshot_id: Optional[int] = None,
        catalog_properties: Dict[str, str] = EMPTY_DICT,
        **kwargs,
    ) -> Optional[StreamLocator]:
        if obj is None:
            return None
        metadata = _resolve_table_version_metadata(
            obj, metadata_timestamp, catalog_properties
        )
        snapshot = _resolve_stream_snapshot(metadata, snapshot_id)
        return StreamLocator.of(
            table_version_locator=TableVersionLocatorMapper.map(
                obj, metadata_timestamp
            ),
            stream_id=str(snapshot.snapshot_id),
            stream_format=StreamFormat.ICEBERG.value,
        )


class StreamMapper(OneWayModelMapper[IcebergTable, Stream]):
    @staticmethod
    def map(
        obj: Optional[IcebergTable],
        # TODO (pdames): infer state from Iceberg metadata?
        state: Optional[CommitState] = CommitState.COMMITTED,
        metadata_timestamp: Optional[int] = None,
        snapshot_id: Optional[int] = None,
        catalog_properties: Dict[str, str] = EMPTY_DICT,
        **kwargs,
    ) -> Optional[Stream]:
        if obj is None:
            return None
        metadata = _resolve_table_version_metadata(
            obj, metadata_timestamp, catalog_properties
        )
        if not metadata.snapshots:
            return Stream.of(locator=None, partition_scheme=None)
        snapshot = _resolve_stream_snapshot(metadata, snapshot_id)
        schema = _get_current_schema_for_meta(metadata)
        partition_spec = _get_current_spec_for_meta(metadata)
        parent_snapshot_bytes = (
            snapshot.parent_snapshot_id.to_bytes(8, "big")
            if snapshot.parent_snapshot_id
            else None
        )
        return Stream.of(
            locator=StreamLocatorMapper.map(
                obj, metadata_timestamp, snapshot_id, catalog_properties
            ),
            partition_scheme=PartitionSchemeMapper.map(partition_spec, schema),
            state=state,
            previous_stream_id=parent_snapshot_bytes,
            native_object=snapshot,
        )
