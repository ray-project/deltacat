from typing import Optional

import pyarrow as pa
from pyarrow import Field as PaField
from daft import Schema as DaftSchema, DataType
from daft.daft import (
    PartitionField as DaftPartitionField,
    PartitionTransform as DaftTransform,
)
from daft.logical.schema import Field as DaftField
from daft.io.scan import make_partition_field

from deltacat.storage.model.schema import Schema
from deltacat.storage.model.interop import ModelMapper
from deltacat.storage.model.partition import PartitionKey
from deltacat.storage.model.transform import (
    BucketingStrategy,
    Transform,
    BucketTransform,
    HourTransform,
    DayTransform,
    MonthTransform,
    YearTransform,
    IdentityTransform,
    TruncateTransform,
)


class DaftFieldMapper(ModelMapper[DaftField, PaField]):
    @staticmethod
    def map(
        obj: Optional[DaftField],
        **kwargs,
    ) -> Optional[PaField]:
        """Convert Daft Field to PyArrow Field.

        Args:
            obj: The Daft Field to convert
            **kwargs: Additional arguments

        Returns:
            Converted PyArrow Field object
        """
        if obj is None:
            return None

        return pa.field(
            name=obj.name,
            type=obj.dtype.to_arrow_dtype(),
        )

    @staticmethod
    def unmap(
        obj: Optional[PaField],
        **kwargs,
    ) -> Optional[DaftField]:
        """Convert PyArrow Field to Daft Field.

        Args:
            obj: The PyArrow Field to convert
            **kwargs: Additional arguments

        Returns:
            Converted Daft Field object
        """
        if obj is None:
            return None

        return DaftField.create(
            name=obj.name,
            dtype=DataType.from_arrow_type(obj.type),  # type: ignore
        )


class DaftTransformMapper(ModelMapper[DaftTransform, Transform]):
    @staticmethod
    def map(
        obj: Optional[DaftTransform],
        **kwargs,
    ) -> Optional[Transform]:
        """Convert DaftTransform to DeltaCAT Transform.

        Args:
            obj: The DaftTransform to convert
            **kwargs: Additional arguments

        Returns:
            Converted Transform object
        """

        # daft.PartitionTransform doesn't have a Python interface for accessing its attributes,
        # thus conversion is not possible.
        # TODO: request Daft to expose Python friendly interface for daft.PartitionTransform
        raise NotImplementedError(
            "Converting transform from Daft to DeltaCAT is not supported"
        )

    @staticmethod
    def unmap(
        obj: Optional[Transform],
        **kwargs,
    ) -> Optional[DaftTransform]:
        """Convert DeltaCAT Transform to DaftTransform.

        Args:
            obj: The Transform to convert
            **kwargs: Additional arguments

        Returns:
            Converted DaftTransform object
        """
        if obj is None:
            return None

        # Map DeltaCAT transforms to Daft transforms using isinstance

        if isinstance(obj, IdentityTransform):
            return DaftTransform.identity()
        elif isinstance(obj, HourTransform):
            return DaftTransform.hour()
        elif isinstance(obj, DayTransform):
            return DaftTransform.day()
        elif isinstance(obj, MonthTransform):
            return DaftTransform.month()
        elif isinstance(obj, YearTransform):
            return DaftTransform.year()
        elif isinstance(obj, BucketTransform):
            if obj.parameters.bucketing_strategy == BucketingStrategy.ICEBERG:
                return DaftTransform.iceberg_bucket(obj.parameters.num_buckets)
            else:
                raise ValueError(
                    f"Unsupported Bucketing Strategy: {obj.parameters.bucketing_strategy}"
                )
        elif isinstance(obj, TruncateTransform):
            return DaftTransform.iceberg_truncate(obj.parameters.width)

        raise ValueError(f"Unsupported Transform: {obj}")


class DaftPartitionKeyMapper(ModelMapper[DaftPartitionField, PartitionKey]):
    @staticmethod
    def map(
        obj: Optional[DaftPartitionField],
        schema: Optional[DaftSchema] = None,
        **kwargs,
    ) -> Optional[PartitionKey]:
        """Convert DaftPartitionField to PartitionKey.

        Args:
            obj: The DaftPartitionField to convert
            schema: The Daft schema containing field information
            **kwargs: Additional arguments

        Returns:
            Converted PartitionKey object
        """
        # Daft PartitionField only exposes 1 attribute `field` which is not enough
        # to convert to DeltaCAT PartitionKey
        # TODO: request Daft to expose more Python friendly interface for PartitionField
        raise NotImplementedError(
            f"Converting Daft PartitionField to DeltaCAT PartitionKey is not supported"
        )

    @staticmethod
    def unmap(
        obj: Optional[PartitionKey],
        schema: Optional[Schema] = None,
        **kwargs,
    ) -> Optional[DaftPartitionField]:
        """Convert PartitionKey to DaftPartitionField.

        Args:
            obj: The DeltaCAT PartitionKey to convert
            schema: The Schema containing field information
            **kwargs: Additional arguments

        Returns:
            Converted DaftPartitionField object
        """
        if obj is None:
            return None
        if obj.name is None:
            raise ValueError("Name is required for PartitionKey conversion")
        if not schema:
            raise ValueError("Schema is required for PartitionKey conversion")
        if len(obj.key) < 1:
            raise ValueError(
                f"At least 1 PartitionKey FieldLocator is expected, instead got {len(obj.key)}. FieldLocators: {obj.key}."
            )

        # Get the source field from schema - FieldLocator in PartitionKey.key points to the source field of partition field
        dc_source_field = schema.field(obj.key[0]).arrow
        daft_source_field = DaftFieldMapper.unmap(obj=dc_source_field)
        # Convert transform if present
        daft_transform = DaftTransformMapper.unmap(obj.transform)
        daft_partition_field = DaftPartitionKeyMapper.get_daft_partition_field(
            partition_field_name=obj.name,
            daft_source_field=daft_source_field,
            dc_transform=obj.transform,
        )

        # Create DaftPartitionField
        return make_partition_field(
            field=daft_partition_field,
            source_field=daft_source_field,
            transform=daft_transform,
        )

    @staticmethod
    def get_daft_partition_field(
        partition_field_name: str,
        daft_source_field: Optional[DaftField],
        # TODO: replace DeltaCAT transform with Daft Transform for uniformality
        # We cannot use Daft Transform here because Daft Transform doesn't have a Python interface for us to
        # access its attributes.
        # TODO: request Daft to provide a more python friendly interface for Daft Tranform
        dc_transform: Optional[Transform],
    ) -> DaftField:
        """Generate Daft Partition Field given partition field name, source field and transform.
        Partition field type is inferred using source field type and transform.

        Args:
            partition_field_name (str): the specified result field name
            daft_source_field (DaftField): the source field of the partition field
            daft_transform (DaftTransform): transform applied on the source field to create partition field

        Returns:
            DaftField: Daft Field representing the partition field
        """
        if daft_source_field is None:
            raise ValueError("Source field is required for PartitionField conversion")
        if dc_transform is None:
            raise ValueError("Transform is required for PartitionField conversion")

        result_type = None
        # Below type conversion logic references Daft - Iceberg conversion logic:
        # https://github.com/Eventual-Inc/Daft/blob/7f2e9b5fb50fdfe858be17572f132b37dd6e5ab2/daft/iceberg/iceberg_scan.py#L61-L85
        if isinstance(dc_transform, IdentityTransform):
            result_type = daft_source_field.dtype
        elif isinstance(dc_transform, YearTransform):
            result_type = DataType.int32()
        elif isinstance(dc_transform, MonthTransform):
            result_type = DataType.int32()
        elif isinstance(dc_transform, DayTransform):
            result_type = DataType.int32()
        elif isinstance(dc_transform, HourTransform):
            result_type = DataType.int32()
        elif isinstance(dc_transform, BucketTransform):
            result_type = DataType.int32()
        elif isinstance(dc_transform, TruncateTransform):
            result_type = daft_source_field.dtype
        else:
            raise ValueError(f"Unsupported transform: {dc_transform}")

        return DaftField.create(
            name=partition_field_name,
            dtype=result_type,
        )
