from typing import Optional, Dict, Any

import pyarrow as pa
from pyarrow import Field as PaField
from daft import Schema as DaftSchema, DataType, TimeUnit
from daft.daft import PartitionField as DaftPartitionField, PartitionTransform as DaftTransform
from daft.logical.schema import Field as DaftField
from daft.io.scan import make_partition_field

from deltacat.storage.model.schema import Schema
from deltacat.storage.model.interop import ModelMapper
from deltacat.storage.model.partition import PartitionKey, PartitionScheme
from deltacat.storage.model.transform import (
    BucketingStrategy, Transform, TransformName, BucketTransform, BucketTransformParameters,
    HourTransform, DayTransform, MonthTransform, YearTransform,
    IdentityTransform, TruncateTransform, TruncateTransformParameters,
    VoidTransform, UnknownTransform
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

        # Create Daft field with same name and nullable property
        return DaftField.create(
            name=obj.name,
            dtype=DataType.from_arrow_type(obj.type), # type: ignore
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

        # Given a daft.PartitionTransform, there is currently no way to inspect its type
        # or attribute, thus conversion is not possible.
        # TODO: request Daft to expose Python friendly interface for daft.PartitionTransform
        raise NotImplementedError("Converting transform from Daft to DeltaCAT is not supported")

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
                raise ValueError(f"Unsupported Bucketing Strategy: {obj.parameters.bucketing_strategy}")
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
        raise NotImplementedError(f"Converting Daft PartitionField to DeltaCAT PartitionKey is not supported")

    @staticmethod
    def unmap(
            obj: Optional[PartitionKey],
            schema: Optional[Schema] = None,
            **kwargs,
    ) -> Optional[DaftPartitionField]:
        """Convert PartitionKey to DaftPartitionField.
        
        Args:
            obj: The PartitionKey to convert
            schema: The Schema containing field information
            **kwargs: Additional arguments
            
        Returns:
            Converted DaftPartitionField object
        """
        if obj is None:
            return None
        if not schema:
            raise ValueError("Schema is required for PartitionKey conversion")
        if len(obj.key) < 1:
            raise ValueError(f"At least 1 PartitionKey FieldLocator is expected, instead got {len(obj.key)}. FieldLocators: {obj.key}.")

        # Get the source field from schema
        dc_field = schema.field(obj.key[0]).arrow
        daft_field = DaftFieldMapper.unmap(obj=dc_field)
        # Convert transform if present
        daft_transform = DaftTransformMapper.unmap(obj.transform)

        # Create DaftPartitionField
        return make_partition_field(
            field=daft_field, # type: ignore
            transform=daft_transform
        )
    

