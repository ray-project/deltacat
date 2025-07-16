import pytest
import pyarrow as pa
from daft import DataType, TimeUnit
from daft.logical.schema import Field as DaftField

from deltacat.storage.model.transform import IdentityTransform
from deltacat.storage.model.partition import PartitionKey
from deltacat.utils.daft import DaftFieldMapper, DaftPartitionKeyMapper

from deltacat.storage.model.schema import Field, Schema


class TestDaftFieldMapper:
    def test_field_mapper_basic_types(self):
        """Test mapping basic data types between Daft and PyArrow fields"""
        test_cases = [
            (DataType.int32(), pa.int32()),
            (DataType.int64(), pa.int64()),
            (DataType.float32(), pa.float32()),
            (DataType.float64(), pa.float64()),
            (DataType.string(), pa.large_string()),
            (DataType.bool(), pa.bool_()),
            (DataType.binary(), pa.large_binary()),
            (DataType.date(), pa.date32()),
            (DataType.timestamp(TimeUnit.ns()), pa.timestamp("ns")),
        ]

        for daft_type, pa_type in test_cases:
            # Create test fields
            daft_field = DaftField.create(
                name="test_field",
                dtype=daft_type,
            )

            # Daft to PyArrow
            pa_field = DaftFieldMapper.map(daft_field)
            assert pa_field is not None
            assert pa_field.name == "test_field"
            assert pa_field.type == pa_type  # type: ignore
            assert pa_field.nullable is True

            # PyArrow to Daft
            daft_field_back = DaftFieldMapper.unmap(pa_field)
            assert daft_field_back is not None
            assert daft_field_back.name == daft_field.name
            assert daft_field_back.dtype == daft_field.dtype


class TestDaftPartitionKeyMapper:
    def test_unmap(self):
        """
        Test unmap method of DaftPartitionKeyMapper when obj is not None, schema is provided,
        len(obj.key) is 1, and dc_field is found in the schema.

        This test verifies that the method correctly converts a PartitionKey to a DaftPartitionField
        when all conditions are met and the field exists in the schema.
        """
        # Create a mock schema
        schema = Schema.of(schema=[Field.of(pa.field("test_field", pa.int32()))])
        # Create a PartitionKey object
        partition_key = PartitionKey(
            key=["test_field"], transform=IdentityTransform(), name="partition_field"
        )

        result = DaftPartitionKeyMapper.unmap(obj=partition_key, schema=schema)
        assert result is not None
        assert result.field.name() == "partition_field"
        assert DataType._from_pydatatype(result.field.dtype()) == DataType.int32()

    def test_unmap_no_field_locator(self):
        schema = Schema.of(schema=[Field.of(pa.field("test_field", pa.int32()))])
        partition_key = PartitionKey(key=[], name="partition_field")

        with pytest.raises(ValueError) as excinfo:
            DaftPartitionKeyMapper.unmap(partition_key, schema)

        assert "At least 1 PartitionKey FieldLocator is expected" in str(excinfo.value)

    def test_unmap_partition_key_not_found(self):
        schema = Schema.of(schema=[Field.of(pa.field("test_field", pa.int32()))])
        partition_key = PartitionKey(
            key=["test_field_2"], transform=IdentityTransform(), name="partition_field"
        )

        with pytest.raises(KeyError) as excinfo:
            DaftPartitionKeyMapper.unmap(partition_key, schema)

        assert "Column test_field_2 does not exist in schema" in str(excinfo.value)

    def test_unmap_partition_name_not_defined(self):
        schema = Schema.of(schema=[Field.of(pa.field("test_field", pa.int32()))])
        partition_key = PartitionKey(key=[])

        with pytest.raises(ValueError) as excinfo:
            DaftPartitionKeyMapper.unmap(partition_key, schema)

        assert "Name is required for PartitionKey conversion" in str(excinfo.value)
