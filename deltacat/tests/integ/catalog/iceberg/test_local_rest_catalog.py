import logging
from typing import Dict

import pytest

from pyarrow.fs import S3FileSystem

from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import IdentityTransform, DayTransform
from pyiceberg.types import (
    NestedField,
    TimestampType,
    StringType,
    FloatType,
    DoubleType,
    StructType,
)

import deltacat as dc

from deltacat import logs
from deltacat import IcebergCatalog, get_catalog
from deltacat.storage.iceberg.model import (
    PartitionSchemeMapper,
    SchemaMapper,
    SortSchemeMapper,
    TableLocatorMapper,
)
from deltacat.utils.common import sha1_hexdigest

logger = logs.configure_application_logger(logging.getLogger(__name__))


@pytest.fixture()
def catalog_properties() -> Dict[str, str]:
    return {
        "type": "rest",
        "uri": "http://localhost:8181",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
    }


@pytest.fixture()
def mock_s3_file_system() -> S3FileSystem:
    return S3FileSystem(
        access_key="admin",
        secret_key="password",
        endpoint_override="http://localhost:9000",
    )


"""
import codecs
import json

from pyiceberg.io import load_file_io
from pyiceberg.serializers import FromInputFile, Compressor

def log_table_metadata():
    try:
        io = load_file_io(
            properties=catalog.properties,
            location=table.metadata_location,
        )
        input_file = io.new_input(table.metadata_location)
        with input_file.open() as byte_stream:
            compression = Compressor.get_compressor(location=input_file.location)
            with compression.stream_decompressor(byte_stream) as byte_stream:
                reader = codecs.getreader("utf-8")
                json_bytes = reader(byte_stream)
                metadata_json = json_bytes.read()
                json_obj = json.loads(metadata_json)
                metadata_json_pretty = json.dumps(json_obj, indent=2)
        identifier = table.identifier
        namespace = Catalog.namespace_from(table.identifier)
        raise ValueError(
            f"identifier: {identifier}, namespace: {namespace}, table.metadata_location: {table.metadata_location}, table.metadata: {table.metadata}, metadata_json: {metadata_json_pretty}"
        )
    except ValueError as e:
        logger.debug("Exception: ", e)
"""


@pytest.mark.integration
def test_create_table(
    catalog_properties: Dict[str, str], mock_s3_file_system: S3FileSystem
) -> None:

    try:
        get_catalog("iceberg")
        assert False
    except ValueError as e:
        logger.debug(f"Caught Expected Exception: {e}")
        assert True

    catalog_name = "test"
    dc.init(
        catalogs={
            "iceberg": dc.Catalog(
                # Apache Iceberg implementation of deltacat.catalog.interface
                impl=IcebergCatalog,
                # kwargs for pyiceberg.catalog.load_catalog start here...
                name=catalog_name,
                # for additional properties see:
                # https://py.iceberg.apache.org/configuration/
                properties=catalog_properties,
            )
        },
    )
    dc_catalog = get_catalog("iceberg")
    assert dc_catalog.impl == IcebergCatalog
    catalog: Catalog = get_catalog("iceberg").native_object
    assert catalog
    assert catalog.name == catalog_name
    assert catalog.properties == catalog_properties

    try:
        get_catalog("na")
        assert False
    except ValueError as e:
        logger.debug("Caught Expected Exception: ", e)
        assert True

    schema = Schema(
        NestedField(
            field_id=1, name="datetime", field_type=TimestampType(), required=True
        ),
        NestedField(field_id=2, name="symbol", field_type=StringType(), required=True),
        NestedField(field_id=3, name="bid", field_type=FloatType(), required=False),
        NestedField(field_id=4, name="ask", field_type=DoubleType(), required=False),
        NestedField(
            field_id=5,
            name="details",
            field_type=StructType(
                NestedField(
                    field_id=6,
                    name="created_by",
                    field_type=StringType(),
                    required=False,
                ),
            ),
            required=False,
        ),
    )

    partition_spec = PartitionSpec(
        PartitionField(
            source_id=1, field_id=1000, transform=DayTransform(), name="datetime_day"
        )
    )

    sort_order = SortOrder(SortField(source_id=2, transform=IdentityTransform()))

    table_name = "test_table"
    namespace = "test_namespace"
    table_definition = dc.create_table(
        table=table_name,
        namespace=namespace,
        schema=SchemaMapper.map(schema),
        partition_scheme=PartitionSchemeMapper.map(partition_spec, schema),
        sort_keys=SortSchemeMapper.map(sort_order, schema),
    )
    assert table_definition
    assert table_definition.table.table_name == table_name
    assert table_definition.table.namespace == f"{catalog_name}.{namespace}"
    namespace_digest = sha1_hexdigest(
        table_definition.table.namespace_locator.canonical_string().encode("utf-8")
    )
    iceberg_table = catalog.load_table(
        TableLocatorMapper.unmap(table_definition.table.locator, catalog.name)
    )
    assert table_definition.table.native_object == iceberg_table
    assert (
        table_definition.table.locator.canonical_string()
        == f"{namespace_digest}|{table_name}"
    )
