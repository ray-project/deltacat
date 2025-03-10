import os
import logging
import deltacat as dc

from deltacat import logs
from deltacat import IcebergCatalog
from deltacat.examples.common.fixtures import (
    create_runtime_environment,
    store_cli_args_in_os_environ,
)

from pyiceberg.schema import (
    Schema,
    NestedField,
    DoubleType,
    StringType,
    TimestampType,
    FloatType,
    StructType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.table.sorting import SortField, SortOrder

from deltacat.exceptions import TableAlreadyExistsError
from deltacat.storage.iceberg.model import (
    SchemaMapper,
    PartitionSchemeMapper,
    SortSchemeMapper,
)

# initialize the driver logger
driver_logger = logs.configure_application_logger(logging.getLogger(__name__))


def run(warehouse="s3://my-bucket/my/key/prefix", **kwargs):
    # create any runtime environment required to run the example
    runtime_env = create_runtime_environment()

    # Start by initializing DeltaCAT and registering available Catalogs.
    # Ray will be initialized automatically via `ray.init()`.
    # Only the `iceberg` data catalog is provided so it will become the default.
    # If initializing multiple catalogs, use the `default_catalog_name` param
    # to specify which catalog should be the default.
    dc.init(
        catalogs={
            # the name of the DeltaCAT catalog is "iceberg"
            "iceberg": dc.Catalog(
                # Apache Iceberg implementation of deltacat.catalog.interface
                impl=IcebergCatalog,
                # kwargs for pyiceberg.catalog.load_catalog start here...
                # the name of the Iceberg catalog is "example-iceberg-catalog"
                name="example-iceberg-catalog",
                # for additional properties see:
                # https://py.iceberg.apache.org/configuration/
                properties={
                    "type": "glue",
                    "region_name": "us-east-1",
                    "warehouse": warehouse,
                },
            )
        },
        # pass the runtime environment into ray.init()
        ray_init_args={"runtime_env": runtime_env},
    )

    # define a native Iceberg table schema
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

    # define a native Iceberg partition spec
    partition_spec = PartitionSpec(
        PartitionField(
            source_id=1, field_id=1000, transform=DayTransform(), name="datetime_day"
        )
    )

    # define a native Iceberg sort order
    sort_order = SortOrder(SortField(source_id=2, transform=IdentityTransform()))

    # create a table named `test_namespace.test_table`
    # we don't need to specify which catalog to create this table in since
    # only the "iceberg" catalog is available
    table_name = "test_table"
    namespace = "test_namespace"
    print(f"Creating Glue Table: {namespace}.{table_name}")
    try:
        table_definition = dc.create_table(
            table=table_name,
            namespace=namespace,
            schema=SchemaMapper.map(schema),
            partition_scheme=PartitionSchemeMapper.map(partition_spec, schema),
            sort_keys=SortSchemeMapper.map(sort_order, schema),
        )
        print(f"Created Glue Table: {table_definition}")
    except TableAlreadyExistsError:
        print(f"Glue Table `{namespace}.{table_name}` already exists.")

    print(f"Getting Glue Table: {namespace}.{table_name}")
    table_definition = dc.get_table(table_name, namespace)
    print(f"Retrieved Glue Table: {table_definition}")


if __name__ == "__main__":
    example_script_args = [
        (
            [
                "--warehouse",
            ],
            {
                "help": "S3 path for Iceberg file storage.",
                "type": str,
            },
        ),
        (
            [
                "--STAGE",
            ],
            {
                "help": "Example runtime environment stage (e.g. dev, alpha, beta, prod).",
                "type": str,
            },
        ),
    ]

    # store any CLI args in the runtime environment
    store_cli_args_in_os_environ(example_script_args)

    # run the example using os.environ as kwargs
    run(**os.environ)
