# DeltaCAT

DeltaCAT provides a portable Pythonic Data Catalog API powered by Ray. It provides distributed compute implementations
allowing you to efficiently read, write, and manage your existing data catalogs reliably at scale using Ray. It lets you
define and manage fast, scalable, ACID-compliant data catalogs and has been used to successfully manage exabyte-scale
enterprise data lakes.

DeltaCAT uses the Ray distributed compute framework together with Apache Arrow and Daft to efficiently scale common
table management tasks, like petabyte-scale merge-on-read and copy-on-write operations.

## Getting Started

### Apache Iceberg
#### Installation
```shell
pip install deltacat[iceberg]
```

#### AWS Glue 
##### Initializing a Catalog
We can initialize a DeltaCAT catalog against either a new or existing Iceberg catalog. If you initialize DeltaCAT
against an existing catalog, you can immediately start reading and writing to any tables it contains. If you create
a new catalog, you can immediately start adding new tables to it and managing them.
```python
import deltacat as dc
from deltacat import IcebergCatalog


# Start by initializing DeltaCAT and registering all Catalogs you'd like to make available to your Ray application.
# Ray will be initialized automatically via `ray.init()`.

# Use top-level DeltaCAT APIs (e.g. `dc.create_table`) to manage tables in any registered catalog.
# DeltaCAT APIs will act on a default catalog unless a different catalog name is explicitly specified.

# Here, since only the `iceberg` data catalog is registered, it will become the default.
# When initializing multiple catalogs, you can use the `default_catalog_name` param
# to specify which catalog should be used by default.
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
            properties={"type": "glue"},
        )
    },
)
```
The `iceberg` catalog is now registered as the default catalog for your Ray application and is ready to start receiving
requests.

##### Creating a New Table
We can now create our first table inside of `example-iceberg-catalog` .

```python
import deltacat as dc

from pyiceberg.schema import (
    Schema,
    NestedField,
    DoubleType,
    StringType,
    TimestampType,
    FloatType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.table.sorting import SortField, SortOrder

# define a simple native Iceberg Schema
schema = Schema(
    NestedField(
        field_id=1, name="datetime", field_type=TimestampType(), required=True
    ),
    NestedField(field_id=2, name="symbol", field_type=StringType(), required=True),
    NestedField(field_id=3, name="bid", field_type=FloatType(), required=False),
    NestedField(field_id=4, name="ask", field_type=DoubleType(), required=False),
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
# we don't need to specify which catalog to create this table in since only the "iceberg" catalog is available
table_name = "test_table"
namespace = "test_namespace"
table_definition = dc.create_table(
    table=table_name,
    namespace=namespace,
    schema=schema,  # can be either dc.schema or pyiceberg.schema.Schema
    partition_scheme=partition_spec,  # can be either dc.PartitionScheme or pyiceberg.partitioning.PartitionSpec
    sort_keys=sort_order,  # can be either dc.SortOrder or pyiceberg.table.sorting.SortOrder
)
print(f"Created Glue Iceberg Table: {table_definition}")

# get the definition of the table we just created
table_definition = dc.get_table(table_name, namespace)
print(f"Retrieved Glue Iceberg Table: {table_definition}")
```
