# Tables

DeltaCAT tables can be read and written using any Arrow-compatible **Dataset Type** with Ray.
Tables may either be schemaless or backed by a schema based on the [Arrow type system](https://arrow.apache.org/docs/python/api/datatypes.html) (see [DeltaCAT Schemas](../schema/README.md)). Tables can be created in explicit **Namespaces** or written to a default global namespace. Tables can be read/written within the context of an implicit single-table transaction, or within the context of a multi-table transaction spanning any number of tables and namespaces.

## Supported Dataset Types

DeltaCAT tables can be written and read using either local ([PyArrow](https://arrow.apache.org/docs/python),
[Pandas](https://pandas.pydata.org/docs/), [Polars](https://docs.pola.rs/), [NumPy](https://numpy.org/)) or distributed ([Daft](https://github.com/Eventual-Inc/Daft), [Ray Data](https://docs.ray.io/en/latest/data/data.html)) dataset types.

For example, the following code automatically creates the DeltaCAT table `"my_table"` from a Pandas DataFrame:
```python
import pandas as pd
import deltacat as dc

data = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35],
    "city": ["New York", "Los Angeles", "Chicago"],
})
dc.write(
    data,
    "my_table",
)
```

We can now append another Pandas DataFrame to the table we just created:
```python
pyarrow_table = pa.Table.from_pydict({
    "id": [4, 5, 6],
    "name": ["Dave", "Eve", "Frank"],
    "age": [40, 45, 50],
    "city": ["San Francisco", "Seattle", "Boston"],
})
dc.write(
    pyarrow_table,
    "my_table",
)
```

And read it back using Daft (DeltaCAT's default dataset reader):
```python
daft_dataframe = dc.read("my_table")
```
Daft dataframes support distribution across a Ray cluster and are lazily evaluated, so the data is not loaded into memory until it is needed. This makes them great for reading large tables. For more information, see the [Daft DataFrame API](https://docs.daft.ai/en/stable/api/dataframe/).

Small tables that fit in local memory can also be read using PyArrow, Pandas, Polars, or NumPy. For example,
the following code reads the same table into a Polars DataFrame:
```python
polars_dataframe = dc.read("my_table", read_as=dc.DatasetType.POLARS)
```
This dataframe will be eagerly materialized at read time. For tables that only contain Parquet files (default behavior), you can also use the `PYARROW_PARQUET` dataset type to retrieve unmaterialized PyArrow `ParquetFile` objects:

**Single write case (returns single ParquetFile):**
```python
# Table with only one write operation returns a single ParquetFile
pyarrow_parquet_file = dc.read("my_table", read_as=dc.DatasetType.PYARROW_PARQUET)
# Access the file directly
table_data = pyarrow_parquet_file.read()
```

**Multiple writes case (returns list of ParquetFile objects):**
```python
# After multiple write operations to the same table
dc.write(additional_data, "my_table")  # Second write
pyarrow_parquet_files = dc.read("my_table", read_as=dc.DatasetType.PYARROW_PARQUET)
# Now it returns a list of ParquetFile objects
for parquet_file in pyarrow_parquet_files:
    table_data = parquet_file.read()
```
Each `ParquetFile` can be materialized by calling `read` on the `ParquetFile` reference (see the [PyArrow ParquetFile API](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html))

## Write Modes
Data from any supported dataset type is written to a table using `dc.write` with one of the following write modes:

**AUTO (default)**
CREATE the table if it doesn't exist, ADD to the table if it exists without schema merge keys, and MERGE if the table exists with merge keys.

**CREATE**
Create the table if it doesn't exist, throw an error if it does.

**ADD**
Add unordered data to the table if it exists without merge keys, throw an error if it doesn't.

**APPEND**
Append ordered data to the table if it exists without merge keys, throw an error if it doesn't.

**REPLACE**
Replace existing table contents with the data to write.

**MERGE**
Insert or update records matching table merge keys, or throw an error if the table has no merge keys.

**DELETE**
Delete records matching table merge keys, or throw an error if the table has no merge keys.

## Ordered vs. Unordered Writes
Note that the only difference between **APPEND** and **ADD** writes is whether the data write order is
preserved on read. To ensure that the data order is preserved, concurrent **APPEND** writes to the same
table partition always fail with a concurrent modification exception. On the other hand, concurrent
**ADD** writes will always succeed unless concurrent writers ALSO trigger concurrent copy-on-write
table compaction (whose frequency is controlled by the READ_OPTIMIZATION_LEVEL and COMPACTION_TRIGGER
table properties listed below).

## Merge Keys
Table schemas may be defined with one more more merge keys. Merge keys are used to identify records that
should be updated or deleted when writing to a table. They are defined in the table's schema via by setting
the `is_merge_key` Field property to `True`.

For example, the following schema defines a table with a merge key on the `id` field:
```python
import pyarrow as pa
from deltacat import Schema, Field

schema = Schema.of(
    [
        Field.of(pa.field("id", pa.int64()), is_merge_key=True),
        Field.of(pa.field("name", pa.string())),
        Field.of(pa.field("age", pa.int32())),
        Field.of(pa.field("city", pa.string())),
    ]
)
```
**MERGE** writes to this table will update the `"name"`, `"age"`, and `"city"` fields for records with the same `"id"` value, and insert new records for any `"id"` values that don't already exist in the table. **DELETE** writes will remove records with matching `"id"` values.

For example, the following **MERGE** updates the `"name"`, `"age"`, and `"city"` fields for `"id"` values `1`, `2`, and `3`, and inserts new records for `"id"` values `4` and `5`:
```python
import deltacat as dc
import pandas as pd
import pyarrow as pa
from deltacat import Schema, Field

# Define schema with id as merge key (required for merge operations)
schema = Schema.of([
    Field.of(pa.field("id", pa.int64()), is_merge_key=True),
    Field.of(pa.field("name", pa.string())),
    Field.of(pa.field("age", pa.int32())),
    Field.of(pa.field("city", pa.string())),
])

data = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35],
    "city": ["New York", "Los Angeles", "Chicago"],
})
dc.write(
    data,
    "my_table",
    schema=schema,
    mode=dc.TableWriteMode.CREATE,
)
upsert_data = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alfred", "Bruce", "Chuck", "David", "Eve"],
    "age": [35, 45, 55, 65, 75],
    "city": ["London", "San Francisco", "Seattle", "Sydney", "Zurich"],
})
dc.write(
    upsert_data,
    "my_table",
    mode=dc.TableWriteMode.MERGE,
)
final_table = dc.read("my_table")
print(final_table)
# Output:
# id  name  age city
# 1   Alfred  35  London
# 2   Bruce   45  San Francisco
# 3   Chuck   55  Seattle
# 4   David   65  Sydney
# 5    Eve  75  Zurich
```

The following **DELETE** removes records with `"id"` values `1` and `2`:
```python
import deltacat as dc
import pandas as pd
import pyarrow as pa
from deltacat import Schema, Field

# Define schema with id as merge key (required for delete operations)
schema = Schema.of([
    Field.of(pa.field("id", pa.int64()), is_merge_key=True),
    Field.of(pa.field("name", pa.string())),
    Field.of(pa.field("age", pa.int32())),
    Field.of(pa.field("city", pa.string())),
])

data = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35],
    "city": ["New York", "Los Angeles", "Chicago"],
})
dc.write(
    data,
    "my_table",
    schema=schema,
    mode=dc.TableWriteMode.CREATE,
)
delete_data = pd.DataFrame({
    "id": [1, 2],
})
dc.write(
    delete_data,
    "my_table",
    mode=dc.TableWriteMode.DELETE,
)
final_table = dc.read("my_table")
print(final_table)
# Output:
# id  name  age city
# 3  Charlie  35  Chicago
```

Note that `dc.write` and `dc.read` are just aliases for `dc.write_to_table` and `dc.read_table`. Their behavior is identical, and the choice of which to use is a matter of personal preference.

## Table Properties
Table properties are used to configure the table's read/write behavior and optimization level. Custom table properties can be specified during table writes via `dc.write`, when the table is created via `dc.create_table`, and when the table is updated via `dc.alter_table`. Any properties that are not explicitly set at table creation time will inherit default values. Table properties can be read via a table version's `read_table_property` method and updated via `dc.alter_table`.

For example, the following code reads the schema evolution mode of a table and updates it to `MANUAL`:
```python
import deltacat as dc

table_info = dc.get_table("my_table")
schema_evolution_mode = table_info.table_version.read_table_property(
    TableProperty.SCHEMA_EVOLUTION_MODE
)
dc.alter_table(
    "my_table",
    table_version_properties={
        TableProperty.SCHEMA_EVOLUTION_MODE: SchemaEvolutionMode.MANUAL,
    },
)
```

DeltaCAT table's support the following properties:

**READ_OPTIMIZATION_LEVEL (default: MAX)**
Controls the read optimization level for the table. For more information, see [DeltaCAT Table Read Optimization Levels](#table-read-optimization-levels).

**RECORDS_PER_COMPACTED_FILE (default: 4000000)**
Target number of output records to write per file.

**APPENDED_RECORD_COUNT_COMPACTION_TRIGGER (default: 8000000)**
Number of records that can be appended to the table before a compaction job will be triggered to merge them into a single delta with `RECORDS_PER_COMPACTED_FILE` records per file.

**APPENDED_FILE_COUNT_COMPACTION_TRIGGER (default: 1000)**
Number of files that can be appended to the table before a compaction job will be triggered to merge them into a single delta with `RECORDS_PER_COMPACTED_FILE` records per file.

**APPENDED_DELTA_COUNT_COMPACTION_TRIGGER (default: 100)**
Number of deltas that can be appended to the table before a compaction job will be triggered to merge them into a single delta with `RECORDS_PER_COMPACTED_FILE` records per file.

**DEFAULT_COMPACTION_HASH_BUCKET_COUNT (default: 8)**
Number of hash buckets to use during compaction to distribute records across multiple workers. Higher values enable better parallelism for large datasets but may create more files for small datasets. Set to 1 to produce a minimal number of compacted file per partition while still honoring the `RECORDS_PER_COMPACTED_FILE` target.

**SCHEMA_EVOLUTION_MODE (default: AUTO)**
Controls how schema changes are handled when writing to a table (see [DeltaCAT Schemas](../schema/README.md)).

**DEFAULT_SCHEMA_CONSISTENCY_TYPE (default: COERCE)**
Controls the default schema consistency type for new fields added to the table (see [DeltaCAT Schemas](../schema/README.md)).

**SUPPORTED_READER_TYPES (default: [PANDAS, POLARS, PYARROW, NUMPY, DAFT, RAY_DATASET])**
Supported dataset types that should be able to read from the table. Writes that would break one or more of these readers will be rejected.

## Table Read Optimization Levels
Table read optimization levels let you choose the most appropriate balance between read and write performance.
DeltaCAT table's support the following read optimization levels:

**NONE**
No read optimization. Deletes and updates are resolved by finding the values
that match merge key predicates by running compaction at read time. Provides the
fastest/cheapest writes but slow/expensive reads. Resilient to conflicts with concurrent
writes, including table management jobs like compaction.

**MODERATE**
Discover record indexes that match merge key predicates at write time and record
those values as logically deleted (e.g., using a bitmask). Provides faster/cheaper reads but
slower/more-expensive writes. May conflict with concurrent writes that remove/replace data
files like compaction.

**MAX (default)**
Materialize all deletes and updates at write time by running compaction during
every write. Provides fast/cheap reads but slow/expensive writes. May conflict with
concurrent writes, including table management jobs like compaction.

## Altering Tables
Tables can be altered via `dc.alter_table`. This API lets you alter the table's schema, partition scheme, sort keys,
lifecycle state, description, and properties.

### Schema Evolution
By default, DeltaCAT will automatically evolve the table's schema to match the schema of the data written.
This is controlled by the `SCHEMA_EVOLUTION_MODE` and `DEFAULT_SCHEMA_CONSISTENCY_TYPE` table properties (see [DeltaCAT Schemas](../schema/README.md)).

To manually evolve the table's schema, you can use the `update` method on the table version's schema.
For example, the following code updates the documentation of the `name` field to `"first name"`:

```python
table = dc.get_table("my_table")
dc.alter_table(
    "my_table",
    schema_updates=table.table_version.schema.update().update_field_doc("name", "first name"),
)
```

You can also optionally preview a schema update before committing it by using the `apply` method on the schema update object:
```python
schema_update = table.table_version.schema.update().update_field_doc("name", "first name")
updated_schema_preview = schema_update.apply()
print(updated_schema_preview)

# after previewing the updated schema, you can commit it using `alter_table`
dc.alter_table(
    "my_table",
    schema_updates=schema_update,
)
```

### Future and Past Defaults
When writing data that is missing a field defined in the table's schema, DeltaCAT will use the table schema's
**Future Default** field value to set the value written. When reading historic data missing a field defined in the table's schema, DeltaCAT will use the table schema's **Past Default** field value to set the value read.

For example, the following code defines a `"priority"` Schema field with past default of `"low"` and future default of `"medium"`:
```python
# Create field with both past_default and explicit future_default
field = Field.of(
    field=pa.field("priority", pa.string(), nullable=True),
    field_id=1,
    past_default="low",
    future_default="medium",  # Explicitly set to different value
)
```
All future reads of data missing the `"priority"` field will inherit the past default of `"low"`, and all future
writes of data missing the `"priority"` field will inherit the future default of `"medium"`.


## Table Versions and Lifecycle Management
Internally, all DeltaCAT tables are versioned. Unless you specify an explicit table version to write to
or read from, DeltaCAT automatically resolves each operation to the latest **ACTIVE** table version. The
latest active table version is determined by its version number (max is latest) and lifecycle state,
which is one of the following:

**CREATED**: The table version has been created but is not ready for others to write to it or read from it.

**UNRELEASED**: The table version is not yet ready for public consumption. It may be in the process of being updated.

**ACTIVE**: The table version is ready for public consumption.

**DEPRECATED**: The table version is deprecated. Consumers should migrate off of this version before it is deleted.

**BETA**: The table version is ready for public consumption beta testing.

**DELETED**: The table version and its underlying data has been physically deleted.

You can specify an explicit table version to write to or read from by passing the `table_version` parameter to `dc.write` or `dc.read`.
For example, the following code will attempt to write to version `"1"` of `"my_table"`, regardless of its lifecycle state:
```python
dc.write(
    data,
    "my_table",
    table_version="1",
)
```
By default, any new table that you write to will have a new table version created with its lifecycle state set to **ACTIVE**.
You can specify a different lifecycle state by passing the `lifecycle_state` parameter to `dc.write`, `dc.create_table`, or `dc.alter_table`.

For example, the following code will write to new table version `"2"` of `"my_table"` with its lifecycle state set to **UNRELEASED**:

```python
dc.write(
    data,
    "my_table",
    table_version="2",
    lifecycle_state=LifecycleState.UNRELEASED,
)
```

Since table version `"2"` has its lifecycle state set to **UNRELEASED**, table consumers will continue to read from the previous **ACTIVE** version `"1"` of `"my_table"` unless they explicitly target the new version `"2"`. For example:

```python
# Read from ACTIVE version "1" of "my_table" (implicit version resolution)
dataframe_from_v1 = dc.read("my_table")

# Read from UNRELEASED version "2" of "my_table" (explicit version specification)
dataframe_from_v2 = dc.read("my_table", table_version="2")
```

By default, table versions inherit their properties from their parent table's properties at creation time. Any
table properties that are not explicitly specified on either the parent table or table version will inherit default values.

## Transactions

DeltaCAT provides ACID-compliant transactions that can span multiple tables and namespaces within a given catalog. Transactions ensure that all operations within a transaction either succeed together or fail together, maintaining data consistency across your entire data lake.

### Basic Multi-Table Transactions

The simplest way to use transactions is with the `dc.transaction()` context manager. All table operations within the transaction will be executed atomically:

```python
import deltacat as dc
import pandas as pd

# Sample data
users_data = pd.DataFrame({
    "user_id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "email": ["alice@example.com", "bob@example.com", "charlie@example.com"]
})

orders_data = pd.DataFrame({
    "order_id": [101, 102, 103],
    "user_id": [1, 2, 1],
    "amount": [25.99, 15.50, 42.00],
    "status": ["completed", "pending", "completed"]
})

# Execute multiple table operations atomically
with dc.transaction():
    # Create users table
    dc.write(users_data, "users", mode=dc.TableWriteMode.CREATE)

    # Create orders table
    dc.write(orders_data, "orders", mode=dc.TableWriteMode.CREATE)

    # Read and create a derived analytics table
    users_df = dc.read("users", read_as=dc.DatasetType.PANDAS)
    orders_df = dc.read("orders", read_as=dc.DatasetType.PANDAS)

    # Calculate user statistics
    user_stats = orders_df.groupby("user_id").agg({
        "amount": ["sum", "count"]
    }).round(2)
    user_stats.columns = ["total_spent", "order_count"]
    user_stats = user_stats.reset_index()

    # Write analytics table
    dc.write(user_stats, "user_analytics", mode=dc.TableWriteMode.CREATE)

# All three tables are now created atomically
print(f"Users table exists: {dc.table_exists('users')}")
print(f"Orders table exists: {dc.table_exists('orders')}")
print(f"Analytics table exists: {dc.table_exists('user_analytics')}")
```

### Transaction Rollback

If any operation within a transaction fails, all changes are automatically rolled back:

```python
import deltacat as dc
import pandas as pd

initial_data = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})

# Create initial table outside transaction
dc.write(initial_data, "rollback_test", mode=dc.TableWriteMode.CREATE)

try:
    with dc.transaction():
        # This operation will succeed
        dc.write(
            pd.DataFrame({"id": [3], "value": ["c"]}),
            "rollback_test",
            mode=dc.TableWriteMode.APPEND
        )

        # Force an error to test rollback
        raise ValueError("Simulated transaction error")

except ValueError:
    pass  # Expected error

# Verify original data is unchanged
result = dc.read("rollback_test", read_as=dc.DatasetType.PANDAS)
print(f"Records after rollback: {len(result)}")  # 2
print(result["value"].tolist())  # ["a", "b"]
```

### Nested Transactions

DeltaCAT supports nested transactions, where each nested transaction is independent and has its own context:

```python
import deltacat as dc
import pandas as pd

base_data = pd.DataFrame({"id": [1, 2], "level": ["outer", "outer"]})
inner_data = pd.DataFrame({"id": [3, 4], "level": ["inner", "inner"]})
additional_data = pd.DataFrame({"id": [5, 6], "level": ["additional", "additional"]})

# Nested transactions with independent contexts
with dc.transaction():
    # Outer transaction operations
    dc.write(base_data, "nested_outer", mode=dc.TableWriteMode.CREATE)

    # Inner (nested) transaction - independent of outer transaction
    with dc.transaction():
        # Inner transaction operations
        dc.write(inner_data, "nested_inner", mode=dc.TableWriteMode.CREATE)

        # Verify inner transaction works independently
        inner_check = dc.read("nested_inner", read_as=dc.DatasetType.PANDAS)
        print(f"Inner transaction records: {len(inner_check)}")

    # Back to outer transaction context
    dc.write(additional_data, "nested_outer", mode=dc.TableWriteMode.APPEND)

# Verify both transactions completed independently
outer_result = dc.read("nested_outer", read_as=dc.DatasetType.PANDAS)
inner_result = dc.read("nested_inner", read_as=dc.DatasetType.PANDAS)

print(f"Outer table records: {len(outer_result)}")  # Should be 4 (base + additional)
print(f"Inner table records: {len(inner_result)}")  # Should be 2
```

Note that, if the outer transaction fails after the inner transaction succeeded, the inner transaction remains committed:
```python
import deltacat as dc

base_data = pd.DataFrame({"id": [1, 2], "level": ["outer", "outer"]})
inner_data = pd.DataFrame({"id": [3, 4], "level": ["inner", "inner"]})

# Nested transactions with independent contexts
with dc.transaction():
    # Outer transaction operations
    dc.write(base_data, "nested_outer", mode=dc.TableWriteMode.CREATE)

    # Inner (nested) transaction - independent of outer transaction
    with dc.transaction():
        # Inner transaction operations
        dc.write(inner_data, "nested_inner", mode=dc.TableWriteMode.CREATE)

        # Verify inner transaction works independently
        inner_check = dc.read("nested_inner", read_as=dc.DatasetType.PANDAS)
        print(f"Inner transaction records: {len(inner_check)}")

    # Back to outer transaction context
    raise ValueError("Simulated outer transaction error")

# Verify that the outer transaction failed and the inner transaction remains committed
assert not dc.table_exists("nested_outer")
inner_result = dc.read("nested_inner", read_as=dc.DatasetType.PANDAS)
print(f"Inner table records: {len(inner_result)}")  # 2
```


### Cross-Catalog Transactions

Transactions can target specific catalogs, enabling operations across multiple isolated data catalogs:

```python
import deltacat as dc
import pandas as pd
import tempfile
import pyarrow as pa

# Set up two separate catalogs
with tempfile.TemporaryDirectory() as temp_dir_a, \
     tempfile.TemporaryDirectory() as temp_dir_b:

    # Create catalog configurations
    catalog_a_config = dc.CatalogProperties(
        root=temp_dir_a,
        filesystem=pa.fs.LocalFileSystem()
    )
    catalog_b_config = dc.CatalogProperties(
        root=temp_dir_b,
        filesystem=pa.fs.LocalFileSystem()
    )

    # Register catalogs
    dc.put_catalog("catalog_a", dc.Catalog(config=catalog_a_config))
    dc.put_catalog("catalog_b", dc.Catalog(config=catalog_b_config))

    # Create namespaces
    dc.create_namespace("production", catalog="catalog_a")
    dc.create_namespace("staging", catalog="catalog_b")

    # Sample data
    prod_data = pd.DataFrame({"id": [1, 2], "env": ["prod", "prod"]})
    staging_data = pd.DataFrame({"id": [3, 4], "env": ["staging", "staging"]})

    # Transaction targeting catalog_a
    with dc.transaction("catalog_a"):
        dc.write(
            prod_data,
            "app_data",
            namespace="production",
            mode=dc.TableWriteMode.CREATE,
            auto_create_namespace=True,
        )

    # Separate transaction targeting catalog_b
    with dc.transaction("catalog_b"):
        dc.write(
            staging_data,
            "app_data",
            namespace="staging",
            mode=dc.TableWriteMode.CREATE,
            auto_create_namespace=True,
        )

    # Verify catalog isolation
    assert dc.table_exists("app_data", namespace="production", catalog="catalog_a")
    assert dc.table_exists("app_data", namespace="staging", catalog="catalog_b")

    # Tables with the same name exist independently in each catalog
    prod_result = dc.read(
        "app_data",
        namespace="production",
        read_as=dc.DatasetType.PANDAS,
        catalog="catalog_a",
    )
    staging_result = dc.read(
        "app_data",
        namespace="staging",
        read_as=dc.DatasetType.PANDAS,
        catalog="catalog_b",
    )

    print(f"Production records: {prod_result['env'].tolist()}")  # ["prod", "prod"]
    print(f"Staging records: {staging_result['env'].tolist()}")  # ["staging", "staging"]
```

### Nested Transactions with Different Catalogs

You can even nest transactions that target different catalogs:

```python
import deltacat as dc
import pandas as pd

outer_data = pd.DataFrame({"id": [1, 2], "location": ["outer", "outer"]})
inner_data = pd.DataFrame({"id": [3, 4], "location": ["inner", "inner"]})

# Nested transactions with different catalogs
with dc.transaction("catalog_a"):
    # Outer transaction in catalog_a
    dc.write(
        outer_data,
        "nested_table",
        namespace="production",
        mode=dc.TableWriteMode.CREATE,
        auto_create_namespace=True,
    )

    # Inner transaction in catalog_b
    with dc.transaction("catalog_b"):
        dc.write(
            inner_data,
            "nested_table",
            namespace="staging",
            mode=dc.TableWriteMode.CREATE,
            auto_create_namespace=True,
        )

        # Verify inner transaction context
        inner_check = dc.read(
            "nested_table",
            namespace="staging",
            read_as=dc.DatasetType.PANDAS,
        )
        print(f"Inner catalog records: {len(inner_check)}")

    # Back to outer transaction context (catalog_a)
    outer_check = dc.read(
        "nested_table",
        namespace="production",
        read_as=dc.DatasetType.PANDAS,
    )
    print(f"Outer catalog records: {len(outer_check)}")

# Verify catalog isolation was maintained
assert dc.table_exists("nested_table", namespace="production", catalog="catalog_a")
assert dc.table_exists("nested_table", namespace="staging", catalog="catalog_b")
assert not dc.table_exists("nested_table", namespace="production", catalog="catalog_b")
assert not dc.table_exists("nested_table", namespace="staging", catalog="catalog_a")
```

### Transaction Best Practices

1. **Keep transactions focused**: Group related operations together, but avoid unnecessarily long transactions.

2. **Handle errors gracefully**: Always wrap transactions in try-catch blocks to handle potential failures.

3. **Test rollback scenarios**: Verify that your application handles transaction rollbacks gracefully.

## Sort Keys (coming soon)
Sort keys are used to control how a table's data is sorted at write time.

## Partition Schemes (coming soon)
Partition schemes are used to control how a table's data is partitioned at write time.
