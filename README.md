<p align="center">
  <img src="https://github.com/ray-project/deltacat/raw/2.0/media/deltacat-logo-alpha-750.png" alt="deltacat logo" style="width:55%; height:auto; text-align: center;">
</p>

DeltaCAT is a portable Pythonic Data Lakehouse powered by [Ray](https://github.com/ray-project/ray). It lets you define and manage
fast, scalable, ACID-compliant multimodal data lakes, and has been used to [successfully manage exabyte-scale enterprise
data lakes](https://aws.amazon.com/blogs/opensource/amazons-exabyte-scale-migration-from-apache-spark-to-ray-on-amazon-ec2/).

It uses the Ray distributed compute framework together with [Apache Arrow](https://github.com/apache/arrow) and
[Daft](https://github.com/Eventual-Inc/Daft) to efficiently scale common table management tasks, like petabyte-scale
merge-on-read and copy-on-write operations.

DeltaCAT provides the following high-level components:
1. [**Catalog**](https://github.com/ray-project/deltacat/tree/2.0/deltacat/catalog/interface.py): High-level APIs to create, discover, organize, share, and manage datasets.
2. [**Compute**](https://github.com/ray-project/deltacat/tree/2.0/deltacat/compute/): Distributed data management procedures to read, write, and optimize datasets.
3. [**Storage**](https://github.com/ray-project/deltacat/tree/2.0/deltacat/storage/): In-memory and on-disk multimodal dataset formats.
4. **Sync** (in development): Synchronize DeltaCAT datasets to data warehouses and other table formats.

## Overview
DeltaCAT's **Catalog**, **Compute**, and **Storage** layers work together to bring ACID-compliant data management to any Ray application. These components automate data indexing, change management, dataset read/write optimization, schema evolution, and other common data management tasks across any set of data files readable by Ray Data, Daft, Pandas, Polars, PyArrow, or NumPy.

<p align="center">
  <img src="https://github.com/ray-project/deltacat/raw/2.0/media/deltacat-tech-overview.png" alt="deltacat tech overview" style="width:100%; height:auto; text-align: center;">
</p>

Data consumers that prefer to stay within the ecosystem of Pythonic data management tools can use DeltaCAT's native table format to manage their data with minimal overhead. For integration with other data analytics frameworks (e.g., Apache Spark, Trino, Apache Flink), DeltaCAT's **Sync** component offers zero-copy synchronization of your tables to Apache Iceberg and other table formats.

## Getting Started
DeltaCAT applications run anywhere that Ray runs, including your local laptop, cloud computing cluster, or on-premise cluster.

DeltaCAT lets you manage **Tables** across one or more **Catalogs**. A **Table** can be thought of as a named collection of one or more data files. A **Catalog** provides a root location (e.g., a local file path or S3 Bucket) to store table information, and can be rooted in any [PyArrow-compatible Filesystem](https://arrow.apache.org/docs/python/filesystems.html). **Tables** can be created, read, and written using the `dc.write` and `dc.read` APIs.

### Quick Start

```python
import deltacat as dc
import pandas as pd

# Initialize DeltaCAT with a default local catalog.
# Ray will be initialized automatically.
# Catalog files will be stored in .deltacat/ in the current working directory.
dc.init_local()

# Create data to write.
data = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Cheshire", "Dinah", "Felix"],
    "age": [3, 7, 5]
})

# Write data to a table.
# Table creation is handled automatically.
dc.write(data, "users")

# Read the data back as a Daft DataFrame.
# Daft lazily and automatically distributes data across your Ray cluster.
daft_df = dc.read("users")  # Returns Daft DataFrame (default)
daft_df.show()  # Materialize and print the DataFrame

# Append more data and add a new column.
# Compaction and schema evolution are handled automatically.
data = pd.DataFrame({
    "id": [4, 5, 6],
    "name": ["Tom", "Simpkin", "Delta"],
    "age": [2, 12, 4],
    "city": ["Hollywood", "Gloucester", "San Francisco"]
})
dc.write(data, "users")

# Read the full table back into a Daft DataFrame.
daft_df = dc.read("users")
# Print the names, ages, and cities (missing cities will be null).
daft_df.select("name", "age", "city").show()
```

### Core Concepts
DeltaCAT can do much more than just append data to tables and read it back again. Expand the sections below to see examples of other core DeltaCAT concepts and APIs.

<details>

<summary><span style="font-size: 1.25em; font-weight: bold;">Replacing and Dropping Tables</span></summary>

If you run the quick start example repeatedly from the same working directory, you'll notice that the table it writes to just keeps growing larger. This is because DeltaCAT always **appends** table data by default. One way to prevent this perpetual table growth and make the example idempotent is to use the **REPLACE** write mode if the table already exists:

```python
import deltacat as dc
import pandas as pd

# Initialize DeltaCAT with a default local catalog.
# Ray will be initialized automatically.
# Catalog files will be stored in .deltacat/ in the current working directory.
dc.init_local()

# Create data to write.
data = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Cheshire", "Dinah", "Felix"],
    "age": [3, 7, 5]
})

# Default write mode to CREATE.
# This will fail if the table already exists.
write_mode = dc.TableWriteMode.CREATE

# Change write mode to REPLACE if the table already exists.
if dc.table_exists("users"):
    write_mode = dc.TableWriteMode.REPLACE

# Write data to a fresh, empty table.
dc.write(data, "users", mode=write_mode)

# Read the data back as a Daft DataFrame.
# Daft lazily and automatically distributes data across your Ray cluster.
daft_df = dc.read("users")  # Returns Daft DataFrame (default)
daft_df.show()  # Materialize and print the DataFrame

# Explicitly append more data and add a new column.
# Compaction and schema evolution are handled automatically.
data = pd.DataFrame({
    "id": [4, 5, 6],
    "name": ["Tom", "Simpkin", "Delta"],
    "age": [2, 12, 4],
    "city": ["Hollywood", "Gloucester", "San Francisco"]
})
dc.write(data, "users", mode=dc.TableWriteMode.APPEND)

# Read the full table back into a Daft DataFrame.
daft_df = dc.read("users")
# Print the names, ages, and cities (missing cities will be null).
daft_df.select("name", "age", "city").show()
# Ensure that the table length is always 6.
assert dc.dataset_length(daft_df) == 6
```

No matter how many times you run the above code, the table will always contain 6 records. Another way to achieve the same result is to use `dc.drop_table`:

```python
import deltacat as dc
from deltacat.exceptions import TableNotFoundError
import pandas as pd

# Initialize DeltaCAT with a default local catalog.
# Ray will be initialized automatically.
# Catalog files will be stored in .deltacat/ in the current working directory.
dc.init_local()

# Create data to write.
data = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Cheshire", "Dinah", "Felix"],
    "age": [3, 7, 5]
})

# Drop the table if it exists.
try:
    dc.drop_table("users")
    print("Dropped 'users' table.")
except TableNotFoundError:
    print("Table 'users' not found. Creating it...")

# Write data to a new table.
dc.write(data, "users", mode=dc.TableWriteMode.CREATE)

# Read the data back as a Daft DataFrame.
# Daft lazily and automatically distributes data across your Ray cluster.
daft_df = dc.read("users")  # Returns Daft DataFrame (default)
daft_df.show()  # Materialize and print the DataFrame

# Explicitly append more data and add a new column.
# Compaction and schema evolution are handled automatically.
data = pd.DataFrame({
    "id": [4, 5, 6],
    "name": ["Tom", "Simpkin", "Delta"],
    "age": [2, 12, 4],
    "city": ["Hollywood", "Gloucester", "San Francisco"]
})
dc.write(data, "users", mode=dc.TableWriteMode.APPEND)

# Read the full table back into a Daft DataFrame.
daft_df = dc.read("users")
# Print the names, ages, and cities (missing cities will be null).
daft_df.select("name", "age", "city").show()
# Ensure that the table length is always 6.
assert dc.dataset_length(daft_df) == 6
```

</details>

<details>

<summary><span style="font-size: 1.25em; font-weight: bold;">Supported Dataset and File Formats</span></summary>

DeltaCAT natively supports a variety of open dataset and file formats already integrated with Ray and Arrow. You can use `dc.read` to read tables back as a Daft DataFrame, Ray Dataset, Pandas DataFrame, PyArrow Table, Polars DataFrame, NumPy Array, or list of PyArrow ParquetFile objects:

```python
# Read directly into eagerly materialized local datasets.
# Local datasets are best for reading small tables that fit in local memory:
pandas_df = dc.read("users", read_as=dc.DatasetType.PANDAS)  # Pandas DataFrame
print("\n=== Pandas ===")
print(pandas_df)

pyarrow_table = dc.read("users", read_as=dc.DatasetType.PYARROW)  # PyArrow Table
print("\n=== PyArrow ===")
print(pyarrow_table)

polars_df = dc.read("users", read_as=dc.DatasetType.POLARS)  # Polars DataFrame
print("\n=== Polars ===")
print(polars_df)

numpy_array = dc.read("users", read_as=dc.DatasetType.NUMPY)  # NumPy Array
print("\n=== NumPy ===")
print(numpy_array)

# Or read into lazily materialized PyArrow ParquetFile objects.
# PyArrow ParquetFile objects are useful for reading larger tables that don't
# fit in local memory, but you'll need to manually handle data distribution
# and materialization:
pyarrow_pq_files = dc.read("users", read_as=dc.DatasetType.PYARROW_PARQUET)  # ParquetFile or List[ParquetFile]
print("\n=== PyArrow Parquet Unmaterialized ===")
print(pyarrow_pq_files)
print("\n=== PyArrow Parquet Materialized ===")
print(dc.to_pyarrow(pyarrow_pq_files))  # Materialize and print the ParquetFile refs

# Or read into distributed datasets for scalable data processing.
# Distributed datasets are the easiest way to read large tables that don't fit
# in either local or distributed Ray cluster memory. They automatically handle
# data distribution and materialization:
daft_df = dc.read("users", read_as=dc.DatasetType.DAFT)  # Daft DataFrame (Default)
print("\n=== Daft ===")
daft_df.show()  # Materialize and print the Daft DataFrame

ray_dataset = dc.read("users", read_as=dc.DatasetType.RAY_DATASET)  # Ray Dataset
print("\n=== Ray Data ===")
ray_dataset.show()  # Materialize and print the Ray Dataset
```

 `dc.write` can also write any of these dataset types:

```python
import pyarrow as pa

# Create a pyarrow table to write.
pyarrow_table = pa.Table.from_pydict({
    "id": [4, 5, 6],
    "name": ["Tom", "Simpkin", "Delta"],
    "age": [2, 12, 4],
    "city": ["Hollywood", "Gloucester", "San Francisco"]
})

# Write different dataset types to the default table file format (Parquet):
dc.write(pyarrow_table, "my_pyarrow_table")  # Write PyArrow Table
print("\n=== PyArrow Table ===")
dc.read("my_pyarrow_table").show()

daft_df = dc.from_pyarrow(pyarrow_table, dc.DatasetType.DAFT)
dc.write(daft_df, "my_daft_table")  # Write Daft DataFrame
print("\n=== Daft Table ===")
dc.read("my_daft_table").show()

ray_dataset = dc.from_pyarrow(pyarrow_table, dc.DatasetType.RAY_DATASET)
dc.write(ray_dataset, "my_ray_dataset")  # Write Ray Dataset
print("\n=== Ray Dataset ===")
dc.read("my_ray_dataset").show()

pandas_df = dc.from_pyarrow(pyarrow_table, dc.DatasetType.PANDAS)
dc.write(pandas_df, "my_pandas_table")  # Write Pandas DataFrame
print("\n=== Pandas Table ===")
dc.read("my_pandas_table").show()

polars_df = dc.from_pyarrow(pyarrow_table, dc.DatasetType.POLARS)
dc.write(polars_df, "my_polars_table")  # Write Polars DataFrame
print("\n=== Polars Table ===")
dc.read("my_polars_table").show()

numpy_array = dc.from_pyarrow(pyarrow_table, dc.DatasetType.NUMPY)
dc.write(numpy_array, "my_numpy_table")  # Write NumPy Array
print("\n=== NumPy Table ===")
dc.read("my_numpy_table").show()
```

Or write to different table file formats:

```python
data = pd.DataFrame({"id": [1], "name": ["Cheshire"], "age": [3]})

# Start by writing to a new table with a custom list of supported readers.
# Define the content types we want to write.
content_types_to_write = {
    dc.ContentType.PARQUET,
    dc.ContentType.AVRO,
    dc.ContentType.ORC,
    dc.ContentType.FEATHER,
}
# Limit supported readers to dataset types can read the above content types.
# By default, DeltaCAT will raise an error if we attempt to write data to
# a file format that can't be read by one or more dataset types.
supported_reader_types = [
    reader_type for reader_type in dc.DatasetType
    if content_types_to_write <= reader_type.readable_content_types()
]
# Write to a new table with our custom list of supported readers.
dc.write(
    data,
    "my_mixed_format_table",
    content_type=dc.ContentType.PARQUET, # Write Parquet (Default)
    table_properties={dc.TableProperty.SUPPORTED_READER_TYPES: supported_reader_types}
)

# Now write the same data to other file formats:
dc.write(data, "my_mixed_format_table", content_type=dc.ContentType.AVRO)
dc.write(data, "my_mixed_format_table", content_type=dc.ContentType.ORC)
dc.write(data, "my_mixed_format_table", content_type=dc.ContentType.FEATHER)

# Read the table back.
# All formats are automatically unified into the requested Pandas DataFrame:
pandas_df = dc.read("my_mixed_format_table", read_as=dc.DatasetType.PANDAS)
print(pandas_df)
```

</details>

<details>

<summary><span style="font-size: 1.25em; font-weight: bold;">Merging and Deleting Data</span></summary>

DeltaCAT can automatically merge and delete data by defining a table schema with one or more merge keys:

```python
import deltacat as dc
import pandas as pd
import pyarrow as pa
import tempfile

# Initialize DeltaCAT with a fresh temporary catalog
dc.init_local(tempfile.mkdtemp())

# Define a schema with user_id as a merge key.
schema = dc.Schema.of([
    dc.Field.of(pa.field("user_id", pa.int64()), is_merge_key=True),
    dc.Field.of(pa.field("name", pa.string())),
    dc.Field.of(pa.field("age", pa.int32())),
    dc.Field.of(pa.field("status", pa.string())),
])

# Initial user data
initial_users = pd.DataFrame({
    "user_id": [1, 2, 3],
    "name": ["Cheshire", "Dinah", "Felix"],
    "age": [3, 7, 2],
    "status": ["active", "active", "inactive"]
})

# Write initial data with the merge key schema
dc.write(initial_users, "users", schema=schema)

# Read the data back as a Pandas DataFrame.
df = dc.read("users", read_as=dc.DatasetType.PANDAS)
print("=== Initial Users ===")
print(df.sort_values("user_id"))

# Update data for existing users + add new users
updated_users = pd.DataFrame({
    "user_id": [2, 3, 4, 5, 6],
    "name": ["Dinah", "Felix", "Tom", "Simpkin", "Delta"],
    "age": [7, 2, 5, 12, 4],
    "status": ["premium", "active", "active", "active", "active"]
})

# Write automatically detects that the schema has a merge key and:
# 1. Updates existing records with matching user IDs.
# 2. Inserts new records with new user IDs.
dc.write(updated_users, "users", schema=schema)

# Read back to see merged results
df = dc.read("users", read_as=dc.DatasetType.PANDAS)
print("\n=== After Merge ===")
print(df.sort_values("user_id"))

# - Cheshire (user_id=1) remains unchanged
# - Dinah (user_id=2) status updated to "premium"
# - Felix (user_id=3) updated to "active"
# - New users (4,5,6), (Tom, Simpkin, Delta) added
# - No duplicate user_id values exist

# Specify the users to delete.
# We only need to specify matching merge key values.
users_to_delete = pd.DataFrame({
    "user_id": [3, 5],
})

# Delete the records that match our merge keys.
dc.write(users_to_delete, "users", schema=schema, mode=dc.TableWriteMode.DELETE)

# Read the table back to confirm target users have been deleted.
df = dc.read("users", read_as=dc.DatasetType.PANDAS)
print("\n=== After Deletion ===")
print(df.sort_values("user_id"))

# - Felix (user_id=3) has been removed
# - Simpkin (user_id=5) has been removed
# - All other users remain unchanged
```

</details>

<details>

<summary><span style="font-size: 1.25em; font-weight: bold;">Organizing Tables with Namespaces</span></summary>

In DeltaCAT, table **Namespaces** are optional but useful for organizing related tables within a catalog:

```python
import deltacat as dc
import pandas as pd
import tempfile

# Initialize DeltaCAT with a fresh temporary catalog
dc.init_local(tempfile.mkdtemp())

# Create some sample data for different business domains
user_data = pd.DataFrame({
    "user_id": [1, 2, 3],
    "name": ["Cheshire", "Dinah", "Felix"],
})

product_data = pd.DataFrame({
    "product_id": [101, 102, 103],
    "name": ["Mushrooms", "Fish", "Milk"],
    "price": [12.99, 8.99, 3.99]
})

order_data = pd.DataFrame({
    "order_id": [1001, 1002, 1003],
    "user_id": [1, 2, 3],
    "product_id": [101, 102, 103],
    "quantity": [2, 1, 2]
})

# Write tables to different namespaces to organize them by domain
dc.write(user_data, "users", namespace="identity")
dc.write(product_data, "catalog", namespace="inventory")
dc.write(order_data, "transactions", namespace="sales")

# Read from specific namespaces
users_df = dc.read("users", namespace="identity", read_as=dc.DatasetType.PANDAS)
products_df = dc.read("catalog", namespace="inventory", read_as=dc.DatasetType.PANDAS)
orders_df = dc.read("transactions", namespace="sales", read_as=dc.DatasetType.PANDAS)

# Tables with the same name can exist in different namespaces
# Create separate marketing and finance views of users
marketing_users = pd.DataFrame({
    "user_id": [1, 2, 3],
    "segment": ["premium", "standard", "premium"],
    "acquisition_channel": ["social", "search", "referral"]
})

finance_users = pd.DataFrame({
    "user_id": [1, 2, 3],
    "lifetime_payments": [25.98, 8.99, 7.98],
    "preferred_payment_method": ["credit", "cash", "paypal"]
})

dc.write(marketing_users, "users", namespace="marketing")
dc.write(finance_users, "users", namespace="finance")

# Each namespace maintains its own "users" table with different schemas
marketing_df = dc.read("users", namespace="marketing", read_as=dc.DatasetType.PANDAS)
finance_df = dc.read("users", namespace="finance", read_as=dc.DatasetType.PANDAS)

print(f"\n=== Identity Namespace Users ===")
print(users_df)
print(f"\n=== Inventory Namespace Products ===")
print(products_df)
print(f"\n=== Sales Namespace Transactions ===")
print(orders_df)
print(f"\n=== Marketing Namespace Users ===")
print(marketing_df)
print(f"\n=== Finance Namespace Users ===")
print(finance_df)
```

</details>

<details>

<summary><span style="font-size: 1.25em; font-weight: bold;">Multi-Table Transactions</span></summary>

DeltaCAT transactions can span multiple tables and namespaces. Since all operations within a transaction either succeed or fail together, this simplifies keeping related datasets in sync across your entire catalog.

Consider the previous example that organized tables with namespaces. One table tracked customer orders, and another table tracked the lifetime payments of each customer. If one table was updated but not the other, then it would result in an accounting discrepancy. This edge case can be eliminated by using multi-table transactions:

```python
import deltacat as dc
import pandas as pd
import pyarrow as pa
import tempfile

# Initialize DeltaCAT with a fresh temporary catalog
dc.init_local(tempfile.mkdtemp())

# Create sample product data.
product_data = pd.DataFrame({
    "product_id": [101, 102, 103],
    "name": ["Mushrooms", "Fish", "Milk"],
    "price": [12.99, 8.99, 3.99]
})

# The product catalog can be created independently.
dc.write(product_data, "catalog", namespace="inventory")

print(f"\n=== Initial Product Data ===")
print(dc.read("catalog", namespace="inventory", read_as=dc.DatasetType.PANDAS))

# Create sample user and finance data.
user_data = pd.DataFrame({
    "user_id": [1, 2, 3],
    "name": ["Cheshire", "Dinah", "Felix"],
})
initial_finance = pd.DataFrame({
    "user_id": [1, 2, 3],
    "preferred_payment_method": ["credit", "cash", "paypal"]
})

# Define a finance schema.
# User ID is the merge key, and lifetime payments are defaulted to 0.00.
finance_schema = dc.Schema.of([
    dc.Field.of(pa.field("user_id", pa.int64()), is_merge_key=True),
    dc.Field.of(pa.field("lifetime_payments", pa.float64()), future_default=0.00),
    dc.Field.of(pa.field("preferred_payment_method", pa.string())),
])

# Create user identities and user finance data within a single transaction.
# Since transactions are atomic, this prevents accounting discrepancies.
with dc.transaction():
    dc.write(user_data, "users", namespace="identity")
    dc.write(initial_finance, "users", namespace="finance", schema=finance_schema)

print(f"\n=== Initial User Data ===")
print(dc.read("users", namespace="identity", read_as=dc.DatasetType.PANDAS))
print(f"\n=== Initial Finance Data ===")
print(dc.read("users", namespace="finance", read_as=dc.DatasetType.PANDAS))

# Create new order data
new_orders = pd.DataFrame({
    "order_id": [1001, 1002, 1003],
    "user_id": [1, 2, 3],
    "product_id": [101, 102, 103],
    "quantity": [2, 1, 2]
})

# Process new orders and update lifetime payment totals within a single transaction.
with dc.transaction():
    # Step 1: Write the new orders
    dc.write(new_orders, "transactions", namespace="sales")

    # Step 2: Read back transactions and products to compute actual totals
    orders_df = dc.read("transactions", namespace="sales", read_as=dc.DatasetType.PANDAS)
    products_df = dc.read("catalog", namespace="inventory", read_as=dc.DatasetType.PANDAS)

    # Step 3: Compute lifetime payment totals by joining orders with product prices
    orders_with_prices = orders_df.merge(products_df, on="product_id")
    orders_with_prices["total"] = orders_with_prices["quantity"] * orders_with_prices["price"]

    # Calculate lifetime totals per user
    finance_updates = orders_with_prices.groupby("user_id")["total"].sum().reset_index()
    finance_updates.columns = ["user_id", "lifetime_payments"]

    # Step 4: Write the computed totals
    dc.write(finance_updates, "users", namespace="finance", mode=dc.TableWriteMode.MERGE)

# Verify that orders and and lifetime payments are kept in sync.
print(f"\n=== New Orders Processed ===")
print(dc.read("transactions", namespace="sales", read_as=dc.DatasetType.PANDAS))
print(f"\n=== Updated Finance Records ===")
print(dc.read("users", namespace="finance", read_as=dc.DatasetType.PANDAS))
```

</details>

<details>

<summary><span style="font-size: 1.25em; font-weight: bold;">Working with Multiple Catalogs</span></summary>

DeltaCAT lets you work with multiple catalogs in a single application. All catalogs registered with DeltaCAT are tracked by a Ray Actor to make them available to all workers in your Ray application.

For example, you may want to test a write against a local staging catalog before committing it to a shared production catalog:

```python
import deltacat as dc
from deltacat.exceptions import TableValidationError
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import tempfile
from decimal import Decimal

# Initialize catalogs with separate names and catalog roots.
dc.init(catalogs={
    "staging": dc.Catalog(config=dc.CatalogProperties(
        root=tempfile.mkdtemp(),  # Use temporary directory for staging
        filesystem=pa.fs.LocalFileSystem()
    )),
    "prod": dc.Catalog(config=dc.CatalogProperties(
        root=tempfile.mkdtemp(),  # Use temporary directory for prod
        filesystem=pa.fs.LocalFileSystem()
    ))
})

# Create a PyArrow table with decimal256 data
decimal_table = pa.table({
    "item_id": [1, 2, 3],
    "price": pa.array([
        Decimal("999.99"),
        Decimal("1234.56"),
        Decimal("567.89")
    ], type=pa.decimal256(10, 2))
})

# Try to write decimal256 data to the staging table.
# DeltaCAT auto-detects that decimal256 isn't readable
# by several default supported table reader types
# (Polars, Daft, Ray Data) and raises a TableValidationError.
try:
    dc.write(decimal_table, "financial_data", catalog="staging")
    print("Decimal256 write succeeded")
except TableValidationError as e:
    print(f"\n=== Validation Error ===")
    print(e)
    print("Decimal256 may break existing data consumers in prod, trying decimal128...")

    # Cast the price column from decimal256 to decimal128
    decimal_table = decimal_table.set_column(
        decimal_table.schema.get_field_index("price"),
        "price",
        pc.cast(decimal_table["price"], pa.decimal128(10, 2))
    )

# Write the validated decimal data to staging and ensure that the write succeeds
dc.write(decimal_table, "financial_data", catalog="staging")
print(f"\n=== Successfully Staged Data ===")
print(dc.read("financial_data", catalog="staging", read_as=dc.DatasetType.PANDAS))

# Read from staging to verify
staging_data = dc.read("financial_data", catalog="staging", read_as=dc.DatasetType.PANDAS)
assert staging_data["price"].tolist() == [Decimal("999.99"), Decimal("1234.56"), Decimal("567.89")]

# Now write the validated data to production
dc.write(decimal_table, "financial_data", catalog="prod")
print(f"\n=== Production Data ===")
print(dc.read("financial_data", catalog="prod", read_as=dc.DatasetType.PANDAS))
```

</details>

<details>

<summary><span style="font-size: 1.25em; font-weight: bold;">Transaction History & Time Travel</span></summary>

DeltaCAT supports time travel queries that let you read all tables in a catalog as they existed at any point in the past. Combined with multi-table transactions, this enables consistent point-in-time views across your entire data catalog.

```python
import deltacat as dc
import pandas as pd
import tempfile
import time

# Initialize DeltaCAT with a fresh temporary catalog
dc.init_local(tempfile.mkdtemp())

# Create initial state with existing users, products, and orders
initial_users = pd.DataFrame({
    "user_id": [1, 2, 3],
    "name": ["Cheshire", "Dinah", "Felix"],
})

initial_products = pd.DataFrame({
    "product_id": [101, 102, 103],
    "name": ["Mushrooms", "Fish", "Milk"],
    "price": [12.99, 8.99, 3.99]
})

initial_orders = pd.DataFrame({
    "order_id": [1001, 1002, 1003],
    "user_id": [1, 2, 3],
    "product_id": [101, 102, 103],
    "quantity": [2, 1, 2]
})

initial_finance = pd.DataFrame({
    "user_id": [1, 2, 3],
    "lifetime_payments": [25.98, 8.99, 7.98],
})

# Write initial state atomically with a commit message
with dc.transaction(commit_message="Initial data load: users, products, orders, and finance"):
    dc.write(initial_users, "users", namespace="identity")
    dc.write(initial_products, "catalog", namespace="inventory")
    dc.write(initial_orders, "transactions", namespace="sales")
    dc.write(initial_finance, "users", namespace="finance")

# Sleep briefly to ensure transaction timestamp separation
time.sleep(0.1)

# Later, add new orders for existing and new users
new_orders = pd.DataFrame({
    "order_id": [1004, 1005, 1006, 1007, 1008],
    "user_id": [1, 2, 1, 4, 5],
    "product_id": [101, 102, 101, 104, 105],
    "quantity": [1, 2, 3, 5, 1]
})

new_users = pd.DataFrame({
    "user_id": [4, 5],
    "name": ["Tom", "Simpkin"],
})

new_products = pd.DataFrame({
    "product_id": [104, 105],
    "name": ["Tuna", "Salmon"],
    "price": [6.99, 9.99]
})

# Update finance data with new lifetime payment totals
updated_finance = pd.DataFrame({
    "user_id": [1, 2, 3, 4, 5],
    "lifetime_payments": [51.96, 26.97, 15.96, 34.95, 9.99]  # Updated totals
})

# Execute all updates atomically - either all succeed or all fail
with dc.transaction(commit_message="Add new users and products, update finance totals"):
    # Add new users, products, orders, and lifetime payment totals
    dc.write(new_users, "users", namespace="identity")
    dc.write(new_products, "catalog", namespace="inventory")
    dc.write(new_orders, "transactions", namespace="sales")
    dc.write(updated_finance, "users", namespace="finance", mode=dc.TableWriteMode.REPLACE)

# Query transaction history to find the right timestamp for time travel
print("=== Transaction History ===")
txn_history = dc.transactions(read_as=dc.DatasetType.PANDAS)
print(f"Found {len(txn_history)} transactions:")
print(txn_history[["transaction_id", "commit_message", "start_time", "end_time"]])

# Find the transaction we want to time travel back to
initial_load_txn = txn_history[
    txn_history["commit_message"] == "Initial data load: users, products, orders, and finance"
]
checkpoint_time = initial_load_txn["end_time"].iloc[0] + 1

print(f"\nUsing checkpoint time from transaction: {initial_load_txn['transaction_id'].iloc[0]}")
print(f"Commit message: {initial_load_txn['commit_message'].iloc[0]}")

# Compare current state vs historic state across all tables
print("\n=== Current State (After Updates) ===")
current_users = dc.read("users", namespace="identity", read_as=dc.DatasetType.PANDAS)
current_orders = dc.read("transactions", namespace="sales", read_as=dc.DatasetType.PANDAS)
current_finance = dc.read("users", namespace="finance", read_as=dc.DatasetType.PANDAS)

print("== Users ==")
print(current_users)
print("== Orders ==")
print(current_orders)
print("== Finance ==")
print(current_finance)

# Now query all tables as they existed at the checkpoint
print("\n=== Historic State (Before Updates) ===")
# DeltaCAT only reads transactions with end times strictly less than the given as-of time,
# so add 1 to the checkpoint time of the transaction we want to travel back to.
with dc.transaction(as_of=checkpoint_time + 1):
    historic_users = dc.read("users", namespace="identity", read_as=dc.DatasetType.PANDAS)
    historic_orders = dc.read("transactions", namespace="sales", read_as=dc.DatasetType.PANDAS)
    historic_finance = dc.read("users", namespace="finance", read_as=dc.DatasetType.PANDAS)

    print("== Users ==")
    print(historic_users)
    print("== Orders ==")
    print(historic_orders)
    print("== Finance ==")
    print(historic_finance)

    # Validate historic state
    assert not any(historic_users["name"] == "Tom")
    assert not any(historic_users["name"] == "Simpkin")
    assert len(historic_orders) == 3  # Only original 3 orders

    # Finance data reflects original payment totals
    historic_payments = historic_finance[historic_finance["user_id"] == 1]["lifetime_payments"].iloc[0]
    assert historic_payments == 25.98  # Original total, not updated 51.96

print("\nTime travel validation successful!")
```

</details>

<details>

<summary><span style="font-size: 1.25em; font-weight: bold;">Multimodal Batch Inference</span></summary>

DeltaCAT's support for merging new fields into existing records and multimodal datasets can be used to build a multimodal batch inference pipeline. For example, the following code indexes images of cats, then merges in new fields with breed precitions predictions for each image:

> **Requirements**: This example requires PyTorch ≥ 2.8.0 and torchvision ≥ 0.23.0. Install via: `pip install torch>=2.8.0 torchvision>=0.23.0`

```python
import deltacat as dc
import tempfile
import daft
import torch
import pyarrow as pa

# Initialize DeltaCAT with a temporary catalog.
dc.init_local(tempfile.mkdtemp())

# Define initial schema with image_id as merge key (no prediction fields yet).
initial_schema = dc.Schema.of([
    dc.Field.of(pa.field("image_id", pa.large_string()), is_merge_key=True),
    dc.Field.of(pa.field("image_path", pa.large_string())),
    dc.Field.of(pa.field("true_breed", pa.large_string())),
    dc.Field.of(pa.field("image_bytes", pa.binary())),
])

# Create sample Daft DataFrame with image URLs/paths.
df = daft.from_pydict({
    "image_id": ["cat_001", "cat_002", "cat_003"],
    "image_path": ["media/tuxedo.jpg", "media/calico.jpg", "media/siamese.jpg"],
    "true_breed": ["Tuxedo", "Calico", "Siamese"]
})

# Load images and prepare for processing.
df = df.with_column("image_bytes", df["image_path"].url.download())

# Write initial dataset to DeltaCAT.
dc.write(df, "cool_cats", schema=initial_schema)

# Define an ImageClassifier UDF for cat breed prediction.
@daft.udf(return_dtype=daft.DataType.fixed_size_list(dtype=daft.DataType.string(), size=2))
class ImageClassifier:
    def __init__(self):
        """Initialize model once per worker for efficiency"""
        self.model = torch.hub.load("NVIDIA/DeepLearningExamples:torchhub", "nvidia_resnet50", pretrained=True)
        self.utils = torch.hub.load("NVIDIA/DeepLearningExamples:torchhub", "nvidia_convnets_processing_utils")
        self.model.eval().to(torch.device("cpu"))

    def __call__(self, image_paths):
        """Process batch of images efficiently using NVIDIA utilities"""
        batch = torch.cat([self.utils.prepare_input_from_uri(uri) for uri in image_paths.to_pylist()]).to(torch.device("cpu"))

        with torch.no_grad():
            output = torch.nn.functional.softmax(self.model(batch), dim=1)

        results = self.utils.pick_n_best(predictions=output, n=1)
        return [result[0] for result in results]

# Apply the UDF first to get predictions.
df_with_predictions = df.with_column("prediction", ImageClassifier(df["image_path"]))

# Run batch inference and prepare partial update data (merge key + new fields only)
# Adds predicted breed and confidence to existing records with matching image IDs.
prediction_data = df_with_predictions.select(
    df_with_predictions["image_id"],
    df_with_predictions["prediction"].list.get(0).alias("predicted_breed"),
    (df_with_predictions["prediction"].list.get(1).str.replace("%", "").cast(daft.DataType.float64()) / 100.0).alias("confidence")
)

# Write the predictions back to the table.
dc.write(prediction_data, "cool_cats")

# Read back the merged results.
final_df = dc.read("cool_cats")
final_df = final_df.with_column("image", final_df["image_bytes"].image.decode())

# Calculate accuracy and display results.
results = final_df.select("image_id", "true_breed", "predicted_breed", "confidence").to_pandas()
accuracy = (results.apply(lambda row: row["true_breed"].lower() in row["predicted_breed"].lower(), axis=1)).mean()

print("=== Results ===")
print(f"Classification Accuracy: {accuracy:.1%}")

# Display final dataset with decoded images for visual inspection.
# Run this example in a Jupyter notebook to view the actual image data stored in DeltaCAT.
print(f"Final dataset with images and predictions:")
final_df.show()
```

</details>

<details>

<summary><span style="font-size: 1.25em; font-weight: bold;">LLM Batch Inference</span></summary>

DeltaCAT multi-table transactions, time travel queries, and automatic schema evolution can be used to create auditable LLM batch inference pipelines. For example, the following code tries different approaches to analyze the overall tone of customer feedback, then generates customer service responses based on the analysis:

```python
import deltacat as dc
import pandas as pd
import pyarrow as pa
import tempfile
import time
import daft
from transformers import pipeline

# Initialize DeltaCAT with a temporary catalog.
dc.init_local(tempfile.mkdtemp())

# Load customer feedback
daft_docs = daft.from_pydict({
    "doc_id": [1, 2, 3],
    "path": ["media/customer_feedback_001.txt", "media/customer_feedback_002.txt", "media/customer_feedback_003.txt"]
})
daft_docs = daft_docs.with_column("content", daft_docs["path"].url.download().decode("utf-8"))

# Doc processing V1.0
# Capture basic feedback sentiment analysis in a parallel multi-table transaction
with dc.transaction():
    # Write the full customer feedback to a new "documents" table.
    dc.write(daft_docs, "documents", namespace="analysis")

    # Define a UDF to analyze customer feedback sentiment.
    @daft.udf(return_dtype=daft.DataType.struct({
        "analysis": daft.DataType.string(),
        "confidence": daft.DataType.float64(),
        "model_version": daft.DataType.string()
    }))
    def analyze_sentiment(content_series):
        classifier = pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment-latest")
        results = []
        for content in content_series.to_pylist():
            result = classifier(content[:500])[0]  # Truncate for model limits
            results.append({
                "analysis": result['label'],
                "confidence": result['score'],
                "model_version": "roberta-v1.0"
            })
        return results

    # Run sentiment analysis in parallel.
    print("Running parallel customer feedback sentiment analysis...")
    daft_results = daft_docs.with_column("analysis", analyze_sentiment(daft_docs["content"]))
    daft_results = daft_results.select(
        daft_docs["doc_id"],
        daft_results["analysis"]["analysis"].alias("analysis"),
        daft_results["analysis"]["confidence"].alias("confidence"),
        daft_results["analysis"]["model_version"].alias("model_version")
    )

    # Write sentiment analysis to a new table with doc_id as the merge key.
    initial_schema = dc.Schema.of([
        dc.Field.of(pa.field("doc_id", pa.int64()), is_merge_key=True),
        dc.Field.of(pa.field("analysis", pa.large_string())),
        dc.Field.of(pa.field("confidence", pa.float64())),
        dc.Field.of(pa.field("model_version", pa.large_string())),
    ])
    dc.write(daft_results, "insights", namespace="analysis", schema=initial_schema)

    # Write to a new audit trail table.
    audit_df = pd.DataFrame([{
        "version": "v1.0",
        "docs_processed": dc.dataset_length(daft_docs),
    }])
    dc.write(audit_df, "audit", namespace="analysis")

print("=== V1.0: Customer feedback sentiment analysis processing complete! ===")

# Create checkpoint after v1.0 transaction commits.
time.sleep(0.1)
checkpoint_v1 = time.time_ns()
time.sleep(0.1)

# Doc processing V2.0
# Switch to a model that captures customer feedback emotion details.
with dc.transaction():
    # Define a UDF to analyze customer feedback emotion details.
    @daft.udf(return_dtype=daft.DataType.struct({
        "analysis": daft.DataType.string(),
        "confidence": daft.DataType.float64(),
        "model_version": daft.DataType.string(),
    }))
    def analyze_emotions(content_series):
        classifier_v2 = pipeline("sentiment-analysis", model="j-hartmann/emotion-english-distilroberta-base")
        results = []
        for content in content_series.to_pylist():
            result = classifier_v2(content[:500])[0]
            results.append({
                "analysis": result['label'],
                "confidence": result['score'],
                "model_version": "distilroberta-v2.0",
            })
        return results

    # Run emotion detail analysis in parallel.
    print("Running parallel customer feedback emotion detail analysis...")
    daft_emotions = daft_docs.with_column("analysis", analyze_emotions(daft_docs["content"]))
    daft_emotions = daft_emotions.select(
        daft_docs["doc_id"],
        daft_emotions["analysis"]["analysis"].alias("analysis"),
        daft_emotions["analysis"]["confidence"].alias("confidence"),
        daft_emotions["analysis"]["model_version"].alias("model_version"),
    )

    # Merge new V2.0 insights into the existing V1.0 insights table.
    dc.write(daft_emotions, "insights", namespace="analysis")
    audit_df = pd.DataFrame([{"version": "v2.0", "docs_processed": dc.dataset_length(daft_docs)}])
    dc.write(audit_df, "audit", namespace="analysis")

print("=== V2.0: Customer feedback emotion analysis processing complete! ===")

time.sleep(0.1)
checkpoint_v2 = time.time_ns()
time.sleep(0.1)

# Doc processing V3.0
# Generate customer service responses based on emotion analysis results.
with dc.transaction():
    # First, read the current insights table with emotion analysis
    current_insights = dc.read("insights", namespace="analysis")

    # Define a UDF to generate customer service responses based on analysis results.
    @daft.udf(return_dtype=daft.DataType.struct({
        "response_text": daft.DataType.string(),
        "response_model": daft.DataType.string(),
        "generated_at": daft.DataType.int64()
    }))
    def generate_responses_from_analysis(analysis_series):
        response_generator = pipeline("text-generation", model="microsoft/DialoGPT-medium")
        results = []

        for analysis in analysis_series.to_pylist():
            # Create appropriate response prompt based on emotion analysis.
            if analysis in ["sadness"]:
                prompt = "Dear valued customer, we sincerely apologize for the inconvenience and"
            elif analysis in ["joy"]:
                prompt = "Thank you so much for your wonderful feedback! We're thrilled to hear"
            elif analysis in ["fear"]:
                prompt = "We understand your concerns and want to assure you that"
            else:
                prompt = "Thank you for contacting us. We appreciate your feedback and"

            # Generate customer service responses.
            generated = response_generator(prompt, max_length=100, num_return_sequences=1, pad_token_id=50256)
            response_text = generated[0]['generated_text']
            results.append({
                "response_text": response_text,
                "response_model": "dialogpt-medium-v3.0",
                "generated_at": time.time_ns()
            })
        return results

    # Run customer service response generation based on analysis results.
    print("Running parallel customer service response generation based on sentiment/emotion analysis...")
    daft_responses = current_insights.with_column(
        "response",
        generate_responses_from_analysis(current_insights["analysis"])
    )
    daft_responses = daft_responses.select(
        current_insights["doc_id"],
        daft_responses["response"]["response_text"].alias("response_text"),
        daft_responses["response"]["response_model"].alias("response_model"),
        daft_responses["response"]["generated_at"].alias("generated_at")
    )
    # Merge new V3.0 responses into the existing V2.0 insights table.
    # The new response columns are automatically joined by document ID.
    dc.write(daft_responses, "insights", namespace="analysis")
    audit_df = pd.DataFrame([{"version": "v3.0", "docs_processed": dc.dataset_length(current_insights)}])
    dc.write(audit_df, "audit", namespace="analysis")

print("=== V3.0: Customer service response generation processing complete! ===")

print("\n=== Time Travel Comparison of all Versions ===")
with dc.transaction(as_of=checkpoint_v1):
    print(f"== V1.0 Insights (sentiment) ==")
    print(dc.read("insights", namespace="analysis").show())
    print(f"== V1.0 Audit ==")
    print(dc.read("audit", namespace="analysis").show())

with dc.transaction(as_of=checkpoint_v2):
    print(f"== V2.0 Insights (emotion) ==")
    print(dc.read("insights", namespace="analysis").show())
    print(f"== V2.0 Audit ==")
    print(dc.read("audit", namespace="analysis").show())

v3_results = dc.read("insights", namespace="analysis")
print(f"== V3.0 Insights (customer service response) ==")
print(dc.read("insights", namespace="analysis").show())
print(f"== V3.0 Audit ==")
print(dc.read("audit", namespace="analysis").show())
```

</details>

## Runtime Environment Requirements

DeltaCAT's transaction system assumes that the host machine provides strong system clock accuracy guarantees, and that the filesystem hosting the catalog root directory offers strong consistency.

Taken together, these requirements make DeltaCAT suitable for production use on most major cloud computing hosts (e.g., EC2, GCE, Azure VMs) and storage systems (e.g., S3, GCS, Azure Blob Storage), but local laptops should typically be limited to testing/experimental purposes.

## Additional Resources
### Table Documentation

The [Table](https://github.com/ray-project/deltacat/tree/2.0/deltacat/docs/table/README.md) documentation provides a more comprehensive overview of DeltaCAT's table management APIs, including how to create, read, write, and manage tables.

### Schema Documentation

The [Schema](https://github.com/ray-project/deltacat/tree/2.0/deltacat/docs/schema/README.md) documentation provides a more comprehensive overview of DeltaCAT's schema management APIs, supported data types, file formats, and data consistency guarantees.

### DeltaCAT URLs and Filesystem APIs
The [DeltaCAT API Tests](https://github.com/ray-project/deltacat/tree/2.0/deltacat/tests/test_deltacat_api.py) provide examples of how to efficiently explore, clone, and manipulate DeltaCAT catalogs by using DeltaCAT URLs together with filesystem-like list/copy/get/put APIs.

### DeltaCAT Catalog APIs
The [Default Catalog Tests](https://github.com/ray-project/deltacat/tree/2.0/deltacat/tests/catalog/test_default_catalog_impl.py) provide more exhaustive examples of DeltaCAT **Catalog** API behavior.

### Examples

The [DeltaCAT Examples](https://github.com/ray-project/deltacat/tree/2.0/deltacat/examples/) show how to build more advanced applications like external data source indexers and custom dataset compactors. They also demonstrate some experimental features like Apache Iceberg and Apache Beam integrations.
