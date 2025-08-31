<p align="center">
  <img src="media/deltacat-logo-alpha-750.png" alt="deltacat logo" style="width:55%; height:auto; text-align: center;">
</p>

DeltaCAT is a portable Pythonic Data Lakehouse powered by [Ray](https://github.com/ray-project/ray). It lets you define and manage
fast, scalable, ACID-compliant multimodal data lakes, and has been used to [successfully manage exabyte-scale enterprise
data lakes](https://aws.amazon.com/blogs/opensource/amazons-exabyte-scale-migration-from-apache-spark-to-ray-on-amazon-ec2/).

It uses the Ray distributed compute framework together with [Apache Arrow](https://github.com/apache/arrow) and
[Daft](https://github.com/Eventual-Inc/Daft) to efficiently scale common table management tasks, like petabyte-scale
merge-on-read and copy-on-write operations.

DeltaCAT provides the following high-level components:
1. [**Catalog**](deltacat/catalog/interface.py): High-level APIs to create, discover, organize, share, and manage datasets.
2. [**Compute**](deltacat/compute/): Distributed data management procedures to read, write, and optimize datasets.
3. [**Storage**](deltacat/storage/): In-memory and on-disk multimodal dataset formats.
4. **Sync** (in development): Synchronize DeltaCAT datasets to data warehouses and other table formats.

## Overview
DeltaCAT's **Catalog**, **Compute**, and **Storage** layers work together to bring ACID-compliant data management to any Ray application. These components automate data indexing, change management, dataset read/write optimization, schema evolution, and other common data management tasks across any set of data files readable by Ray Data, Daft, Pandas, Polars, PyArrow, or NumPy.

<p align="center">
  <img src="media/deltacat-tech-overview.png" alt="deltacat tech overview" style="width:100%; height:auto; text-align: center;">
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
    "name": ["Whiskers", "Mittens", "Ginger"],
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
# Compaction and zero-copy schema evolution are handled automatically.
data = pd.DataFrame({
    "id": [4, 5, 6],
    "name": ["Stripes", "Patches", "Delta"],
    "age": [2, 12, 4],
    "city": ["London", "New York", "San Francisco"]
})
dc.write(data, "users")

# Read the full table back into a Daft DataFrame.
daft_df = dc.read("users")
# Print the names and cities (missing cities will be null).
daft_df.select("name", "city").show()
```

### Supported Dataset and File Formats
DeltaCAT natively supports a variety of open dataset and file formats already integrated with Ray and Arrow. You can use `dc.read` to read tables back as a Daft DataFrame, Ray Dataset, Pandas DataFrame, PyArrow Table, Polars DataFrame, NumPy Array, or list of PyArrow ParquetFile objects:

```python
# Read directly into eagerly materialized local datasets:
pandas_df = dc.read("users", read_as=dc.DatasetType.PANDAS)  # Returns Pandas DataFrame
pyarrow_table = dc.read("users", read_as=dc.DatasetType.PYARROW)  # Returns PyArrow Table
polars_df = dc.read("users", read_as=dc.DatasetType.POLARS)  # Returns Polars DataFrame
numpy_array = dc.read("users", read_as=dc.DatasetType.NUMPY)  # Returns NumPy Array

# Or into a lazily materialized list of PyArrow ParquetFile objects:
pyarrow_pq_files = dc.read("users", read_as=dc.DatasetType.PYARROW_PARQUET)  # Returns List[ParquetFile]

# Or as distributed datasets (for larger data):
daft_df = dc.read("users", read_as=dc.DatasetType.DAFT)  # Returns Daft DataFrame (Default)
ray_dataset = dc.read("users", read_as=dc.DatasetType.RAY_DATASET)  # Returns Ray Dataset
```

 `dc.write` can also write any of these dataset types:

```python
import pyarrow as pa

# Create a pyarrow table to write.
data = pa.Table.from_pydict({
    "id": [4, 5, 6],
    "name": ["Stripes", "Patches", "Delta"],
    "age": [2, 12, 4],
    "city": ["London", "New York", "San Francisco"]
})

# Write different dataset types to the default table file format (Parquet):
daft_df = dc.from_pyarrow(data, dc.DatasetType.DAFT)  # Convert to Daft DataFrame
dc.write(daft_df, "my_daft_table")

ray_dataset = dc.from_pyarrow(data, dc.DatasetType.RAY_DATASET)  # Convert to Ray Dataset
dc.write(ray_dataset, "my_ray_dataset")

pandas_df = dc.from_pyarrow(data, dc.DatasetType.PANDAS)  # Convert to Pandas DataFrame
dc.write(pandas_df, "my_pandas_table")

pyarrow_table = dc.from_pyarrow(data, dc.DatasetType.PYARROW)  # Convert to PyArrow Table
dc.write(pyarrow_table, "my_pyarrow_table")

polars_df = dc.from_pyarrow(data, dc.DatasetType.POLARS)  # Convert to Polars DataFrame
dc.write(polars_df, "my_polars_table")

numpy_array = dc.from_pyarrow(data, dc.DatasetType.NUMPY)  # Convert to NumPy Array
dc.write(numpy_array, "my_numpy_table")
```

Or write to different table file formats:

```python
# Start by writing to a new table with a custom list of supported readers.
# By default, DeltaCAT will raise an error if we attempt to write data to
# a file format that can't be read by one or more dataset types.
content_types_to_write = {
    dc.ContentType.PARQUET,
    dc.ContentType.AVRO,
    dc.ContentType.ORC,
    dc.ContentType.FEATHER,
}
supported_reader_types = [
    reader_type for reader_type in dc.DatasetType
    if content_types_to_write & reader_type.readable_content_types
]
dc.write(
    data,
    "my_mixed_format_table",
    content_type=dc.ContentType.PARQUET, # Write Parquet (Default)
    table_properties={dc.TableProperty.SUPPORTED_READER_TYPES: supported_reader_types}
)

# Now apppend the same data using our remaining target file formats:
dc.write(data, "my_mixed_format_table", content_type=dc.ContentType.AVRO)
dc.write(data, "my_mixed_format_table", content_type=dc.ContentType.ORC)
dc.write(data, "my_mixed_format_table", content_type=dc.ContentType.FEATHER)

# All file formats will be automatically unified at read time:
pandas_df = dc.read("my_mixed_format_table", read_as=dc.DatasetType.PANDAS)
pandas_df.show()
```

### Merging and Deleting Data

DeltaCAT can automatically merge and delete data by defining a table schema with one or more merge keys:

```python
import deltacat as dc
import pandas as pd
import pyarrow as pa
from deltacat.storage.model.schema import Schema, Field

# Initialize DeltaCAT with a default local catalog
dc.init_local()

# Define a schema with user_id as a merge key.
schema = Schema.of([
    Field.of(pa.field("user_id", pa.int64()), is_merge_key=True),
    Field.of(pa.field("name", pa.string())),
    Field.of(pa.field("age", pa.int32())),
    Field.of(pa.field("status", pa.string())),
])

# Initial user data
initial_users = pd.DataFrame({
    "user_id": [1, 2, 3],
    "name": ["Whiskers", "Mittens", "Stripes"],
    "age": [3, 7, 2],
    "status": ["active", "active", "inactive"]
})

# Write initial data with the merge key schema
dc.write(initial_users, "users", schema=schema)

# Read the data back as a Pandas DataFrame.
df = dc.read("users", read_as=dc.DatasetType.PANDAS)
print("=== Initial Users ===")
print(df)

# Update data for existing users + add new users
updated_users = pd.DataFrame({
    "user_id": [2, 3, 4, 5, 6],  # Mittens & Stripes updated, new users added
    "name": ["Mittens", "Stripes", "Ginger", "Patches", "Delta"],
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

# - Whiskers (user_id=1) remains unchanged
# - Mittens (user_id=2) status updated to "premium"
# - Stripes (user_id=3) updated to "active"
# - New users (4,5,6), (Ginger, Patches, Delta) added
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

# - Stripes (user_id=3) has been removed
# - Patches (user_id=5) has been removed
# - All other users remain unchanged
```

### Organizing Tables with Namespaces

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
    "name": ["Ginger", "Stripes", "Delta"],
})

product_data = pd.DataFrame({
    "product_id": [101, 102, 103],
    "name": ["Lasagna", "Tuna", "Parquet"],
    "price": [12.99, 8.99, 0.01]
})

order_data = pd.DataFrame({
    "order_id": [1001, 1002, 1003],
    "user_id": [1, 2, 3],
    "product_id": [101, 102, 103],
    "quantity": [2, 1, 1_000_000_000]
})

# Write tables to different namespaces to organize them by domain
dc.write(user_data, "users", namespace="identity")
dc.write(product_data, "catalog", namespace="inventory")
dc.write(order_data, "transactions", namespace="sales")

# Read from specific namespaces
users_df = dc.read("users", namespace="identity", read_as=dc.DatasetType.PANDAS)
products_df = dc.read("catalog", namespace="inventory", read_as=dc.DatasetType.PANDAS)
orders_df = dc.read("transactions", namespace="sales", read_as=dc.DatasetType.PANDAS)

print(f"Identity namespace has {len(users_df)} users")
print(f"Inventory namespace has {len(products_df)} products")
print(f"Sales namespace has {len(orders_df)} transactions")

# Tables with the same name can exist in different namespaces
# Create separate marketing and finance views of the same data
marketing_users = pd.DataFrame({
    "user_id": [1, 2, 3],
    "segment": ["premium", "standard", "premium"],
    "acquisition_channel": ["social", "search", "referral"]
})

finance_users = pd.DataFrame({
    "user_id": [1, 2, 3],
    "lifetime_payments": [25.98, 8.99, 10000000.00],
    "preferred_payment_method": ["credit", "cash", "paypal"]
})

dc.write(marketing_users, "users", namespace="marketing")
dc.write(finance_users, "users", namespace="finance")

# Each namespace maintains its own "users" table with different schemas
marketing_df = dc.read("users", namespace="marketing", read_as=dc.DatasetType.PANDAS)
finance_df = dc.read("users", namespace="finance", read_as=dc.DatasetType.PANDAS)

print(f"Marketing users fields: {list(marketing_df.columns)}")  # ['user_id', 'segment', 'acquisition_channel']
print(f"Finance users fields: {list(finance_df.columns)}")  # ['user_id', 'lifetime_payments', 'preferred_payment_method']
```

### Multi-Table Transactions

DeltaCAT transactions can span multiple tables and namespaces. Since all operations within a transaction either succeed or fail together, this simplifies keeping related datasets in sync across your entire catalog.

Consider the previous example that organized tables with namespaces. One table tracked customer orders, and another table tracked the lifetime payments of each customer. If one table was updated but not the other, then it would result in an accounting discrepancy. This edge case can be eliminated by using multi-table transactions:

```python
import deltacat as dc
import pandas as pd
import tempfile

# Initialize DeltaCAT with a fresh temporary catalog
dc.init_local(tempfile.mkdtemp())

# Create sample product data.
product_data = pd.DataFrame({
    "product_id": [101, 102, 103],
    "name": ["Lasagna", "Tuna", "Parquet"],
    "price": [12.99, 8.99, 0.01]
})

# The product catalog can be created independently.
dc.write(product_data, "catalog", namespace="inventory")

# Create sample user and finance data.
user_data = pd.DataFrame({
    "user_id": [1, 2, 3],
    "name": ["Ginger", "Stripes", "Delta"],
})
initial_finance = pd.DataFrame({
    "user_id": [1, 2, 3],
    "lifetime_payments": [0.00, 0.00, 0.00],
    "preferred_payment_method": ["credit", "cash", "paypal"]
})

# Create user identities and user finance data within a single transaction.
# Since transactions are atomic, this will prevent user data discrepancies.
with dc.transaction():
    dc.write(user_data, "users", namespace="identity")
    dc.write(initial_finance, "users", namespace="finance")

# Create new order data
new_orders = pd.DataFrame({
    "order_id": [1001, 1002, 1003],
    "user_id": [1, 2, 3],
    "product_id": [101, 102, 103],
    "quantity": [2, 1, 1_000_000_000]
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
    lifetime_totals = orders_with_prices.groupby("user_id")["total"].sum().reset_index()
    lifetime_totals.columns = ["user_id", "lifetime_payments"]

    # Step 4: Write the computed totals
    dc.write(lifetime_totals, "users", namespace="finance", mode=dc.TableWriteMode.REPLACE)

# Verify consistency - orders and finance totals are guaranteed to match
orders_df = dc.read("transactions", namespace="sales", read_as=dc.DatasetType.PANDAS)
finance_df = dc.read("users", namespace="finance", read_as=dc.DatasetType.PANDAS)

print(f"Orders processed: {len(orders_df)}")
print(f"Finance records updated: {len(finance_df)}")

# Without multi-table transactions, we could wind up with:
# - User data without corresponding finance records (or vice versa).
# - Orders successfully recorded but finance totals not updated.
# - Finance totals updated without any corresponding orders.
```

### Working with Multiple Catalogs

DeltaCAT lets you work with multiple catalogs in a single application. All catalogs registered with DeltaCAT are tracked by a Ray Actor to make them available to all workers in your Ray application.

For example, you may want to test a write against a local staging catalog before committing it to a shared production catalog:

```python
import deltacat as dc
import pandas as pd
import pyarrow as pa
import tempfile
from decimal import Decimal

# Initialize catalogs with separate names and catalog roots.
dc.init(catalogs={
    "staging": dc.Catalog(config=dc.CatalogProperties(
        root=tempfile.mkdtemp(),  # Use temporary directory for staging
        filesystem=pa.fs.LocalFileSystem()
    )),
    "prod": dc.Catalog(config=dc.CatalogProperties(
        root="s3://deltacat_prod/",
        filesystem=pa.fs.S3FileSystem()
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
except dc.TableValidationError as e:
    print(f"Validation error: {e}")
    print("Decimal256 may break existing data consumers in prod, trying decimal128...")

    # Cast the price column from decimal256 to decimal128
    decimal_table = decimal_table.set_column(
        decimal_table.schema.get_field_index("price"),
        "price",
        pa.cast(decimal_table["price"], pa.decimal128(10, 2))
    )

# Write decimal128 data to staging and ensure that the write succeeds
dc.write(decimal_table, "financial_data", catalog="staging")

# Read from staging to verify
staging_data = dc.read("financial_data", catalog="staging", read_as=dc.DatasetType.PANDAS)
assert staging_data["price"].tolist() == [Decimal("999.99"), Decimal("1234.56"), Decimal("567.89")]

# Now write the validated data to production
dc.write(decimal_table, "financial_data", catalog="prod")
```

For more information, see the DeltaCAT [Schema](deltacat/docs/schema/README.md) and [Table](deltacat/docs/table/README.md) documentation.

### Time Travel

DeltaCAT supports time travel queries that let you read all tables in a catalog as they existed at any point in the past. Combined with multi-table transactions, this enables consistent point-in-time views across your entire data catalog:

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
    "name": ["Ginger", "Stripes", "Delta"],
})

initial_products = pd.DataFrame({
    "product_id": [101, 102, 103],
    "name": ["Lasagna", "Tuna", "Parquet"],
    "price": [12.99, 8.99, 0.01]
})

initial_orders = pd.DataFrame({
    "order_id": [1001, 1002, 1003],
    "user_id": [1, 2, 3],
    "product_id": [101, 102, 103],
    "quantity": [2, 1, 1_000_000_000]
})

initial_finance = pd.DataFrame({
    "user_id": [1, 2, 3],
    "lifetime_payments": [25.98, 8.99, 10000000.00],
})

# Write initial state atomically
with dc.transaction():
    dc.write(initial_users, "users", namespace="identity")
    dc.write(initial_products, "catalog", namespace="inventory")
    dc.write(initial_orders, "transactions", namespace="sales")
    dc.write(initial_finance, "users", namespace="finance")

# Capture timestamp for time travel
time.sleep(0.1)
checkpoint_time = time.time_ns()
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
    "name": ["Whiskers", "Patches"],
})

new_products = pd.DataFrame({
    "product_id": [104, 105],
    "name": ["Canary", "Milk"],
    "price": [6.99, 3.99]
})

# Update finance data with new lifetime payment totals
updated_finance = pd.DataFrame({
    "user_id": [1, 2, 3, 4, 5],
    "lifetime_payments": [77.94, 26.97, 10000000.00, 34.95, 3.99]  # Updated totals
})

# Execute all updates atomically - either all succeed or all fail
with dc.transaction():
    # Add new users, products, orders, and lifetime payment totals
    dc.write(new_users, "users", namespace="identity")
    dc.write(new_products, "catalog", namespace="inventory")
    dc.write(new_orders, "transactions", namespace="sales")
    dc.write(updated_finance, "users", namespace="finance", mode=dc.TableWriteMode.REPLACE)

# Compare current state vs historic state across all tables
print("=== Current State (After Updates) ===")
current_users = dc.read("users", namespace="identity", read_as=dc.DatasetType.PANDAS)
current_orders = dc.read("transactions", namespace="sales", read_as=dc.DatasetType.PANDAS)
current_finance = dc.read("users", namespace="finance", read_as=dc.DatasetType.PANDAS)

print(f"Users: {len(current_users)} total ({list(current_users['name'])})")
print(f"Orders: {len(current_orders)} total")
print(f"Finance records: {len(current_finance)} total")

# Now query all tables as they existed at the checkpoint
print("\n=== Historic State (Before Updates) ===")
with dc.transaction(as_of=checkpoint_time):
    historic_users = dc.read("users", namespace="identity", read_as=dc.DatasetType.PANDAS)
    historic_orders = dc.read("transactions", namespace="sales", read_as=dc.DatasetType.PANDAS)
    historic_finance = dc.read("users", namespace="finance", read_as=dc.DatasetType.PANDAS)

    print(f"Users: {len(historic_users)} total ({list(historic_users['name'])})")
    print(f"Orders: {len(historic_orders)} total")
    print(f"Finance records: {len(historic_finance)} total")

    # Validate historic state - Whiskers & Patches didn't exist yet
    assert not any(historic_users["name"] == "Whiskers")
    assert not any(historic_users["name"] == "Patches")
    assert len(historic_orders) == 3  # Only original 3 orders

    # Finance data reflects original payment totals
    historic_payments = historic_finance[historic_finance["user_id"] == 1]["lifetime_payments"].iloc[0]
    assert historic_payments == 25.98  # Original total, not updated 77.94

print("\nTime travel validation successful!")
```

### Multimodal Batch Inference

DeltaCAT supports native multimodal data storage and processing with partial field updates. For example, the following code classifies images of cats by breed, then adds predictions using partial updates (only merge key + new fields):

> **Requirements**: This example requires PyTorch ≥ 2.8.0 and torchvision ≥ 0.23.0. Install via: `pip install torch>=2.8.0 torchvision>=0.23.0`

```python
import deltacat as dc
import tempfile
import daft
import torch
import pyarrow as pa
from deltacat.storage.model.schema import Schema, Field

# Initialize DeltaCAT with a temporary catalog
dc.init_local(tempfile.mkdtemp())

# Define initial schema with image_id as merge key (no prediction fields yet)
initial_schema = Schema.of([
    Field.of(pa.field("image_id", pa.large_string()), is_merge_key=True),
    Field.of(pa.field("image_path", pa.large_string())),
    Field.of(pa.field("true_breed", pa.large_string())),
    Field.of(pa.field("image_bytes", pa.binary())),
])

# Create sample Daft DataFrame with image URLs/paths
df = daft.from_pydict({
    "image_id": ["cat_001", "cat_002", "cat_003"],
    "image_path": ["media/tuxedo.jpg", "media/calico.jpg", "media/siamese.jpg"],
    "true_breed": ["Tuxedo", "Calico", "Siamese"]
})

# Load images and prepare for processing
df = df.with_column("image_bytes", df["image_path"].url.download())

# Write initial dataset to DeltaCAT
dc.write(df, "cool_cats", schema=initial_schema)

# Define an ImageClassifier UDF for cat breed prediction
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

# Apply the UDF first to get predictions
df_with_predictions = df.with_column("prediction", ImageClassifier(df["image_path"]))

# Run batch inference and prepare partial update data (merge key + new fields only)
prediction_data = df_with_predictions.select(
    df_with_predictions["image_id"],  # Merge key to match existing records
    df_with_predictions["prediction"].list.get(0).alias("predicted_breed"),  # New field to update
    (df_with_predictions["prediction"].list.get(1).str.replace("%", "").cast(daft.DataType.float64()) / 100.0).alias("confidence")  # New field to update
)

# Write the predictions back to the table
dc.write(prediction_data, "cool_cats")

# Read back the merged results
final_df = dc.read("cool_cats")
final_df = final_df.with_column("image", final_df["image_bytes"].image.decode())

# Calculate accuracy and display results
results = final_df.select("image_id", "true_breed", "predicted_breed", "confidence").to_pandas()
accuracy = (results.apply(lambda row: row["true_breed"].lower() in row["predicted_breed"].lower(), axis=1)).mean()

print(f"Classification Accuracy: {accuracy:.1%}")

# Display final dataset with decoded images for visual inspection
print(f"Final dataset with images and predictions:")
final_df.show()
```

Unfortunately, this example achieves only 33% accuracy using generic ImageNet classes. To achieve better accuracy, you can also [fine-tune your own cat breed classifier using DeltaCAT](deltacat/docs/examples/fine-tuning-guide.md).

### LLM Document Processing with Time Travel

The following example combines multi-table transactions, time travel queries, and automatic schema evolution to build a document processing pipeline:

```python
import deltacat as dc
import pandas as pd
import tempfile
import time
import daft
from transformers import pipeline

# Initialize DeltaCAT
dc.init_local(tempfile.mkdtemp())

# Load customer feedback
daft_docs = daft.from_pydict({
    "doc_id": [1, 2, 3],
    "path": ["media/customer_feedback_001.txt", "media/customer_feedback_002.txt", "media/customer_feedback_003.txt"]
})
daft_docs = daft_docs.with_column("content", daft_docs["path"].url.download())

# Doc processing V1.0
# Capture basic feedback sentiment analysis in a parallel multi-table transaction
with dc.transaction():
    # Write the full customer feedback to a new "documents" table.
    dc.write(daft_docs, "documents", namespace="analysis")

    # Define a UDF to analyze customer feedback sentiment.
    @daft.udf(return_dtype=daft.DataType.struct({
        "sentiment": daft.DataType.string(),
        "confidence": daft.DataType.float64(),
        "model_version": daft.DataType.string()
    }))
    def analyze_sentiment(content_series):
        classifier = pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment-latest")
        results = []
        for content in content_series.to_pylist():
            result = classifier(content[:500])[0]  # Truncate for model limits
            results.append({
                "sentiment": result['label'],
                "confidence": result['score'],
                "model_version": "roberta-v1.0"
            })
        return results

    # Run sentiment analysis in parallel.
    print("Running parallel customer feedback sentiment analysis...")
    daft_results = daft_docs.with_column("analysis", analyze_sentiment(daft_docs["content"]))
    daft_results = daft_results.select(
        daft_docs["doc_id"],
        daft_results["analysis"]["sentiment"].alias("sentiment"),
        daft_results["analysis"]["confidence"].alias("confidence"),
        daft_results["analysis"]["model_version"].alias("model_version")
    )

    # Write sentiment analysis to a new table with doc_id as the merge key.
    initial_schema = Schema.of([
        Field.of(pa.field("doc_id", pa.int64()), is_merge_key=True),
        Field.of(pa.field("sentiment", pa.large_string())),
        Field.of(pa.field("confidence", pa.float64())),
        Field.of(pa.field("model_version", pa.large_string())),
    ])
    dc.write(daft_results, "insights", namespace="analysis", schema=initial_schema)

    # Write to a new audit trail table.
    audit_df = pd.DataFrame([{
        "version": "v1.0", 
        "docs_processed": dc.dataset_length(daft_docs),
    }])
    dc.write(audit_df, "audit", namespace="analysis")

print("V1.0: Customer feedback sentiment analysis processing complete!")

# Create checkpoint after v1.0 transaction commits.
time.sleep(0.1)
checkpoint_v1 = time.time_ns()
time.sleep(0.1)

# Doc processing V2.0
# Upgrade to a model that also captures customer feedback emotion details.
with dc.transaction():
    # Define a UDF to analyze customer feedback emotion details.
    @daft.udf(return_dtype=daft.DataType.struct({
        "sentiment": daft.DataType.string(),
        "confidence": daft.DataType.float64(),
        "model_version": daft.DataType.string(),
        "emotion_detail": daft.DataType.string()
    }))
    def analyze_emotions(content_series):
        classifier_v2 = pipeline("sentiment-analysis", model="j-hartmann/emotion-english-distilroberta-base")
        results = []
        for content in content_series.to_pylist():
            result = classifier_v2(content[:500])[0]
            results.append({
                "sentiment": result['label'],
                "confidence": result['score'],
                "model_version": "distilroberta-v2.0",
                "emotion_detail": result['label']  # New column
            })
        return results

    # Run emotion detail analysis in parallel.
    print("Running parallel customer feedback emotion detail analysis...")
    daft_emotions = daft_docs.with_column("analysis", analyze_emotions(daft_docs["content"]))
    daft_emotions = daft_emotions.select(
        daft_docs["doc_id"],
        daft_emotions["analysis"]["sentiment"].alias("sentiment"),
        daft_emotions["analysis"]["confidence"].alias("confidence"),
        daft_emotions["analysis"]["model_version"].alias("model_version"),
        daft_emotions["analysis"]["emotion_detail"].alias("emotion_detail")
    )

    # Merge new V2.0 insights into the existing V1.0 insights table.
    # The new "emotion_detail" column is automatically zipper-merged by document ID.
    dc.write(daft_emotions, "insights", namespace="analysis", merge_key="doc_id")
    audit_df = pd.DataFrame([{"version": "v2.0", "docs_processed": dc.dataset_length(daft_docs)}])
    dc.write(audit_df, "audit", namespace="analysis")

print("V2.0: Customer feedback emotion detail analysis processing complete!")

time.sleep(0.1)
checkpoint_v2 = time.time_ns()
time.sleep(0.1)

# Doc processing V3.0
# Generate customer service responses based on sentiment and emotion detail.
with dc.transaction():
    # Define a UDF to generate customer service responses.
    @daft.udf(return_dtype=daft.DataType.struct({
        "response_text": daft.DataType.string(),
        "response_model": daft.DataType.string(),
        "generated_at": daft.DataType.int64()
    }))
    def generate_responses(content_series):
        response_generator = pipeline("text-generation", model="microsoft/DialoGPT-medium")
        results = []
        for content in content_series.to_pylist():
            # Create appropriate response prompt based on content.
            if "disappointed" in content.lower() or "unacceptable" in content.lower():
                prompt = "Dear valued customer, we sincerely apologize for"
            elif "outstanding" in content.lower() or "excellent" in content.lower():
                prompt = "Thank you so much for your wonderful feedback! We're thrilled"
            else:
                prompt = "Thank you for contacting us. We understand your concern and"

            # Generate truncated responses.
            generated = response_generator(prompt, max_length=100, num_return_sequences=1, pad_token_id=50256)
            response_text = generated[0]['generated_text']
            results.append({
                "response_text": response_text,
                "response_model": "dialogpt-medium-v3.0",
                "generated_at": time.time_ns()
            })
        return results

    # Run customer service response generation in parallel.
    print("Running parallel customer service response generation...")
    daft_responses = daft_docs.with_column("response", generate_responses(daft_docs["content"]))
    daft_responses = daft_responses.select(
        daft_docs["doc_id"],
        daft_responses["response"]["response_text"].alias("response_text"),
        daft_responses["response"]["response_model"].alias("response_model"),
        daft_responses["response"]["generated_at"].alias("generated_at")
    )
    dc.write(daft_responses, "insights", namespace="analysis")
    audit_df = pd.DataFrame([{"version": "v3.0", "docs_processed": dc.dataset_length(daft_docs)}])
    dc.write(audit_df, "audit", namespace="analysis")

print("V3.0: Customer service response generation processing complete!")

print("\nTime Travel Comparison of all Versions:")
with dc.transaction(as_of=checkpoint_v1):
    v1_results = dc.read("insights", namespace="analysis")
    print(f"V1.0 Insights (sentiment only): {dc.to_pandas(v1_results)}")
    v1_audit = dc.read("audit", namespace="analysis")
    print(f"V1.0 Audit: {dc.to_pandas(v1_audit)}")

with dc.transaction(as_of=checkpoint_v2):
    v2_results = dc.read("insights", namespace="analysis")
    print(f"V2.0 Insights (with emotion detail): {dc.to_pandas(v2_results)}")
    v2_audit = dc.read("audit", namespace="analysis")
    print(f"V2.0 Audit: {dc.to_pandas(v2_audit)}")

v3_results = dc.read("insights", namespace="analysis")
print(f"V3.0 Insights (with customer service response): {dc.to_pandas(v3_results)}")
v3_audit = dc.read("audit", namespace="analysis")
print(f"V3.0 Audit: {dc.to_pandas(v3_audit)}")
```

## Runtime Environment Requirements

DeltaCAT's transaction system assumes that the host machine provides strong system clock accuracy guarantees, and that the filesystem hosting the catalog root directory offers strong consistency.

Taken together, these requirements make DeltaCAT suitable for production use on most major cloud computing hosts (e.g., EC2, GCE, Azure VMs) and storage systems (e.g., S3, GCS, Azure Blob Storage), but local laptops should typically be limited to testing/experimental purposes.

## Additional Resources
### Table Documentation

The [Table](deltacat/docs/table/README.md) documentation provides a more comprehensive overview of DeltaCAT's table management APIs, including how to create, read, write, and manage tables.

### Schema Documentation

The [Schema](deltacat/docs/schema/README.md) documentation provides a more comprehensive overview of DeltaCAT's schema management APIs, supported data types, file formats, and data consistency guarantees.

### Examples

The [DeltaCAT Examples](deltacat/examples/) show how to build more advanced application like external data source indexers and custom dataset compactors. They also demonstrate some experimental Apache Iceberg and Beam integrations.

### DeltaCAT URLs and Filesystem APIs
The [DeltaCAT API Tests](deltacat/tests/test_deltacat_api.py) provide examples of how to efficiently explore, clone, and manipulate DeltaCAT catalogs by using DeltaCAT URLs together with filesystem-like list/copy/get/put APIs.

### DeltaCAT Catalog APIs
The [Default Catalog Tests](deltacat/tests/catalog/test_default_catalog_impl.py) provide more exhaustive examples of DeltaCAT **Catalog** API behavior.
