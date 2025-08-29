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
    "age": [83, 84, 47]
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
    "age": [45, 105, 4],
    "city": ["Chagrin Falls", "New York", "San Francisco"]
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
    "age": [45, 105, 4],
    "city": ["Chagrin Falls", "New York", "San Francisco"]
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

# Initialize DeltaCAT with a default local catalog
dc.init_local()

# Define a schema with user_id as a merge key.
from deltacat.storage.model.schema import Schema, Field

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
    "age": [83, 84, 45],
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
    "age": [84, 45, 47, 105, 4],
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

DeltaCAT transactions can span multiple tables and namespaces. Since all operations within a transaction either succeed or fail together, this makes it easier to keep related datasets consistent across your entire catalog.

Consider our previous example of organizing tables using namespaces. We had one table that kept track of customer orders, and another
table that tracked the lifetime payments of each customer. If we successfully updated one table but not the other, then
we'd be left with an accounting discrepancy. We can resolve this using multi-table transactions:

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

DeltaCAT lets you work with multiple catalogs in a single application. All catalogs registered with DeltaCAT are tracked by a Ray Actor to make them automatically available to all workers in your cluster or local machine.

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
time.sleep(1)
checkpoint_time = time.time_ns()
time.sleep(1)

# Later, add new orders for existing users and new users Whiskers & Patches
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
    whiskers_historic_payments = historic_finance[historic_finance["user_id"] == 1]["lifetime_payments"].iloc[0]
    assert whiskers_historic_payments == 25.98  # Original total, not updated 77.94

print("\nTime travel validation successful! 🕰️")
```

### Multimodal Batch Inference

DeltaCAT supports native multimodal data storage and processing. For example, the following code classifies images of cats by breed:

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
    Field.of(pa.field("image_id", pa.string()), is_merge_key=True),
    Field.of(pa.field("image_path", pa.string())),
    Field.of(pa.field("true_breed", pa.string())),
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

# Write initial dataset to DeltaCAT (only image data, no predictions yet)
initial_data = df.select("image_id", "image_path", "true_breed", "image_bytes").to_pandas()
dc.write(initial_data, "cool_cats", schema=initial_schema)

# Define efficient class-based UDF following Daft best practices
@daft.udf(return_dtype=daft.DataType.struct({
    "predicted_breed": daft.DataType.string(),
    "confidence": daft.DataType.float64()
}))
class ImageClassifier:
    def __init__(self):
        """Initialize model once per worker for efficiency (following Daft patterns)"""
        print("Loading NVIDIA ResNet50 classifier...")
        self.model = torch.hub.load(
            "NVIDIA/DeepLearningExamples:torchhub",
            "nvidia_resnet50",
            pretrained=True
        )
        self.model.eval().to(torch.device("cpu"))  # Consistent CPU processing
        self.utils = torch.hub.load(
            "NVIDIA/DeepLearningExamples:torchhub",
            "nvidia_convnets_processing_utils"
        )

    def __call__(self, image_paths):
        """Process batch of images efficiently using NVIDIA utilities"""
        results = []
        for path in image_paths.to_pylist():
            # Work directly with file paths (NVIDIA utilities expect URIs/paths)
            # Use NVIDIA's built-in preprocessing directly with file path
            tensor = self.utils.prepare_input_from_uri(path)

            with torch.no_grad():
                output = self.model(tensor)
                # Use NVIDIA's utilities for top prediction
                predictions = self.utils.pick_n_best(predictions=output, n=1)

                # NVIDIA utils return format: [[('class_name', 'confidence_percent_string')]]
                prediction_tuple = predictions[0][0]  # First prediction tuple
                predicted_breed = prediction_tuple[0]  # Class name
                # Convert confidence from string like "753.0%" to float
                confidence_str = prediction_tuple[1].replace('%', '')
                confidence = float(confidence_str) / 100.0  # Convert to 0-1 range

                results.append({
                    "predicted_breed": predicted_breed,
                    "confidence": confidence
                })
        return results

# Run batch inference on file paths and prepare prediction data for merge
predictions_df = df.with_column("prediction", ImageClassifier(df["image_path"]))
prediction_data = predictions_df.select(
    df["image_id"],  # Merge key to match existing records
    df["image_path"],  # Include all existing fields for complete record
    df["true_breed"],  # Include all existing fields for complete record
    df["image_bytes"], # Include all existing fields for complete record
    predictions_df["prediction"]["predicted_breed"].alias("predicted_breed"),  # New field
    predictions_df["prediction"]["confidence"].alias("confidence")  # New field
)

# Schema evolution: Write complete records with new prediction fields - DeltaCAT
# automatically evolves the schema and preserves all existing data
dc.write(prediction_data.to_pandas(), "cool_cats")

# Read back the merged results and add decoded images for display
final_df = dc.read("cool_cats", read_as=dc.DatasetType.DAFT)
final_df = final_df.with_column("image", final_df["image_bytes"].image.decode())

# Calculate accuracy and display results
results = final_df.select("image_id", "true_breed", "predicted_breed", "confidence").to_pandas()
accuracy = (results["true_breed"] == results["predicted_breed"]).mean()

print(f"Classification Results (using NVIDIA ResNet50):")
print(f"Classification Accuracy: {accuracy:.1%}")
for _, row in results.iterrows():
    status = "✓" if row["true_breed"] == row["predicted_breed"] else "✗"
    print(f"{status} {row['image_id']}: {row['true_breed']} → {row['predicted_breed']} ({row['confidence']:.1%})")

# Display final dataset with decoded images for visual inspection
print(f"\nFinal dataset with images:")
final_df.show()
```

### Putting it All Together: LLM Document Processing with Time Travel

Putting it all together, we can build a document processing pipeline with ACID transactions across tables, time travel queries, and automatic schema evolution. Here's a concise example with real document processing:

```python
import deltacat as dc
import pandas as pd
import tempfile
import time
import daft
from transformers import pipeline

# Initialize DeltaCAT
dc.init_local(tempfile.mkdtemp())

# Load sample customer documents (fictional data for demonstration)
docs = pd.DataFrame([
    {"doc_id": "feedback_001", "path": "media/customer_feedback_001.txt"},
    {"doc_id": "feedback_002", "path": "media/customer_feedback_002.txt"},
    {"doc_id": "feedback_003", "path": "media/customer_feedback_003.txt"}
])

# Read document content and convert to Daft for parallel processing
docs['content'] = docs['path'].apply(lambda p: open(p).read())
daft_docs = daft.from_pandas(docs)

# === UNIQUE VALUE: Multi-table ACID transaction ===

with dc.transaction():  # Atomic across ALL tables
    dc.write(docs, "documents", namespace="analysis")

    # Parallel LLM sentiment analysis (v1.0) using Daft
    print("🚀 Running parallel sentiment analysis with Daft...")

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

    # Parallel execution across available cores
    daft_results = daft_docs.with_column("analysis", analyze_sentiment(daft_docs["content"]))
    daft_results = daft_results.select(
        daft_docs["doc_id"],
        daft_results["analysis"]["sentiment"].alias("sentiment"),
        daft_results["analysis"]["confidence"].alias("confidence"),
        daft_results["analysis"]["model_version"].alias("model_version")
    )

    insights_v1 = daft_results.to_pandas()

    # Store with merge capability (automatic upserts)
    dc.write(insights_v1, "insights", namespace="analysis", merge_key="doc_id")

    # Audit trail
    dc.write(pd.DataFrame([{"version": "v1.0", "docs_processed": len(docs)}]),
             "audit", namespace="analysis")

print("✅ V1.0 processing complete")

# Create checkpoint AFTER v1.0 transaction commits
time.sleep(1)
checkpoint_v1 = time.time_ns()
time.sleep(1)

# === Model upgrade simulation ===
time.sleep(1)
checkpoint_v2 = time.time_ns()
time.sleep(1)

with dc.transaction():
    # Parallel emotion analysis upgrade (v2.0) using Daft
    print("🚀 Running parallel emotion analysis with Daft...")

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
                "emotion_detail": result['label']  # New column - automatic schema evolution!
            })
        return results

    # Parallel execution for emotion analysis
    daft_emotions = daft_docs.with_column("analysis", analyze_emotions(daft_docs["content"]))
    daft_emotions = daft_emotions.select(
        daft_docs["doc_id"],
        daft_emotions["analysis"]["sentiment"].alias("sentiment"),
        daft_emotions["analysis"]["confidence"].alias("confidence"),
        daft_emotions["analysis"]["model_version"].alias("model_version"),
        daft_emotions["analysis"]["emotion_detail"].alias("emotion_detail")
    )

    insights_v2 = daft_emotions.to_pandas()

    # Automatic schema evolution: new emotion_detail column added seamlessly
    dc.write(insights_v2, "insights", namespace="analysis", merge_key="doc_id")
    dc.write(pd.DataFrame([{"version": "v2.0", "docs_processed": len(docs)}]),
             "audit", namespace="analysis")

print("✅ V2.0 upgrade complete with schema evolution")

# === V3.0 Customer Service Response Generation ===
time.sleep(1)
checkpoint_v3 = time.time_ns()
time.sleep(1)

with dc.transaction():
    # Parallel customer service response generation using Daft
    print("🚀 Running parallel response generation with Daft...")

    @daft.udf(return_dtype=daft.DataType.struct({
        "response_text": daft.DataType.string(),
        "response_model": daft.DataType.string(),
        "generated_at": daft.DataType.int64()
    }))
    def generate_responses(content_series):
        response_generator = pipeline("text-generation", model="microsoft/DialoGPT-medium")
        results = []
        for content in content_series.to_pylist():
            # Create appropriate response prompt based on content
            if "disappointed" in content.lower() or "unacceptable" in content.lower():
                prompt = "Dear valued customer, we sincerely apologize for"
            elif "outstanding" in content.lower() or "excellent" in content.lower():
                prompt = "Thank you so much for your wonderful feedback! We're thrilled"
            else:
                prompt = "Thank you for contacting us. We understand your concern and"

            # Generate response (truncated for demo)
            generated = response_generator(prompt, max_length=100, num_return_sequences=1, pad_token_id=50256)
            response_text = generated[0]['generated_text']

            results.append({
                "response_text": response_text,
                "response_model": "dialogpt-medium-v3.0",
                "generated_at": time.time_ns()
            })
        return results

    # Parallel execution for response generation
    daft_responses = daft_docs.with_column("response", generate_responses(daft_docs["content"]))
    daft_responses = daft_responses.select(
        daft_docs["doc_id"],
        daft_responses["response"]["response_text"].alias("response_text"),
        daft_responses["response"]["response_model"].alias("response_model"),
        daft_responses["response"]["generated_at"].alias("generated_at")
    )

    responses_df = daft_responses.to_pandas()
    dc.write(responses_df, "responses", namespace="analysis", merge_key="doc_id")
    dc.write(pd.DataFrame([{"version": "v3.0", "docs_processed": len(docs)}]),
             "audit", namespace="analysis")

print("✅ V3.0 customer service automation complete")

# === UNIQUE VALUE: Time travel analysis across all versions ===
print("\n🕰️ Time Travel Comparison:")

# V1.0 results (point-in-time query)
with dc.transaction(as_of=checkpoint_v1):
    old_results = dc.read("insights", namespace="analysis")
    print(f"V1.0: {list(old_results.columns)}")

# Current V2.0 results
new_results = dc.read("insights", namespace="analysis")
print(f"V2.0: {list(new_results.columns)}")

# Show actual predictions
for _, row in new_results.iterrows():
    print(f"{row['doc_id']}: {row['sentiment']} ({row['confidence']:.2f})")

# Show generated customer service responses
print("\n💬 Generated Customer Service Responses:")
responses = dc.read("responses", namespace="analysis")
for _, row in responses.iterrows():
    response_preview = row['response_text'][:80].replace('\n', ' ')
    print(f"{row['doc_id']}: {response_preview}...")

# Complete audit trail
audit_history = dc.read("audit", namespace="analysis")
print(f"\n📊 Complete Processing History:")
for _, audit in audit_history.iterrows():
    print(f"  {audit['version']}: {audit['docs_processed']} documents processed")

print(f"\n✅ Complete enterprise ML pipeline with automated customer service!")
print(f"📊 Multi-table ACID transactions, time travel, and LLM automation")
print(f"🚀 Capabilities impossible with raw Parquet/Ray Data alone")
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
