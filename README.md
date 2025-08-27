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
1. [**Catalog**](deltacat/catalog/): High-level APIs to create, discover, organize, share, and manage datasets.
2. [**Compute**](deltacat/compute/): Distributed data management procedures to read, write, and optimize datasets.
3. [**Storage**](deltacat/storage/): In-memory and on-disk multimodal dataset formats.
4. **Sync** (in development): Synchronize DeltaCAT datasets to data warehouses and other table formats.

## Overview
DeltaCAT's **Catalog**, **Compute**, and **Storage** layers work together to bring ACID-compliant data management to any Ray application. These components automate data indexing, change management, dataset read/write optimization, schema evolution, and other common data management tasks across any set of data files readable by Ray Data, Daft, Pandas, Polars, PyArrow, or NumPy.

<p align="center">
  <img src="media/deltacat-tech-overview.png" alt="deltacat tech overview" style="width:100%; height:auto; text-align: center;">
</p>

Data consumers that prefer to stay within the ecosystem of Pythonic data management tools can use DeltaCAT's native table format to manage their data with minimal overhead. For integration with other data analytics frameworks (e.g., Apache Spark, Trino, Apache Flink), DeltaCAT's **Sync** component lets you synchronize your tables to Apache Iceberg and other table formats with minimal overhead.

## Getting Started
DeltaCAT applications run anywhere that Ray apps run, including your local laptop, cloud computing clusters, or on-premise clusters.

DeltaCAT lets you manage **Tables** across one or more **Catalogs**. A **Table** can be thought of as a named collection of one or more data files. A **Catalog** provides a root location (e.g., a local file path or S3 Bucket) to store table information, and can be rooted in any [PyArrow-compatible Filesystem](https://arrow.apache.org/docs/python/filesystems.html). **Tables** can be created, read, and written using the `dc.write` and `dc.read` APIs.

### Quick Start

```python
import deltacat as dc
import pandas as pd

# Initialize DeltaCAT with a local catalog.
# Ray will be initialized automatically.
# Catalog files will be stored in .deltacat/ in the current working directory.
dc.init(catalogs={"my_catalog": dc.Catalog()})

# Create data to write.
data = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35]
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
    "name": ["Diana", "Ethan", "Fiona"],
    "age": [40, 45, 50],
    "city": ["New York", "Los Angeles", "Chicago"]
})
dc.write(data, "users")

# Read the full table back into a Daft DataFrame.
daft_df = dc.read("users")
daft_df.select("name", "city").show()  # Just print the names and cities
```

### Supported Dataset and Content Types
DeltaCAT natively supports a variety of open dataset and content types already integrated with Ray and Arrow. You can use `dc.read` to read tables back as a Daft DataFrame, Ray Dataset, Pandas DataFrame, PyArrow Table, Polars DataFrame, NumPy Array, or a list of PyArrow ParquetFile objects:

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
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35]
})


# Write different dataset types to the default table data file format (Parquet):
daft_df = dc.from_pyarrow(data, dc.DatasetType.DAFT)  # Convert to Daft DataFrame
dc.write(daft_df, "my_daft_table")  # Daft DataFrame

ray_dataset = dc.from_pyarrow(data, dc.DatasetType.RAY_DATASET)  # Convert to Ray Dataset
dc.write(ray_dataset, "my_ray_dataset")  # Ray Dataset

pandas_df = dc.from_pyarrow(data, dc.DatasetType.PANDAS)  # Convert to Pandas DataFrame
dc.write(pandas_df, "my_pandas_table")  # Pandas DataFrame

pyarrow_table = dc.from_pyarrow(data, dc.DatasetType.PYARROW)  # Convert to PyArrow Table
dc.write(pyarrow_table, "my_pyarrow_table")  # PyArrow Table

polars_df = dc.from_pyarrow(data, dc.DatasetType.POLARS)  # Convert to Polars DataFrame
dc.write(polars_df, "my_polars_table")  # Polars DataFrame

numpy_array = dc.from_pyarrow(data, dc.DatasetType.NUMPY)  # Convert to NumPy Array
dc.write(numpy_array, "my_numpy_table")  # NumPy Array

# Write the same dataset type to different table data file formats:
dc.write(data, "my_mixed_format_table", content_type=dc.ContentType.PARQUET)  # Write Parquet (Default)
dc.write(data, "my_mixed_format_table", content_type=dc.ContentType.AVRO)  # Write Avro
dc.write(data, "my_mixed_format_table", content_type=dc.ContentType.ORC)  # Write ORC
dc.write(data, "my_mixed_format_table", content_type=dc.ContentType.FEATHER)  # Write Feather

```
For more information, see the DeltaCAT [Schema](deltacat/docs/schema/README.md) and [Table](deltacat/docs/table/README.md) documentation.

### Additional Resources
#### Examples

The [DeltaCAT Examples](deltacat/examples/) show how to build more advanced application like external data source indexers and custom dataset compactors. They also demonstrate some experimental Apache Iceberg and Beam integrations.

#### DeltaCAT URLs and Filesystem APIs
The [DeltaCAT API Tests](deltacat/tests/test_deltacat_api.py) provide examples of how to efficiently explore, clone, and manipulate DeltaCAT catalogs by using DeltaCAT URLs together with filesystem-like list/copy/get/put APIs.

#### DeltaCAT Catalog APIs
The [Default Catalog Tests](deltacat/tests/catalog/test_default_catalog_impl.py) provide more exhaustive examples of DeltaCAT **Catalog** API behavior.
