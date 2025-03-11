# DeltaCAT - Daft Integration

This package provides integration between [Daft](https://github.com/Eventual-Inc/Daft) DataFrames and DeltaCAT's storage formats through the Writer interface.

## Usage

Apply the monkey patch:

```python
from deltacat.dev.data_access_layer.daft_integration import patch_daft

# Add write_deltacat method to Daft's DataFrame class
patch_daft()
```

Then use the `write_deltacat` method on any Daft DataFrame:

```python
import daft
from deltacat.dev.data_access_layer.storage.rivulet_writer import RivuletWriter, RivuletWriteOptions

# Create a DataFrame
df = daft.from_pydict({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "value": [10.5, 20.1, 30.9]
})

# Create a writer
writer = RivuletWriter(
    base_path="/path/to/dataset",
    schema={
        "fields": [
            {"name": "id", "type": "long", "nullable": False},
            {"name": "name", "type": "string", "nullable": True},
            {"name": "value", "type": "double", "nullable": True}
        ],
        "merge_key": "id"
    }
)

# Create write options
write_options = RivuletWriteOptions(
    write_mode="upsert",
    file_format="parquet"
)

# Write to DeltaCAT storage
result = df.write_deltacat(writer, write_options)
```

## How It Works

The integration uses Daft's UDF mechanism to execute custom operations that:

1. Convert Daft DataFrames to PyArrow record batches
2. Pass the record batches to the Writer's `write_batches` method
3. Call `finalize_local` and `commit` on the Writer
4. Return the commit results as a DataFrame

This approach works with any writer that implements the DeltaCAT Writer interface, including:

- `RivuletWriter` for DeltaCAT's Rivulet format
- `IcebergWriter` for Apache Iceberg

## Using with Ray

The integration works with both local and Ray execution modes:

```python
import daft
from deltacat.dev.data_access_layer.daft_integration import patch_daft

# Initialize Ray
import ray

ray.init()

# Set Daft to use Ray
daft.context.set_runner_ray()

# Apply the monkey patch
patch_daft()

# Create a DataFrame and write to DeltaCAT as before
# ...
```

## Examples

See the example script at `deltacat/examples/daft_integration_example.py` for complete examples.