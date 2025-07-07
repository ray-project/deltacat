# DeltaCAT Beam Managed I/O Integration

## Overview

This module provides upsert support for Apache Beam pipelines writing to Apache Iceberg tables using a Beam Managed I/O wrapper that automatically converts new appends to upserts. Use this to:

1. **Upsert Data** by defining merge keys for data appended to a table by Beam managed Iceberg I/O.
2. **Monitor Tables** for updates using a DeltaCAT job on Ray with PyIceberg.
3. **Trigger Data Conversion Jobs** using a DeltaCAT job on Ray to automatically merge new updates by key. Under the hood, it uses either Iceberg positional deletes (Iceberg Spec V2) or delete vectors (Iceberg Spec V3).

## Limitations
- **Eventually Consistent**: Upserts are not applied inline when Beam appends data to an Iceberg table. Appended records are merged by key in a subsequent table update.
- **Merge on Read**: Uses Iceberg merge-on-read tables to **merge data logically**, rather than physically by overwriting the original data files, using Iceberg **positional delete files**.
- **Beam Reads**: Beam managed Iceberg I/O cannot currently read Iceberg tables that contain positional deletes (i.e., had their appends converted to upserts by DeltaCAT). Until this issue is resolved, workarounds include reading the table directly via Spark SQL, Flink SQL, or via PyIceberg-compatible clients (e.g. Daft).

## Usage

### Quickstart

```python
import apache_beam as beam
import pyarrow.fs as pafs
from deltacat.experimental.converter_agent.beam.managed import write

# Monkey-patch Beam's managed I/O
beam.managed.Write = write

# Configure your pipeline
with beam.Pipeline() as p:
    data = p | "Create data" >> beam.Create([...])
    
    data | "Write to Iceberg" >> beam.managed.Write(
        beam.managed.ICEBERG,
        config={
            "table": "default.my_table",
            "write_mode": "append",
            "catalog_properties": {
                "warehouse": "/path/to/warehouse",
                "catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
                "uri": "http://localhost:8181",
            },
            "deltacat_converter_properties": {
                "deltacat_converter_interval": 3.0,  # Monitor every 3 seconds
                "merge_keys": ["id"],  # Merge keys for duplicate detection
                "ray_inactivity_timeout": 300,  # Shutdown Ray after 5 minutes of inactivity
                "filesystem": pafs.LocalFileSystem(),  # Optional filesystem
                "cluster_cfg_file_path": "./deltacat.yaml",  # Optional: Path to Ray cluster config for remote deployments
                "max_converter_parallelism": 128,  # Optional: Max concurrent converter tasks
            }
        }
    )
```

### Configuration Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| `deltacat_converter_properties.deltacat_converter_interval` | Monitoring interval (seconds) | 3.0 |
| `deltacat_converter_properties.merge_keys` | List of column names for duplicate detection | `["id"]` |
| `deltacat_converter_properties.ray_inactivity_timeout` | Ray cluster shutdown timeout after inactivity (seconds) | 10 |
| `deltacat_converter_properties.filesystem` | PyArrow filesystem instance | Auto-resolved |
| `deltacat_converter_properties.cluster_cfg_file_path` | Path to Ray cluster configuration file for remote deployments | `None` (local) |
| `deltacat_converter_properties.max_converter_parallelism` | Maximum number of concurrent converter tasks | `DEFAULT_CONVERTER_TASK_MAX_PARALLELISM` |
| `catalog_properties.catalog-impl` | Iceberg catalog implementation | Required |
| `catalog_properties.warehouse` | Warehouse path | Required |
| `catalog_properties.uri` | Catalog URI (for REST/Hive catalogs) | Required |

## Supported Catalog Types

| Beam Catalog Implementation | PyIceberg Type | Status |
|----------------------------|----------------|--------|
| `org.apache.iceberg.rest.RESTCatalog` | `rest` | ✅ Supported |
| `org.apache.iceberg.hive.HiveCatalog` | `hive` | ✅ Supported |
| `org.apache.iceberg.aws.glue.GlueCatalog` | `glue` | ✅ Supported |
| `org.apache.iceberg.jdbc.JdbcCatalog` | `sql` | ✅ Supported |
| `org.apache.iceberg.hadoop.HadoopCatalog` | N/A | ❌ Not supported |
| `org.apache.iceberg.nessie.NessieCatalog` | N/A | ❌ Not supported |

## References

- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Apache Beam Iceberg I/O](https://beam.apache.org/documentation/io/built-in/iceberg/)
- [DeltaCAT Converter](../../compute/converter/)
- [Ray Documentation](https://docs.ray.io/)