# DeltaCAT Beam Managed I/O Integration

## Overview

This module provides automatic duplicate resolution for Apache Beam pipelines writing to Apache Iceberg tables using DeltaCAT's Ray-based converter sessions. When duplicate records are detected across multiple write operations, the system automatically creates position delete files to maintain data consistency.

## Key Features

- **Automatic Duplicate Detection**: Monitors Iceberg table snapshots for potential duplicates
- **Ray-based Processing**: Uses Ray clusters for efficient conversion processing  
- **Position Delete Support**: Creates position delete files for merge-on-read functionality
- **REST Catalog Support**: Full compatibility with Iceberg REST catalogs
- **Format Version Validation**: Ensures tables use format version 2+ for optimal performance

## Prerequisites

### Iceberg Version Requirements

**Required**: Apache Iceberg 1.4.0+

For Docker deployments, use:
```bash
docker run -d -p 8181:8181 --name iceberg-rest-catalog tabulario/iceberg-rest:1.6.0
```

**Important**: Older Iceberg versions (pre-1.4.0) default to format version 1, which doesn't support position deletes. If you encounter a format version error, upgrade your Iceberg installation.

### Python Dependencies

- Apache Beam 2.65.0+
- PyIceberg 
- Ray
- DeltaCAT converter modules

## Usage

### Basic Integration

```python
import apache_beam as beam
import pyarrow.fs as pafs
from deltacat.experimental.beam.managed import read, write

# Monkey-patch Beam's managed I/O
beam.managed.Write = write
beam.managed.Read = read

# Configure your pipeline
with beam.Pipeline() as p:
    data = p | "Create data" >> beam.Create([...])
    
    data | "Write to Iceberg" >> beam.managed.Write(
        beam.managed.ICEBERG,
        config={
            "table": "default.my_table",
            "write_mode": "append",
            "deltacat_converter_properties": {
                "deltacat_converter_interval": 3.0,  # Monitor every 3 seconds
                "merge_keys": ["id"],  # Primary keys for duplicate detection
                "ray_inactivity_timeout": 300,  # Shutdown Ray after 5 minutes of inactivity
                "filesystem": pafs.LocalFileSystem(),  # Optional filesystem
            },
            "catalog_properties": {
                "warehouse": "/path/to/warehouse",
                "catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
                "uri": "http://localhost:8181",
            }
        }
    )
```

### Configuration Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| `deltacat_converter_properties.deltacat_converter_interval` | Monitoring interval (seconds) | 1.0 |
| `deltacat_converter_properties.merge_keys` | List of column names for duplicate detection | `["id"]` |
| `deltacat_converter_properties.ray_inactivity_timeout` | Ray cluster shutdown timeout after inactivity (seconds) | 300 |
| `deltacat_converter_properties.filesystem` | PyArrow filesystem instance | Auto-resolved |
| `catalog_properties.catalog-impl` | Iceberg catalog implementation | Required |
| `catalog_properties.warehouse` | Warehouse path | Required |
| `catalog_properties.uri` | Catalog URI (for REST/Hive catalogs) | Required |

**Note**: The old flat configuration structure (`deltacat_optimizer_interval`, `merge_keys`, `ray_cluster_shutdown_timeout`) is still supported for backward compatibility but deprecated.

## Supported Catalog Types

| Beam Catalog Implementation | PyIceberg Type | Status |
|----------------------------|----------------|--------|
| `org.apache.iceberg.rest.RESTCatalog` | `rest` | ✅ Supported |
| `org.apache.iceberg.hive.HiveCatalog` | `hive` | ✅ Supported |
| `org.apache.iceberg.aws.glue.GlueCatalog` | `glue` | ✅ Supported |
| `org.apache.iceberg.jdbc.JdbcCatalog` | `sql` | ✅ Supported |
| `org.apache.iceberg.hadoop.HadoopCatalog` | N/A | ❌ Not supported |
| `org.apache.iceberg.nessie.NessieCatalog` | N/A | ❌ Not supported |

## How It Works

1. **Table Monitoring**: Background threads monitor each table for new snapshots
2. **Duplicate Detection**: When multiple snapshots exist, potential duplicates are detected
3. **Ray Cluster Management**: Ray cluster is initialized on-demand and kept alive for efficiency
4. **Position Deletes**: Converter creates position delete files to resolve duplicates
5. **Automatic Cleanup**: Ray cluster shuts down after configured inactivity timeout

## Error Handling

### Format Version Validation

If you encounter this error:
```
❌ TABLE FORMAT VERSION 1 DETECTED

Table 'default.my_table' is using Iceberg format version 1, but DeltaCAT converter
requires format version 2 or higher for position delete support.
```

**Solution**: Upgrade to Iceberg 1.4.0+ which defaults to format version 2:

```bash
# Stop old container
docker stop iceberg-rest-catalog
docker rm iceberg-rest-catalog

# Start with Iceberg 1.6.0
docker run -d -p 8181:8181 --name iceberg-rest-catalog tabulario/iceberg-rest:1.6.0
```

### Common Issues

- **Connection Refused**: Ensure your Iceberg catalog server is running
- **Table Not Found**: Check table names and namespaces are correct
- **Ray Initialization**: Ensure sufficient memory and no port conflicts
- **Beam Read Limitation**: Apache Beam cannot read back Iceberg tables with positional delete files created by DeltaCAT. Use PyIceberg or other Iceberg readers (e.g., Spark) to read tables with positional deletes.

## Example Application

See the example application in this directory:

```bash
# Start Iceberg REST catalog
docker run -d -p 8181:8181 --name iceberg-rest-catalog tabulario/iceberg-rest:1.6.0

# Run the example
python main.py --mode write --input-text "Test Data"

```

```python
# Note: Beam reads may fail if positional deletes exist
# Use Spark or PyIceberg directly instead:
from pyiceberg.catalog import load_catalog
catalog = load_catalog(
    'test', 
    type='rest', 
    warehouse='/tmp/iceberg_rest_warehouse', 
    uri='http://localhost:8181',
)
print(catalog.load_table('default.demo_table').scan().to_pandas())
```

## Performance Considerations

- **Monitoring Interval**: Shorter intervals provide faster duplicate resolution but higher overhead
- **Ray Resources**: Local mode is suitable for MB-GB scale tests; [launch a Ray cluster](https://docs.ray.io/en/latest/cluster/getting-started.html) for large-scale tests 
- **Merge Keys**: Choose selective merge keys to minimize false duplicates
- **Table Size**: Larger tables may require longer conversion times

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Beam Pipeline │───▶│  Iceberg Table   │───▶│  Table Monitor  │
│   (Write Data)  │    │  (Multiple       │    │  (Background    │
│                 │    │   Snapshots)     │    │   Thread)       │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                        │
                       ┌─────────────────┐             ▼
                       │  Position       │    ┌─────────────────┐
                       │  Delete Files   │◀───│  Ray Converter  │
                       │  (Duplicates    │    │  Session        │
                       │   Resolved)     │    │  (Temporary)    │
                       └─────────────────┘    └─────────────────┘
```

## Contributing

When modifying this module:

1. Ensure compatibility with supported catalog types
2. Test with both Iceberg table format version 1 (error handling) and 2 (success path)
3. Verify Ray cluster lifecycle management
4. Update documentation for any configuration changes

## References

- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Apache Beam Iceberg I/O](https://beam.apache.org/documentation/io/built-in/iceberg/)
- [DeltaCAT Converter](../../compute/converter/)
- [Ray Documentation](https://docs.ray.io/) 