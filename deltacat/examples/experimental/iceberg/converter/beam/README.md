# DeltaCAT Apache Beam Iceberg Upsert Example

This example demonstrates how to use DeltaCAT's experimental Apache Beam managed I/O wrapper with an Iceberg REST Catalog to:

1. **Upsert Data** by defining merge keys for data appended by Apache Beam managed Iceberg I/O
2. **Monitor Tables** for updates using a local or remote DeltaCAT agent with PyIceberg
3. **Trigger Table Management Jobs** on Ray to automatically merge data by key
4. **Cleanup** by shutting down Ray automatically after a configurable interval with no updates

## Component Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Apache Beam   │───▶│  REST Catalog   │◀───│   PyIceberg     │
│  (Managed I/O)  │    │    (Shared)     │    │  (Python I/O)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │ Table Monitoring│
                       │   (Threading)   │
                       └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │  Ray Converter  │
                       │  (Merge by Key) │
                       │                 │
                       └─────────────────┘
```

## Limitations
- **Iceberg >=1.4.0**: Iceberg Java >=1.4.0
- **Iceberg Spec V2+**: Iceberg table format version >=2
- **Eventually Consistent**: Upserts are not applied inline when Beam appends data to an Iceberg table. Instead, appended records are merged by key in a subsequent table update.
- **Merge on Read**: This example uses Iceberg merge-on-read tables to **merge data logically**, rather than physically by overwriting the original data files, using Iceberg **positional delete files**.
- **Beam Read Limitation**: Apache Beam cannot read back Iceberg tables that contain positional delete files created by DeltaCAT. Use PyIceberg or other Iceberg readers (e.g., Spark) to read tables with positional deletes.

## Quick Start

### 1. Start REST Catalog Server
```bash
docker run -d -p 8181:8181 --name iceberg-rest-catalog tabulario/iceberg-rest:1.6.0
```

### 2. Write Data (Automatically Triggers Upsert Merge)
```bash
# Basic usage
python main.py --mode write --input-text "Your Name"

# With custom configuration
python main.py --mode write --input-text "Your Name" \
  --table-name "default.my_table" \
  --deltacat-converter-interval 5.0 \
  --ray-inactivity-timeout 30
```

### 3. Read Data
```bash
# Note: This will fail if positional deletes exist due to Beam limitation
python main.py --mode read
```

```python
# Use Spark or PyIceberg directly to read tables with positional deletes:
from pyiceberg.catalog import load_catalog
catalog = load_catalog(
    'test', 
    type='rest', 
    warehouse='/tmp/iceberg_rest_warehouse', 
    uri='http://localhost:8181',
)
print(catalog.load_table('default.demo_table').scan().to_pandas())
```

### 4. Full E2E Workflow Test
```bash
python test_workflow.py
```

## Configuration Options

### Command Line Options
```bash
python main.py [OPTIONS]

Options:
  --mode {write,read}                     Operation mode (default: write)
  --input-text TEXT                       Custom text for sample data
  --rest-uri TEXT                         REST catalog URI (default: http://localhost:8181)
  --warehouse-path TEXT                   Custom warehouse path (optional)
  --table-name TEXT                       Table name including namespace (default: default.demo_table)
  --deltacat-converter-interval FLOAT     Monitoring interval in seconds (default: 3.0)
  --ray-inactivity-timeout FLOAT          Ray cluster idle shutdown timeout in seconds (default: 10.0)
```

### DeltaCAT Configuration
- **Monitoring Interval**: 3 seconds (configurable via `deltacat_converter_interval`)
- **Merge Keys**: `["id"]` (configurable via `merge_keys`)
- **Ray Timeout**: 10 seconds (configurable via `ray_inactivity_timeout`)
- **Ray Mode**: Local mode with reinitialization error handling and automatic cleanup

## Data Flow

1. **Write Phase**: 
   - Creates initial data (IDs 1-4)
   - Appends additional data (IDs 5-8)
   - Writes updates (ID 2: Bob→Robert, ID 3: Charlie→Charles, ID 9: new)

2. **Monitor Phase**:
   - Detects new Iceberg table snapshots
   - Launches DeltaCAT data upsert sessions on Ray to merge data by key

3. **Upsert Phase**:
   - Initializes Ray
   - Runs DeltaCAT data upsert session on Ray
   - Shuts down Ray cluster after a configurable period of inactivity

## Performance & Scalability
- **Monitoring**: Lightweight threading with configurable intervals
- **Ray Integration**: Local mode for development, production-ready for distributed
- **Resource Management**: Automatic cleanup prevents resource leaks
- **Error Recovery**: Graceful handling of Ray initialization failures

## Troubleshooting

**REST Catalog Connection**:
```bash
# Check if REST catalog is running
curl http://localhost:8181/v1/config

# Restart if needed
docker restart iceberg-rest-catalog
```

## Cleanup

```bash
# Stop and remove REST catalog docker container
docker stop iceberg-rest-catalog && docker rm iceberg-rest-catalog
```

## Related Documentation

- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
- [Apache Iceberg REST Catalog](https://iceberg.apache.org/docs/latest/rest/)
- [DeltaCAT Converter](../../../../compute/converter/)
- [Apache Beam Iceberg I/O](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/iceberg/package-summary.html) 