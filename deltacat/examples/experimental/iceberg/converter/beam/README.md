# DeltaCAT Apache Beam Iceberg Upsert Example

This example demonstrates how to use DeltaCAT's experimental [Apache Beam managed I/O](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.managed.html#module-apache_beam.transforms.managed) wrapper with an [Apache Iceberg REST Catalog](https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml) and [Apache Spark](https://spark.apache.org/) to:

1. **Upsert Data** by defining merge keys for data appended to a table by Beam managed Iceberg I/O
2. **Monitor Tables** for updates using a DeltaCAT job on Ray with PyIceberg
3. **Trigger Data Conversion Jobs** using a DeltaCAT job on Ray to automatically merge table data by key
4. **Rewrite Data Files** with Spark to materialize positional deletes written by DeltaCAT
5. **Conserve Compute Resources** by shutting down the DeltaCAT job automatically after an idle timeout with no table updates

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
                       │  (Job-Based)    │
                       └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │  Ray Converter  │
                       │  (Merge by Key) │
                       └─────────────────┘
```

## Requirements
- **PySpark 3.3.0**: Install PySpark (`pip install "pyspark>=3.3.0,<4.0.0"`) to read back modified Iceberg tables
- **Iceberg 1.4.0**: Iceberg Java >=1.4.0 for Beam managed Iceberg I/O
- **Iceberg Spec V2+**: Iceberg table format version >=2 to write positional delete files

## Limitations
- **Eventually Consistent**: Upserts are not applied inline when Beam appends data to an Iceberg table. Appended records are merged by key in a subsequent table update.
- **Merge on Read**: Uses Iceberg merge-on-read tables to **merge data logically**, rather than physically by overwriting the original data files, using Iceberg **positional delete files**.
- **Beam Reads**: Beam managed Iceberg I/O cannot read Iceberg tables that either contain positional deletes or had their data files rewritten by Spark. This example includes workaround that automatically handles positional deletes by embedding **Spark SQL** in a Beam pipeline.
- **Beam Writes**: Beam managed Iceberg I/O cannot write to Iceberg tables that contain positional deletes or had their data files rewritten by Spark. The same Spark SQL workaround demonstrated for Beam Reads can also be used to write additional data.

## Quick Start

### 1. Setup Virtual Environment
Run the following commands from the DeltaCAT project root directory
```bash
# Create virtual environment and install dependencies
make install

# Activate virtual environment
source venv/bin/activate

# Navigate to example directory
cd deltacat/examples/experimental/iceberg/converter/beam
```

### 2. Start REST Catalog Server
```bash
docker run -d -p 8181:8181 --name iceberg-rest-catalog -e CATALOG_WAREHOUSE=/tmp/iceberg_rest_warehouse tabulario/iceberg-rest:1.6.0
```

### 3. Install PySpark (Required for Read Mode)
```bash
pip install "pyspark>=3.3.0,<4.0.0"
```

**Note**: PySpark is only needed for read functionality due to Beam's limitations with positional deletes. Write operations work without PySpark.

### 4. Write Data (Automatically Triggers Upsert Merge)
```bash
# Run a basic write to "deltacat.hello_world"
python main.py --mode write --table-name "deltacat.hello_world"

# Or optionally specify additional arguments
python main.py --mode write --table-name "deltacat.hello_world" \
  --deltacat-converter-interval 5.0 \
  --ray-inactivity-timeout 30 \
  --max-converter-parallelism 2
```
Unless a table name is specified, writes will generate a unique table name printed to
stdout at the beginning and end of the write operation.

Records with duplicate "ID" values should be automatically merged by DeltaCAT within a few seconds of Beam writing them.

### 5. Monitor Upsert Conversion Job Logs
By default, DeltaCAT system logs are written to:
```
/tmp/deltacat/var/output/logs/
```
This can be changed by setting the `DELTACAT_SYS_LOG_DIR` environment variable.

Tail DEBUG-level log events to see detailed conversion job information:
```bash
tail -f /tmp/deltacat/var/output/logs/deltacat-python.debug.log
```

### 5. Read Data
```bash
# Read merged data using Spark SQL with Beam (applies positional deletes on read)
python main.py --mode read --table-name "deltacat.hello_world"
```

```python
# You can also use PyIceberg directly to read small tables with positional deletes into memory:
from pyiceberg.catalog import load_catalog
catalog = load_catalog(
    'test',
    type='rest',
    warehouse='/tmp/iceberg_rest_warehouse',
    uri='http://localhost:8181',
)
print(catalog.load_table('deltacat.hello_world').scan().to_pandas())
```

### 6. Full E2E Workflow Test
```bash
# Run e2e write → read workflow with verification
python test_workflow.py
```

### 7. Review Logs
By default, DeltaCAT system logs are written to:
```
/tmp/deltacat/var/output/logs/
```
This can be changed by setting the `DELTACAT_SYS_LOG_DIR` environment variable.

### 8. Cleanup
```bash
# Stop and remove REST catalog docker container
docker stop iceberg-rest-catalog && docker rm iceberg-rest-catalog
```

## Configuration Options

### Command Line Options
```bash
python main.py [OPTIONS]

Options:
  --mode {write,read,rewrite}             Operation mode (default: write)
  --rest-uri TEXT                         REST catalog URI (default: http://localhost:8181)
  --warehouse-path TEXT                   Custom warehouse path (optional)
  --table-name TEXT                       Table name including namespace (default: autogenerated table name)
  --deltacat-converter-interval FLOAT     Table monitor polling interval in seconds (default: 5.0)
  --ray-inactivity-timeout INT            Ray idle shutdown timeout in seconds (default: 20)
  --max-converter-parallelism INT         Maximum number of concurrent converter tasks (default: 1)
```

## Data Flow

1. **Write Phase**:
   - Creates initial data (IDs 1-4)
   - Appends additional data (IDs 5-9)
   - Writes updates (ID 2: Bob→Robert, ID 3: Charlie→Charles)

2. **Monitor Phase**:
   - Detects new Iceberg table snapshots
   - Launches DeltaCAT data upsert sessions on Ray to merge data by key

3. **Upsert Phase**:
   - Initializes Ray
   - Runs DeltaCAT data upsert session on Ray
   - Shuts down Ray cluster after a configurable period of inactivity

## Troubleshooting

**REST Catalog Connection**:
```bash
# Check if REST catalog is running
curl http://localhost:8181/v1/config

# Restart if needed
docker restart iceberg-rest-catalog
```

## Related Documentation

- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
- [Apache Iceberg REST Catalog](https://iceberg.apache.org/docs/latest/rest/)
- [DeltaCAT Converter](../../../../compute/converter/)
- [Apache Beam Iceberg I/O](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/iceberg/package-summary.html)
