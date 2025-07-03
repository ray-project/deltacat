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
- **Beam Read Limitation**: Apache Beam cannot read back Iceberg tables that contain positional delete files created by DeltaCAT. This limitation exists even when using Spark as the runner, as it's a problem with Beam's native Iceberg I/O connector. Use PyIceberg, Spark directly, or Spark SQL within Beam transforms to read tables with positional deletes.

## Testing Spark Compatibility

### Spark Test Setup

This directory includes a comprehensive test to verify that Apache Spark can properly read and apply positional deletes created by DeltaCAT.

#### Prerequisites

1. **Install Spark dependencies**:
   ```bash
   pip install -r requirements-spark.txt
   ```

2. **Set up Docker services** (MinIO + Iceberg REST catalog):
   ```bash
   chmod +x docker-setup.sh
   ./docker-setup.sh
   ```

   This will start:
   - MinIO S3-compatible storage on port 9000
   - MinIO Console on port 9001  
   - Iceberg REST catalog on port 8181

#### Running the Spark Test

1. **First, create test data using the Beam converter**:
   ```bash
   python main.py --table-name default.demo_table --deltacat-converter-interval 3.0 --ray-inactivity-timeout 15
   ```

2. **Then run the Spark test**:
   ```bash
   python test_spark_read_positional_deletes.py --table-name default.demo_table
   ```

#### Full Workflow Test

For automated end-to-end testing, use the comprehensive workflow test:

```bash
# Run complete workflow (Docker setup + Beam converter + Spark test + cleanup)
python test_full_workflow.py

# Run with custom table name
python test_full_workflow.py --table-name default.my_table

# Run only Beam converter
python test_full_workflow.py --beam-only

# Run only Spark test (assumes services are running)
python test_full_workflow.py --spark-only --skip-docker-setup

# Test Beam with Spark runner (expected to fail - demonstrates limitation)
python test_full_workflow.py --test-beam-spark-runner --skip-docker-setup

# Test Beam with Spark SQL (workaround that should succeed)
python test_full_workflow.py --test-beam-spark-sql --skip-docker-setup

# Run all tests (comprehensive validation)
python test_full_workflow.py --test-all

# Skip cleanup (keep services running)
python test_full_workflow.py --skip-cleanup
```

#### Test Features

The Spark test performs comprehensive validation:

- **Catalog Connectivity**: Verifies connection to the Iceberg REST catalog
- **Table Structure Analysis**: Examines table metadata, snapshots, and delete files
- **Positional Delete Verification**: Confirms Spark correctly applies positional deletes
- **Duplicate Detection**: Ensures no duplicates remain after applying deletes
- **Query Performance**: Tests various query patterns with positional deletes

#### Expected Output

```
🚀 Starting Spark positional deletes test
✅ Spark session created successfully
✅ Found namespaces: ['default']
✅ Found tables in default namespace: ['default.demo_table']
🔍 Analyzing table structure: default.demo_table
Total snapshots: 3
Total data files: 2
Total delete files: 1
🧪 Testing positional deletes for table: default.demo_table
Total records after positional deletes: 9
Duplicate records found: 0
✅ Positional deletes test PASSED
✅ All 9 records are unique (no duplicates)
🎉 Spark can successfully read positional deletes created by DeltaCAT!
```

#### Cleanup

To stop and clean up the Docker services:
```bash
# Stop services
docker-compose down

# Remove all data (complete cleanup)
docker-compose down -v
```

### Service URLs

When running the Docker setup:
- **MinIO Console**: http://localhost:9001 (admin/password)
- **MinIO S3 API**: http://localhost:9000
- **Iceberg REST Catalog**: http://localhost:8181

## Testing Beam with Spark Runner

### Beam Runner vs I/O Connector Investigation

To investigate whether Beam's positional delete limitation is due to the execution runner or the I/O connector, we created tests that use Spark as Beam's execution engine.

#### Test 1: Beam with Spark Runner

**Purpose**: Test if Beam can read positional deletes when using Spark as the execution runner.

```bash
python test_beam_with_spark_runner.py --table-name default.demo_table
```

**Expected Result**: ❌ **FAILS** - Even with Spark runner, Beam still uses its native Iceberg I/O connector which has the GenericDeleteFilter issue.

**Key Finding**: The limitation is in Beam's Iceberg I/O implementation, not the execution runner.

#### Test 2: Beam with Spark SQL

**Purpose**: Test if we can bypass Beam's Iceberg I/O by using Spark SQL directly within Beam transforms.

```bash
python test_beam_with_spark_sql.py --table-name default.demo_table
```

**Expected Result**: ✅ **SUCCEEDS** - Using Spark SQL within Beam transforms bypasses Beam's Iceberg I/O and uses Spark's implementation.

**Key Finding**: This provides a workaround for reading positional deletes within Beam pipelines.

#### Workaround Pattern

For Beam pipelines that need to read tables with positional deletes:

```python
import apache_beam as beam
from pyspark.sql import SparkSession

class SparkSQLIcebergRead(beam.DoFn):
    def setup(self):
        # Create Spark session with Iceberg configuration
        self.spark = SparkSession.builder \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.rest.type", "rest") \
            .config("spark.sql.catalog.rest.uri", "http://localhost:8181") \
            .getOrCreate()
    
    def process(self, element):
        # Use Spark SQL to read Iceberg table (handles positional deletes correctly)
        df = self.spark.sql("SELECT * FROM default.demo_table")
        for row in df.collect():
            yield row.asDict()

# Use in pipeline
with beam.Pipeline() as p:
    trigger = p | "Create trigger" >> beam.Create([None])
    data = trigger | "Read with Spark SQL" >> beam.ParDo(SparkSQLIcebergRead())
```

### Comprehensive Testing

Run all tests to validate different approaches:

```bash
# Test all approaches: Spark, Beam+Spark runner, Beam+Spark SQL
python test_full_workflow.py --test-all
```

## Quick Start

### 1. Start REST Catalog Server
```bash
docker run -d -p 8181:8181 --name iceberg-rest-catalog tabulario/iceberg-rest:1.6.0
```

### Alternative: Use Full Docker Setup
```bash
# For comprehensive testing with MinIO + REST catalog
chmod +x docker-setup.sh
./docker-setup.sh
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