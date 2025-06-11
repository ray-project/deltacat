# DeltaCAT Compactor Examples

This directory contains example scripts for using DeltaCAT's V1 and V2 compactors. The examples demonstrate how to compact DeltaCAT partitions using either compactor version, with support for both local execution and distributed execution on Ray clusters in AWS or GCP.

## Files

- `compactor.py` - Main compactor script that can run standalone or as part of a Ray job
- `job_runner.py` - Job orchestration script for running multiple compactor jobs on Ray clusters
- `bootstrap.py` - Creates test data for compaction testing
- `catalog_inspector.py` - Inspects catalog metadata to find stream IDs and other compaction parameters
- `aws/deltacat.yaml` - Ray cluster configuration for AWS
- `gcp/deltacat.yaml` - Ray cluster configuration for GCP

## Quick Start

### Bootstrapping Test Data

Before running compaction, you can create test data using the bootstrap script:

```bash
# Create test data in a temporary directory
python bootstrap.py --catalog-root /tmp/deltacat_test

# This creates:
# - Namespace: compactor_test
# - Table: events
# - 2 batches of data with overlapping IDs (good for compaction testing)
# - All necessary deltacat metadata (table version, stream, partition, deltas)
```

### Inspecting Catalog Metadata

Use the catalog inspector to find the correct parameters for compaction:

```bash
# Inspect the entire catalog
python catalog_inspector.py --catalog-root /tmp/deltacat_test

# Inspect specific namespace and table
python catalog_inspector.py \
  --catalog-root /tmp/deltacat_test \
  --namespace compactor_test \
  --table events
```

The inspector will show:
- Stream IDs (needed for `--stream-id`)
- Partition information
- Maximum stream positions (needed for `--last-stream-position`)
- Table schema and metadata

### Local Execution

Run the compactor locally with Ray:

```bash
# V2 Compactor (recommended)
python compactor.py \
  --namespace 'events' \
  --table-name 'user_events' \
  --table-version '1' \
  --stream-id 'events_stream' \
  --partition-values '' \
  --dest-namespace 'events' \
  --dest-table-name 'compacted_events' \
  --dest-table-version '1' \
  --dest-stream-id 'compacted_stream' \
  --dest-partition-values '' \
  --last-stream-position 10000 \
  --primary-keys 'user_id,event_id' \
  --compactor-version 'V2' \
  --hash-bucket-count 4

# V1 Compactor with sort keys
python compactor.py \
  --namespace 'events' \
  --table-name 'user_events' \
  --table-version '1' \
  --stream-id 'events_stream' \
  --partition-values 'region=us-west-2' \
  --dest-namespace 'events' \
  --dest-table-name 'compacted_events' \
  --dest-table-version '1' \
  --dest-stream-id 'compacted_stream' \
  --dest-partition-values 'region=us-west-2' \
  --last-stream-position 5000 \
  --primary-keys 'user_id,event_id' \
  --sort-keys 'timestamp,event_type' \
  --compactor-version 'V1' \
  --records-per-file 500000 \
  --table-writer-compression 'snappy'
```

### Distributed Execution

Run multiple compactor jobs on a Ray cluster:

```bash
# Submit multiple jobs asynchronously to AWS cluster
python job_runner.py \
  --namespace 'events' \
  --table-name 'user_events' \
  --table-version '1' \
  --stream-id 'events_stream' \
  --partition-values 'region=us-west-2' \
  --dest-namespace 'events' \
  --dest-table-name 'compacted_events' \
  --dest-table-version '1' \
  --dest-stream-id 'compacted_stream' \
  --dest-partition-values 'region=us-west-2' \
  --last-stream-position 10000 \
  --primary-keys 'user_id,event_id' \
  --compactor-version 'V2' \
  --hash-bucket-count 4 \
  --asynchronous \
  --jobs-to-submit 5 \
  --job-timeout 1800 \
  --cloud-provider aws
```

## Parameters

### Required Parameters

- `--namespace`: Source table namespace
- `--table-name`: Source table name
- `--table-version`: Source table version
- `--stream-id`: Source stream ID
- `--dest-namespace`: Destination table namespace
- `--dest-table-name`: Destination table name
- `--dest-table-version`: Destination table version
- `--dest-stream-id`: Destination stream ID
- `--last-stream-position`: Last stream position to compact
- `--primary-keys`: Comma-separated primary keys (e.g., `user_id,event_id`)

### Optional Parameters

- `--partition-values`: Comma-separated partition values (default: empty)
- `--dest-partition-values`: Destination partition values (default: empty)
- `--catalog-root`: Root path for catalog (default: temp directory)
- `--compactor-version`: Compactor version `V1` or `V2` (default: `V2`)
- `--sort-keys`: Comma-separated sort keys (optional)
- `--hash-bucket-count`: Number of hash buckets (required for V2, ignored for V1)
- `--records-per-file`: Records per compacted file (default: 1000000)
- `--table-writer-compression`: Compression type: `lz4`, `snappy`, `gzip`, `brotli`, `zstd` (default: `lz4`)

### Job Runner Parameters

- `--asynchronous`: Run jobs asynchronously (default: synchronous)
- `--jobs-to-submit`: Number of jobs to submit (default: 1)
- `--job-timeout`: Job timeout in seconds (default: 1800)
- `--cloud-provider`: Cloud provider `aws` or `gcp` (default: `aws`)
- `--restart-ray`: Restart Ray on existing cluster

## Compactor Versions

### V1 Compactor
- **Features**: Supports sort keys, incremental compaction, rebase compaction
- **Use Cases**: When you need sort keys or working with legacy data
- **Parameters**: Does not require `hash-bucket-count`

### V2 Compactor (Recommended)
- **Features**: Better performance, drop duplicates, intelligent resource estimation
- **Use Cases**: New workloads, performance-critical applications
- **Parameters**: Requires `hash-bucket-count` parameter

## Ray Cluster Configuration

### AWS Configuration (`aws/deltacat.yaml`)
- **Head Node**: `r8g.4xlarge` (16 vCPUs, 128 GB RAM)
- **Worker Nodes**: `r8g.xlarge` (4 vCPUs, 32 GB RAM)
- **Max Workers**: 200
- **Object Store**: 20% of memory allocated for compaction workloads
- **Shared Memory**: 10% for intermediate data processing

### GCP Configuration (`gcp/deltacat.yaml`)
- **Head Node**: `e2-highmem-16` (16 vCPUs, 128 GB RAM)
- **Worker Nodes**: `e2-highmem-4` (4 vCPUs, 32 GB RAM)
- **Max Workers**: 200
- **Object Store**: 20% of memory allocated for compaction workloads
- **Shared Memory**: 10% for intermediate data processing

## Performance Tuning

### For Large Datasets
- Increase `--hash-bucket-count` for V2 compactor (try 8, 16, or 32)
- Reduce `--records-per-file` to create smaller, more manageable files
- Use `zstd` or `snappy` compression for better compression ratios
- Increase `--job-timeout` for long-running compactions

### For High Throughput
- Use `lz4` compression for faster processing
- Increase `--records-per-file` to reduce number of output files
- Use `--asynchronous` mode with multiple jobs
- Scale up Ray cluster with more worker nodes

### Memory Optimization
- For memory-constrained environments, reduce `--hash-bucket-count`
- Use V1 compactor if V2 uses too much memory
- Adjust Ray cluster configuration to allocate more memory per node

## Troubleshooting

### Common Issues

1. **V2 requires hash-bucket-count**: Add `--hash-bucket-count 2` (or higher)
2. **Out of memory errors**: Reduce `--hash-bucket-count` or increase cluster resources
3. **Job timeouts**: Increase `--job-timeout` or reduce data size per job
4. **Partition not found**: Verify namespace, table, and partition values are correct

### Debugging

1. **Enable verbose logging**:
   ```bash
   export DELTACAT_LOG_LEVEL=DEBUG
   python compactor.py [args...]
   ```

2. **Test with small data first**:
   - Use low `--last-stream-position`
   - Start with `--hash-bucket-count 2`
   - Use `--records-per-file 1000`

3. **Check Ray cluster status**:
   ```bash
   ray status  # when connected to cluster
   ```

## Examples for Different Use Cases

### Incremental Compaction
```bash
python compactor.py \
  --namespace 'logs' \
  --table-name 'application_logs' \
  --table-version '1' \
  --stream-id 'main' \
  --last-stream-position 50000 \
  --primary-keys 'log_id' \
  --compactor-version 'V2' \
  --hash-bucket-count 8
```

### Partitioned Data Compaction
```bash
python compactor.py \
  --namespace 'sales' \
  --table-name 'transactions' \
  --table-version '1' \
  --stream-id 'daily' \
  --partition-values 'year=2024,month=01,day=15' \
  --dest-partition-values 'year=2024,month=01,day=15' \
  --last-stream-position 100000 \
  --primary-keys 'transaction_id' \
  --sort-keys 'timestamp,customer_id' \
  --compactor-version 'V1'
```

### Bulk Processing with Job Runner
```bash
python job_runner.py \
  --namespace 'analytics' \
  --table-name 'events' \
  --table-version '1' \
  --stream-id 'hourly' \
  --last-stream-position 1000000 \
  --primary-keys 'event_id,user_id' \
  --compactor-version 'V2' \
  --hash-bucket-count 16 \
  --asynchronous \
  --jobs-to-submit 10 \
  --cloud-provider aws
```

## Contributing

When modifying these examples:

1. **Test both V1 and V2 compactors** with your changes
2. **Update documentation** if you add new parameters
3. **Verify Ray cluster configs** work in both AWS and GCP
4. **Test with different data sizes** to ensure scalability

For questions or issues, please refer to the main DeltaCAT documentation or file an issue in the repository.
