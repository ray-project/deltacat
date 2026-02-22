# Initial Benchmark Results - Converter

## Overview

This document captures the initial benchmark results for the largest table that can be sustainably processed by the converter.

## High-Level Statistics

### Data Volume and Memory Usage

- Input file size (on disk): 15.85 TB (all columns)
- Primary key data loaded into memory: 10.02 TB
  - 3 SHA1 hashed primary key columns
  - 20 bytes per record per column
  - 167.16 billion records total
- Additional columns appended into memory: File path, record index, and global record index (across files within same bucket)
- Total in-memory usage reported: ~48.76 TB

### Infrastructure

- Cluster Configuration:
  - Number of nodes: 114
  - Instance type: 8xlarge EC2
  - CPU per node: 32
  - Memory per node: 256 GiB

- Resource Allocation:
  - Ray task CPUs scheduled: 31 per node (out of 32)
  - Heap memory allocation: Up to 225 GB per node
  - Ray object store memory: 5 GB per node
  - Note: All compute jobs run exhausting Python heap memory during execution. Memory is fully utilized with on-average peak memory reported 390% of estimated reserved Ray memory resources.

### Summary Table

| Metric | Value |
|--------|-------|
| Data Volume & Memory | |
| Input file size (on disk) | 15.85 TB |
| Primary key columns | 3 |
| Total records | 167.16 billion |
| Total data in memory | 48.76 TB |
| Peak memory usage on-average| 390% |
| Infrastructure | |
| EC2 nodes | 114 |
| Instance type | 8xlarge |
| CPUs per node | 32 |
| Memory per node | 256 GiB |
| Resource Allocation | |
| Scheduled CPUs per node | 31 |
| Heap memory per node | Up to 225 GB |
| Ray object store per node | 5 GB |

### Key Observations

- Only SHA1 hashed primary key columns are loaded into memory, significantly reducing memory footprint compared to loading all columns
- The converter appends file path, record index, and global record index columns to the primary hash columns
- Resource allocation reserves 1 CPU and ~31 GB memory per node for system operations
- Memory-intensive operations exhaust Python heap memory during job runs, with minimal reliance on Ray's native object store

## Observed Scalability Limitations and Best Practices

Testing Environment: Most heavily tested with Ray version 2.30 and AWS Rtype.8xlarge instances.

### Best Practices

1. Head Node Configuration
   - Do not schedule tasks on the head node
   - Reserve head node resources for cluster management and coordination

2. Cluster Size Limitations
   - Performs best with up to <1,000 8xlarge worker nodes
   - Beyond this threshold:
     - Significant latency increase in `ray.get()` remote task operations
     - Cluster becomes unstable when communicating with >1,000 worker nodes
     - Connection overhead impacts overall performance

3. Parallel Task Scheduling
   - Recommended: Schedule <4,096 remote tasks in parallel
   - This limit depends on workload intensity
   - Exceeding this threshold results in:
     - Performance degradation
     - Significant latency increase in `ray.get()` operations

4. PyArrow Compute Task Sizing
   - For complicated combined PyArrow compute functions:
     - Recommended: Process <100M records per remote task
     - Larger batches may lead to memory pressure and performance issues

## Performance Benchmarks

Average records processed per remote task: 46.44 million

### File I/O Performance

| Operation | Details | Duration | Records |
|-----------|---------|----------|---------|
| Download | 3 primary key hash columns using Daft + S3 | 2:43 (163s) | 86 files |
| Upload | Write results to S3 | 0.62s | 293,212 records |

### PyArrow Compute Functions

| Function | Latency | Memory Copy | Observed Memory Usage |
|----------|---------|-------------|----------------------|
| Concat table | 0.0006s | Zero copy | 0 |
| Append global index | 5.68s | Zero copy | Adds new column appended memory |
| Group by + aggregate (dedupe) | 30.47s | Zero copy | Adds ~50% of origin table memory usage |
| Filter | 0.65s | Zero copy | 0 |
| Sorting | 0.5s | Zero copy | 0 |
