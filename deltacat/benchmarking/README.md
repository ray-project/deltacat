# Benchmarking

## Setup

1. Install DeltaCat's `benchmark-requirements.txt`
2. Ensure the appropriate versions of additional packages that are being benchmarked are installed (e.g. `pip install getdaft==X.X.XX`)

## Adding Benchmarks

### Parquet Reads

Modify the `SINGLE_COLUMN_BENCHMARKS` and `ALL_COLUMN_BENCHMARKS` fixtures in `deltacat/benchmarking/benchmark_parquet_reads.py`
to add more files and benchmark test cases.

## Running Benchmarks

### Parquet Reads

We recommend running these benchmarks in an AWS environment configured for high bandwidth access to AWS S3. For example, on an EC2 instance with enhanced networking support: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/enhanced-networking.html.

```bash
pytest deltacat/benchmarking/benchmark_parquet_reads.py --benchmark-only --benchmark-group-by=group,param:name
```

Grab a coffee, it will be a few minutes!
