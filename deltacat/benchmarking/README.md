# Benchmarking

## Setup

1. Install DeltaCat's `requirements.txt` and `dev-requirements.txt`
2. Install the appropriate versions of additional packages that are being benchmarked (e.g. `pip install getdaft`)

## Running Benchmarks

### Parquet Reads

We recommend running these benchmarks in a production AWS environment with the appropriate setup for high bandwidth access to AWS S3.

```bash
pytest deltacat/benchmarking/benchmark_parquet_reads.py --benchmark-only --benchmark-group-by=group,param:name
```

Grab a coffee, it will be a few minutes!
