#!/usr/bin/env python3
"""
DeltaCAT Compactor Bootstrap Script

This script creates test data suitable for compaction testing by:
1. Creating a namespace and table with schema
2. Writing 2 test parquet files as separate deltas
3. Staging and committing all necessary deltacat metadata (table version, stream, partition, deltas)

Usage:
    # Use default catalog location
    python bootstrap.py
    
    # Use custom catalog location
    python bootstrap.py --catalog-root /path/to/catalog

The script creates:
- A test namespace "compactor_test"
- A test table "events" with columns: id, timestamp, user_id, event_type, data
- 2 parquet files with overlapping data (suitable for compaction)
- All necessary deltacat metadata structures

After running this script, you can test compaction using the compactor.py example.
"""

import argparse
import os

import pandas as pd
import pyarrow as pa

from deltacat.catalog import write_to_table, create_namespace, namespace_exists
from deltacat.catalog.model.properties import CatalogProperties
from deltacat.storage.model.types import LifecycleState
from deltacat.types.media import ContentType
from deltacat.types.tables import TableWriteMode


def create_test_data_batch_1() -> pd.DataFrame:
    """Create the first batch of test data with some overlapping IDs."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "timestamp": pd.to_datetime(
                [
                    "2024-01-01 10:00:00",
                    "2024-01-01 10:05:00",
                    "2024-01-01 10:10:00",
                    "2024-01-01 10:15:00",
                    "2024-01-01 10:20:00",
                ]
            ),
            "user_id": [101, 102, 103, 104, 105],
            "event_type": ["login", "view", "click", "purchase", "logout"],
            "data": [
                '{"page": "home"}',
                '{"product_id": 123}',
                '{"button": "add_to_cart"}',
                '{"amount": 99.99}',
                '{"session_duration": 1200}',
            ],
        }
    )


def create_test_data_batch_2() -> pd.DataFrame:
    """Create the second batch of test data with some overlapping IDs (good for compaction)."""
    return pd.DataFrame(
        {
            "id": [3, 4, 5, 6, 7, 8],  # IDs 3, 4, 5 overlap with batch 1
            "timestamp": pd.to_datetime(
                [
                    "2024-01-01 11:00:00",  # Later timestamp for ID 3 (should replace)
                    "2024-01-01 11:05:00",  # Later timestamp for ID 4 (should replace)
                    "2024-01-01 11:10:00",  # Later timestamp for ID 5 (should replace)
                    "2024-01-01 11:15:00",  # New ID 6
                    "2024-01-01 11:20:00",  # New ID 7
                    "2024-01-01 11:25:00",  # New ID 8
                ]
            ),
            "user_id": [103, 104, 105, 106, 107, 108],
            "event_type": ["view", "click", "purchase", "login", "view", "logout"],
            "data": [
                '{"page": "product", "updated": true}',  # Updated data for ID 3
                '{"button": "buy_now", "updated": true}',  # Updated data for ID 4
                '{"amount": 149.99, "updated": true}',  # Updated data for ID 5
                '{"page": "signup"}',  # New data for ID 6
                '{"product_id": 456}',  # New data for ID 7
                '{"session_duration": 800}',  # New data for ID 8
            ],
        }
    )


def setup_test_namespace_and_table(catalog_root: str) -> None:
    """Set up the test namespace and table with proper schema."""
    print("Setting up test namespace and table...")

    # Initialize deltacat with the catalog
    catalog = CatalogProperties(root=catalog_root)

    # Initialize deltacat and register the catalog
    from deltacat.catalog import Catalog, put_catalog

    catalog_obj = Catalog(config=catalog)
    put_catalog("default", catalog_obj)

    namespace = "compactor_test"
    table_name = "events"

    # Create namespace if it doesn't exist
    if not namespace_exists(namespace, catalog="default"):
        print(f"Creating namespace: {namespace}")
        create_namespace(
            namespace=namespace,
            catalog="default",
        )
    else:
        print(f"Namespace {namespace} already exists")

    # Define schema for the table
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("timestamp", pa.timestamp("ns")),
            ("user_id", pa.int64()),
            ("event_type", pa.string()),
            ("data", pa.string()),
        ]
    )

    print(f"Table schema: {schema}")

    # Create test data batches
    print("Creating test data batches...")
    batch_1 = create_test_data_batch_1()
    batch_2 = create_test_data_batch_2()

    print(f"Batch 1 shape: {batch_1.shape}")
    print(f"Batch 1 data:\n{batch_1}")
    print(f"\nBatch 2 shape: {batch_2.shape}")
    print(f"Batch 2 data:\n{batch_2}")

    # Write first batch (creates table)
    print(f"\nWriting batch 1 to table {namespace}.{table_name}...")
    write_to_table(
        data=batch_1,
        table=table_name,
        namespace=namespace,
        mode=TableWriteMode.AUTO,
        content_type=ContentType.PARQUET,
        catalog="default",
        # Table creation parameters
        lifecycle_state=LifecycleState.ACTIVE,
        description="Test events table for compaction testing",
    )

    # Write second batch (appends to existing table)
    print(f"Writing batch 2 to table {namespace}.{table_name}...")
    write_to_table(
        data=batch_2,
        table=table_name,
        namespace=namespace,
        mode=TableWriteMode.APPEND,
        content_type=ContentType.PARQUET,
        catalog="default",
    )

    print(f"\n✅ Successfully created test data in {namespace}.{table_name}")
    print(f"📁 Catalog root: {catalog_root}")
    print(f"🔧 Total records: {len(batch_1) + len(batch_2)}")
    print(
        f"🔄 Overlapping IDs: {set(batch_1['id']) & set(batch_2['id'])} (good for compaction)"
    )

    # Print compaction command example
    print(f"\n🚀 Next steps:")
    print(f"1. Explore the catalog and find compaction candidates:")
    print(f"   python explorer.py --show-compaction-candidates")
    print(f"")
    print(f"2. Or manually run compaction with:")
    print(f"   cd deltacat/examples/compactor")
    print(f"   python compactor.py \\")
    print(f"     --namespace '{namespace}' \\")
    print(f"     --table-name '{table_name}' \\")
    print(f"     --table-version '1' \\")
    print(f"     --stream-id 'REPLACE_WITH_ACTUAL_STREAM_ID' \\")
    print(f"     --partition-values '' \\")
    print(f"     --dest-namespace '{namespace}' \\")
    print(f"     --dest-table-name '{table_name}_compacted' \\")
    print(f"     --dest-table-version '1' \\")
    print(f"     --dest-stream-id 'compacted_stream' \\")
    print(f"     --dest-partition-values '' \\")
    print(f"     --last-stream-position 1000 \\")
    print(f"     --primary-keys 'id' \\")
    print(f"     --compactor-version 'V2' \\")
    print(f"     --hash-bucket-count 2 \\")
    print(f"     --catalog-root '{catalog_root}'")


def main():
    """Main entry point for the bootstrap script."""
    parser = argparse.ArgumentParser(
        description="Bootstrap test data for DeltaCAT compaction testing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create test data in default location 
  python bootstrap.py

  # Create test data in a specific directory
  python bootstrap.py --catalog-root /tmp/deltacat_test

  # Create test data in current directory
  python bootstrap.py --catalog-root ./test_catalog

  # Create test data in a temporary directory
  python bootstrap.py --catalog-root $(mktemp -d)
        """,
    )

    parser.add_argument(
        "--catalog-root",
        type=str,
        default="/tmp/deltacat_test",
        help="Root directory for the DeltaCAT catalog where test data will be created (default: /tmp/deltacat_test)",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Force recreation of test data even if catalog already exists",
    )

    args = parser.parse_args()

    # Validate and prepare catalog root
    catalog_root = os.path.abspath(args.catalog_root)

    if os.path.exists(catalog_root) and not args.force:
        response = input(
            f"Catalog directory {catalog_root} already exists. Continue? [y/N]: "
        )
        if response.lower() != "y":
            print("Aborted.")
            return

    # Create directory if it doesn't exist
    os.makedirs(catalog_root, exist_ok=True)

    print(f"🚀 Starting DeltaCAT compactor bootstrap")
    print(f"📁 Catalog root: {catalog_root}")

    try:
        setup_test_namespace_and_table(catalog_root)

        print(f"\n✅ Bootstrap completed successfully!")
        print(f"📊 Test data is ready for compaction testing")
        print(f"💡 Run 'python explorer.py' to explore the catalog and find compaction candidates")

    except Exception as e:
        print(f"\n❌ Bootstrap failed: {str(e)}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
