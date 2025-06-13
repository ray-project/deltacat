#!/usr/bin/env python3
"""
DeltaCAT Compactor Bootstrap Script

This script creates test data suitable for compaction testing by:
1. Creating source and destination namespaces and tables with schema
2. Writing 2 test parquet files as separate deltas to the source table
3. Staging and committing all necessary deltacat metadata (table version, stream, partition, deltas)
4. Running compaction using the direct API (not the CLI script)

Usage:
    # Use default catalog location
    python bootstrap.py
    
    # Use custom catalog location
    python bootstrap.py --catalog-root /path/to/catalog
    
    # Run compaction automatically
    python bootstrap.py --run-compaction
    
    # Auto-respond to prompts (for testing)
    python bootstrap.py --auto-run-compaction yes

The script creates:
- A source namespace "compactor_test_source" 
- A destination namespace "compactor_test_dest"
- Source table "events" with columns: id, timestamp, user_id, event_type, data
- Destination table "events_compacted" 
- 2 parquet files with overlapping data (suitable for compaction)
- All necessary deltacat metadata (table version, stream, partition, deltas)
- Working end-to-end compaction demonstration
"""

import argparse
import os
import subprocess
import sys
import tempfile
from typing import Optional

import pandas as pd
import pyarrow as pa

from deltacat.catalog import create_namespace, namespace_exists
from deltacat.catalog.model.properties import CatalogProperties, get_catalog_properties
from deltacat.storage.model.types import LifecycleState
from deltacat.types.media import ContentType, DistributedDatasetType
from deltacat.storage import metastore
from deltacat.storage.model.schema import Schema
from deltacat.storage.model.delta import DeltaType

# Import compaction API directly
from deltacat.compute.compactor_v2.compaction_session import compact_partition
from deltacat.compute.compactor.model.compact_partition_params import CompactPartitionParams


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


def setup_test_namespace_and_table(catalog_root: str) -> tuple:
    """Set up the test namespace and table with proper schema. Returns the stream ID, table version, namespace, table name, and actual stream position."""
    print("Setting up test namespaces and tables...")

    # Initialize deltacat with the catalog
    catalog = CatalogProperties(root=catalog_root)

    # Initialize deltacat and register the catalog
    from deltacat.catalog import Catalog, put_catalog

    catalog_obj = Catalog(config=catalog)
    put_catalog("default", catalog_obj)

    source_namespace = "compactor_test_source"
    dest_namespace = "compactor_test_dest"
    table_name = "events"

    # Create source namespace if it doesn't exist
    if not namespace_exists(source_namespace, catalog="default"):
        print(f"Creating source namespace: {source_namespace}")
        create_namespace(
            namespace=source_namespace,
            catalog="default",
        )
    else:
        print(f"Source namespace {source_namespace} already exists")

    # Create destination namespace if it doesn't exist
    if not namespace_exists(dest_namespace, catalog="default"):
        print(f"Creating destination namespace: {dest_namespace}")
        create_namespace(
            namespace=dest_namespace,
            catalog="default",
        )
    else:
        print(f"Destination namespace {dest_namespace} already exists")

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

    # Create SOURCE table using metastore API
    print(f"\nCreating SOURCE table {source_namespace}.{table_name} using metastore API...")
    
    # Create source table, table version, and stream using metastore
    source_table, source_table_version, source_stream = metastore.create_table_version(
        namespace=source_namespace,
        table_name=table_name,
        catalog=catalog,
        schema=Schema.of(schema=schema),
        table_description="Test events table for compaction testing (source)",
        lifecycle_state=LifecycleState.ACTIVE,
    )
    
    print(f"✅ Created source table: {source_table.table_name}")
    print(f"📋 Source Stream ID: {source_stream.stream_id}")
    
    # Create and commit SOURCE partition
    source_partition = metastore.stage_partition(
        stream=source_stream,
        catalog=catalog,
    )
    source_partition = metastore.commit_partition(
        partition=source_partition,
        catalog=catalog,
    )
    
    print(f"✅ Created source partition: {source_partition.locator.partition_id}")
    
    # Stage and commit first delta to SOURCE
    print(f"Writing batch 1 as delta 1 to SOURCE...")
    staged_delta_1 = metastore.stage_delta(
        data=batch_1,
        partition=source_partition,
        catalog=catalog,
        content_type=ContentType.PARQUET,
        delta_type=DeltaType.UPSERT,
    )
    
    source_delta_1 = metastore.commit_delta(
        delta=staged_delta_1,
        catalog=catalog,
    )
    
    print(f"✅ Committed source delta 1 at stream position: {source_delta_1.stream_position}")
    
    # Stage and commit second delta to SOURCE
    print(f"Writing batch 2 as delta 2 to SOURCE...")
    staged_delta_2 = metastore.stage_delta(
        data=batch_2,
        partition=source_partition,
        catalog=catalog,
        content_type=ContentType.PARQUET,
        delta_type=DeltaType.UPSERT,
    )
    
    source_delta_2 = metastore.commit_delta(
        delta=staged_delta_2,
        catalog=catalog,
    )
    
    print(f"✅ Committed source delta 2 at stream position: {source_delta_2.stream_position}")
    
    # Create DESTINATION table using metastore API
    print(f"\nCreating DESTINATION table {dest_namespace}.{table_name}_compacted using metastore API...")
    
    # Create destination table, table version, and stream using metastore
    dest_table, dest_table_version, dest_stream = metastore.create_table_version(
        namespace=dest_namespace,
        table_name=f"{table_name}_compacted",
        catalog=catalog,
        schema=Schema.of(schema=schema),
        table_description="Compacted events table (destination)",
        lifecycle_state=LifecycleState.ACTIVE,
    )
    
    print(f"✅ Created destination table: {dest_table.table_name}")
    print(f"📋 Destination Stream ID: {dest_stream.stream_id}")
    
    # Create and commit DESTINATION partition
    dest_partition = metastore.stage_partition(
        stream=dest_stream,
        catalog=catalog,
    )
    dest_partition = metastore.commit_partition(
        partition=dest_partition,
        catalog=catalog,
    )
    
    print(f"✅ Created destination partition: {dest_partition.locator.partition_id}")
    
    # Get the final stream position
    actual_stream_position = source_delta_2.stream_position
    
    print(f"\n✅ Successfully created test data in {source_namespace}.{table_name}")
    print(f"📁 Catalog root: {catalog_root}")
    print(f"🔧 Total records: {len(batch_1) + len(batch_2)}")
    print(
        f"🔄 Overlapping IDs: {set(batch_1['id']) & set(batch_2['id'])} (good for compaction)"
    )
    print(f"📋 Source Stream ID: {source_stream.stream_id}")
    print(f"📋 Destination Stream ID: {dest_stream.stream_id}")
    print(f"📋 Table Version: {source_table_version.table_version}")
    print(f"📋 Actual Stream Position: {actual_stream_position}")
    print(f"📋 Number of Source Deltas: 2")

    # Print compaction command example with actual stream ID and position
    print(f"\n🚀 Next steps:")
    print(f"1. Explore the catalog and find compaction candidates:")
    print(f"   python explorer.py --show-compaction-candidates")
    print(f"")
    print(f"2. Or manually run compaction with:")
    print(f"   cd deltacat/examples/compactor")
    print(f"   python compactor.py \\")
    print(f"     --namespace '{source_namespace}' \\")
    print(f"     --table-name '{table_name}' \\")
    print(f"     --table-version '{source_table_version.table_version}' \\")
    print(f"     --stream-id '{source_stream.stream_id}' \\")
    print(f"     --partition-values '' \\")
    print(f"     --dest-namespace '{dest_namespace}' \\")
    print(f"     --dest-table-name '{table_name}_compacted' \\")
    print(f"     --dest-table-version '1' \\")
    print(f"     --dest-stream-id '{dest_stream.stream_id}' \\")
    print(f"     --dest-partition-values '' \\")
    print(f"     --last-stream-position {actual_stream_position} \\")
    print(f"     --primary-keys 'id' \\")
    print(f"     --compactor-version 'V2' \\")
    print(f"     --hash-bucket-count 2 \\")
    print(f"     --catalog-root '{catalog_root}'")
    
    return (source_stream.stream_id, source_table_version.table_version, source_namespace, 
            table_name, catalog_root, actual_stream_position, dest_stream.stream_id, 
            dest_namespace, source_partition, dest_partition, catalog)


def show_table_data(partition, catalog, label: str) -> None:
    """Show table data for a given partition."""
    try:
        print(f"\n{label} partition data:")
        
        # List deltas in the partition
        partition_deltas = metastore.list_partition_deltas(
            partition_like=partition,
            include_manifest=True,
            catalog=catalog,
        )
        
        delta_list = partition_deltas.all_items()
        delta_count = len(delta_list)
        
        if delta_count == 0:
            print(f"   No deltas found in {label} partition")
            return
            
        print(f"   Found {delta_count} delta(s) in {label} partition:")
        
        total_records = 0
        for i, delta in enumerate(delta_list):
            record_count = delta.meta.record_count if delta.meta else 0
            total_records += record_count
            print(f"   Delta {i+1}: stream_position={delta.stream_position}, type={delta.type}, records={record_count}")
        
        print(f"   Total records across all deltas: {total_records}")
        
        # Try to read and display some sample data if there are records
        if total_records > 0:
            try:
                # Use metastore to read delta data
                if delta_list:
                    # Read the first delta as a sample
                    first_delta = delta_list[0]
                    print(f"   Sample data from first delta:")
                    
                    # Try to read delta manifest entries
                    if first_delta.manifest and first_delta.manifest.entries:
                        print(f"   Delta has {len(first_delta.manifest.entries)} manifest entries")
                        # Just show that we have data without trying to read it
                        # (reading delta data requires more complex setup)
                    else:
                        print(f"   Delta manifest: {first_delta.manifest}")
                        
            except Exception as read_error:
                print(f"   Could not read sample data: {read_error}")
        
    except Exception as e:
        print(f"   Error reading {label} partition data: {e}")


def run_compaction(source_partition, dest_partition, catalog, actual_stream_position):
    """Run compaction using the direct API (not CLI script)."""
    print("\n🚀 Running compaction using direct API...")
    
    try:
        # Show data before compaction
        print("\n📊 Data before compaction:")
        show_table_data(source_partition, catalog, "SOURCE")
        show_table_data(dest_partition, catalog, "DESTINATION")
        
        # Run compaction using the direct API (following test pattern exactly)
        print(f"\n🔄 Starting compaction...")
        print(f"   Source partition: {source_partition.locator}")
        print(f"   Destination partition: {dest_partition.locator}")
        print(f"   Last stream position to compact: {actual_stream_position}")
        
        rcf_url = compact_partition(
            CompactPartitionParams.of(
                {
                    "catalog": catalog,
                    "compacted_file_content_type": ContentType.PARQUET,
                    "dd_max_parallelism_ratio": 1.0,
                    "deltacat_storage": metastore,
                    "deltacat_storage_kwargs": {"catalog": catalog},
                    "destination_partition_locator": dest_partition.locator,
                    "drop_duplicates": True,
                    "hash_bucket_count": 1,
                    "last_stream_position_to_compact": actual_stream_position,
                    "list_deltas_kwargs": {
                        "catalog": catalog,
                        "equivalent_table_types": [],
                    },
                    "primary_keys": ["id"],
                    "rebase_source_partition_locator": None,
                    "rebase_source_partition_high_watermark": None,
                    "records_per_compacted_file": 4000,
                    "source_partition_locator": source_partition.locator,
                }
            )
        )
        
        print(f"✅ Compaction completed successfully!")
        print(f"📁 RCF URL: {rcf_url}")
        
        # Show data after compaction
        print("\n📊 Data after compaction:")
        show_table_data(source_partition, catalog, "SOURCE")
        
        # Get updated destination partition to see new deltas
        updated_dest_partition = metastore.get_partition(
            stream_locator=dest_partition.stream_locator,
            partition_values=None,  # unpartitioned
            catalog=catalog,
        )
        show_table_data(updated_dest_partition, catalog, "DESTINATION")
        
        # List deltas in destination partition
        dest_partition_deltas = metastore.list_partition_deltas(
            partition_like=updated_dest_partition,
            include_manifest=True,
            catalog=catalog,
        )
        
        delta_count = len(dest_partition_deltas.all_items())
        print(f"\n📈 Compaction Results:")
        print(f"   Destination partition now has {delta_count} delta(s)")
        
        # Print info about the delta(s) found
        for i, delta in enumerate(dest_partition_deltas.all_items()):
            print(
                f"   Delta {i+1}: stream_position={delta.stream_position}, type={delta.type}, record_count={delta.meta.record_count if delta.meta else 'N/A'}"
            )
        
        return True
        
    except Exception as e:
        print(f"❌ Compaction failed with error: {e}")
        print(f"🔍 Error type: {type(e).__name__}")
        
        # Provide helpful troubleshooting information
        print(f"\n🛠️  Troubleshooting:")
        print(f"   • This error suggests the compaction API encountered an issue")
        print(f"   • The source and destination partitions were created successfully")
        print(f"   • You can still explore the catalog using: python explorer.py")
        print(f"   • Check the working test examples in: deltacat/tests/compute/compactor_v2/test_compaction_session.py")
        print(f"   • The direct API approach should work - this may be a configuration issue")
        
        return False


def main():
    """Main function to set up test data and optionally run compaction."""
    parser = argparse.ArgumentParser(
        description="""
DeltaCAT Compactor Bootstrap Script

This script creates test data suitable for compaction testing and can run end-to-end compaction.

Examples:
    # Use default catalog location
    python bootstrap.py --catalog-root /path/to/catalog
    
    # Run compaction automatically
    python bootstrap.py --run-compaction
    
    # Auto-respond to prompts (for testing)
    python bootstrap.py --auto-run-compaction yes
        """,
    )
    parser.add_argument(
        "--catalog-root",
        default="/tmp/deltacat_test",
        help="Root directory for the deltacat catalog (default: /tmp/deltacat_test)",
    )
    parser.add_argument(
        "--run-compaction",
        action="store_true",
        help="Run compaction automatically after creating test data",
    )
    parser.add_argument(
        "--auto-run-compaction",
        choices=["yes", "no"],
        help="Automatically respond to compaction prompt (for testing)",
    )

    args = parser.parse_args()
    catalog_root = args.catalog_root

    print("🚀 DeltaCAT Compactor Bootstrap")
    print("=" * 40)
    print(f"📁 Catalog root: {catalog_root}")
    
    # Initialize Ray for compaction API
    print("🔧 Initializing Ray for compaction...")
    try:
        import ray
        ray.init(local_mode=True, ignore_reinit_error=True)
        print("✅ Ray initialized successfully")
    except Exception as e:
        print(f"⚠️  Ray initialization failed: {e}")
        print("   Compaction may not work without Ray")

    try:
        stream_id, table_version, namespace, table_name, catalog_root, actual_stream_position, dest_stream_id, dest_namespace, source_partition, dest_partition, catalog = setup_test_namespace_and_table(catalog_root)

        print(f"\n✅ Bootstrap completed successfully!")
        print(f"📋 Summary:")
        print(f"   • Source: {namespace}.{table_name} (Stream ID: {stream_id})")
        print(f"   • Destination: {dest_namespace}.{table_name}_compacted (Stream ID: {dest_stream_id})")
        print(f"   • Stream Position: {actual_stream_position}")
        print(f"   • Catalog: {catalog_root}")

        # Interactive compaction option
        if args.run_compaction:
            run_compaction(source_partition, dest_partition, catalog, actual_stream_position)
        elif args.auto_run_compaction:
            # Automatically respond based on the argument
            if args.auto_run_compaction == "yes":
                print(f"\n🤔 Would you like to run compaction now and see the before/after results? [y/N]: y (auto)")
                run_compaction(source_partition, dest_partition, catalog, actual_stream_position)
            else:
                print(f"\n🤔 Would you like to run compaction now and see the before/after results? [y/N]: n (auto)")
                print(f"💡 Run 'python explorer.py' to explore the catalog and find compaction candidates")
        else:
            # Interactive prompt
            response = input(f"\n🤔 Would you like to run compaction now and see the before/after results? [y/N]: ").lower().strip()
            
            if response == "y":
                run_compaction(source_partition, dest_partition, catalog, actual_stream_position)
            else:
                print(f"💡 Run 'python explorer.py' to explore the catalog and find compaction candidates")

    except Exception as e:
        print(f"❌ Bootstrap failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        # Clean up Ray
        try:
            import ray
            ray.shutdown()
            print("🔧 Ray shutdown complete")
        except:
            pass


if __name__ == "__main__":
    exit(main())
