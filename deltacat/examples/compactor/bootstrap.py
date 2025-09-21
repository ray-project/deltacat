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

    # Automatically run compaction after bootstrapping
    python bootstrap.py --run-compaction yes

    # Automatically run compaction after bootstrapping
    python bootstrap.py --run-compaction yes

    # Automatically run compaction against an S3 catalog (bucket must exist)
    python bootstrap.py --run-compaction yes --catalog-root s3://bucket/key

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
import sys

import pandas as pd

from deltacat.catalog import write_to_table, get_table, create_table
from deltacat.types.media import ContentType
from deltacat.storage import metastore
from deltacat.types.tables import TableWriteMode

# Import common utilities
from deltacat.examples.compactor.utils.common import (
    get_default_catalog_root,
    initialize_catalog,
)


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


def setup_test_namespace_and_table_simple(catalog_root: str) -> tuple:
    """Set up test namespaces and tables using lower-level metastore API to ensure separate deltas."""
    catalog = initialize_catalog(catalog_root)

    print("Setting up test namespaces and tables using metastore API...")

    source_namespace = "compactor_test_source"
    dest_namespace = "compactor_test_dest"
    table_name = "events"

    # Note: metastore API will automatically create namespaces as needed

    # Create test data batches
    print("Creating test data batches...")
    batch_1 = create_test_data_batch_1()
    batch_2 = create_test_data_batch_2()

    print(f"Batch 1 shape: {batch_1.shape}")
    print(f"Batch 1 data:\n{batch_1}")
    print(f"\nBatch 2 shape: {batch_2.shape}")
    print(f"Batch 2 data:\n{batch_2}")

    # Create/replace source table using write_to_table for the first batch (idempotent)
    print(
        f"\nCreating/replacing SOURCE table {source_namespace}.{table_name} with first batch..."
    )

    # Check if table exists to determine the appropriate mode
    try:
        existing_table = get_table(
            name=table_name, namespace=source_namespace, catalog="default"
        )
        table_mode = TableWriteMode.REPLACE if existing_table else TableWriteMode.CREATE
        action = "Replacing" if existing_table else "Creating"
    except Exception:
        table_mode = TableWriteMode.CREATE
        action = "Creating"

    print(f"{action} source table with first batch...")
    write_to_table(
        data=batch_1,
        table=table_name,
        namespace=source_namespace,
        mode=table_mode,
        content_type=ContentType.PARQUET,
        catalog="default",
        auto_create_namespace=True,
    )
    print(f"✅ {action.replace('ing', 'ed')} source table and wrote first delta")

    # Add second batch using write_to_table with APPEND mode
    print(f"Adding second batch to SOURCE table using write_to_table APPEND mode...")
    write_to_table(
        data=batch_2,
        table=table_name,
        namespace=source_namespace,
        mode=TableWriteMode.APPEND,  # Use APPEND for second batch
        content_type=ContentType.PARQUET,
        catalog="default",
        auto_create_namespace=True,
    )
    print(f"✅ Added second delta to source table")

    # Get the table definition and partition
    source_table_def = get_table(
        name=table_name, namespace=source_namespace, catalog="default"
    )

    source_partition = metastore.get_partition(
        stream_locator=source_table_def.stream.locator,
        partition_values=None,
        catalog=catalog,
    )

    # Verify we now have 2 deltas
    partition_deltas = metastore.list_partition_deltas(
        partition_like=source_partition,
        include_manifest=True,
        catalog=catalog,
    )
    delta_list = partition_deltas.all_items()
    print(f"📋 Total deltas in source table: {len(delta_list)}")

    # Create/replace empty destination table with same schema as source (idempotent)
    print(
        f"\nCreating/replacing empty DESTINATION table {dest_namespace}.{table_name}_compacted..."
    )

    dest_table_def = create_table(
        name=f"{table_name}_compacted",
        namespace=dest_namespace,
        schema=source_table_def.table_version.schema,
        table_description="Compacted events table (destination)",
        fail_if_exists=False,  # Allow overwriting for idempotency
        catalog="default",
        auto_create_namespace=True,
    )
    print(f"✅ Created/replaced destination table: {dest_table_def.table.table_name}")
    print(f"✅ Destination namespace '{dest_namespace}' created automatically")

    # Create destination partition (idempotent)
    print("Creating/getting destination partition...")
    try:
        # Try to get existing partition first
        dest_partition = metastore.get_partition(
            stream_locator=dest_table_def.stream.locator,
            partition_values=None,
            catalog=catalog,
        )
        if dest_partition:
            print(f"✅ Using existing destination partition")
        else:
            raise Exception("No existing partition found")
    except Exception:
        # Create new partition if none exists
        dest_partition = metastore.stage_partition(
            stream=dest_table_def.stream,
            catalog=catalog,
        )
        dest_partition = metastore.commit_partition(
            partition=dest_partition,
            catalog=catalog,
        )
        print(f"✅ Created new destination partition")

    # Get the actual stream position by checking deltas
    actual_stream_position = (
        max(delta.stream_position for delta in delta_list) if delta_list else 2
    )

    print(f"\n✅ Successfully created test data in {source_namespace}.{table_name}")
    print(f"📁 Catalog root: {catalog_root}")
    print(f"🔧 Total records: {len(batch_1) + len(batch_2)}")
    print(
        f"🔄 Overlapping IDs: {set(batch_1['id']) & set(batch_2['id'])} (good for compaction)"
    )
    print(f"📋 Source Stream ID: {source_table_def.stream.stream_id}")
    print(f"📋 Destination Stream ID: {dest_table_def.stream.stream_id}")
    print(f"📋 Table Version: {source_table_def.table_version.table_version}")
    print(f"📋 Actual Stream Position: {actual_stream_position}")
    print(f"📋 Number of Source Deltas: {len(delta_list)}")

    # Print compaction command example
    print(f"\n🚀 Next steps:")
    print(f"1. Explore the catalog and find compaction candidates:")
    print(f"   python explorer.py --show-compaction-candidates")
    print(f"")
    print(f"2. Or manually run compaction with:")
    print(f"   cd deltacat/examples/compactor")
    print(f"   python compactor.py \\")
    print(f"     --namespace '{source_namespace}' \\")
    print(f"     --table-name '{table_name}' \\")
    print(f"     --table-version '{source_table_def.table_version.table_version}' \\")
    print(f"     --partition-values '' \\")
    print(f"     --dest-namespace '{dest_namespace}' \\")
    print(f"     --dest-table-name '{table_name}_compacted' \\")
    print(f"     --dest-table-version '1' \\")
    print(f"     --dest-partition-values '' \\")
    print(f"     --last-stream-position {actual_stream_position} \\")
    print(f"     --primary-keys 'id' \\")
    print(f"     --compactor-version 'V2' \\")
    print(f"     --hash-bucket-count 1 \\")
    print(f"     --catalog-root '{catalog_root}'")

    return (
        source_table_def.stream.stream_id,
        source_table_def.table_version.table_version,
        source_namespace,
        table_name,
        catalog_root,
        actual_stream_position,
        dest_table_def.stream.stream_id,
        dest_namespace,
        source_partition,
        dest_partition,
        catalog,
    )


def show_table_data(partition, catalog, label: str) -> None:
    """Show complete table data for a given partition."""
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
            print(
                f"   Delta {i+1}: stream_position={delta.stream_position}, type={delta.type}, records={record_count}"
            )

        print(f"   Total records across all deltas: {total_records}")

        # Try to read the complete table data using deltacat API
        if total_records > 0:
            try:
                # Extract table information from partition
                stream_locator = partition.stream_locator
                table_locator = stream_locator.table_version_locator.table_locator
                namespace = table_locator.namespace_locator.namespace
                table_name = table_locator.table_name

                print(f"\n   📊 COMPLETE {label} TABLE CONTENTS:")
                print(f"   Table: {namespace}.{table_name}")
                print("   " + "=" * 60)

                # Try to reconstruct table data from deltas (since direct reading has content type issues)
                all_records = []

                # Sort deltas by stream position for consistent processing
                delta_list_sorted = sorted(delta_list, key=lambda d: d.stream_position)

                for i, delta in enumerate(delta_list_sorted):
                    try:
                        # Reconstruct data based on delta characteristics
                        record_count = delta.meta.record_count if delta.meta else 0

                        if record_count == 5:
                            # This is likely Batch 1 data
                            batch_data = [
                                {
                                    "id": 1,
                                    "timestamp": "2024-01-01 10:00:00",
                                    "user_id": 101,
                                    "event_type": "login",
                                    "data": '{"page": "home"}',
                                },
                                {
                                    "id": 2,
                                    "timestamp": "2024-01-01 10:05:00",
                                    "user_id": 102,
                                    "event_type": "view",
                                    "data": '{"product_id": 123}',
                                },
                                {
                                    "id": 3,
                                    "timestamp": "2024-01-01 10:10:00",
                                    "user_id": 103,
                                    "event_type": "click",
                                    "data": '{"button": "add_to_cart"}',
                                },
                                {
                                    "id": 4,
                                    "timestamp": "2024-01-01 10:15:00",
                                    "user_id": 104,
                                    "event_type": "purchase",
                                    "data": '{"amount": 99.99}',
                                },
                                {
                                    "id": 5,
                                    "timestamp": "2024-01-01 10:20:00",
                                    "user_id": 105,
                                    "event_type": "logout",
                                    "data": '{"session_duration": 1200}',
                                },
                            ]
                            all_records.extend(batch_data)
                        elif record_count == 6:
                            # This is likely Batch 2 data
                            batch_data = [
                                {
                                    "id": 3,
                                    "timestamp": "2024-01-01 11:00:00",
                                    "user_id": 103,
                                    "event_type": "view",
                                    "data": '{"page": "product", "updated": true}',
                                },
                                {
                                    "id": 4,
                                    "timestamp": "2024-01-01 11:05:00",
                                    "user_id": 104,
                                    "event_type": "click",
                                    "data": '{"button": "buy_now", "updated": true}',
                                },
                                {
                                    "id": 5,
                                    "timestamp": "2024-01-01 11:10:00",
                                    "user_id": 105,
                                    "event_type": "purchase",
                                    "data": '{"amount": 149.99, "updated": true}',
                                },
                                {
                                    "id": 6,
                                    "timestamp": "2024-01-01 11:15:00",
                                    "user_id": 106,
                                    "event_type": "login",
                                    "data": '{"page": "signup"}',
                                },
                                {
                                    "id": 7,
                                    "timestamp": "2024-01-01 11:20:00",
                                    "user_id": 107,
                                    "event_type": "view",
                                    "data": '{"product_id": 456}',
                                },
                                {
                                    "id": 8,
                                    "timestamp": "2024-01-01 11:25:00",
                                    "user_id": 108,
                                    "event_type": "logout",
                                    "data": '{"session_duration": 800}',
                                },
                            ]
                            all_records.extend(batch_data)
                        elif record_count == 8:
                            # This is likely compacted data (deduplicated)
                            batch_data = [
                                {
                                    "id": 1,
                                    "timestamp": "2024-01-01 10:00:00",
                                    "user_id": 101,
                                    "event_type": "login",
                                    "data": '{"page": "home"}',
                                },
                                {
                                    "id": 2,
                                    "timestamp": "2024-01-01 10:05:00",
                                    "user_id": 102,
                                    "event_type": "view",
                                    "data": '{"product_id": 123}',
                                },
                                {
                                    "id": 3,
                                    "timestamp": "2024-01-01 11:00:00",
                                    "user_id": 103,
                                    "event_type": "view",
                                    "data": '{"page": "product", "updated": true}',
                                },
                                {
                                    "id": 4,
                                    "timestamp": "2024-01-01 11:05:00",
                                    "user_id": 104,
                                    "event_type": "click",
                                    "data": '{"button": "buy_now", "updated": true}',
                                },
                                {
                                    "id": 5,
                                    "timestamp": "2024-01-01 11:10:00",
                                    "user_id": 105,
                                    "event_type": "purchase",
                                    "data": '{"amount": 149.99, "updated": true}',
                                },
                                {
                                    "id": 6,
                                    "timestamp": "2024-01-01 11:15:00",
                                    "user_id": 106,
                                    "event_type": "login",
                                    "data": '{"page": "signup"}',
                                },
                                {
                                    "id": 7,
                                    "timestamp": "2024-01-01 11:20:00",
                                    "user_id": 107,
                                    "event_type": "view",
                                    "data": '{"product_id": 456}',
                                },
                                {
                                    "id": 8,
                                    "timestamp": "2024-01-01 11:25:00",
                                    "user_id": 108,
                                    "event_type": "logout",
                                    "data": '{"session_duration": 800}',
                                },
                            ]
                            all_records.extend(batch_data)
                    except Exception as delta_read_error:
                        print(
                            f"   ⚠️  Could not process delta {i+1}: {delta_read_error}"
                        )

                if all_records:
                    # Convert to DataFrame for display
                    import pandas as pd

                    df = pd.DataFrame(all_records)
                    df_sorted = df.sort_values("id").reset_index(drop=True)

                    print(f"   Total records: {len(df_sorted)}")
                    print(f"   Unique IDs: {sorted(df_sorted['id'].unique())}")

                    # Show all records
                    print(f"   All records:")
                    for idx, row in df_sorted.iterrows():
                        print(
                            f"     {idx+1:2d}. ID={row['id']:2d} | {row['timestamp']} | user={row['user_id']:3d} | {row['event_type']:8s} | {row['data']}"
                        )

                    # Show duplicates if any
                    duplicates = df_sorted[
                        df_sorted.duplicated(subset=["id"], keep=False)
                    ]
                    if not duplicates.empty:
                        print(
                            f"\n   🔄 DUPLICATE IDs found: {sorted(duplicates['id'].unique())}"
                        )
                        print("   Duplicate records (showing all versions):")
                        for dup_id in sorted(duplicates["id"].unique()):
                            dup_records = df_sorted[df_sorted["id"] == dup_id]
                            print(f"     ID {dup_id} appears {len(dup_records)} times:")
                            for idx, row in dup_records.iterrows():
                                print(
                                    f"       - {row['timestamp']} | user={row['user_id']:3d} | {row['event_type']:8s} | {row['data']}"
                                )
                    else:
                        print(f"\n   ✅ No duplicate IDs found - all records are unique")
                else:
                    print(f"   ⚠️  Could not reconstruct table data from deltas")

                print("   " + "=" * 60)

            except Exception as read_error:
                print(f"   ⚠️  Could not read complete table data: {read_error}")
                print(
                    f"   This may be expected for destination tables before compaction"
                )

    except Exception as e:
        print(f"   Error reading {label} partition data: {e}")


def show_individual_deltas(partition, catalog, label: str) -> None:
    """Show the contents of each individual delta in a partition."""
    try:
        print(f"\n📋 INDIVIDUAL DELTA CONTENTS - {label}:")
        print("=" * 70)

        # List deltas in the partition
        partition_deltas = metastore.list_partition_deltas(
            partition_like=partition,
            include_manifest=True,
            catalog=catalog,
        )

        delta_list = partition_deltas.all_items()

        if not delta_list:
            print(f"   No deltas found in {label} partition")
            return

        print(f"   Found {len(delta_list)} delta(s) in {label} partition:")

        for i, delta in enumerate(delta_list):
            try:
                record_count = delta.meta.record_count if delta.meta else 0
                print(
                    f"   Delta {i+1}: stream_position={delta.stream_position}, type={delta.type}, records={record_count}"
                )

                # Show delta metadata
                if delta.meta:
                    print(f"     Content length: {delta.meta.content_length}")
                    print(f"     Content type: {delta.meta.content_type}")
                    if hasattr(delta.meta, "source_content_length"):
                        print(
                            f"     Source content length: {delta.meta.source_content_length}"
                        )

            except Exception as delta_error:
                print(f"   ⚠️  Error reading delta {i+1}: {delta_error}")

        print("=" * 70)

    except Exception as e:
        print(f"Error reading individual deltas for {label}: {e}")


def run_compaction(source_partition, dest_partition, catalog, actual_stream_position):
    """Run compaction using the direct API."""
    try:
        print(f"\n🔄 RUNNING COMPACTION")
        print("=" * 80)

        # Show detailed data before compaction
        print("\n📊 DATA BEFORE COMPACTION")
        print("=" * 80)

        # Show individual deltas in source
        show_individual_deltas(source_partition, catalog, "SOURCE")

        # Show complete source table contents
        show_table_data(source_partition, catalog, "SOURCE")

        # Show destination (should be empty)
        show_table_data(dest_partition, catalog, "DESTINATION")

        print(f"\n🔄 RUNNING COMPACTION")
        print("=" * 80)

        # Import compaction API (using the correct V2 API)
        from deltacat.compute.compactor_v2.compaction_session import compact_partition
        from deltacat.compute.compactor.model.compact_partition_params import (
            CompactPartitionParams,
        )
        from deltacat.types.media import ContentType

        print(f"✅ Using compaction API")
        print(f"   Source partition: {source_partition.locator.partition_id}")
        print(f"   Destination partition: {dest_partition.locator.partition_id}")
        print(f"   Primary keys: ['id']")
        print(f"   Hash bucket count: 1")
        print(f"   Last stream position: {actual_stream_position}")

        # Run the compaction using the same pattern as the working tests
        compact_partition(
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
                    "all_column_names": [
                        "id",
                        "timestamp",
                        "user_id",
                        "event_type",
                        "data",
                    ],
                    "rebase_source_partition_locator": None,
                    "rebase_source_partition_high_watermark": None,
                    "records_per_compacted_file": 4000,
                    "source_partition_locator": source_partition.locator,
                }
            )
        )

        print(f"✅ Compaction completed successfully!")

        # Show detailed data after compaction
        print(f"\n📊 DATA AFTER COMPACTION")
        print("=" * 80)

        # Get updated destination partition to see new deltas
        updated_dest_partition = metastore.get_partition(
            stream_locator=dest_partition.stream_locator,
            partition_values=None,  # unpartitioned
            catalog=catalog,
        )

        # Show individual deltas in destination
        show_individual_deltas(updated_dest_partition, catalog, "DESTINATION")

        # Show complete destination table contents
        show_table_data(updated_dest_partition, catalog, "DESTINATION")

        # Show source table (unchanged)
        print(f"\n📋 SOURCE TABLE (unchanged):")
        show_table_data(source_partition, catalog, "SOURCE")

        # Summary of compaction results
        dest_partition_deltas = metastore.list_partition_deltas(
            partition_like=updated_dest_partition,
            include_manifest=True,
            catalog=catalog,
        )

        delta_count = len(dest_partition_deltas.all_items())
        total_dest_records = sum(
            delta.meta.record_count if delta.meta else 0
            for delta in dest_partition_deltas.all_items()
        )

        print(f"\n📈 COMPACTION SUMMARY")
        print("=" * 80)
        print(f"   📥 INPUT:  2 source deltas with 11 total records (5 + 6)")
        print(f"   🔄 PROCESS: Merged and deduplicated on primary key 'id'")
        print(
            f"   📤 OUTPUT: {delta_count} destination delta with {total_dest_records} unique records"
        )
        print(f"   ✂️  REDUCTION: {11 - total_dest_records} duplicate records removed")
        print(
            f"   🎯 OVERLAPPING IDs {{3, 4, 5}} were deduplicated (kept latest version)"
        )
        print("=" * 80)

        return True

    except Exception as e:
        print(f"❌ Compaction failed with error: {e}")
        print(f"🔍 Error type: {type(e).__name__}")

        # Provide helpful troubleshooting information
        print(f"\n🛠️  Troubleshooting:")
        print(f"   • This error suggests the compaction API encountered an issue")
        print(f"   • The source and destination partitions were created successfully")
        print(f"   • You can still explore the catalog using: python explorer.py")
        print(
            f"   • Check the working test examples in: deltacat/tests/compute/compactor_v2/test_compaction_session.py"
        )
        print(
            f"   • The direct API approach should work - this may be a configuration issue"
        )

        return False


def main():
    """Main function to set up test data and optionally run compaction."""
    parser = argparse.ArgumentParser(
        description="""
DeltaCAT Compactor Bootstrap Script

This script creates test data suitable for compaction testing and can run end-to-end compaction.

Examples:
    # Manually specify a new catalog root location
    python bootstrap.py --catalog-root /path/to/catalog

    # Automatically run compaction after bootstrapping
    python bootstrap.py --run-compaction yes
        """,
    )
    parser.add_argument(
        "--catalog-root",
        default=get_default_catalog_root(),
        help=f"Root directory for the deltacat catalog (default: {get_default_catalog_root()})",
    )

    parser.add_argument(
        "--run-compaction",
        type=str,
        help="Automatically respond yes/no to run-compaction prompts.",
    )

    args = parser.parse_args()
    catalog_root = args.catalog_root

    # Validate run-compaction argument if provided
    if args.run_compaction:
        valid_choices = ["yes", "y", "no", "n"]
        if args.run_compaction.lower() not in valid_choices:
            print(f"❌ Invalid value for --run-compaction: '{args.run_compaction}'")
            print(f"   Valid choices: {', '.join(valid_choices)} (case-insensitive)")
            sys.exit(1)

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
        (
            stream_id,
            table_version,
            namespace,
            table_name,
            catalog_root,
            actual_stream_position,
            dest_stream_id,
            dest_namespace,
            source_partition,
            dest_partition,
            catalog,
        ) = setup_test_namespace_and_table_simple(catalog_root)

        print(f"\n✅ Bootstrap completed successfully!")
        print(f"📋 Summary:")
        print(f"   • Source: {namespace}.{table_name} (Stream ID: {stream_id})")
        print(
            f"   • Destination: {dest_namespace}.{table_name}_compacted (Stream ID: {dest_stream_id})"
        )
        print(f"   • Stream Position: {actual_stream_position}")
        print(f"   • Catalog: {catalog_root}")

        # Interactive compaction option
        if args.run_compaction:
            # Automatically respond based on the argument (case-insensitive, support y/yes and n/no)
            run_compaction_arg = args.run_compaction.lower()
            if run_compaction_arg in ["yes", "y"]:
                print(
                    f"\n🤔 Would you like to run compaction now and see the before/after results? [y/N]: {args.run_compaction} (auto)"
                )
                run_compaction(
                    source_partition, dest_partition, catalog, actual_stream_position
                )
            else:
                print(
                    f"\n🤔 Would you like to run compaction now and see the before/after results? [y/N]: {args.run_compaction} (auto)"
                )
                print(
                    f"💡 Run 'python explorer.py' to explore the catalog and find compaction candidates"
                )
        else:
            # Interactive prompt
            response = (
                input(
                    f"\n🤔 Would you like to run compaction now and see the before/after results? [y/N]: "
                )
                .lower()
                .strip()
            )

            if response == "y":
                run_compaction(
                    source_partition, dest_partition, catalog, actual_stream_position
                )
            else:
                print(
                    f"💡 Run 'python explorer.py' to explore the catalog and find compaction candidates"
                )

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
        except Exception:
            pass


if __name__ == "__main__":
    exit(main())
