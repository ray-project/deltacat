"""
DeltaCAT Catalog Explorer

Discover candidate streams and partitions for compaction.
It provides an easy way to explore catalog contents and generates example compaction commands.

Usage:
    # Explore default test catalog (from bootstrap.py)
    python explorer.py

    # Explore with custom catalog root
    python explorer.py --catalog-root /path/to/catalog

    # Explore specific URL
    python explorer.py --url "dc://my_catalog/my_namespace"

    # Non-recursive listing
    python explorer.py --no-recursive

Examples:
    # After running bootstrap.py
    python bootstrap.py --catalog-root /tmp/deltacat_test
    python explorer.py --catalog-root /tmp/deltacat_test

    # Explore and find compaction candidates
    python explorer.py --catalog-root /tmp/deltacat_test --show-compaction-candidates
"""

import argparse
import os
import sys
from typing import List, Tuple

import deltacat as dc
from deltacat import DeltaCatUrl
from deltacat.storage.model.namespace import Namespace
from deltacat.storage.model.table import Table
from deltacat.storage.model.table_version import TableVersion
from deltacat.storage.model.stream import Stream
from deltacat.storage.model.partition import Partition
from deltacat.storage.model.delta import Delta

# Import common utilities
from deltacat.examples.compactor.utils.common import (
    get_default_catalog_root,
    initialize_deltacat_url_catalog,
    format_partition_values_for_command,
    get_max_stream_position_from_partition,
    get_bootstrap_destination_info,
)


def setup_catalog(
    catalog_root: str, catalog_name: str = "compactor_test_catalog"
) -> DeltaCatUrl:
    """Initialize and register the catalog."""
    print(f"🔧 Initializing catalog...")
    print(f"   Catalog root: {catalog_root}")
    print(f"   Catalog name: {catalog_name}")

    return initialize_deltacat_url_catalog(catalog_root, catalog_name)


def find_compaction_candidates(
    all_objects: List,
) -> List[Tuple[Partition, Stream, TableVersion, Table, Namespace]]:
    """Find partitions that are candidates for compaction."""
    candidates = []

    # Group objects by type for easier lookup
    namespaces = {ns.namespace: ns for ns in all_objects if isinstance(ns, Namespace)}
    tables = {
        (t.namespace, t.table_name): t for t in all_objects if isinstance(t, Table)
    }
    table_versions = {
        (tv.namespace, tv.table_name, tv.table_version): tv
        for tv in all_objects
        if isinstance(tv, TableVersion)
    }
    streams = {
        (s.namespace, s.table_name, s.table_version, s.stream_id): s
        for s in all_objects
        if isinstance(s, Stream)
    }
    partitions = [p for p in all_objects if isinstance(p, Partition)]
    deltas = [d for d in all_objects if isinstance(d, Delta)]

    # Group deltas by partition for counting
    deltas_by_partition = {}
    for delta in deltas:
        partition_key = (
            delta.namespace,
            delta.table_name,
            delta.table_version,
            delta.stream_id,
            delta.partition_id,
        )
        if partition_key not in deltas_by_partition:
            deltas_by_partition[partition_key] = []
        deltas_by_partition[partition_key].append(delta)

    for partition in partitions:
        # Find the related objects for this partition
        namespace = namespaces.get(partition.namespace)
        table = tables.get((partition.namespace, partition.table_name))
        table_version = table_versions.get(
            (partition.namespace, partition.table_name, partition.table_version)
        )
        stream = streams.get(
            (
                partition.namespace,
                partition.table_name,
                partition.table_version,
                partition.stream_id,
            )
        )

        # Check if this partition has deltas
        partition_key = (
            partition.namespace,
            partition.table_name,
            partition.table_version,
            partition.stream_id,
            partition.partition_id,
        )
        partition_deltas = deltas_by_partition.get(partition_key, [])

        if all([namespace, table, table_version, stream]):
            # Check if this partition is a good candidate for compaction
            # Must have committed stream and at least one delta (preferably multiple)
            if stream.state == "committed" and len(partition_deltas) > 0:
                candidates.append((partition, stream, table_version, table, namespace))

    return candidates


def generate_compaction_command(
    partition: Partition,
    stream: Stream,
    table_version: TableVersion,
    table: Table,
    namespace: Namespace,
    catalog_root: str,
) -> str:
    """Generate an example compaction command for the given partition."""

    # Format partition values for command line
    partition_values = format_partition_values_for_command(partition.partition_values)

    dest_namespace, dest_table_name = get_bootstrap_destination_info(
        namespace.namespace, table.table_name
    )

    # Get stream position for generic tables too
    from deltacat.catalog.model.properties import CatalogProperties

    catalog = CatalogProperties(root=catalog_root)

    partition_values_list = (
        list(partition.partition_values) if partition.partition_values else []
    )
    max_stream_position = get_max_stream_position_from_partition(
        namespace.namespace,
        table.table_name,
        table_version.table_version,
        partition_values_list,
        catalog,
    )

    command = f"""python compactor.py \\
  --namespace '{namespace.namespace}' \\
  --table-name '{table.table_name}' \\
  --table-version '{table_version.table_version}' \\
  --partition-values '{partition_values}' \\
  --dest-namespace '{dest_namespace}' \\
  --dest-table-name '{dest_table_name}' \\
  --dest-table-version '1' \\
  --dest-partition-values '{partition_values}' \\
  --last-stream-position {max_stream_position} \\
  --primary-keys 'id' \\
  --compactor-version 'V2' \\
  --hash-bucket-count 1 \\
  --catalog-root '{catalog_root}'"""

    return command


def print_catalog_summary(all_objects: List) -> None:
    """Print a summary of the catalog contents."""
    namespaces = [obj for obj in all_objects if isinstance(obj, Namespace)]
    tables = [obj for obj in all_objects if isinstance(obj, Table)]
    table_versions = [obj for obj in all_objects if isinstance(obj, TableVersion)]
    streams = [obj for obj in all_objects if isinstance(obj, Stream)]
    partitions = [obj for obj in all_objects if isinstance(obj, Partition)]
    deltas = [obj for obj in all_objects if isinstance(obj, Delta)]

    print(f"\n📊 Catalog Summary:")
    print(f"   Namespaces: {len(namespaces)}")
    print(f"   Tables: {len(tables)}")
    print(f"   Table Versions: {len(table_versions)}")
    print(f"   Streams: {len(streams)}")
    print(f"   Partitions: {len(partitions)}")
    print(f"   Deltas: {len(deltas)}")
    print(f"   Total Objects: {len(all_objects)}")


def print_detailed_listing(all_objects: List) -> None:
    """Print detailed listing of all objects."""
    print(f"\n📋 Detailed Catalog Listing:")

    # Group deltas by partition for better display
    deltas = [d for d in all_objects if isinstance(d, Delta)]
    deltas_by_partition = {}
    for delta in deltas:
        partition_key = (
            delta.namespace,
            delta.table_name,
            delta.table_version,
            delta.stream_id,
            delta.partition_id,
        )
        if partition_key not in deltas_by_partition:
            deltas_by_partition[partition_key] = []
        deltas_by_partition[partition_key].append(delta)

    for obj in all_objects:
        if isinstance(obj, Namespace):
            obj.namespace
            print(f"📁 Namespace: {obj.namespace}")
        elif isinstance(obj, Table):
            obj.table_name
            print(f"  📊 Table: {obj.table_name}")
        elif isinstance(obj, TableVersion):
            obj.table_version
            print(f"    📌 Table Version: {obj.table_version} (state: {obj.state})")
        elif isinstance(obj, Stream):
            obj.stream_id
            print(f"      🌊 Stream: {obj.stream_id}")
            print(f"         Format: {obj.stream_format}")
            print(f"         State: {obj.state}")
        elif isinstance(obj, Partition):
            print(f"        📦 Partition: {obj.partition_id}")
            if obj.partition_values:
                print(f"           Values: {obj.partition_values}")

            # Show deltas for this partition
            partition_key = (
                obj.namespace,
                obj.table_name,
                obj.table_version,
                obj.stream_id,
                obj.partition_id,
            )
            partition_deltas = deltas_by_partition.get(partition_key, [])
            if partition_deltas:
                # Sort deltas by stream position
                sorted_deltas = sorted(
                    partition_deltas, key=lambda d: d.stream_position
                )
                for delta in sorted_deltas:
                    print(f"          📄 Delta at position: {delta.stream_position}")
            else:
                print(f"          ⚠️  No deltas found")


def print_compaction_candidates(candidates: List, catalog_root: str) -> None:
    """Print compaction candidates with enhanced information."""
    if candidates:
        print(f"\n🎯 Compaction Candidates:")
        print(f"   Found {len(candidates)} partition(s) ready for compaction")

        for i, (partition, stream, table_version, table, namespace) in enumerate(
            candidates, 1
        ):
            # Count deltas for this partition
            try:
                from deltacat.storage import metastore
                from deltacat.catalog.model.properties import CatalogProperties

                catalog = CatalogProperties(root=catalog_root)

                # Create partition locator
                partition_locator = {
                    "streamLocator": {
                        "tableVersionLocator": {
                            "tableLocator": {
                                "namespaceLocator": {"namespace": namespace.namespace},
                                "tableName": table.table_name,
                            },
                            "tableVersion": table_version.table_version,
                        },
                        "streamId": stream.stream_id,
                        "format": "deltacat",
                    },
                    "partitionValues": None,
                    "partitionId": partition.partition_id,
                }

                # Get deltas to count them
                partition_deltas = metastore.list_partition_deltas(
                    partition_like=type(
                        "obj", (object,), {"locator": partition_locator}
                    )(),
                    include_manifest=True,
                    catalog=catalog,
                )

                delta_list = partition_deltas.all_items()
                delta_count = len(delta_list)
                max_stream_position = (
                    max(delta.stream_position for delta in delta_list)
                    if delta_list
                    else 0
                )
                total_records = sum(
                    delta.meta.record_count if delta.meta else 0 for delta in delta_list
                )
            except Exception:
                delta_count = "unknown"
                max_stream_position = "unknown"
                total_records = "unknown"

            print(f"\n📦 Candidate {i}:")
            print(f"   Namespace: {namespace.namespace}")
            print(f"   Table: {table.table_name}")
            print(f"   Table Version: {table_version.table_version}")
            print(f"   Stream: {stream.stream_id}")
            print(f"   Partition: {partition.partition_id}")
            print(f"   Stream State: {stream.state}")
            print(f"   Deltas: {delta_count}")
            if delta_count != "unknown" and delta_count > 0:
                print(f"   Total Records: {total_records}")
                print(f"   Max Stream Position: {max_stream_position}")
                if delta_count > 1:
                    print(
                        f"   🎯 Good candidate: Multiple deltas available for compaction"
                    )
                else:
                    print(f"   ⚠️  Single delta: Limited compaction benefit")

            if i == 1:  # Show command for first candidate
                command = generate_compaction_command(
                    partition, stream, table_version, table, namespace, catalog_root
                )
                print(f"\n🚀 Compaction command for candidate {i}:")
                print(f"   cd deltacat/examples/compactor")
                for line in command.split("\n"):
                    if line.strip():
                        print(f"   {line}")
    else:
        print(f"\n⚠️  No compaction candidates found.")
        print(
            f"💡 Tip: Compaction candidates are partitions with committed streams and deltas."
        )
        print(f"   Tables need multiple deltas to benefit from compaction.")


def main():
    """Main entry point for the explorer script."""
    parser = argparse.ArgumentParser(
        description="Explore DeltaCAT catalog contents and find compaction candidates",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Explore default test catalog (after running bootstrap.py)
  python explorer.py --catalog-root /tmp/deltacat_test

  # Explore specific URL
  python explorer.py --url "dc://my_catalog/my_namespace"

  # Show compaction candidates with example commands
  python explorer.py --catalog-root /tmp/deltacat_test --show-compaction-candidates

  # Non-recursive listing (step by step)
  python explorer.py --catalog-root /tmp/deltacat_test --no-recursive
        """,
    )

    parser.add_argument(
        "--catalog-root",
        type=str,
        default=get_default_catalog_root(),
        help=f"Root directory for the DeltaCAT catalog (default: {get_default_catalog_root()}, same as bootstrap.py)",
    )

    parser.add_argument(
        "--url",
        type=str,
        help="Specific DeltaCAT URL to explore (e.g., 'dc://catalog/namespace'). If not provided, uses the full catalog.",
    )

    parser.add_argument(
        "--no-recursive",
        action="store_true",
        help="Disable recursive listing (only list top-level objects)",
    )

    parser.add_argument(
        "--show-compaction-candidates",
        action="store_true",
        help="Show partitions that are candidates for compaction with example commands",
    )

    parser.add_argument(
        "--catalog-name",
        type=str,
        default="compactor_test_catalog",
        help="Name to register the catalog under (default: compactor_test_catalog)",
    )

    args = parser.parse_args()

    # Validate catalog root exists
    if not os.path.exists(args.catalog_root):
        print(f"❌ Error: Catalog root directory does not exist: {args.catalog_root}")
        print(f"💡 Tip: Run bootstrap.py first to create test data:")
        print(f"   python bootstrap.py --catalog-root {args.catalog_root}")
        return 1

    print(f"🔍 DeltaCAT Catalog Explorer")
    print(f"=" * 50)

    try:
        # Setup catalog
        catalog_url = setup_catalog(args.catalog_root, args.catalog_name)

        # Determine what URL to explore
        if args.url:
            explore_url = DeltaCatUrl(args.url)
            print(f"🎯 Exploring specific URL: {args.url}")
        else:
            explore_url = catalog_url
            print(f"🎯 Exploring full catalog: {catalog_url.url}")

        # List objects
        recursive = not args.no_recursive
        print(f"📖 Listing mode: {'Recursive' if recursive else 'Non-recursive'}")

        all_objects = dc.list(explore_url, recursive=recursive)

        if not all_objects:
            print(f"\n⚠️  No objects found in catalog.")
            print(f"💡 Tip: Run bootstrap.py to create test data:")
            print(f"   python bootstrap.py --catalog-root {args.catalog_root}")
            return 0

        # Print summary
        print_catalog_summary(all_objects)

        # Print detailed listing
        print_detailed_listing(all_objects)

        # Show compaction candidates if requested
        if args.show_compaction_candidates:
            candidates = find_compaction_candidates(all_objects)

            print_compaction_candidates(candidates, args.catalog_root)

        print(f"\n✅ Catalog exploration completed!")

    except Exception as e:
        print(f"\n❌ Error exploring catalog: {str(e)}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
