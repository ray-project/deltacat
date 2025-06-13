#!/usr/bin/env python3
"""
DeltaCAT Catalog Explorer

This script helps users discover candidate streams and partitions for compaction.
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
from typing import List, Optional, Tuple

import deltacat as dc
from deltacat import DeltaCatUrl
from deltacat.storage.model.namespace import Namespace
from deltacat.storage.model.table import Table
from deltacat.storage.model.table_version import TableVersion
from deltacat.storage.model.stream import Stream
from deltacat.storage.model.partition import Partition
from deltacat.storage.model.delta import Delta

# Import common utilities
from utils.common import (
    get_default_catalog_root,
    initialize_deltacat_url_catalog,
    format_partition_values_for_command,
    get_max_stream_position_from_partition,
    is_bootstrap_test_table,
    get_bootstrap_destination_info,
)


def setup_catalog(catalog_root: str, catalog_name: str = "compactor_test_catalog") -> DeltaCatUrl:
    """Initialize and register the catalog."""
    print(f"🔧 Initializing catalog...")
    print(f"   Catalog root: {catalog_root}")
    print(f"   Catalog name: {catalog_name}")
    
    return initialize_deltacat_url_catalog(catalog_root, catalog_name)


def find_compaction_candidates(all_objects: List) -> List[Tuple[Partition, Stream, TableVersion, Table, Namespace]]:
    """Find partitions that are candidates for compaction."""
    candidates = []
    
    # Group objects by type for easier lookup
    namespaces = {ns.namespace: ns for ns in all_objects if isinstance(ns, Namespace)}
    tables = {(t.namespace, t.table_name): t for t in all_objects if isinstance(t, Table)}
    table_versions = {(tv.namespace, tv.table_name, tv.table_version): tv for tv in all_objects if isinstance(tv, TableVersion)}
    streams = {(s.namespace, s.table_name, s.table_version, s.stream_id): s for s in all_objects if isinstance(s, Stream)}
    partitions = [p for p in all_objects if isinstance(p, Partition)]
    deltas = [d for d in all_objects if isinstance(d, Delta)]
    
    # Group deltas by partition for counting
    deltas_by_partition = {}
    for delta in deltas:
        partition_key = (delta.namespace, delta.table_name, delta.table_version, delta.stream_id, delta.partition_id)
        if partition_key not in deltas_by_partition:
            deltas_by_partition[partition_key] = []
        deltas_by_partition[partition_key].append(delta)
    
    for partition in partitions:
        # Find the related objects for this partition
        namespace = namespaces.get(partition.namespace)
        table = tables.get((partition.namespace, partition.table_name))
        table_version = table_versions.get((partition.namespace, partition.table_name, partition.table_version))
        stream = streams.get((partition.namespace, partition.table_name, partition.table_version, partition.stream_id))
        
        # Check if this partition has deltas
        partition_key = (partition.namespace, partition.table_name, partition.table_version, partition.stream_id, partition.partition_id)
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
    catalog_root: str
) -> str:
    """Generate an example compaction command for the given partition."""
    
    # Format partition values for command line
    partition_values = format_partition_values_for_command(partition.partition_values)
    
    # Check if this looks like a bootstrap-created structure
    is_bootstrap_source = is_bootstrap_test_table(namespace.namespace, table.table_name)
    
    if is_bootstrap_source:
        # Generate command that works with bootstrap structure
        # Get the destination table info
        dest_namespace, dest_table_name = get_bootstrap_destination_info(namespace.namespace, table.table_name)
        
        # Try to get the actual destination stream ID from the catalog
        try:
            from deltacat.catalog import get_table, Catalog, put_catalog
            
            # Initialize catalog like the working approach
            catalog_obj = Catalog(config=catalog)
            put_catalog("default", catalog_obj)
            
            dest_table_def = get_table(
                name=dest_table_name,
                namespace=dest_namespace,
                catalog="default"
            )
            dest_stream_id = dest_table_def.stream.stream_id if dest_table_def and dest_table_def.stream else f"dest_{stream.stream_id[:8]}"
        except:
            dest_stream_id = f"dest_{stream.stream_id[:8]}"
        
        # Get actual stream position by trying to read deltas
        from deltacat.catalog.model.properties import CatalogProperties
        catalog = CatalogProperties(root=catalog_root)
        
        partition_values_list = list(partition.partition_values) if partition.partition_values else []
        max_stream_position = get_max_stream_position_from_partition(
            namespace.namespace,
            table.table_name,
            table_version.table_version,
            partition_values_list,
            catalog
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
    else:
        # Generate generic command for non-bootstrap tables
        dest_namespace, dest_table_name = get_bootstrap_destination_info(namespace.namespace, table.table_name)
        
        # Get stream position for generic tables too
        from deltacat.catalog.model.properties import CatalogProperties
        catalog = CatalogProperties(root=catalog_root)
        
        partition_values_list = list(partition.partition_values) if partition.partition_values else []
        max_stream_position = get_max_stream_position_from_partition(
            namespace.namespace,
            table.table_name,
            table_version.table_version,
            partition_values_list,
            catalog
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
        partition_key = (delta.namespace, delta.table_name, delta.table_version, delta.stream_id, delta.partition_id)
        if partition_key not in deltas_by_partition:
            deltas_by_partition[partition_key] = []
        deltas_by_partition[partition_key].append(delta)
    
    current_namespace = None
    current_table = None
    current_table_version = None
    current_stream = None
    
    for obj in all_objects:
        if isinstance(obj, Namespace):
            current_namespace = obj.namespace
            print(f"📁 Namespace: {obj.namespace}")
        elif isinstance(obj, Table):
            current_table = obj.table_name
            print(f"  📊 Table: {obj.table_name}")
        elif isinstance(obj, TableVersion):
            current_table_version = obj.table_version
            print(f"    📌 Table Version: {obj.table_version} (state: {obj.state})")
        elif isinstance(obj, Stream):
            current_stream = obj.stream_id
            print(f"      🌊 Stream: {obj.stream_id}")
            print(f"         Format: {obj.stream_format}")
            print(f"         State: {obj.state}")
        elif isinstance(obj, Partition):
            print(f"        📦 Partition: {obj.partition_id}")
            if obj.partition_values:
                print(f"           Values: {obj.partition_values}")
            
            # Show deltas for this partition
            partition_key = (obj.namespace, obj.table_name, obj.table_version, obj.stream_id, obj.partition_id)
            partition_deltas = deltas_by_partition.get(partition_key, [])
            if partition_deltas:
                # Sort deltas by stream position
                sorted_deltas = sorted(partition_deltas, key=lambda d: d.stream_position)
                for delta in sorted_deltas:
                    print(f"          📄 Delta at position: {delta.stream_position}")
            else:
                print(f"          ⚠️  No deltas found")


def print_compaction_candidates(candidates: List, catalog_root: str) -> None:
    """Print compaction candidates with enhanced information."""
    if candidates:
        print(f"\n🎯 Compaction Candidates:")
        print(f"   Found {len(candidates)} partition(s) ready for compaction")
        
        for i, (partition, stream, table_version, table, namespace) in enumerate(candidates, 1):
            # Count deltas for this partition
            try:
                from deltacat.storage import metastore
                from deltacat.catalog.model.properties import CatalogProperties
                catalog = CatalogProperties(root=catalog_root)
                
                # Create partition locator
                partition_locator = {
                    'streamLocator': {
                        'tableVersionLocator': {
                            'tableLocator': {
                                'namespaceLocator': {'namespace': namespace.namespace},
                                'tableName': table.table_name
                            },
                            'tableVersion': table_version.table_version
                        },
                        'streamId': stream.stream_id,
                        'format': 'deltacat'
                    },
                    'partitionValues': None,
                    'partitionId': partition.partition_id
                }
                
                # Get deltas to count them
                partition_deltas = metastore.list_partition_deltas(
                    partition_like=type('obj', (object,), {'locator': partition_locator})(),
                    include_manifest=True,
                    catalog=catalog,
                )
                
                delta_list = partition_deltas.all_items()
                delta_count = len(delta_list)
                max_stream_position = max(delta.stream_position for delta in delta_list) if delta_list else 0
                total_records = sum(delta.meta.record_count if delta.meta else 0 for delta in delta_list)
            except:
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
                    print(f"   🎯 Good candidate: Multiple deltas available for compaction")
                else:
                    print(f"   ⚠️  Single delta: Limited compaction benefit")
            
            if i == 1:  # Show command for first candidate
                command = generate_compaction_command(
                    partition, stream, table_version, table, namespace, catalog_root
                )
                print(f"\n🚀 Compaction command for candidate {i}:")
                print(f"   cd deltacat/examples/compactor")
                for line in command.split('\n'):
                    if line.strip():
                        print(f"   {line}")
    else:
        print(f"\n⚠️  No compaction candidates found.")
        print(f"💡 Tip: Compaction candidates are partitions with committed streams and deltas.")
        print(f"   Tables need multiple deltas to benefit from compaction.")


def find_bootstrap_tables(catalog_root: str) -> List:
    """Find bootstrap-created tables using metastore API directly."""
    try:
        from deltacat.storage import metastore
        from deltacat.catalog.model.properties import CatalogProperties
        from deltacat.catalog import get_table
        
        catalog = CatalogProperties(root=catalog_root)
        
        # Initialize deltacat and register the catalog
        from deltacat.catalog import Catalog, put_catalog
        catalog_obj = Catalog(config=catalog)
        put_catalog("default", catalog_obj)
        
        bootstrap_objects = []
        
        # Try to find the bootstrap source table
        try:
            source_table_def = get_table(
                name="events",
                namespace="compactor_test_source",
                catalog="default"
            )
            
            if source_table_def:
                print(f"🔍 Found bootstrap source table: compactor_test_source.events")
                
                # Create objects for the source table
                from deltacat.storage.model.namespace import Namespace
                from deltacat.storage.model.table import Table
                from deltacat.storage.model.table_version import TableVersion
                from deltacat.storage.model.stream import Stream
                from deltacat.storage.model.partition import Partition
                
                # Create namespace object
                namespace_obj = type('Namespace', (), {
                    'namespace': 'compactor_test_source'
                })()
                bootstrap_objects.append(namespace_obj)
                
                # Create table object
                table_obj = type('Table', (), {
                    'namespace': 'compactor_test_source',
                    'table_name': 'events'
                })()
                bootstrap_objects.append(table_obj)
                
                # Create table version object
                table_version_obj = type('TableVersion', (), {
                    'namespace': 'compactor_test_source',
                    'table_name': 'events',
                    'table_version': source_table_def.table_version.table_version,
                    'state': 'created'
                })()
                bootstrap_objects.append(table_version_obj)
                
                # Create stream object
                stream_obj = type('Stream', (), {
                    'namespace': 'compactor_test_source',
                    'table_name': 'events',
                    'table_version': source_table_def.table_version.table_version,
                    'stream_id': source_table_def.stream.stream_id,
                    'stream_format': 'deltacat',
                    'state': 'committed'
                })()
                bootstrap_objects.append(stream_obj)
                
                # Try to get partition and deltas using metastore
                try:
                    # Get partition using metastore
                    partition = metastore.get_partition(
                        stream_locator=source_table_def.stream.locator,
                        partition_values=None,  # unpartitioned
                        catalog=catalog,
                    )
                    
                    if partition:
                        # Create partition object
                        partition_obj = type('Partition', (), {
                            'namespace': 'compactor_test_source',
                            'table_name': 'events',
                            'table_version': source_table_def.table_version.table_version,
                            'stream_id': source_table_def.stream.stream_id,
                            'partition_id': partition.locator.partition_id,
                            'partition_values': None
                        })()
                        bootstrap_objects.append(partition_obj)
                        
                        # Get deltas for this partition
                        partition_deltas = metastore.list_partition_deltas(
                            partition_like=partition,
                            include_manifest=True,
                            catalog=catalog,
                        )
                        
                        delta_list = partition_deltas.all_items()
                        print(f"🔍 Found {len(delta_list)} deltas in source partition")
                        
                        for delta in delta_list:
                            # Create delta object
                            delta_obj = type('Delta', (), {
                                'namespace': 'compactor_test_source',
                                'table_name': 'events',
                                'table_version': source_table_def.table_version.table_version,
                                'stream_id': source_table_def.stream.stream_id,
                                'partition_id': partition.locator.partition_id,
                                'stream_position': delta.stream_position
                            })()
                            bootstrap_objects.append(delta_obj)
                
                except Exception as partition_error:
                    print(f"⚠️  Could not get partition/deltas: {partition_error}")
        
        except Exception as source_error:
            print(f"⚠️  Could not find bootstrap source table: {source_error}")
        
        # Try to find the bootstrap destination table
        try:
            dest_table_def = get_table(
                name="events_compacted",
                namespace="compactor_test_dest",
                catalog="default"
            )
            
            if dest_table_def:
                print(f"🔍 Found bootstrap destination table: compactor_test_dest.events_compacted")
                
                # Create namespace object for destination
                dest_namespace_obj = type('Namespace', (), {
                    'namespace': 'compactor_test_dest'
                })()
                bootstrap_objects.append(dest_namespace_obj)
                
                # Create table object for destination
                dest_table_obj = type('Table', (), {
                    'namespace': 'compactor_test_dest',
                    'table_name': 'events_compacted'
                })()
                bootstrap_objects.append(dest_table_obj)
        
        except Exception as dest_error:
            print(f"⚠️  Could not find bootstrap destination table: {dest_error}")
        
        return bootstrap_objects
        
    except Exception as e:
        print(f"⚠️  Error in bootstrap table detection: {e}")
        return []


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
        
        try:
            all_objects = dc.list(explore_url, recursive=recursive)
        except Exception as list_error:
            print(f"\n⚠️  Standard catalog listing failed: {list_error}")
            print(f"🔄 Trying bootstrap table detection...")
            all_objects = find_bootstrap_tables(args.catalog_root)
        
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