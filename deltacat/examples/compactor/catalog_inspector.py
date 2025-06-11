#!/usr/bin/env python3
"""
DeltaCAT Compactor Inspection Script

This script inspects a DeltaCAT catalog to find table metadata, stream IDs, 
and partition information needed for compaction testing.

Usage:
    python inspect.py --catalog-root /path/to/catalog [--namespace namespace] [--table table]

This helps users find the correct parameters to use with the compactor.py script.
"""

import argparse
import sys
from typing import Optional

import deltacat
from deltacat.catalog import (
    Catalog, 
    put_catalog, 
    list_namespaces, 
    list_tables, 
    get_table,
    CatalogProperties
)
from deltacat.storage import metastore


def inspect_catalog(catalog_root: str, namespace_filter: Optional[str] = None, table_filter: Optional[str] = None):
    """Inspect a DeltaCAT catalog and print useful information for compaction."""
    
    print(f"🔍 Inspecting DeltaCAT catalog: {catalog_root}")
    print("=" * 60)
    
    # Initialize catalog
    catalog = CatalogProperties(root=catalog_root)
    catalog_obj = Catalog(config=catalog)
    put_catalog("default", catalog_obj)
    
    # List namespaces
    print("\n📁 Namespaces:")
    try:
        namespaces = list_namespaces(catalog="default")
        for ns in namespaces.all_items():
            if namespace_filter and ns.namespace != namespace_filter:
                continue
            print(f"  - {ns.namespace}")
            
            # List tables in this namespace
            try:
                tables = list_tables(namespace=ns.namespace, catalog="default")
                for table_def in tables.all_items():
                    if table_filter and table_def.table.table_name != table_filter:
                        continue
                        
                    table = table_def.table
                    table_version = table_def.table_version
                    
                    print(f"    📊 Table: {table.table_name}")
                    print(f"       Version: {table_version.table_version}")
                    print(f"       Description: {table.description or 'N/A'}")
                    print(f"       Lifecycle: {table_version.lifecycle_state}")
                    print(f"       Schema: {table_version.schema.schema if table_version.schema else 'N/A'}")
                    
                    # Get stream information
                    try:
                        streams = metastore.list_streams(
                            namespace=ns.namespace,
                            table_name=table.table_name,
                            table_version=table_version.table_version,
                            catalog="default"
                        )
                        
                        print(f"       🌊 Streams:")
                        for stream in streams.all_items():
                            print(f"         - ID: {stream.stream_id}")
                            print(f"           Format: {stream.stream_format}")
                            print(f"           State: {stream.state}")
                            
                            # Get partitions for this stream
                            try:
                                partitions = metastore.list_stream_partitions(
                                    stream=stream,
                                    catalog="default"
                                )
                                
                                print(f"           📦 Partitions ({len(partitions.all_items())}):")
                                for i, partition in enumerate(partitions.all_items()[:3]):  # Show first 3
                                    print(f"             [{i+1}] ID: {partition.partition_id}")
                                    print(f"                 Values: {partition.partition_values or 'None (unpartitioned)'}")
                                    print(f"                 Stream Position: {partition.stream_position}")
                                    
                                    # Get deltas for this partition
                                    try:
                                        deltas = metastore.list_partition_deltas(
                                            partition_like=partition,
                                            catalog="default"
                                        )
                                        print(f"                 🔄 Deltas: {len(deltas.all_items())}")
                                        
                                        max_stream_pos = 0
                                        for delta in deltas.all_items():
                                            if delta.stream_position and delta.stream_position > max_stream_pos:
                                                max_stream_pos = delta.stream_position
                                        
                                        if max_stream_pos > 0:
                                            print(f"                 📈 Max Stream Position: {max_stream_pos}")
                                        
                                    except Exception as e:
                                        print(f"                 ❌ Error listing deltas: {str(e)}")
                                
                                if len(partitions.all_items()) > 3:
                                    print(f"             ... and {len(partitions.all_items()) - 3} more partitions")
                                    
                            except Exception as e:
                                print(f"           ❌ Error listing partitions: {str(e)}")
                    
                    except Exception as e:
                        print(f"       ❌ Error listing streams: {str(e)}")
                    
                    print()
                    
            except Exception as e:
                print(f"    ❌ Error listing tables: {str(e)}")
    
    except Exception as e:
        print(f"❌ Error listing namespaces: {str(e)}")
        return
    
    print("\n🚀 Example compaction command:")
    print("PYTHONPATH=/path/to/deltacat python deltacat/examples/compactor/compactor.py \\")
    print("  --namespace 'YOUR_NAMESPACE' \\")
    print("  --table-name 'YOUR_TABLE' \\")
    print("  --table-version 'YOUR_VERSION' \\")
    print("  --stream-id 'YOUR_STREAM_ID' \\")
    print("  --partition-values '' \\")
    print("  --dest-namespace 'YOUR_NAMESPACE' \\")
    print("  --dest-table-name 'YOUR_TABLE_compacted' \\")
    print("  --dest-table-version '1' \\")
    print("  --dest-stream-id 'compacted_stream' \\")
    print("  --dest-partition-values '' \\")
    print("  --last-stream-position YOUR_MAX_STREAM_POSITION \\")
    print("  --primary-keys 'id' \\")
    print("  --compactor-version 'V2' \\")
    print("  --hash-bucket-count 2 \\")
    print(f"  --catalog-root '{catalog_root}'")


def main():
    """Main entry point for the inspection script."""
    parser = argparse.ArgumentParser(
        description="Inspect DeltaCAT catalog for compaction testing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Inspect entire catalog
  python inspect.py --catalog-root /tmp/deltacat_test
  
  # Inspect specific namespace
  python inspect.py --catalog-root /tmp/deltacat_test --namespace compactor_test
  
  # Inspect specific table
  python inspect.py --catalog-root /tmp/deltacat_test --namespace compactor_test --table events
        """
    )
    
    parser.add_argument(
        "--catalog-root",
        type=str,
        required=True,
        help="Root directory of the DeltaCAT catalog to inspect"
    )
    
    parser.add_argument(
        "--namespace",
        type=str,
        help="Filter by specific namespace (optional)"
    )
    
    parser.add_argument(
        "--table",
        type=str,
        help="Filter by specific table name (optional)"
    )
    
    args = parser.parse_args()
    
    try:
        inspect_catalog(args.catalog_root, args.namespace, args.table)
    except Exception as e:
        print(f"\n❌ Inspection failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main()) 