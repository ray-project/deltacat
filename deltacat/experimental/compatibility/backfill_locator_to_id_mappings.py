#!/usr/bin/env python3
"""
Backfill script for backwards compatibility with canonical_string changes.

This script migrates existing DeltaCAT catalogs from the old global canonical string
format (with parent hexdigest) to the new hierarchical format (without parent hexdigest).

The old format was: {parent_hexdigest}|{name_parts}
The new format is: {name_parts}

Strategy:
1. Patch canonical_string method to use old format for reading existing name mappings
2. Use dc.list() to recursively discover all objects with old canonical_string
3. Copy each object's name mappings using new canonical_string format for writing
4. Works with any PyArrow-supported filesystem (local, S3, GCS, etc.)

Usage:
    python deltacat/experimental/compatibility/backfill_locator_to_id_mappings.py --catalog-root /path/to/catalog
"""

import argparse
import logging
import contextlib

import deltacat as dc
from deltacat.utils.url import DeltaCatUrl
from deltacat.storage.model.locator import Locator


logger = logging.getLogger(__name__)



def canonical_string_old(locator, separator: str = "|") -> str:
    """
    Old implementation of canonical_string that included parent hexdigest.
    This is used to read existing name resolution directories.
    """
    parts = []
    parent_hexdigest = locator.parent.hexdigest() if locator.parent else None
    if parent_hexdigest:
        parts.append(parent_hexdigest)
    parts.extend(locator.name.parts())
    return separator.join([str(part) for part in parts])


@contextlib.contextmanager
def patched_canonical_string(use_old_format: bool = True):
    """
    Context manager that temporarily patches the canonical_string method.

    Args:
        use_old_format: If True, use old format; if False, use new format
    """
    # Store original method
    original_method = Locator.canonical_string

    try:
        if use_old_format:
            # Patch with old implementation
            Locator.canonical_string = canonical_string_old
        # If use_old_format is False, keep the current (new) implementation

        yield

    finally:
        # Always restore original method
        Locator.canonical_string = original_method


def _build_destination_url(base_dst_url: DeltaCatUrl, metafile_obj: dict) -> DeltaCatUrl:
    """
    Build the appropriate destination URL based on the type of metafile object.

    Args:
        base_dst_url: Base destination URL (e.g., dc://catalog_name/)
        metafile_obj: The metafile object (dict) to be written

    Returns:
        DeltaCatUrl with the appropriate path for the object type
    """

    # Extract the catalog name from the base URL
    catalog_name = base_dst_url.catalog_name

    # Determine object type and build URL accordingly
    if 'namespaceLocator' in metafile_obj and 'tableName' not in metafile_obj:
        # This is a Namespace object
        namespace_name = metafile_obj['namespaceLocator']['namespace']
        return DeltaCatUrl(f"dc://{catalog_name}/{namespace_name}/")

    elif 'tableLocator' in metafile_obj and 'tableVersion' not in metafile_obj:
        # This is a Table object
        namespace_name = metafile_obj['tableLocator']['namespaceLocator']['namespace']
        table_name = metafile_obj['tableLocator']['tableName']
        return DeltaCatUrl(f"dc://{catalog_name}/{namespace_name}/{table_name}/")

    elif 'tableVersionLocator' in metafile_obj and 'streamId' not in metafile_obj:
        # This is a TableVersion object
        table_loc = metafile_obj['tableVersionLocator']['tableLocator']
        namespace_name = table_loc['namespaceLocator']['namespace']
        table_name = table_loc['tableName']
        table_version = metafile_obj['tableVersionLocator']['tableVersion']
        return DeltaCatUrl(f"dc://{catalog_name}/{namespace_name}/{table_name}/{table_version}/")

    elif 'streamLocator' in metafile_obj and 'partitionValues' not in metafile_obj:
        # This is a Stream object
        table_version_loc = metafile_obj['streamLocator']['tableVersionLocator']
        table_loc = table_version_loc['tableLocator']
        namespace_name = table_loc['namespaceLocator']['namespace']
        table_name = table_loc['tableName']
        table_version = table_version_loc['tableVersion']
        stream_format = metafile_obj['streamLocator'].get('format', 'deltacat')

        # Ensure stream_format is not None (use default if needed)
        if stream_format is None:
            stream_format = 'deltacat'

        return DeltaCatUrl(f"dc://{catalog_name}/{namespace_name}/{table_name}/{table_version}/{stream_format}/")

    elif 'partitionLocator' in metafile_obj and 'deltaLocator' not in metafile_obj:
        # This is a Partition object
        stream_loc = metafile_obj['partitionLocator']['streamLocator']
        table_version_loc = stream_loc['tableVersionLocator']
        table_loc = table_version_loc['tableLocator']
        namespace_name = table_loc['namespaceLocator']['namespace']
        table_name = table_loc['tableName']
        table_version = table_version_loc['tableVersion']
        stream_format = stream_loc.get('format', 'deltacat')
        if stream_format is None:
            stream_format = 'deltacat'

        partition_values = metafile_obj['partitionLocator']['partitionValues']

        # Build partition URL - this is more complex as it needs partition values encoded
        if partition_values:
            import json
            partition_str = json.dumps(partition_values)
        else:
            partition_str = "null"
        return DeltaCatUrl(f"dc://{catalog_name}/{namespace_name}/{table_name}/{table_version}/{stream_format}/{partition_str}/")

    elif 'deltaLocator' in metafile_obj:
        # This is a Delta object
        partition_loc = metafile_obj['deltaLocator']['partitionLocator']
        stream_loc = partition_loc['streamLocator']
        table_version_loc = stream_loc['tableVersionLocator']
        table_loc = table_version_loc['tableLocator']
        namespace_name = table_loc['namespaceLocator']['namespace']
        table_name = table_loc['tableName']
        table_version = table_version_loc['tableVersion']
        stream_format = stream_loc.get('format', 'deltacat')
        if stream_format is None:
            stream_format = 'deltacat'

        partition_values = partition_loc['partitionValues']
        stream_position = metafile_obj['deltaLocator']['streamPosition']

        if partition_values:
            import json
            partition_str = json.dumps(partition_values)
        else:
            partition_str = "null"
        return DeltaCatUrl(f"dc://{catalog_name}/{namespace_name}/{table_name}/{table_version}/{stream_format}/{partition_str}/{stream_position}/")

    else:
        raise ValueError(f"Unknown metafile object type: {metafile_obj}")


def migrate_catalog(source_url: str, destination_url: str, dry_run: bool = False) -> bool:
    """
    Migrate a catalog from old to new canonical string format.

    Args:
        source_url: Source catalog URL (e.g., 'dc://catalog_root/')
        destination_url: Destination catalog URL (e.g., 'dc://new_catalog_root/')
        dry_run: If True, just show what would be migrated

    Returns:
        True if migration successful, False otherwise
    """
    try:
        src_url = DeltaCatUrl(source_url)
        dst_url = DeltaCatUrl(destination_url)

        logger.info(f"Starting migration from {source_url} to {destination_url}")

        if dry_run:
            logger.info("DRY RUN - No actual changes will be made")

        if dry_run:
            # Step 1: List all objects using old canonical_string format for dry run
            logger.info("DRY RUN - Discovering objects using old canonical string format...")
            with patched_canonical_string(use_old_format=True):
                src_objects = dc.list(src_url, recursive=True)

            if hasattr(src_objects, '__len__'):
                logger.info(f"DRY RUN - Found {len(src_objects)} objects to migrate")
            else:
                logger.info("DRY RUN - Found objects to migrate (count unknown)")

            logger.info("DRY RUN - Would copy objects using new canonical string format")
            return True

        # Step 2: Read objects with old format, then write with new format
        logger.info("Step 1: Reading all objects using old canonical string format...")
        with patched_canonical_string(use_old_format=True):
            src_objects = dc.list(src_url, recursive=True)

        if hasattr(src_objects, '__len__'):
            logger.info(f"Found {len(src_objects)} objects to migrate")
        else:
            logger.info("Found objects to migrate (count unknown)")

        logger.info("Step 2: Writing objects using new canonical string format...")
        with patched_canonical_string(use_old_format=False):
            # Sort objects by dependency order and then by version numbers
            sorted_objects = _sort_objects_for_migration(src_objects)

            # For each object, copy it individually to the destination
            for i, obj in enumerate(sorted_objects):
                try:
                    obj_name = _get_object_name(obj)

                    # Check if this object should be skipped
                    if _should_skip_object_during_migration(obj):
                        logger.info(f"Skipping object {i+1}/{len(sorted_objects)}: {obj_name}")
                        continue

                    logger.info(f"Copying object {i+1}/{len(sorted_objects)}: {obj_name}")

                    # Build the correct destination URL based on object type
                    dest_obj_url = _build_destination_url(dst_url, obj)
                    logger.info(f"Destination URL for object: {dest_obj_url}")

                    # Prepare metafile for migration
                    migration_obj = _prepare_metafile_for_migration(obj)

                    # Use dc.put to write individual object to destination
                    dc.put(dest_obj_url, metafile=migration_obj)
                    logger.info(f"Successfully copied object {i+1}")
                except Exception as e:
                    logger.error(f"Failed to copy object {i+1}: {e}")
                    raise

        logger.info("Migration completed successfully!")
        return True

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def _get_object_name(obj: dict) -> str:
    """Get a human-readable name for a metafile object for logging."""
    if 'namespaceLocator' in obj and 'tableName' not in obj:
        return f"Namespace({obj['namespaceLocator']['namespace']})"
    elif 'tableLocator' in obj and 'tableVersion' not in obj:
        ns = obj['tableLocator']['namespaceLocator']['namespace']
        table = obj['tableLocator']['tableName']
        return f"Table({ns}.{table})"
    elif 'tableVersionLocator' in obj and 'streamId' not in obj:
        table_loc = obj['tableVersionLocator']['tableLocator']
        ns = table_loc['namespaceLocator']['namespace']
        table = table_loc['tableName']
        version = obj['tableVersionLocator']['tableVersion']
        return f"TableVersion({ns}.{table}.{version})"
    elif 'streamLocator' in obj and 'partitionValues' not in obj:
        table_version_loc = obj['streamLocator']['tableVersionLocator']
        table_loc = table_version_loc['tableLocator']
        ns = table_loc['namespaceLocator']['namespace']
        table = table_loc['tableName']
        version = table_version_loc['tableVersion']
        stream_format = obj['streamLocator']['format']
        return f"Stream({ns}.{table}.{version}.{stream_format})"
    elif 'partitionLocator' in obj and 'deltaLocator' not in obj:
        stream_loc = obj['partitionLocator']['streamLocator']
        table_version_loc = stream_loc['tableVersionLocator']
        table_loc = table_version_loc['tableLocator']
        ns = table_loc['namespaceLocator']['namespace']
        table = table_loc['tableName']
        version = table_version_loc['tableVersion']
        stream_format = stream_loc.get('format', 'deltacat')
        partition_id = obj['partitionLocator']['partitionId']
        return f"Partition({ns}.{table}.{version}.{stream_format}.{partition_id})"
    elif 'deltaLocator' in obj:
        partition_loc = obj['deltaLocator']['partitionLocator']
        stream_loc = partition_loc['streamLocator']
        table_version_loc = stream_loc['tableVersionLocator']
        table_loc = table_version_loc['tableLocator']
        ns = table_loc['namespaceLocator']['namespace']
        table = table_loc['tableName']
        version = table_version_loc['tableVersion']
        stream_format = stream_loc.get('format', 'deltacat')
        stream_position = obj['deltaLocator']['streamPosition']
        return f"Delta({ns}.{table}.{version}.{stream_format}.{stream_position})"
    else:
        return f"Unknown({type(obj).__name__})"


def _sort_objects_for_migration(objects: list) -> list:
    """
    Sort objects for migration to ensure dependencies are created in the correct order.

    Order: Namespaces -> Tables -> TableVersions (by version number) -> Streams -> Partitions -> Deltas
    """
    # Separate objects by type
    namespaces = []
    tables = []
    table_versions = []
    streams = []
    partitions = []
    deltas = []

    for obj in objects:
        if 'namespaceLocator' in obj and 'tableName' not in obj:
            namespaces.append(obj)
        elif 'tableLocator' in obj and 'tableVersion' not in obj:
            tables.append(obj)
        elif 'tableVersionLocator' in obj and 'streamId' not in obj:
            table_versions.append(obj)
        elif 'streamLocator' in obj and 'partitionValues' not in obj:
            streams.append(obj)
        elif 'partitionLocator' in obj and 'deltaLocator' not in obj:
            # This is a Partition object (some may have streamPosition)
            partitions.append(obj)
        elif 'deltaLocator' in obj:
            deltas.append(obj)

    # Sort namespaces by name
    namespaces.sort(key=lambda x: x['namespaceLocator']['namespace'])

    # Sort tables by namespace and table name
    tables.sort(key=lambda x: (
        x['tableLocator']['namespaceLocator']['namespace'],
        x['tableLocator']['tableName']
    ))

    # Sort table versions by namespace, table name, and version number
    def table_version_sort_key(obj):
        table_loc = obj['tableVersionLocator']['tableLocator']
        ns = table_loc['namespaceLocator']['namespace']
        table = table_loc['tableName']
        version = obj['tableVersionLocator']['tableVersion']

        # Parse version number for proper sorting
        try:
            # Handle versions like "1", "2", etc.
            version_num = int(version)
        except ValueError:
            # Handle versions like "1.0", "placeholder.0", etc.
            if '.' in version:
                parts = version.split('.')
                try:
                    version_num = int(parts[0])
                except ValueError:
                    version_num = 0  # fallback for non-numeric versions
            else:
                version_num = 0  # fallback

        return (ns, table, version_num)

    table_versions.sort(key=table_version_sort_key)

    # Sort streams by their parent table version
    streams.sort(key=lambda x: (
        x['streamLocator']['tableVersionLocator']['tableLocator']['namespaceLocator']['namespace'],
        x['streamLocator']['tableVersionLocator']['tableLocator']['tableName'],
        x['streamLocator']['tableVersionLocator']['tableVersion'],
        x['streamLocator']['format']
    ))

    # Sort partitions by their parent stream
    partitions.sort(key=lambda x: (
        x['partitionLocator']['streamLocator']['tableVersionLocator']['tableLocator']['namespaceLocator']['namespace'],
        x['partitionLocator']['streamLocator']['tableVersionLocator']['tableLocator']['tableName'],
        x['partitionLocator']['streamLocator']['tableVersionLocator']['tableVersion'],
        x['partitionLocator']['streamLocator']['format'],
        x['partitionLocator']['partitionId']
    ))

    # Sort deltas by their parent partition and stream position
    deltas.sort(key=lambda x: (
        x['deltaLocator']['partitionLocator']['streamLocator']['tableVersionLocator']['tableLocator']['namespaceLocator']['namespace'],
        x['deltaLocator']['partitionLocator']['streamLocator']['tableVersionLocator']['tableLocator']['tableName'],
        x['deltaLocator']['partitionLocator']['streamLocator']['tableVersionLocator']['tableVersion'],
        x['deltaLocator']['partitionLocator']['streamLocator']['format'],
        x['deltaLocator']['partitionLocator']['partitionId'],
        x['deltaLocator']['streamPosition']
    ))

    # Combine in dependency order
    return namespaces + tables + table_versions + streams + partitions + deltas


def _should_skip_object_during_migration(obj: dict) -> bool:
    """
    Determine if an object should be skipped during migration.

    We skip Table objects because TableVersion creation will automatically
    create the parent Table as needed, avoiding version conflicts.

    We skip Delta objects because they reference partition IDs that change
    during migration, and the canonical string format migration doesn't
    need to preserve the actual data files.

    Args:
        obj: The metafile dictionary object

    Returns:
        True if the object should be skipped, False otherwise
    """
    # Skip Table objects (but not TableVersion objects)
    if 'tableLocator' in obj and 'tableVersion' not in obj:
        logger.info(f"Skipping Table object during migration - TableVersion creation will handle table creation")
        return True

    # Skip Delta objects to avoid partition ID conflicts
    if 'deltaLocator' in obj:
        logger.info(f"Skipping Delta object during migration - canonical string format migration preserves structure, not data files")
        return True

    return False


def _prepare_metafile_for_migration(obj: dict) -> dict:
    """
    Prepare a metafile object for migration by cleaning up fields that could cause conflicts.

    For Table objects, we clear the latest_table_version field so that TableVersions
    can be created without sequential validation conflicts.

    Args:
        obj: The metafile dictionary object

    Returns:
        Modified metafile object ready for migration
    """
    # Create a copy to avoid modifying the original
    migration_obj = obj.copy()

    # For Table objects, clear the latest_table_version to avoid conflicts
    if 'tableLocator' in obj and 'tableVersion' not in obj:
        # This is a Table object
        logger.info(f"Clearing latest_table_version for Table object to avoid version conflicts")
        if 'latest_table_version' in migration_obj:
            migration_obj['latest_table_version'] = None
        if 'latest_active_table_version' in migration_obj:
            migration_obj['latest_active_table_version'] = None

    return migration_obj


def main():
    parser = argparse.ArgumentParser(
        description="Backfill locator-to-ID mappings for DeltaCAT canonical string changes"
    )
    parser.add_argument(
        "--catalog-root",
        required=True,
        help="Path to the source DeltaCAT catalog root directory"
    )
    parser.add_argument(
        "--destination",
        required=True,
        help="Path to the destination DeltaCAT catalog root directory"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be migrated without making changes"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging. Writes logs to /tmp/deltacat/ by default."
    )

    args = parser.parse_args()

    # Configure logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Initialize DeltaCAT with the catalog
    catalog_config = {
        'local': {
            'root': args.catalog_root,
        }
    }
    dc.init(catalogs=catalog_config)

    try:
        # Migrate to different location
        source_url = f"dc://{args.catalog_root}/"
        dest_url = f"dc://{args.destination}/"

        if not args.dry_run:
            # Initialize destination catalog
            dest_config = {
                'dest': {
                    'root': args.destination,
                }
            }
            dc.init(catalogs=dest_config)

        success = migrate_catalog(source_url, dest_url, args.dry_run)

        return int(success)

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
