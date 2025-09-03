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
from deltacat.api import _copy_objects_in_order


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


def migrate_catalog(
    source_url: str, destination_url: str, dry_run: bool = False
) -> bool:
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
            logger.info(
                "DRY RUN - Discovering objects using old canonical string format..."
            )
            with patched_canonical_string(use_old_format=True):
                src_objects = dc.list(src_url, recursive=True)

            if hasattr(src_objects, "__len__"):
                logger.info(f"DRY RUN - Found {len(src_objects)} objects to migrate")
            else:
                logger.info("DRY RUN - Found objects to migrate (count unknown)")

            logger.info(
                "DRY RUN - Would copy objects using new canonical string format"
            )
            return True

        # Step 2: Read objects with old format, then write with new format
        logger.info("Step 1: Reading all objects using old canonical string format...")
        with patched_canonical_string(use_old_format=True):
            src_objects = dc.list(src_url, recursive=True)

        if hasattr(src_objects, "__len__"):
            logger.info(f"Found {len(src_objects)} objects to migrate")
        else:
            logger.info("Found objects to migrate (count unknown)")

        logger.info("Step 2: Writing objects using new canonical string format...")
        with patched_canonical_string(use_old_format=False):
            _copy_objects_in_order(src_objects, dst_url)

        logger.info("Migration completed successfully!")
        return True

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Backfill locator-to-ID mappings for DeltaCAT canonical string changes"
    )
    parser.add_argument(
        "--catalog-root",
        required=True,
        help="Path to the source DeltaCAT catalog root directory",
    )
    parser.add_argument(
        "--destination",
        required=True,
        help="Path to the destination DeltaCAT catalog root directory",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be migrated without making changes",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging. Writes logs to /tmp/deltacat/ by default.",
    )

    args = parser.parse_args()

    # Configure logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s")

    # Initialize DeltaCAT with the catalog
    catalog_config = {
        "local": {
            "root": args.catalog_root,
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
                "dest": {
                    "root": args.destination,
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
