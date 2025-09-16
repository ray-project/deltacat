"""
Backfill script for migrating unpartitioned transaction files to partitioned transaction
directories first introduced in deltacat 2.0.0.post3.

This script migrates existing DeltaCAT transaction files from the old unpartitioned
directory structure to the new epoch timestamp partitioned structure introduced
with catalog versioning.

Migration strategy:
1. Check catalog version matches current version (to avoid conflicts)
2. List all files in unpartitioned SUCCESS and FAILED transaction directories
3. Move each transaction file to its partitioned destination path
4. Supports any PyArrow-compatible filesystem (local, S3, GCS, etc.)

Transaction file organization:
- Old: {catalog_root}/txn/success/{transaction_id}
- New: {catalog_root}/txn/success/{epoch_partitions}/{transaction_id}
- Old: {catalog_root}/txn/failed/{transaction_id}
- New: {catalog_root}/txn/failed/{epoch_partitions}/{transaction_id}

The epoch partitioning scheme uses exponential partition transforms to create
hierarchical directories based on transaction start time.
"""

import logging
import os
import posixpath
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Tuple, Optional

import pyarrow.fs
from deltacat.catalog.model.properties import CatalogProperties, CatalogVersion
from deltacat.storage.model.transaction import Transaction
from deltacat.utils.filesystem import list_directory
from deltacat.constants import (
    TXN_DIR_NAME,
    SUCCESS_TXN_DIR_NAME,
    FAILED_TXN_DIR_NAME,
)
from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class MigrationProgressReporter:
    """Real-time progress reporter for transaction migration."""

    def __init__(self, total_transactions: int, show_progress: bool = True):
        self.total_transactions = total_transactions
        self.show_progress = show_progress
        self.completed = 0
        self.skipped = 0
        self.errors = 0
        self.start_time = time.time()
        self.lock = threading.Lock()
        self.last_update_time = 0
        self.update_interval = 0.1  # Update every 100ms
        self._should_stop = False

    def start(self):
        """Start the progress reporting."""
        if not self.show_progress:
            return
        self.start_time = time.time()
        print(f"\nMigrating {self.total_transactions:,} transactions...")
        print("=" * 70)
        self._update_display()

    def update_completed(self):
        """Mark one transaction as completed."""
        with self.lock:
            self.completed += 1
            self._maybe_update_display()

    def update_skipped(self):
        """Mark one transaction as skipped."""
        with self.lock:
            self.skipped += 1
            self._maybe_update_display()

    def update_error(self):
        """Mark one transaction as errored."""
        with self.lock:
            self.errors += 1
            self._maybe_update_display()

    def _maybe_update_display(self):
        """Update display if enough time has passed."""
        if not self.show_progress:
            return
        current_time = time.time()
        if current_time - self.last_update_time >= self.update_interval:
            self._update_display()
            self.last_update_time = current_time

    def _update_display(self):
        """Update the progress display."""
        if not self.show_progress:
            return

        elapsed = time.time() - self.start_time
        processed = self.completed + self.skipped + self.errors

        # Calculate progress
        progress_pct = (
            (processed / self.total_transactions * 100)
            if self.total_transactions > 0
            else 0
        )

        # Calculate rates
        rate = processed / elapsed if elapsed > 0 else 0
        eta_seconds = (self.total_transactions - processed) / rate if rate > 0 else 0

        # Format ETA
        if eta_seconds < 1:
            eta_str = "<1s"
        elif eta_seconds < 60:
            eta_str = f"{eta_seconds:.0f}s"
        elif eta_seconds < 3600:
            eta_str = f"{eta_seconds/60:.1f}m"
        else:
            eta_str = f"{eta_seconds/3600:.1f}h"

        # Create progress bar
        bar_width = 40
        filled_width = int(bar_width * progress_pct / 100)
        bar = "█" * filled_width + "░" * (bar_width - filled_width)

        # Build the progress line
        progress_line = (
            f"[{bar}] {progress_pct:5.1f}% | "
            f"{processed:,}/{self.total_transactions:,} | "
            f"OK: {self.completed:,} SKIP: {self.skipped:,} ERR: {self.errors:,} | "
            f"{rate:.0f} tx/s | ETA: {eta_str}"
        )

        # Clear the entire line and print progress
        # Store the previous line length to ensure we clear any leftover characters
        if not hasattr(self, "_last_line_length"):
            self._last_line_length = 0

        current_length = len(progress_line)
        # Pad with spaces if the current line is shorter than the previous one
        min_padding = max(0, self._last_line_length - current_length)
        padded_line = progress_line + " " * min_padding

        print(f"\r{padded_line}", end="", flush=True)
        self._last_line_length = current_length

    def finish(self):
        """Complete the progress reporting."""
        if not self.show_progress:
            return

        # Force a final update to show 100%
        self._update_display()

        elapsed = time.time() - self.start_time
        processed = self.completed + self.skipped + self.errors
        rate = processed / elapsed if elapsed > 0 else 0

        print()  # New line after progress bar
        print("=" * 70)
        print(f"Migration completed in {elapsed:.2f} seconds.")
        print(f"Results:")
        print(f"   - Migrated: {self.completed:,} transactions")
        if self.skipped > 0:
            print(f"   - Skipped:  {self.skipped:,} transactions (invalid)")
        if self.errors > 0:
            print(f"   - Errors:   {self.errors:,} transactions")
        print(f"   - Rate:     {rate:.0f} transactions/second")
        print(f"   - Total:    {processed:,}/{self.total_transactions:,} processed")
        print()

    def stop(self):
        """Stop the progress reporting."""
        self._should_stop = True


def _migrate_single_transaction(
    item_path: str,
    source_dir: str,
    filesystem: pyarrow.fs.FileSystem,
    is_success: bool,
    progress_reporter: Optional[MigrationProgressReporter] = None,
) -> Tuple[bool, Optional[str]]:
    """
    Migrate a single transaction item from an unpartitioned to partitioned path.

    Args:
        item_path: Path to the transaction item to migrate
        source_dir: Source directory containing the transaction items
        filesystem: Filesystem implementation
        is_success: Whether this is a success directory (affects destination path calculation)

    Returns:
        Tuple of (success, error_message). success=True if migration succeeded,
        success=False if item should be skipped (invalid).

    Raises:
        RuntimeError: If a valid transaction item fails to migrate.
    """
    # Extract transaction ID from item path
    txn_id = posixpath.basename(item_path)

    # First, validate that this is a valid transaction file by attempting to parse the transaction ID
    try:
        Transaction.parse_transaction_id(txn_id)
    except Exception as e:
        # Invalid transaction item - return skip result with warning message
        warning_msg = f"Skipping invalid transaction item {item_path}: cannot parse transaction ID '{txn_id}': {e}"
        if progress_reporter:
            progress_reporter.update_skipped()
        return False, warning_msg

    # This is a valid transaction item - we MUST move it or fail
    try:
        # Calculate destination path using Transaction static methods
        dir_path_generator = (
            Transaction.success_txn_log_dir_path
            if is_success
            else Transaction.failed_txn_log_path
        )
        destination_path = dir_path_generator(source_dir, txn_id)

        # Create destination directory if it doesn't exist
        destination_dir = posixpath.dirname(destination_path)
        filesystem.create_dir(destination_dir, recursive=True)

        # Move the item to partitioned location
        logger.debug(f"Moving {item_path} to {destination_path}")
        filesystem.move(item_path, destination_path)

        # Item moved successfully - report progress
        if progress_reporter:
            progress_reporter.update_completed()

        return True, None

    except Exception as e:
        # Valid transaction item failed to move - fail the migration
        if progress_reporter:
            progress_reporter.update_error()
        error_msg = (
            f"Migration failed: unable to move valid transaction item {item_path}: {e}"
        )
        raise RuntimeError(error_msg) from e


def backfill_transaction_partitions(
    catalog_properties: CatalogProperties, show_progress: bool = True
) -> None:
    """
    Migrate unpartitioned transaction files to partitioned directory structure.

    This function is designed to be lightweight and safe for read-only filesystems when
    there are no transactions to migrate. It only performs more expensive write operations
    when migration work is needed.

    Args:
        catalog_properties: Catalog properties containing root path, filesystem, and version info
        show_progress: Whether to show real-time progress reporting

    Raises:
        ValueError: If catalog version doesn't match current version
        RuntimeError: If migration fails when transactions actually need to be migrated
    """
    # Validate catalog version matches current version to avoid conflicts
    catalog_version = catalog_properties.version
    current_version = CatalogVersion.current()

    if catalog_version is None:
        logger.warning("No catalog version found - proceeding with migration")
    elif catalog_version.version != current_version.version:
        raise ValueError(
            f"Catalog version {catalog_version.version} doesn't match current version {current_version.version}. "
            f"Migration requires matching versions to avoid compatibility issues."
        )

    filesystem = catalog_properties.filesystem
    catalog_root = catalog_properties.root

    logger.info(f"Starting transaction partition migration for catalog: {catalog_root}")

    # Count total transactions to migrate (always needed for early exit logic)
    total_transactions = 0
    success_txn_log_dir = posixpath.join(
        catalog_root, TXN_DIR_NAME, SUCCESS_TXN_DIR_NAME
    )
    failed_txn_log_dir = posixpath.join(catalog_root, TXN_DIR_NAME, FAILED_TXN_DIR_NAME)

    for dir_path in [success_txn_log_dir, failed_txn_log_dir]:
        try:
            transaction_items = list_directory(
                dir_path, filesystem, ignore_missing_path=True
            )
            # Apply same validation logic as during migration to get accurate count
            for item_path, _ in transaction_items:
                item_name = posixpath.basename(item_path)

                # Use the same transaction ID parser as during migration
                try:
                    # If parsing succeeds, count it.
                    Transaction.parse_transaction_id(item_name)
                    total_transactions += 1
                except Exception:
                    # Invalid transaction ID - skip it
                    continue
        except Exception:
            pass  # Directory might not exist, that's OK

    # Create progress reporter only if there are transactions to migrate
    progress_reporter = None
    if show_progress and total_transactions > 0:
        progress_reporter = MigrationProgressReporter(
            total_transactions, show_progress=True
        )
        progress_reporter.start()

    try:
        # If there are no transactions to migrate, we can exit early.
        if total_transactions == 0:
            logger.info(
                "No transactions found to migrate - migration completed successfully"
            )
            return

        # Migrate SUCCESS transactions
        success_migrated = _migrate_transaction_status_directory(
            catalog_root=catalog_root,
            filesystem=filesystem,
            status_dir_name=SUCCESS_TXN_DIR_NAME,
            is_success=True,
            progress_reporter=progress_reporter,
        )

        # Migrate FAILED transactions
        failed_migrated = _migrate_transaction_status_directory(
            catalog_root=catalog_root,
            filesystem=filesystem,
            status_dir_name=FAILED_TXN_DIR_NAME,
            is_success=False,
            progress_reporter=progress_reporter,
        )

        total_migrated = success_migrated + failed_migrated

        if progress_reporter:
            progress_reporter.finish()
        else:
            logger.info(
                f"Migration completed successfully. Migrated {total_migrated} transaction files "
                f"({success_migrated} success, {failed_migrated} failed)"
            )

    except Exception as e:
        if progress_reporter:
            progress_reporter.stop()

        # If there were transactions to migrate, this is a critical failure
        if total_transactions > 0:
            raise RuntimeError(
                f"Failed to migrate {total_transactions} transactions: {e}. "
                f"Please rerun your script, and raise an issue if the problem persists."
            ) from e
        else:
            # If there were no transactions to migrate, log the error but don't fail initialization
            # This handles cases like read-only filesystems where the transaction counting succeeded
            # but other filesystem operations might fail
            logger.warning(
                f"Transaction migration encountered error, but there were no transactions to migrate: {e}. "
            )


def _migrate_transaction_status_directory(
    catalog_root: str,
    filesystem: pyarrow.fs.FileSystem,
    status_dir_name: str,
    is_success: bool,
    progress_reporter: Optional[MigrationProgressReporter] = None,
) -> int:
    """
    Migrate all transaction files in a specific status directory to a partitioned transaction log. Raises an
    exception if any valid transaction file fails migration to the new partitioned structure.

    Args:
        catalog_root: Root directory of the catalog
        filesystem: Filesystem implementation
        status_dir_name: Name of the status directory (SUCCESS_TXN_DIR_NAME or FAILED_TXN_DIR_NAME)
        is_success: Whether this is a success directory (affects destination path calculation)

    Returns:
        Number of transaction files migrated

    Raises:
        Exception: If any valid transaction file fails migration.
    """
    # Build source directory path
    source_dir = posixpath.join(catalog_root, TXN_DIR_NAME, status_dir_name)

    logger.info(f"Migrating {status_dir_name} transactions from: {source_dir}")

    try:
        # List all items (files and directories) in the unpartitioned directory
        transaction_items = list_directory(
            path=source_dir,
            filesystem=filesystem,
            ignore_missing_path=True,
        )
    except FileNotFoundError:
        logger.info(
            f"No {status_dir_name} transaction directory found at {source_dir} - skipping"
        )
        return 0

    if not transaction_items:
        logger.info(f"No transaction items found in {source_dir} - skipping")
        return 0

    # Filter items based on transaction ID validation
    valid_transaction_items = []
    for item_path, _ in transaction_items:
        item_name = posixpath.basename(item_path)

        # Use transaction ID parser to validate items (same as counting logic)
        try:
            Transaction.parse_transaction_id(item_name)
            # If parsing succeeds, assume it's valid for this transaction type
            valid_transaction_items.append(item_path)
        except Exception:
            # Invalid transaction ID - skip it (partition directories, etc.)
            continue

    logger.info(
        f"Found {len(valid_transaction_items)} valid {status_dir_name} transaction items to migrate"
    )

    migrated_count = 0
    skipped_count = 0
    total_items = len(valid_transaction_items)

    if not valid_transaction_items:
        logger.info(f"No valid transaction items to migrate in {status_dir_name}")
        return 0

    # Migrate transaction items in parallel using ThreadPoolExecutor
    # Use number of CPUs available, but don't exceed the number of items to migrate
    max_workers = min(os.cpu_count() or 1, len(valid_transaction_items))

    logger.info(f"Starting parallel migration with {max_workers} worker threads")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all migration tasks with progress reporting
        future_to_item = {
            executor.submit(
                _migrate_single_transaction,
                item_path,
                source_dir,
                filesystem,
                is_success,
                progress_reporter,
            ): item_path
            for item_path in valid_transaction_items
        }

        # Process completed futures and collect results
        for future in as_completed(future_to_item):
            item_path = future_to_item[future]
            try:
                success, warning_msg = future.result()
                if success:
                    migrated_count += 1
                    logger.debug(f"Successfully migrated transaction item: {item_path}")
                else:
                    # Item was skipped due to invalid transaction ID
                    skipped_count += 1
                    if warning_msg and not progress_reporter:
                        # Only log warning if not showing progress (to avoid cluttering progress display)
                        logger.warning(warning_msg)
            except Exception as e:
                # Valid transaction item failed to move - fail the migration
                logger.error(
                    f"Failed to migrate valid transaction item {item_path}: {e}"
                )
                if not progress_reporter:
                    logger.error(
                        f"Migration aborted. Successfully migrated {migrated_count}/{total_items - skipped_count} valid transaction items."
                    )
                # Cancel remaining futures to stop processing
                for remaining_future in future_to_item:
                    remaining_future.cancel()
                raise

    if skipped_count > 0:
        logger.warning(f"Skipped {skipped_count} invalid transaction items")

    logger.info(
        f"Successfully migrated {migrated_count} valid {status_dir_name} transaction items "
        f"({skipped_count} invalid items skipped)"
    )

    # Ensure we migrated all valid items
    expected_valid_items = total_items - skipped_count
    if migrated_count != expected_valid_items:
        raise RuntimeError(
            f"Migration integrity check failed: expected to migrate {expected_valid_items} "
            f"valid items but only migrated {migrated_count}"
        )

    return migrated_count
