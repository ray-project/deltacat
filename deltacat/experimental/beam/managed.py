import apache_beam as beam
import time
import threading
import json
from pathlib import Path


# Store original functions before monkey-patching
_original_write = beam.managed.Write
_original_read = beam.managed.Read

# Global dictionary to track monitoring threads
_monitoring_threads = {}


def _monitor_table_versions(
    warehouse_path: str, table_name: str, interval: float, merge_keys: list
):
    """Monitor table versions in a separate thread."""
    print(
        f"[DELTACAT DEBUG] Starting table version monitor for {table_name} (interval: {interval}s, merge_keys: {merge_keys})"
    )

    last_version = None
    metadata_dir = Path(warehouse_path) / table_name / "metadata"

    while True:
        try:
            if metadata_dir.exists():
                # Find the latest metadata file (pattern: v<number>.metadata.json)
                metadata_files = list(metadata_dir.glob("v*.metadata.json"))
                if metadata_files:
                    # Extract version numbers and find the latest
                    latest_metadata = max(
                        metadata_files,
                        key=lambda f: int(
                            f.stem.split(".")[0][1:]
                        ),  # Extract number from v<number>.metadata
                    )
                    current_version = latest_metadata.stem.split(".")[
                        0
                    ]  # Get v<number> part

                    if last_version != current_version:
                        print(
                            f"[DELTACAT DEBUG] New table version detected: {current_version}"
                        )
                        print(f"[DELTACAT DEBUG] Merge keys configured: {merge_keys}")

                        # Read metadata to get additional information
                        try:
                            with open(latest_metadata, "r") as f:
                                metadata = json.load(f)
                                snapshot_id = metadata.get("current-snapshot-id")
                                print(
                                    f"[DELTACAT DEBUG] Current snapshot ID: {snapshot_id}"
                                )
                                print(
                                    f"[DELTACAT DEBUG] Schema ID: {metadata.get('current-schema-id')}"
                                )

                                # Show data files information
                                snapshots = metadata.get("snapshots", [])
                                if snapshots:
                                    latest_snapshot = snapshots[-1]
                                    manifest_list = latest_snapshot.get("manifest-list")
                                    print(
                                        f"[DELTACAT DEBUG] Manifest list: {manifest_list}"
                                    )

                        except Exception as e:
                            print(f"[DELTACAT DEBUG] Could not read metadata: {e}")

                        last_version = current_version

            time.sleep(interval)

        except Exception as e:
            print(f"[DELTACAT DEBUG] Error in table monitor: {e}")
            time.sleep(interval)


def write(*args, **kwargs):
    """Wrapper function that automatically applies DeltaCatOptimizer to Write operations."""
    print(f"[DELTACAT DEBUG] Initializing DeltaCatOptimizer for WRITE operation")
    print(f"[DELTACAT DEBUG] WRITE operation called with:")
    print(f"  - args: {args}")
    print(f"  - kwargs keys: {list(kwargs.keys()) if kwargs else 'None'}")

    # Extract and pop deltacat-specific config keys
    config = kwargs.get("config", {}).copy() if kwargs.get("config") else {}
    deltacat_optimizer_interval = config.pop("deltacat_optimizer_interval", 1.0)
    merge_keys = config.pop("merge_keys", ["id"])

    # Extract table name and warehouse path
    table_name = config.get("table", "unknown")
    warehouse_path = config.get("catalog_properties", {}).get("warehouse", "")

    print(f"  - table: {table_name}")
    print(f"  - deltacat_optimizer_interval: {deltacat_optimizer_interval}s")
    print(f"  - merge_keys: {merge_keys}")
    print(f"  - warehouse_path: {warehouse_path}")

    # Update kwargs with the modified config
    if "config" in kwargs:
        kwargs["config"] = config

    # Start monitoring thread if not already running for this table
    # TODO: Change this from launching a separate thread to instead launching
    #       or connecting to an existing Ray Cluster (whose cluster name is
    #       based on the target table name).
    monitor_key = f"{warehouse_path}_{table_name}"
    if (
        monitor_key not in _monitoring_threads
        or not _monitoring_threads[monitor_key].is_alive()
    ):
        monitor_thread = threading.Thread(
            target=_monitor_table_versions,
            args=(warehouse_path, table_name, deltacat_optimizer_interval, merge_keys),
            daemon=True,
        )
        monitor_thread.start()
        _monitoring_threads[monitor_key] = monitor_thread
        print(f"[DELTACAT DEBUG] Started monitoring thread for {table_name}")
    else:
        print(f"[DELTACAT DEBUG] Monitoring thread already running for {table_name}")

    print(f"[DELTACAT DEBUG] Delegating to underlying WRITE transform")
    return _original_write(*args, **kwargs)


def read(*args, **kwargs):
    """Wrapper function that automatically applies DeltaCatOptimizer to Read operations."""
    print(f"[DELTACAT DEBUG] Initializing DeltaCatOptimizer for READ operation")
    print(f"[DELTACAT DEBUG] READ operation called with:")
    print(f"  - args: {args}")
    print(f"  - kwargs keys: {list(kwargs.keys()) if kwargs else 'None'}")

    # Extract table name from config if available
    if kwargs and "config" in kwargs and isinstance(kwargs["config"], dict):
        table_name = kwargs["config"].get("table", "unknown")
        print(f"  - table: {table_name}")

    print(f"[DELTACAT DEBUG] Delegating to underlying READ transform")
    return _original_read(*args, **kwargs)
