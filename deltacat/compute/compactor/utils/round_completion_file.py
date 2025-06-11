import json
import logging
import posixpath
from typing import Dict, Any, Optional
import pyarrow.fs as fs
from deltacat import logs
from deltacat.compute.compactor import RoundCompletionInfo
from deltacat.storage import PartitionLocator
from deltacat.utils.metrics import metrics
from deltacat.utils.filesystem import resolve_path_and_filesystem

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def get_round_completion_file_path(
    base_path: str,
    source_partition_locator: PartitionLocator,
    destination_partition_locator: Optional[PartitionLocator] = None,
) -> str:
    """
    Construct a filesystem-agnostic path for the round completion file.
    
    Args:
        base_path: Base path for storing completion files (e.g., "s3://bucket/compaction", "/tmp/compaction")
        source_partition_locator: Source partition locator
        destination_partition_locator: Optional destination partition locator
        
    Returns:
        Complete path to the round completion file
    """
    if destination_partition_locator:
        # Use destination partition path with source partition hash for uniqueness
        partition_path = destination_partition_locator.path(source_partition_locator.hexdigest())
    else:
        # Use source partition path directly
        partition_path = source_partition_locator.path("")
    
    # Remove leading slash to make path relative so posixpath.join works correctly
    if partition_path.startswith('/'):
        partition_path = partition_path[1:]
    
    return posixpath.join(base_path, f"{partition_path}.json")


@metrics
def read_round_completion_file(
    base_path: Optional[str],
    source_partition_locator: PartitionLocator,
    destination_partition_locator: Optional[PartitionLocator] = None,
    completion_file_path: Optional[str] = None,
) -> RoundCompletionInfo:
    """
    Read round completion file from filesystem.
    
    Args:
        base_path: Base path for completion files (deprecated if completion_file_path provided)
        source_partition_locator: Source partition locator
        destination_partition_locator: Optional destination partition locator
        completion_file_path: Direct path to completion file (takes precedence over base_path)
        
    Returns:
        RoundCompletionInfo if found, None otherwise
    """
    all_paths = []
    
    if completion_file_path:
        # Use direct path if provided
        all_paths.append(completion_file_path)
    elif base_path:
        # Construct paths using base_path for backward compatibility
        if destination_partition_locator:
            path_with_destination = get_round_completion_file_path(
                base_path,
                source_partition_locator,
                destination_partition_locator,
            )
            all_paths.append(path_with_destination)

        # Note: we read from RCF at two different paths for backward compatibility reasons
        path_prev = get_round_completion_file_path(
            base_path,
            source_partition_locator,
        )
        all_paths.append(path_prev)
    else:
        raise ValueError("Either base_path or completion_file_path must be provided")

    round_completion_info = None

    for rcf_path in all_paths:
        logger.info(f"Reading round completion file from: {rcf_path}")
        try:
            path, filesystem = resolve_path_and_filesystem(rcf_path)
            with filesystem.open_input_stream(path) as stream:
                content = stream.read().decode("utf-8")
                round_completion_info = RoundCompletionInfo(json.loads(content))
                logger.info(f"Read round completion info: {round_completion_info}")
                break
        except Exception as e:
            logger.warning(f"Round completion file not present at {rcf_path}: {e}")

    return round_completion_info


@metrics
def write_round_completion_file(
    base_path: Optional[str],
    source_partition_locator: Optional[PartitionLocator],
    destination_partition_locator: Optional[PartitionLocator],
    round_completion_info: RoundCompletionInfo,
    completion_file_path: Optional[str] = None,
) -> str:
    """
    Write round completion file to filesystem.
    
    Args:
        base_path: Base path for completion files (deprecated if completion_file_path provided)
        source_partition_locator: Source partition locator
        destination_partition_locator: Optional destination partition locator
        round_completion_info: Round completion info to write
        completion_file_path: Direct path to completion file (takes precedence over base_path)
        
    Returns:
        Path where the file was written
    """
    if completion_file_path:
        target_path = completion_file_path
    elif base_path and source_partition_locator:
        target_path = get_round_completion_file_path(
            base_path,
            source_partition_locator,
            destination_partition_locator,
        )
    else:
        raise ValueError("Either (base_path and source_partition_locator) or completion_file_path must be provided")

    logger.info(f"writing round completion file to: {target_path}")
    
    path, filesystem = resolve_path_and_filesystem(target_path)
    
    # Ensure parent directories exist
    import os
    import pyarrow
    parent_dir = os.path.dirname(path)
    if parent_dir and not filesystem.get_file_info(parent_dir).type == pyarrow.fs.FileType.Directory:
        try:
            filesystem.create_dir(parent_dir, recursive=True)
        except (OSError, pyarrow.lib.ArrowIOError):
            # Directory might already exist or be created by another process
            pass
    
    content = json.dumps(round_completion_info).encode("utf-8")
    with filesystem.open_output_stream(path) as stream:
        stream.write(content)
    
    logger.info(f"round completion file written to: {target_path}")
    return target_path
