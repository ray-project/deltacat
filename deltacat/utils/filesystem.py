from __future__ import annotations

import re
import logging
from typing import Optional, Tuple, Union, List, Callable, Any
from datetime import datetime, timedelta, timezone
from enum import Enum

import sys
import urllib
import pathlib
import posixpath
from pathlib import PosixPath

import pyarrow as pa
from pyarrow.fs import (
    _resolve_filesystem_and_path,
    FileSelector,
    FileInfo,
    FileType,
    FileSystem,
    FSSpecHandler,
    PyFileSystem,
    GcsFileSystem,
    LocalFileSystem,
    S3FileSystem,
    AzureFileSystem,
    HadoopFileSystem,
)

from deltacat.constants import UNSIGNED_INT64_MAX_VALUE

from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

_LOCAL_SCHEME = "local"


class FilesystemType(str, Enum):
    LOCAL = "local"
    S3 = "s3"
    GCS = "gcs"
    AZURE = "azure"
    HADOOP = "hadoop"
    UNKNOWN = "unknown"

    @classmethod
    def from_filesystem(cls, filesystem: FileSystem) -> FilesystemType:
        if isinstance(filesystem, LocalFileSystem):
            return cls.LOCAL
        elif isinstance(filesystem, S3FileSystem):
            return cls.S3
        elif isinstance(filesystem, GcsFileSystem):
            return cls.GCS
        elif isinstance(filesystem, AzureFileSystem):
            return cls.AZURE
        elif isinstance(filesystem, HadoopFileSystem):
            return cls.HADOOP
        else:
            return cls.UNKNOWN

    @classmethod
    def to_filesystem(cls, filesystem_type: FilesystemType) -> FileSystem:
        if filesystem_type == cls.LOCAL:
            return LocalFileSystem()
        elif filesystem_type == cls.S3:
            return S3FileSystem()
        elif filesystem_type == cls.GCS:
            return GcsFileSystem()
        elif filesystem_type == cls.AZURE:
            return AzureFileSystem()
        elif filesystem_type == cls.HADOOP:
            return HadoopFileSystem()
        else:
            raise ValueError(f"Unsupported filesystem type: {filesystem_type}")


def resolve_paths_and_filesystem(
    paths: Union[str, List[str]],
    filesystem: FileSystem = None,
) -> Tuple[List[str], FileSystem]:
    """
    Resolves and normalizes all provided paths, infers a filesystem from the
    paths or validates the provided filesystem against the paths and ensures
    that all paths use the same filesystem.

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories.
        filesystem: The filesystem implementation that should be used for
            reading these files. If None, a filesystem will be inferred. If not
            None, the provided filesystem will still be validated against all
            filesystems inferred from the provided paths to ensure
            compatibility.
    """
    if isinstance(paths, str):
        paths = [paths]
    if isinstance(paths, pathlib.Path):
        paths = [str(paths)]
    elif not isinstance(paths, list) or any(not isinstance(p, str) for p in paths):
        raise ValueError(
            "Expected `paths` to be a `str`, `pathlib.Path`, or `list[str]`, but got "
            f"`{paths}`."
        )
    elif len(paths) == 0:
        raise ValueError("Must provide at least one path.")

    need_unwrap_path_protocol = True
    if filesystem and not isinstance(filesystem, FileSystem):
        err_msg = (
            f"The filesystem passed must either conform to "
            f"pyarrow.fs.FileSystem, or "
            f"fsspec.spec.AbstractFileSystem. The provided "
            f"filesystem was: {filesystem}"
        )
        try:
            import fsspec
            from fsspec.implementations.http import HTTPFileSystem
        except ModuleNotFoundError:
            # If filesystem is not a pyarrow filesystem and fsspec isn't
            # installed, then filesystem is neither a pyarrow filesystem nor
            # an fsspec filesystem, so we raise a TypeError.
            raise TypeError(err_msg) from None
        if not isinstance(filesystem, fsspec.spec.AbstractFileSystem):
            raise TypeError(err_msg) from None
        if isinstance(filesystem, HTTPFileSystem):
            # If filesystem is fsspec HTTPFileSystem, the protocol/scheme of paths
            # should not be unwrapped/removed, because HTTPFileSystem expects full file
            # paths including protocol/scheme. This is different behavior compared to
            # file systems implementation in pyarrow.fs.FileSystem.
            need_unwrap_path_protocol = False

        filesystem = PyFileSystem(FSSpecHandler(filesystem))

    resolved_paths = []
    for path in paths:
        path = _resolve_custom_scheme(path)
        # Normalize trailing slashes to prevent empty path components.
        # Keep single trailing slash for root paths like "s3://bucket/" -> "s3://bucket"
        if path.endswith("/") and not path.endswith("://"):
            path = path.rstrip("/")
        try:
            resolved_filesystem, resolved_path = _resolve_filesystem_and_path(
                path, filesystem
            )
        except pa.lib.ArrowInvalid as e:
            if "Cannot parse URI" in str(e):
                resolved_filesystem, resolved_path = _resolve_filesystem_and_path(
                    _encode_url(path), filesystem
                )
                resolved_path = _decode_url(resolved_path)
            elif "Unrecognized filesystem type in URI" in str(e):
                scheme = urllib.parse.urlparse(path, allow_fragments=False).scheme
                if scheme in ["http", "https"]:
                    # If scheme of path is HTTP and filesystem is not resolved,
                    # try to use fsspec HTTPFileSystem. This expects fsspec is
                    # installed.
                    try:
                        from fsspec.implementations.http import HTTPFileSystem
                    except ModuleNotFoundError:
                        raise ImportError(
                            "Please install fsspec to read files from HTTP."
                        ) from None

                    resolved_filesystem = PyFileSystem(FSSpecHandler(HTTPFileSystem()))
                    resolved_path = path
                    need_unwrap_path_protocol = False
                else:
                    raise
            else:
                raise
        if filesystem is None:
            if isinstance(resolved_filesystem, GcsFileSystem):
                # Configure a retry time limit for GcsFileSystem so that it
                # doesn't hang forever trying to get file info (e.g., when
                # trying to get a public file w/o anonymous=True).
                filesystem = GcsFileSystem(
                    retry_time_limit=timedelta(seconds=60),
                )
            else:
                filesystem = resolved_filesystem
        elif need_unwrap_path_protocol:
            resolved_path = _unwrap_protocol(resolved_path)
        resolved_path = filesystem.normalize_path(resolved_path)
        resolved_paths.append(resolved_path)
    return resolved_paths, filesystem


def resolve_path_and_filesystem(
    path: str,
    filesystem: Optional[FileSystem] = None,
) -> Tuple[str, FileSystem]:
    """
    Resolves and normalizes the provided path, infers a filesystem from the
    path or validates the provided filesystem against the path.

    Args:
        path: A single file/directory path.
        filesystem: The filesystem implementation that should be used for
            reading these files. If None, a filesystem will be inferred. If not
            None, the provided filesystem will still be validated against all
            filesystems inferred from the provided paths to ensure
            compatibility.
    """
    paths, filesystem = resolve_paths_and_filesystem(
        paths=path,
        filesystem=filesystem,
    )
    assert len(paths) == 1, len(paths)
    return paths[0], filesystem


def list_directory(
    path: str,
    filesystem: FileSystem,
    exclude_prefixes: Optional[List[str]] = None,
    ignore_missing_path: bool = False,
    recursive: bool = False,
) -> List[Tuple[str, int]]:
    """
    Expand the provided directory path to a list of file paths.

    Args:
        path: The directory path to expand.
        filesystem: The filesystem implementation that should be used for
            reading these files.
        exclude_prefixes: The file relative path prefixes that should be
            excluded from the returned file set. Default excluded prefixes are
            "." and "_".
        ignore_missing_path: Whether to ignore missing paths or raise an error.
        recursive: Whether to expand subdirectories or not.

    Returns:
        An iterator of (file_path, file_size) tuples.
    """
    if exclude_prefixes is None:
        exclude_prefixes = [".", "_"]

    selector = FileSelector(
        base_dir=path,
        recursive=recursive,
        allow_not_found=ignore_missing_path,
    )
    try:
        files = filesystem.get_file_info(selector)
    except OSError as e:
        if isinstance(e, FileNotFoundError) and ignore_missing_path:
            files = []
        else:
            _handle_read_os_error(e, path)
    base_path = selector.base_dir
    out = []
    for file_ in files:
        file_path = file_.path
        if not file_path.startswith(base_path):
            continue
        relative = file_path[len(base_path) :]
        # Remove leading slash for proper prefix matching
        if relative.startswith("/"):
            relative = relative[1:]
        if any(relative.startswith(prefix) for prefix in exclude_prefixes):
            continue
        out.append((file_path, file_.size))
    # We sort the paths to guarantee a stable order.
    return sorted(out, reverse=True)


def _collect_unpartitioned_files(
    path: str,
    filesystem: FileSystem,
    partition_value: Any,
    partition_file_parser: Callable[[str], Optional[Any]],
    remaining_limit: Optional[int],
    exclude_prefixes: Optional[List[str]],
    ignore_missing_path: bool,
    partition_start_value: Optional[Any] = None,
    target_start_partitions: Optional[List[str]] = None,
) -> List[Tuple[str, int]]:
    """
    Collect unpartitioned files from the base directory that can be parsed and are <= partition_value.

    Args:
        path: The base directory path to search for unpartitioned files.
        filesystem: The filesystem implementation to use.
        partition_value: The partition value to compare against.
        partition_file_parser: Callable that parses file paths to extract partition values.
        remaining_limit: Maximum number of files to return, or None for unlimited.
        exclude_prefixes: File prefixes to exclude.
        ignore_missing_path: Whether to ignore missing paths.

    Returns:
        A list of (file_path, file_size) tuples for valid unpartitioned files,
        sorted by partition value (closest to target first).
    """
    # Get files directly from the base directory (not recursive to avoid partition subdirs)
    base_files = list_directory(
        path=path,
        filesystem=filesystem,
        exclude_prefixes=exclude_prefixes,
        ignore_missing_path=ignore_missing_path,
        recursive=False,  # Don't recurse to avoid getting partition subdirectories
    )

    # Filter unpartitioned files that can be parsed and are within the partition range
    # Only include actual files (not directories)
    valid_unpartitioned_files = []
    for file_path, file_size in base_files:
        # Skip directories (file_size is None for directories)
        if file_size is None:
            continue
        file_partition_value = partition_file_parser(file_path)
        if (
            file_partition_value is not None
            and file_partition_value <= partition_value
            and (
                partition_start_value is None
                or file_partition_value >= partition_start_value
            )
        ):
            valid_unpartitioned_files.append(
                (file_path, file_size, file_partition_value)
            )

    # Sort unpartitioned files by partition value (closest to target first)
    valid_unpartitioned_files.sort(
        key=lambda x: x[2],
        reverse=True,  # Sort by partition value descending
    )

    # Return files up to the remaining limit
    if remaining_limit is None:
        return [(path, size) for path, size, _ in valid_unpartitioned_files]
    else:
        return [
            (path, size)
            for path, size, _ in valid_unpartitioned_files[:remaining_limit]
        ]


def list_directory_partitioned(
    path: str,
    filesystem: FileSystem,
    partition_value: Any,
    partition_transform: Callable[[Any], List[str]],
    partition_file_parser: Callable[[str], Optional[Any]],
    limit: Optional[int] = None,
    partition_dir_parser: Callable[[List[str]], Optional[Any]] = None,
    exclude_prefixes: Optional[List[str]] = None,
    ignore_missing_path: bool = False,
    recursive: bool = False,
    return_unpartitioned: bool = False,
    partition_start_value: Optional[Any] = None,
) -> List[Tuple[str, int]]:
    """
    List files in a partitioned filesystem directory structure, returning files from partitions
    that are within the specified partition range.

    This function implements partition traversal logic where files are returned from partitions
    whose parsed values are within the range [partition_start_value, partition_value]
    (inclusive). If partition_start_value is not provided, it defaults to returning files from
    partitions <= partition_value.

    Args:
        path: The base directory path to search for partitioned files.
        filesystem: The filesystem implementation to use.
        partition_value: The upper bound partition value to compare against (e.g., timestamp, revision number).
        partition_transform: A callable that transforms partition_value into a list of partition directory names.
        limit: Maximum number of files to return. If None, returns all matching files (unlimited).
        partition_dir_parser: Callable that takes a list of partition directory names up to the current level
            and returns a parsed value if the directory should be treated as a partition, or None to skip it.
        partition_file_parser: Callable that takes a file path and returns the original partition value
            used to create that file. Enables precise file-level filtering for files within
            the same partition as the target partition_value.
        exclude_prefixes: File prefixes to exclude (same as list_directory).
        ignore_missing_path: Whether to ignore missing paths (same as list_directory).
        recursive: Whether to search for files in leaf partition directory nodes recursively (same as list_directory).
        return_unpartitioned: Whether to also return files from the unpartitioned base directory
            that can be successfully parsed by partition_file_parser and have values within the partition range.
            This enables backwards compatibility with directories that had files written before
            partitioning was introduced.
        partition_start_value: The lower bound partition value (inclusive). If None, defaults to no lower bound.
            Must be <= partition_value when provided.

    Returns:
        A list of (file_path, file_size) tuples, ordered from partitions closest to
        the input partition_value to those furthest away, up to the specified limit.
        If return_unpartitioned=True, unpartitioned files are included and ordered by their
        parsed partition values.

    Raises:
        ValueError: If partition_start_value > partition_value.

    Example:
        For partition_value with transform ["2024", "01", "15"] (Jan 15, 2024) and partition_start_value
        with transform ["2024", "01", "10"] (Jan 10, 2024):
        - Returns files from partitions like ["2024", "01", "15"], ["2024", "01", "14"], ..., ["2024", "01", "10"]
        - Does NOT return files from ["2024", "01", "09"], ["2024", "01", "16"], etc.
        - Files are ordered by proximity to the input partition.
        - If return_unpartitioned=True, also includes files directly in the base directory
          that have partition values in range ["2024-01-10", "2024-01-15"].
    """
    if exclude_prefixes is None:
        exclude_prefixes = [".", "_"]

    # Validate inputs
    if limit is not None and (not isinstance(limit, int) or limit <= 0):
        raise ValueError(f"limit must be a positive integer or None, got {limit}")

    # Validate partition_start_value if provided
    if partition_start_value is not None and partition_start_value > partition_value:
        raise ValueError(
            f"Start value ({partition_start_value}) must be less than or equal to "
            f"end value ({partition_value})"
        )

    # Get the target partition directories
    target_partitions = partition_transform(partition_value)
    if not isinstance(target_partitions, list):
        raise ValueError(
            f"partition_transform must return a list, got {type(target_partitions)}"
        )
    if not all(isinstance(dir_name, str) for dir_name in target_partitions):
        raise ValueError("All partition directory names must be strings")

    # Get the start partition directories if start_value is provided
    target_start_partitions = None
    if partition_start_value is not None:
        target_start_partitions = partition_transform(partition_start_value)
        if not isinstance(target_start_partitions, list):
            raise ValueError(
                f"partition_transform must return a list, got {type(target_start_partitions)}"
            )
        if not all(isinstance(dir_name, str) for dir_name in target_start_partitions):
            raise ValueError("All partition directory names must be strings")

    # Validate partition directory names
    for dir_name in target_partitions:
        if posixpath.sep in dir_name or (
            posixpath.altsep and posixpath.altsep in dir_name
        ):
            raise ValueError(
                f"Partition directory name cannot contain path separators: '{dir_name}'"
            )

    # Find existing partitions and collect files, stopping early when limit is reached
    collected_files = _find_existing_prior_partitions(
        path,
        filesystem,
        target_partitions,
        limit,
        exclude_prefixes,
        recursive,
        ignore_missing_path,
        partition_dir_parser,
        partition_value,
        partition_file_parser,
        partition_start_value,
        target_start_partitions,
    )

    # Optionally include unpartitioned files from the base directory
    if return_unpartitioned:
        # Calculate remaining limit after partitioned files
        remaining_limit = limit - len(collected_files) if limit is not None else None

        if remaining_limit is None or remaining_limit > 0:
            unpartitioned_files = _collect_unpartitioned_files(
                path=path,
                filesystem=filesystem,
                partition_value=partition_value,
                partition_file_parser=partition_file_parser,
                remaining_limit=remaining_limit,
                exclude_prefixes=exclude_prefixes,
                ignore_missing_path=ignore_missing_path,
                partition_start_value=partition_start_value,
                target_start_partitions=target_start_partitions,
            )
            collected_files.extend(unpartitioned_files)

    # Files are already collected in order of partition proximity due to traversal order
    # and limited to the specified amount due to early termination
    return collected_files


def _find_existing_prior_partitions(
    base_path: str,
    filesystem: FileSystem,
    target_partitions: List[str],
    limit: Optional[int],
    exclude_prefixes: Optional[List[str]],
    recursive: bool,
    ignore_missing_path: bool,
    partition_dir_parser: Callable[[List[str]], Optional[Any]],
    partition_value: Any,
    partition_file_parser: Callable[[str], Optional[Any]],
    partition_start_value: Optional[Any],
    target_start_partitions: Optional[List[str]],
) -> List[Tuple[str, int]]:
    """
    Find existing partition directories that are "prior" to the target partition
    and collect files from them, stopping early when limit is reached (if specified).
    Uses depth-first traversal with ordering by partition value.
    Returns the collected files (up to the limit, or all files if limit is None).
    """
    if not target_partitions:
        return []

    collected_files = []
    _traverse_partitions(
        base_path,
        filesystem,
        target_partitions,
        base_path,
        0,
        [],  # current_partition_path starts empty
        collected_files,
        limit,
        exclude_prefixes,
        recursive,
        ignore_missing_path,
        partition_dir_parser,
        partition_value,
        partition_file_parser,
        partition_start_value,
        target_start_partitions,
    )
    return collected_files


def _traverse_partitions(
    base_path: str,
    filesystem: FileSystem,
    target_partitions: List[str],
    current_path: str,
    depth: int,
    current_partition_path: List[str],
    collected_files: List[Tuple[str, int]],
    remaining_limit: Optional[int],
    exclude_prefixes: Optional[List[str]],
    recursive: bool,
    ignore_missing_path: bool,
    partition_dir_parser: Callable[[List[str]], Optional[Any]],
    partition_value: Any,
    partition_file_parser: Callable[[str], Optional[Any]],
    partition_start_value: Optional[Any],
    target_start_partitions: Optional[List[str]],
) -> Optional[int]:
    """
    Recursively traverse partition directories applying partition value ordering rules.
    Returns the updated remaining_limit after processing this subtree.
    """
    if depth >= len(target_partitions):
        # We've reached a complete partition path - collect files from it
        # Use the user's ignore_missing_path setting since this is the final file collection
        partition_files = list_directory(
            path=current_path,
            filesystem=filesystem,
            exclude_prefixes=exclude_prefixes,
            ignore_missing_path=False,
            recursive=recursive,
        )

        # Determine if we need file-level filtering based on partition boundaries
        # Files in partitions < target partition are guaranteed to be <= partition_value
        # Files in partitions > start partition are guaranteed to be >= partition_start_value

        # Check if we're exactly at the target partition boundary (need upper bound filtering)
        need_upper_bound_filtering = current_partition_path == target_partitions

        # Check if we're exactly at the start partition boundary (need lower bound filtering)
        # Only do expensive file-level start_value checking if we're at the boundary
        need_lower_bound_filtering = (
            partition_start_value is not None
            and target_start_partitions is not None
            and current_partition_path == target_start_partitions
        )

        if need_upper_bound_filtering or need_lower_bound_filtering:
            # We're at a partition boundary - need to filter files by their actual values
            filtered_files = []
            for file_path, file_size in partition_files:
                file_partition_value = partition_file_parser(file_path)
                if file_partition_value is None:
                    logger.warning(
                        f"Skipping invalid partition file path '{file_path}'."
                    )
                    continue

                # Apply upper bound filtering if we're at target partition
                if (
                    need_upper_bound_filtering
                    and file_partition_value > partition_value
                ):
                    logger.debug(
                        f"Skipping file '{file_path}' with partition value '{file_partition_value}' greater than target partition value '{partition_value}'."
                    )
                    continue

                # Apply lower bound filtering if we're at start partition
                if (
                    need_lower_bound_filtering
                    and file_partition_value < partition_start_value
                ):
                    logger.debug(
                        f"Skipping file '{file_path}' with partition value '{file_partition_value}' less than start partition value '{partition_start_value}'."
                    )
                    continue

                filtered_files.append((file_path, file_size))
        else:
            # We're in a partition that's guaranteed to be within bounds - no file-level filtering needed
            filtered_files = partition_files

        # Add files up to the remaining limit (or all files if unlimited)
        if remaining_limit is None:
            files_to_add = filtered_files
        else:
            files_to_add = filtered_files[:remaining_limit]
            remaining_limit -= len(files_to_add)
        collected_files.extend(files_to_add)

        # Return the updated remaining limit
        return remaining_limit

    # Get items in current directory
    # Use user's ignore_missing_path setting for the base directory (depth 0),
    # but use False for subsequent directories since we know they should exist
    should_ignore_missing = ignore_missing_path if depth == 0 else False
    items = list_directory(
        current_path, filesystem, ignore_missing_path=should_ignore_missing
    )

    # Get the target partition path up to the current depth + 1
    target_partition_path = target_partitions[: depth + 1]
    target_value_parsed = partition_dir_parser(target_partition_path)
    target_value = (
        target_value_parsed if target_value_parsed is not None else 0
    )  # fallback for invalid

    # Get the start partition path up to the current depth + 1 (if start_value provided)
    target_start_value = None
    if target_start_partitions is not None and len(target_start_partitions) > depth:
        target_start_partition_path = target_start_partitions[: depth + 1]
        target_start_value_parsed = partition_dir_parser(target_start_partition_path)
        target_start_value = (
            target_start_value_parsed if target_start_value_parsed is not None else 0
        )  # fallback for invalid

    candidates = []
    for item_path, _ in items:
        item_name = posixpath.basename(item_path)

        # Use partition_dir_parser to validate and potentially transform the directory name
        item_partition_path = current_partition_path + [item_name]
        parsed_value = partition_dir_parser(item_partition_path)
        if parsed_value is None:
            logger.warning(
                f"Skipping invalid partition directory path '{item_partition_path}'."
            )
            continue

        # Compare parsed values for ordering - now include start_value filtering
        # Only explore directories within the range [target_start_value, target_value]
        if parsed_value <= target_value and (
            target_start_value is None or parsed_value >= target_start_value
        ):
            candidates.append((parsed_value, item_name))

    # Sort by parsed timestamp value in descending order to get closest partitions first
    candidates.sort(key=lambda x: x[0], reverse=True)

    # Recursively explore each candidate
    for _, candidate_name in candidates:
        if remaining_limit is None or remaining_limit > 0:
            next_path = posixpath.join(current_path, candidate_name)
            next_partition_path = current_partition_path + [candidate_name]
            remaining_limit = _traverse_partitions(
                base_path,
                filesystem,
                target_partitions,
                next_path,
                depth + 1,
                next_partition_path,
                collected_files,
                remaining_limit,
                exclude_prefixes,
                recursive,
                ignore_missing_path,
                partition_dir_parser,
                partition_value,
                partition_file_parser,
                partition_start_value,
                target_start_partitions,
            )
        else:
            # We've already reached the limit, stop traversal
            break

    return remaining_limit


def get_file_info(
    path: str,
    filesystem: FileSystem,
    ignore_missing_path: bool = False,
) -> FileInfo:
    """Get the file info for the provided path."""
    try:
        file_info = filesystem.get_file_info(path)
    except OSError as e:
        _handle_read_os_error(e, path)
    if file_info.type == FileType.NotFound and not ignore_missing_path:
        raise FileNotFoundError(path)

    return file_info


def get_file_info_partitioned(
    path: str,
    filesystem: FileSystem,
    partition_value: Any,
    partition_transform: Callable[[Any], List[str]],
    ignore_missing_path: bool = False,
) -> FileInfo:
    """
    Get the file info for the provided path in an automatically partitioned filesystem.

    This function takes a partition value and a transform function that converts
    the partition value into a list of directory names. The file path is then
    automatically constructed by appending these directory names to the base path
    before the filename.

    Args:
        path: The base file path to get info for.
        filesystem: The filesystem implementation to use.
        partition_value: The value to partition by (can be any type).
        partition_transform: A callable that takes partition_value and returns
            a list of strings representing directory names. Each string in the
            list will become a directory in the partition hierarchy.
        ignore_missing_path: Whether to ignore missing paths or raise an error.

    Returns:
        FileInfo object for the partitioned path.

    Example:
        # Partition by date components
        def date_transform(date_obj):
            return [str(date_obj.year), str(date_obj.month).zfill(2), str(date_obj.day).zfill(2)]

        file_info = get_file_info_partitioned(
            path="/data/events.json",
            filesystem=filesystem,
            partition_value=datetime(2023, 12, 25),
            partition_transform=date_transform
        )
        # Gets info for file at: /data/2023/12/25/events.json
    """
    # Apply the partition transform to get directory names
    partition_dirs = partition_transform(partition_value)

    # Validate that partition_transform returned a list of strings
    if not isinstance(partition_dirs, list):
        raise ValueError(
            f"partition_transform must return a list, got {type(partition_dirs)}"
        )
    if not all(isinstance(dir_name, str) for dir_name in partition_dirs):
        raise ValueError("All partition directory names must be strings")

    # Validate and parse the path using posixpath
    try:
        base_dir = posixpath.dirname(path)
        filename = posixpath.basename(path)
    except Exception as e:
        raise ValueError(f"Failed to parse path as POSIX path: {path}. Error: {e}")

    # Build the full partitioned directory path using posixpath
    partitioned_path = base_dir
    for dir_name in partition_dirs:
        # Ensure directory name doesn't contain path separators that could cause issues
        if posixpath.sep in dir_name or (
            posixpath.altsep and posixpath.altsep in dir_name
        ):
            raise ValueError(
                f"Partition directory name cannot contain path separators: '{dir_name}'"
            )
        partitioned_path = posixpath.join(partitioned_path, dir_name)

    # Add the filename to the partitioned directory path
    partitioned_path = posixpath.join(partitioned_path, filename)

    # Get file info for the partitioned path using the existing get_file_info function
    return get_file_info(
        path=partitioned_path,
        filesystem=filesystem,
        ignore_missing_path=ignore_missing_path,
    )


def write_file(
    path: str,
    data: Union[str, bytes],
    filesystem: Optional[FileSystem] = None,
) -> None:
    """
    Write data to a file using any filesystem.

    Args:
        path: The file path to write to.
        data: The data to write (string or bytes).
        filesystem: The filesystem implementation to use. If None, will be inferred from the path.
    """
    if not filesystem:
        resolved_path, resolved_filesystem = resolve_path_and_filesystem(
            path=path,
            filesystem=filesystem,
        )
    else:
        resolved_path = path
        resolved_filesystem = filesystem

    # Create parent directories if they don't exist
    dir_path = posixpath.dirname(resolved_path)
    if dir_path and dir_path != ".":
        resolved_filesystem.create_dir(dir_path, recursive=True)

    # Convert string to bytes if necessary
    if isinstance(data, str):
        data = data.encode("utf-8")

    with resolved_filesystem.open_output_stream(resolved_path) as f:
        f.write(data)


def write_file_partitioned(
    path: str,
    data: Union[str, bytes],
    partition_value: Any,
    partition_transform: Callable[[Any], List[str]],
    filesystem: Optional[FileSystem] = None,
    partition_levels_above_file: int = 0,
) -> str:
    """
    Write data to a file in an automatically partitioned filesystem.

    This function takes a partition value and a transform function that converts
    the partition value into a list of directory names. The file is then written
    to a path constructed by inserting these directory names at the specified
    level above the file in the directory hierarchy.

    Args:
        path: The base file path to write to.
        data: The data to write (string or bytes).
        partition_value: The value to partition by (can be any type).
        partition_transform: A callable that takes partition_value and returns
            a list of strings representing directory names. Each string in the
            list will become a directory in the partition hierarchy.
        filesystem: The filesystem implementation to use. If None, will be inferred from the path.
        partition_levels_above_file: Number of directory levels above the file
            to insert the partition directories. Default is 0 (partitions directly
            above the file). For example, 1 means partitions are inserted one
            level above the file's immediate parent directory.

    Returns:
        The actual partitioned file path where the file was written.

    Example:
        # Partition by date components directly above file (default behavior)
        def date_transform(date_obj):
            return [str(date_obj.year), str(date_obj.month).zfill(2), str(date_obj.day).zfill(2)]

        write_file_partitioned(
            path="/data/events.json",
            data='{"event": "click"}',
            partition_value=datetime(2023, 12, 25),
            partition_transform=date_transform
        )
        # File will be written to: /data/2023/12/25/events.json

        # Partition one level above the file
        write_file_partitioned(
            path="/data/subdir/events.json",
            data='{"event": "click"}',
            partition_value=datetime(2023, 12, 25),
            partition_transform=date_transform,
            partition_levels_above_file=1
        )
        # File will be written to: /data/2023/12/25/subdir/events.json
    """
    # Apply the partition transform to get directory names
    partition_dirs = partition_transform(partition_value)

    # Validate that partition_transform returned a list of strings
    if not isinstance(partition_dirs, list):
        raise ValueError(
            f"partition_transform must return a list, got {type(partition_dirs)}"
        )
    if not all(isinstance(dir_name, str) for dir_name in partition_dirs):
        raise ValueError("All partition directory names must be strings")

    # Construct the partitioned path
    if not filesystem:
        resolved_path, resolved_filesystem = resolve_path_and_filesystem(
            path=path,
            filesystem=filesystem,
        )
    else:
        resolved_path = path
        resolved_filesystem = filesystem

    # Validate and parse the resolved path using posixpath
    try:
        base_dir = posixpath.dirname(resolved_path)
        filename = posixpath.basename(resolved_path)
    except Exception as e:
        raise ValueError(
            f"Failed to parse resolved path as POSIX path: {resolved_path}. Error: {e}"
        )

    # Build the full partitioned directory path using posixpath
    # Start with the base directory
    partitioned_path = base_dir

    # If we need to partition above the file, traverse up the directory hierarchy
    for _ in range(partition_levels_above_file):
        partitioned_path = posixpath.dirname(partitioned_path)

    # Insert partition directories at the current level
    for dir_name in partition_dirs:
        # Ensure directory name doesn't contain path separators that could cause issues
        if posixpath.sep in dir_name or (
            posixpath.altsep and posixpath.altsep in dir_name
        ):
            raise ValueError(
                f"Partition directory name cannot contain path separators: '{dir_name}'"
            )
        partitioned_path = posixpath.join(partitioned_path, dir_name)

    # Rebuild the path down to the original file location
    for _ in range(partition_levels_above_file):
        partitioned_path = posixpath.join(
            partitioned_path, posixpath.basename(base_dir)
        )
        base_dir = posixpath.dirname(base_dir)

    # Add the filename to the partitioned directory path
    partitioned_path = posixpath.join(partitioned_path, filename)

    # Create the directory structure if it doesn't exist
    # For most filesystems, we need to ensure parent directories exist
    dir_path = posixpath.dirname(partitioned_path)
    if (
        dir_path and dir_path != partitioned_path
    ):  # Only if there's actually a directory component
        resolved_filesystem.create_dir(dir_path, recursive=True)

    # Convert string to bytes if necessary
    if isinstance(data, str):
        data = data.encode("utf-8")

    # Write the file to the partitioned location
    with resolved_filesystem.open_output_stream(partitioned_path) as f:
        f.write(data)

    # Return the actual partitioned file path where the file was written
    return partitioned_path


def read_file(
    path: str,
    filesystem: Optional[FileSystem] = None,
    fail_if_not_found: bool = True,
) -> Optional[bytes]:
    """
    Read data from a file using any filesystem.

    Args:
        path: The file path to read from.
        filesystem: The filesystem implementation to use. If None, will be inferred from the path.
        fail_if_not_found: Whether to raise an error if the file is not found.

    Returns:
        The file data as bytes, or None if file not found and fail_if_not_found is False.
    """
    try:
        resolved_path, resolved_filesystem = resolve_path_and_filesystem(
            path=path,
            filesystem=filesystem,
        )

        with resolved_filesystem.open_input_stream(resolved_path) as f:
            return f.read()
    except FileNotFoundError:
        if fail_if_not_found:
            raise
        return None


def _handle_read_os_error(
    error: OSError,
    paths: Union[str, List[str]],
) -> str:
    # NOTE: this is not comprehensive yet, and should be extended as more errors arise.
    # NOTE: The latter patterns are raised in Arrow 10+, while the former is raised in
    # Arrow < 10.
    aws_error_pattern = (
        r"^(?:(.*)AWS Error \[code \d+\]: No response body\.(.*))|"
        r"(?:(.*)AWS Error UNKNOWN \(HTTP status 400\) during HeadObject operation: "
        r"No response body\.(.*))|"
        r"(?:(.*)AWS Error ACCESS_DENIED during HeadObject operation: No response "
        r"body\.(.*))$"
    )
    gcp_error_pattern = (
        r"^(?:(.*)google::cloud::Status\(UNAVAILABLE:(.*?)Couldn't resolve host name)"
    )
    if re.match(aws_error_pattern, str(error)):
        # Specially handle AWS error when reading files, to give a clearer error
        # message to avoid confusing users. The real issue is most likely that the AWS
        # S3 file credentials have not been properly configured yet.
        if isinstance(paths, str):
            # Quote to highlight single file path in error message for better
            # readability. List of file paths will be shown up as ['foo', 'boo'],
            # so only quote single file path here.
            paths = f'"{paths}"'
        raise OSError(
            (
                f"Failing to read AWS S3 file(s): {paths}. "
                "Please check that file exists and has properly configured access. "
                "You can also run AWS CLI command to get more detailed error message "
                "(e.g., aws s3 ls <file-name>). "
                "See https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3/index.html "  # noqa
                "and https://arrow.apache.org/docs/python/generated/pyarrow.fs.S3FileSystem.html "
                "for more information."
            )
        )
    elif re.match(gcp_error_pattern, str(error)):
        # Special handling for GCP errors (e.g., handling the special case of
        # requiring the filesystem to be instantiated with anonymous access to
        # read public files).
        if isinstance(paths, str):
            paths = f'"{paths}"'
        raise OSError(
            (
                f"Failing to read GCP GS file(s): {paths}. "
                "Please check that file exists and has properly configured access. "
                "If this is a public file, please instantiate a filesystem with "
                "anonymous access via `pyarrow.fs.GcsFileSystem(anonymous=True)` "
                "to read it. See https://google.aip.dev/auth/4110 and "
                "https://arrow.apache.org/docs/python/generated/pyarrow.fs.GcsFileSystem.html"  # noqa
                "for more information."
            )
        )

    else:
        raise error


def _is_local_windows_path(path: str) -> bool:
    """Determines if path is a Windows file-system location."""
    if sys.platform != "win32":
        return False

    if len(path) >= 1 and path[0] == "\\":
        return True
    if (
        len(path) >= 3
        and path[1] == ":"
        and (path[2] == "/" or path[2] == "\\")
        and path[0].isalpha()
    ):
        return True
    return False


def _unwrap_protocol(path):
    """
    Slice off any protocol prefixes on path.
    """
    if sys.platform == "win32" and _is_local_windows_path(path):
        # Represent as posix path such that downstream functions properly handle it.
        # This is executed when 'file://' is NOT included in the path.
        return pathlib.Path(path).as_posix()

    parsed = urllib.parse.urlparse(path, allow_fragments=False)  # support '#' in path
    query = "?" + parsed.query if parsed.query else ""  # support '?' in path
    netloc = parsed.netloc
    if parsed.scheme == "s3" and "@" in parsed.netloc:
        # If the path contains an @, it is assumed to be an anonymous
        # credentialed path, and we need to strip off the credentials.
        netloc = parsed.netloc.split("@")[-1]

    parsed_path = parsed.path
    # urlparse prepends the path with a '/'. This does not work on Windows
    # so if this is the case strip the leading slash.
    if (
        sys.platform == "win32"
        and not netloc
        and len(parsed_path) >= 3
        and parsed_path[0] == "/"  # The problematic leading slash
        and parsed_path[1].isalpha()  # Ensure it is a drive letter.
        and parsed_path[2:4] in (":", ":/")
    ):
        parsed_path = parsed_path[1:]

    return netloc + parsed_path + query


def _encode_url(path):
    return urllib.parse.quote(path, safe="/:")


def _decode_url(path):
    return urllib.parse.unquote(path)


def _resolve_custom_scheme(path: str) -> str:
    """Returns the resolved path if the given path follows a Ray-specific custom
    scheme. Othewise, returns the path unchanged.

    The supported custom schemes are: "local", "example".
    """
    parsed_uri = urllib.parse.urlparse(path)
    if parsed_uri.scheme == _LOCAL_SCHEME:
        path = parsed_uri.netloc + parsed_uri.path
    return path


def append_protocol_prefix_by_type(path: str, filesystem_type: FilesystemType) -> str:
    """
    Appends the appropriate protocol prefix to a path based on the filesystem type enum.

    Args:
        path: The file path (can be with or without existing protocol)
        filesystem_type: The filesystem type enum

    Returns:
        The path with the appropriate protocol prefix

    Examples:
        >>> append_protocol_prefix_by_type("/path/to/file", FilesystemType.LOCAL)
        "file:///path/to/file"
        >>> append_protocol_prefix_by_type("bucket/file", FilesystemType.S3)
        "s3://bucket/file"
    """
    # If path already has a protocol, return as is
    parsed_uri = urllib.parse.urlparse(path)
    if parsed_uri.scheme:
        return path

    if filesystem_type == FilesystemType.LOCAL:
        # For local filesystem, use file:// protocol
        # Handle Windows paths properly
        if sys.platform == "win32" and _is_local_windows_path(path):
            # Convert Windows path to posix format for URI
            posix_path = pathlib.Path(path).as_posix()
            return f"file:///{posix_path.lstrip('/')}"
        else:
            return f"file://{path}"

    elif filesystem_type == FilesystemType.S3:
        return f"s3://{path}"

    elif filesystem_type == FilesystemType.GCS:
        return f"gs://{path}"

    elif filesystem_type == FilesystemType.AZURE:
        return f"az://{path}"

    elif filesystem_type == FilesystemType.HADOOP:
        return f"hdfs://{path}"

    else:
        # For unknown filesystem types, return path as is
        return path


def epoch_timestamp_partition_transform(epoch_timestamp: int) -> List[str]:
    """
    Transform a UTC epoch timestamp into human-readable partition directory names.

    Takes a UTC epoch timestamp integer as input, automatically detects its precision
    (nanoseconds, milliseconds, or seconds), and outputs a list of human-readable
    UTC date components for partition directories.

    Args:
        epoch_timestamp: A non-negative integer UTC epoch timestamp (supports nanoseconds, milliseconds, or seconds precision).
                        Must be >= 0. For second-precision timestamps, must be at least 10 digits long.

    Returns:
        A list of 5 human-readable strings representing UTC date components:
        1. Year (4 digits, e.g., "2024")
        2. Month (2 digits, e.g., "01")
        3. Day (2 digits, e.g., "15")
        4. Hour (2 digits, e.g., "12")
        5. Minute (2 digits, e.g., "30")

    Raises:
        TypeError: If epoch_timestamp is not an integer.
        ValueError: If epoch_timestamp is negative or if second-precision timestamp has fewer than 10 digits.

    Example:
        Input: 1705323000 (2024-01-15 12:30:00 UTC)
        Output: ["2024", "01", "15", "12", "30"]
    """
    if not isinstance(epoch_timestamp, int):
        raise TypeError(
            f"epoch_timestamp must be an integer, got {type(epoch_timestamp)}"
        )

    # Detect timestamp precision by checking the magnitude
    # Current Unix epoch timestamps:
    # - Seconds: ~1.7e9 (2024)
    # - Milliseconds: ~1.7e12 (2024)
    # - Nanoseconds: ~1.7e18 (2024)

    # Validate epoch timestamp
    if epoch_timestamp < 0:
        raise ValueError(f"Epoch timestamp must be non-negative, got {epoch_timestamp}")

    timestamp_str = str(epoch_timestamp)
    if len(timestamp_str) >= 19:  # nanoseconds
        # Convert nanoseconds to seconds
        dt = datetime.fromtimestamp(epoch_timestamp / 1_000_000_000, tz=timezone.utc)
    elif len(timestamp_str) >= 13:  # milliseconds
        # Convert milliseconds to seconds
        dt = datetime.fromtimestamp(epoch_timestamp / 1_000, tz=timezone.utc)
    elif len(timestamp_str) >= 10:  # seconds
        dt = datetime.fromtimestamp(epoch_timestamp, tz=timezone.utc)
    else:
        raise ValueError(
            f"Epoch timestamp is too short ({len(timestamp_str)} digits). Expected at least 10 digits for "
            f"second-precision timestamps (e.g., 1704067200 for Jan 1, 2024)"
        )

    # Generate human-readable partition components
    partition_components = [
        f"{dt.year:04d}",  # Year (4 digits)
        f"{dt.month:02d}",  # Month (2 digits)
        f"{dt.day:02d}",  # Day (2 digits)
        f"{dt.hour:02d}",  # Hour (2 digits)
        f"{dt.minute:02d}",  # Minute (2 digits)
    ]

    return partition_components


def exponential_partition_transform(
    value: int, base: int = 1000, levels: int = 2
) -> List[str]:
    """
    Transform an integer value into exponential partition directory names.

    This function creates a hierarchical partitioning scheme based on powers of a base.
    Each output partition directory represents a capacity level: multiplier * base^(levels-level).
    The algorithm ensures the total capacity across all levels is >= the input value.

    Args:
        value: The positive integer value to partition.
        base: The base for exponential calculations (default: 1000).
        levels: The number of partition levels to generate (default: 2).

    Returns:
        A list of string partition values representing the hierarchical structure.

    Raises:
        ValueError: If value is not positive, base is <= 1, or levels is <= 0.

    Example:
        >>> exponential_partition_transform(1500, base=1000, levels=2)
        ['1000000', '2000']

        >>> exponential_partition_transform(1500000, base=1000, levels=2)
        ['2000000', '500000']
    """
    if not isinstance(value, int):
        raise TypeError(f"value must be an integer, got {type(value)}")
    if not isinstance(base, int):
        raise TypeError(f"base must be an integer, got {type(base)}")
    if not isinstance(levels, int):
        raise TypeError(f"levels must be an integer, got {type(levels)}")

    if value <= 0:
        raise ValueError(f"value must be a positive integer, got {value}")
    if base <= 1:
        raise ValueError(f"base must be greater than 1, got {base}")
    if levels <= 0:
        raise ValueError(f"levels must be positive, got {levels}")

    # Work from highest to lowest level, tracking the remaining value.
    # For each level, calculate the minimum multiplier needed to handle the remaining value,
    # then subtract that level's contribution from the remaining value for subsequent levels.
    # Each level i contributes: (multiplier_i - 1) * base^(levels-i) to total capacity,
    # except the final level which contributes: multiplier_i * base^(levels-i)

    multipliers = []
    remaining_value = value

    for level in range(levels):
        divisor = base ** (levels - level)

        # Calculate minimum multiplier needed for this level
        multiplier = (remaining_value + divisor - 1) // divisor
        multipliers.append(multiplier)

        # Update remaining value for next level
        # Each level contributes (multiplier - 1) * divisor to the total capacity
        # except the last level which contributes multiplier * divisor
        if level < levels - 1:
            remaining_value = max(0, remaining_value - (multiplier - 1) * divisor)

    # Convert multipliers to partition values and validate they don't exceed max uint64
    partitions = []
    for level, multiplier in enumerate(multipliers):
        partition_value = multiplier * (base ** (levels - level))
        if partition_value > UNSIGNED_INT64_MAX_VALUE:
            raise ValueError(
                f"Partition value {partition_value} at level {level} exceeds maximum "
                f"allowed value {UNSIGNED_INT64_MAX_VALUE}. Consider using a smaller "
                f"base ({base}) or fewer levels ({levels})."
            )
        partitions.append(str(partition_value))
    return partitions


def parse_epoch_timestamp_partitions(partition_dirs: List[str]) -> Optional[int]:
    """
    Parser function for human-readable UTC partition directory names.

    This function takes a list of partition directory names (year, month, day, hour, minute)
    and reconstructs the equivalent UTC epoch timestamp for comparison.

    Args:
        partition_dirs: List of partition directory names up to the current level.
                       Should contain human-readable UTC date components.

    Returns:
        The reconstructed UTC epoch timestamp as an integer, or None if invalid.

    Example:
        >>> parse_epoch_timestamp_partitions(["2024", "01", "15"])
        1705276800  # 2024-01-15 00:00:00 UTC
        >>> parse_epoch_timestamp_partitions(["2024", "01", "15", "12", "30"])
        1705323000  # 2024-01-15 12:30:00 UTC
    """
    if not partition_dirs:
        return None

    try:
        # Extract components from the partition directory list
        year = int(partition_dirs[0]) if len(partition_dirs) > 0 else None
        month = int(partition_dirs[1]) if len(partition_dirs) > 1 else 1
        day = int(partition_dirs[2]) if len(partition_dirs) > 2 else 1
        hour = int(partition_dirs[3]) if len(partition_dirs) > 3 else 0
        minute = int(partition_dirs[4]) if len(partition_dirs) > 4 else 0

        # Validate ranges
        if not (1 <= month <= 12):
            return None
        if not (1 <= day <= 31):
            return None
        if not (0 <= hour <= 23):
            return None
        if not (0 <= minute <= 59):
            return None

        # Create UTC datetime object and convert to epoch timestamp
        dt = datetime(year, month, day, hour, minute, 0, tzinfo=timezone.utc)
        return int(dt.timestamp())

    except (ValueError, IndexError):
        # Invalid date components
        return None


def parse_exponential_partitions(
    partition_dirs: List[str], base: int = 1000, levels: int = 2
) -> Optional[int]:
    """
    Parser function for exponential partition directory names.

    This function takes a list of partition directory names created by exponential partition transform
    and converts them into an equivalent integer value for comparison based on the actual
    contribution of each partition level to the total capacity.

    The exponential partition transform creates hierarchical capacity levels where:
    - Each level i contributes: (multiplier_i - 1) * base^(levels-i) to total capacity,
    - Except the final level which contributes: multiplier_i * base^(levels-i)

    Args:
        partition_dirs: List of partition directory names up to the current level.
                       Should contain numeric capacity values from exponential_partition_transform.
        base: The base value used in exponential_partition_transform (default: 1000).
        levels: The expected number of levels from exponential_partition_transform (default: 2).

    Returns:
        The actual capacity contribution as an integer for comparison purposes, or None if invalid.

    Example:
        >>> parse_exponential_partitions(["1000000", "2000"], base=1000, levels=2)  # multipliers [1, 2]
        2000  # (1-1)*1000000 + 2*1000 = 0 + 2000 = 2000
        >>> parse_exponential_partitions(["2000000", "500000"], base=1000, levels=2)  # multipliers [2, 500]
        1500000  # (2-1)*1000000 + 500*1000 = 1000000 + 500000 = 1500000
        >>> parse_exponential_partitions([])
        None
    """
    if not partition_dirs:
        return None

    try:
        # Parse partition directories and extract multipliers
        # Use the provided base and levels parameters
        actual_levels = len(partition_dirs)

        if actual_levels == 0:
            return None

        # Validate that we have the expected number of levels
        if actual_levels != levels:
            # For partial paths (fewer levels than expected), we can still parse
            # but we need to adjust our calculation
            working_levels = actual_levels
        else:
            working_levels = levels

        multipliers = []
        for i, dir_name in enumerate(partition_dirs):
            if not dir_name.isdigit():
                return None
            partition_value = int(dir_name)

            # Validate that partition values are positive (exponential transform constraint)
            if partition_value <= 0:
                return None

            # Extract multiplier: partition_value = multiplier * base^(working_levels-i)
            divisor = base ** (working_levels - i)

            # Validate that partition_value is an exact multiple of the divisor
            # exponential transform ALWAYS generates exact multiples, so any non-multiple is invalid
            if partition_value % divisor != 0:
                return None

            multiplier = partition_value // divisor

            # Validate that multiplier > 0 (partition_value must be >= base^(working_levels-i))
            # This ensures we only parse valid partition values that could be generated by exponential transform
            if multiplier == 0:
                return None

            multipliers.append(multiplier)

        # Calculate actual capacity contribution
        total_contribution = 0
        for i, multiplier in enumerate(multipliers):
            divisor = base ** (working_levels - i)
            if i < working_levels - 1:
                # Non-final levels: (multiplier - 1) * base^(working_levels-i)
                # Ensure we don't get negative contributions
                contribution = max(0, (multiplier - 1) * divisor)
            else:
                # Final level: multiplier * base^(working_levels-i)
                contribution = multiplier * divisor

            # Check for overflow before adding
            if total_contribution > UNSIGNED_INT64_MAX_VALUE - contribution:
                return None  # Overflow would occur, invalid partition
            total_contribution += contribution

        # Validate final result doesn't exceed maximum
        if total_contribution > UNSIGNED_INT64_MAX_VALUE:
            return None

        # Ensure the result is always positive (minimum 1 for valid partitions)
        return max(1, total_contribution)

    except (ValueError, IndexError, ZeroDivisionError):
        return None


def remove_exponential_partitions(
    path: str,
    base: int = 1000,
    levels: int = 2,
    is_directory_path: bool = False,
) -> str:
    """
    Remove exponential partitions from a file path or directory path.

    Takes a path that may contain exponential partitions and returns an unpartitioned version
    of the same path, or returns the path unchanged if there are no exponential partitions.

    Args:
        path: The file or directory path that may contain exponential partitions.
        base: The base used for exponential partitioning (default: 1000).
        levels: The number of partition levels to remove (default: 2).
        is_directory_path: Whether to treat the path as a directory path (default: False).
                          If True, the entire path is treated as directory components.
                          If False, the path is treated as a file path with directory + filename.
    Returns:
        The path with exponential partitions removed, or the original path if no valid
        exponential partitions are found.

    Raises:
        ValueError: If base <= 1 or levels <= 0.

    Example:
        >>> remove_exponential_partitions("/data/1000000/2000/file.json")
        '/data/file.json'
        >>> remove_exponential_partitions("/data/file.json")
        '/data/file.json'
        >>> remove_exponential_partitions("/data/1000000/2000/3000/file.json", levels=3)
        '/data/file.json'
        >>> remove_exponential_partitions("/data/1000000/2000", is_directory_path=True)
        '/data'
        >>> remove_exponential_partitions("/data/1000000/2000/file.json", is_directory_path=True)
        '/data/1000000/2000/file.json'  # No partitions found in directory components
        >>> remove_exponential_partitions("/data/1000000/2000/3000", levels=3, is_directory_path=True)
        '/data'
    """
    if not isinstance(path, str):
        raise TypeError(f"path must be a string, got {type(path)}")
    if not isinstance(base, int):
        raise TypeError(f"base must be an integer, got {type(base)}")
    if not isinstance(levels, int):
        raise TypeError(f"levels must be an integer, got {type(levels)}")

    if base <= 1:
        raise ValueError(f"base must be greater than 1, got {base}")
    if levels <= 0:
        raise ValueError(f"levels must be positive, got {levels}")

    if is_directory_path:
        # Treat the entire path as directory components (for directory-only paths)
        filename = None
        dir_parts = path.split(posixpath.sep) if path else []
    else:
        # Split path into directory and filename components (for file paths)
        base_dir = posixpath.dirname(path)
        filename = posixpath.basename(path)
        # Split base directory into components
        dir_parts = base_dir.split(posixpath.sep) if base_dir else []

    # Check if we have enough directory components for the expected levels
    if len(dir_parts) < levels:
        return path  # Not enough directories to contain partitions

    # Check the last 'levels' directories to see if they're valid exponential partitions
    candidate_partitions = dir_parts[-levels:]

    # Use the exponential partition parser to validate these are real partitions
    parsed_value = parse_exponential_partitions(candidate_partitions, base, levels)
    if parsed_value is None:
        # Not valid exponential partitions, return original path
        return path

    # Remove the partition directories
    unpartitioned_dir_parts = dir_parts[:-levels]
    unpartitioned_dir = (
        posixpath.sep.join(unpartitioned_dir_parts) if unpartitioned_dir_parts else ""
    )

    # Reconstruct the path
    return (
        posixpath.join(unpartitioned_dir, filename) if filename else unpartitioned_dir
    )


def absolute_path_to_relative(root: str, target: str) -> str:
    """
    Takes an absolute root directory path and target absolute path to
    relativize with respect to the root directory. Returns the target
    path relative to the root directory path. Raises an error if the
    target path is not contained in the given root directory path, if
    either path is not an absolute path, or if the target path is equal
    to the root directory path.
    """
    root_path = PosixPath(root)
    target_path = PosixPath(target)
    # TODO (martinezdavid): Check why is_absolute() fails for certain Delta paths
    # if not root_path.is_absolute() or not target_path.is_absolute():
    #     raise ValueError("Both root and target must be absolute paths.")
    if root_path == target_path:
        raise ValueError(
            "Target and root are identical, but expected target to be a child of root."
        )
    try:
        relative_path = target_path.relative_to(root_path)
    except ValueError:
        raise ValueError("Expected target to be a child of root.")
    return str(relative_path)
