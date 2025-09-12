from __future__ import annotations

import re
import logging
import calendar
from typing import Optional, Tuple, Union, List, Callable, Any
from datetime import datetime, timedelta
from enum import Enum

import sys
import urllib
import pathlib
import posixpath

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
    return sorted(out)


def list_directory_partitioned(
    path: str,
    filesystem: FileSystem,
    partition_value: Any,
    partition_transform: Callable[[Any], List[str]],
    limit: int,
    partition_dir_parser: Callable[[str, int], Optional[Any]],
    exclude_prefixes: Optional[List[str]] = None,
    ignore_missing_path: bool = False,
    recursive: bool = False,
) -> List[Tuple[str, int]]:
    """
    List files in a partitioned filesystem directory structure, returning files from partitions
    that are "prior" to the input partition according to lexicographic ordering.

    This function implements complex partition traversal logic where files are returned from
    partitions whose partition directories satisfy specific ordering constraints relative to
    the input partition_value's transformed partition directories.

    Args:
        path: The base directory path to search for partitioned files.
        filesystem: The filesystem implementation to use.
        partition_value: The partition value to compare against (e.g., timestamp).
        partition_transform: A callable that transforms partition_value into a list of partition directory names.
        limit: Maximum number of files to return.
        exclude_prefixes: File prefixes to exclude (same as list_directory).
        ignore_missing_path: Whether to ignore missing paths (same as list_directory).
        recursive: Whether to search recursively (same as list_directory).
        partition_dir_parser: Callable that takes a directory name and level (depth) and returns a parsed value
            if the directory should be treated as a partition, or None to skip it.

    Returns:
        A list of (file_path, file_size) tuples, ordered from partitions closest to
        the input partition_value to those furthest away, up to the specified limit.

    Example:
        For partition_value with transform ["2024", "01", "15"] (Jan 15, 2024):
        - Returns files from partitions like ["2024", "01", "14"], ["2024", "01", "13"], etc.
        - Does NOT return files from ["2024", "01", "15"], ["2024", "01", "16"], etc.
        - Files are ordered by proximity to the input partition.
    """
    if exclude_prefixes is None:
        exclude_prefixes = [".", "_"]

    # Validate inputs
    if not isinstance(limit, int) or limit <= 0:
        raise ValueError(f"limit must be a positive integer, got {limit}")

    # Get the target partition directories
    target_partitions = partition_transform(partition_value)
    if not isinstance(target_partitions, list):
        raise ValueError(f"partition_transform must return a list, got {type(target_partitions)}")
    if not all(isinstance(dir_name, str) for dir_name in target_partitions):
        raise ValueError("All partition directory names must be strings")

    # Validate partition directory names
    for dir_name in target_partitions:
        if posixpath.sep in dir_name or (posixpath.altsep and posixpath.altsep in dir_name):
            raise ValueError(f"Partition directory name cannot contain path separators: '{dir_name}'")

    # Find existing partitions and collect files, stopping early when limit is reached
    collected_files = _find_existing_prior_partitions(
        path,
        filesystem,
        target_partitions,
        limit,
        exclude_prefixes,
        recursive,
        ignore_missing_path,
        partition_dir_parser
    )

    # Files are already collected in order of partition proximity due to traversal order
    # and limited to the specified amount due to early termination
    return collected_files


def _find_existing_prior_partitions(
    base_path: str,
    filesystem: FileSystem,
    target_partitions: List[str],
    limit: int,
    exclude_prefixes: Optional[List[str]],
    recursive: bool,
    ignore_missing_path: bool,
    partition_dir_parser: Callable[[str, int], Optional[Any]]
) -> List[Tuple[str, int]]:
    """
    Find existing partition directories that are "prior" to the target partition
    and collect files from them, stopping early when limit is reached.
    Uses depth-first traversal with lexicographic ordering.
    Returns the collected files (up to the limit).
    """
    if not target_partitions:
        return []

    existing_partitions = []  # Still track partitions for debugging if needed
    collected_files = []
    _traverse_partitions(
        base_path,
        filesystem,
        target_partitions,
        base_path,
        0,
        existing_partitions,
        collected_files,
        limit,
        exclude_prefixes,
        recursive,
        ignore_missing_path,
        partition_dir_parser
    )
    return collected_files


def _traverse_partitions(
    base_path: str,
    filesystem: FileSystem,
    target_partitions: List[str],
    current_path: str,
    depth: int,
    result: List[str],
    collected_files: List[Tuple[str, int]],
    remaining_limit: int,
    exclude_prefixes: Optional[List[str]],
    recursive: bool,
    ignore_missing_path: bool,
    partition_dir_parser: Callable[[str, int], Optional[Any]]
) -> int:
    """
    Recursively traverse partition directories applying lexicographic ordering rules.
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
            recursive=recursive
        )

        # Add files up to the remaining limit
        files_to_add = partition_files[:remaining_limit]
        collected_files.extend(files_to_add)

        # Update remaining limit
        remaining_limit -= len(files_to_add)

        # Return the updated remaining limit
        return remaining_limit

    # Get items in current directory
    # Use user's ignore_missing_path setting for the base directory (depth 0),
    # but use False for subsequent directories since we know they should exist
    should_ignore_missing = ignore_missing_path if depth == 0 else False
    items = list_directory(current_path, filesystem, ignore_missing_path=should_ignore_missing)

    # Filter candidates based on lexicographic ordering
    target_value_raw = target_partitions[depth]
    # Parse the target value using the same parser for consistent types
    target_value_parsed = partition_dir_parser(target_value_raw, depth)
    target_value = target_value_parsed if target_value_parsed is not None else target_value_raw
    is_last_level = (depth == len(target_partitions) - 1)

    candidates = []
    for item_path, _ in items:
        item_name = posixpath.basename(item_path)

        # Use partition_dir_parser to validate and potentially transform the directory name
        parsed_value = partition_dir_parser(item_name, depth)
        if parsed_value is None:
            logger.warning(f"Directory '{item_name}' was skipped by partition_dir_parser")
            continue
        if is_last_level:
            # Last level: strictly less than
            if parsed_value < target_value:
                candidates.append((parsed_value, item_name))
        else:
            # Earlier levels: less than or equal
            if parsed_value <= target_value:
                candidates.append((parsed_value, item_name))

    # Sort by parsed value in descending order to get closest partitions first
    candidates.sort(key=lambda x: x[0], reverse=True)

    # Recursively explore each candidate
    for _, candidate_name in candidates:
        if remaining_limit > 0:
            next_path = posixpath.join(current_path, candidate_name)
            remaining_limit = _traverse_partitions(
                base_path,
                filesystem,
                target_partitions,
                next_path,
                depth + 1,
                result,
                collected_files,
                remaining_limit,
                exclude_prefixes,
                recursive,
                ignore_missing_path,
                partition_dir_parser,
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
        raise ValueError(f"partition_transform must return a list, got {type(partition_dirs)}")
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
        if posixpath.sep in dir_name or (posixpath.altsep and posixpath.altsep in dir_name):
            raise ValueError(f"Partition directory name cannot contain path separators: '{dir_name}'")
        partitioned_path = posixpath.join(partitioned_path, dir_name)

    # Add the filename to the partitioned directory path
    partitioned_path = posixpath.join(partitioned_path, filename)

    # Get file info for the partitioned path
    try:
        file_info = filesystem.get_file_info(partitioned_path)
    except OSError as e:
        _handle_read_os_error(e, partitioned_path)
    if file_info.type == FileType.NotFound and not ignore_missing_path:
        raise FileNotFoundError(partitioned_path)

    return file_info


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
    resolved_path, resolved_filesystem = resolve_path_and_filesystem(
        path=path,
        filesystem=filesystem,
    )

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
) -> None:
    """
    Write data to a file in an automatically partitioned filesystem.

    This function takes a partition value and a transform function that converts
    the partition value into a list of directory names. The file is then written
    to a path constructed by appending these directory names to the base path.

    Args:
        path: The base file path to write to.
        data: The data to write (string or bytes).
        partition_value: The value to partition by (can be any type).
        partition_transform: A callable that takes partition_value and returns
            a list of strings representing directory names. Each string in the
            list will become a directory in the partition hierarchy.
        filesystem: The filesystem implementation to use. If None, will be inferred from the path.

    Example:
        # Partition by date components
        def date_transform(date_obj):
            return [str(date_obj.year), str(date_obj.month).zfill(2), str(date_obj.day).zfill(2)]

        write_file_partitioned(
            path="/data/events.json",
            data='{"event": "click"}',
            partition_value=datetime(2023, 12, 25),
            partition_transform=date_transform
        )
        # File will be written to: /data/2023/12/25/events.json
    """
    # Apply the partition transform to get directory names
    partition_dirs = partition_transform(partition_value)

    # Validate that partition_transform returned a list of strings
    if not isinstance(partition_dirs, list):
        raise ValueError(f"partition_transform must return a list, got {type(partition_dirs)}")
    if not all(isinstance(dir_name, str) for dir_name in partition_dirs):
        raise ValueError("All partition directory names must be strings")

    # Construct the partitioned path
    resolved_path, resolved_filesystem = resolve_path_and_filesystem(
        path=path,
        filesystem=filesystem,
    )

    # Validate and parse the resolved path using posixpath
    try:
        base_dir = posixpath.dirname(resolved_path)
        filename = posixpath.basename(resolved_path)
    except Exception as e:
        raise ValueError(f"Failed to parse resolved path as POSIX path: {resolved_path}. Error: {e}")

    # Build the full partitioned directory path using posixpath
    partitioned_path = base_dir
    for dir_name in partition_dirs:
        # Ensure directory name doesn't contain path separators that could cause issues
        if posixpath.sep in dir_name or (posixpath.altsep and posixpath.altsep in dir_name):
            raise ValueError(f"Partition directory name cannot contain path separators: '{dir_name}'")
        partitioned_path = posixpath.join(partitioned_path, dir_name)

    # Add the filename to the partitioned directory path
    partitioned_path = posixpath.join(partitioned_path, filename)

    # Create the directory structure if it doesn't exist
    # For most filesystems, we need to ensure parent directories exist
    dir_path = posixpath.dirname(partitioned_path)
    if dir_path and dir_path != partitioned_path:  # Only if there's actually a directory component
        resolved_filesystem.create_dir(dir_path, recursive=True)

    # Convert string to bytes if necessary
    if isinstance(data, str):
        data = data.encode("utf-8")

    # Write the file to the partitioned location
    with resolved_filesystem.open_output_stream(partitioned_path) as f:
        f.write(data)


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
    Partition transform function for epoch timestamps.

    Takes an epoch timestamp integer as input, automatically detects its precision
    (nanoseconds, milliseconds, or seconds), and outputs a list of second-precision
    epoch timestamps representing the end of various time periods relative to the input timestamp.

    Args:
        epoch_timestamp: An integer epoch timestamp (supports nanoseconds, milliseconds, or seconds precision)

    Returns:
        A list of 5 second-precision epoch timestamp strings representing:
        1. End of the current year as the input timestamp
        2. End of the current month as the input timestamp
        3. End of the current day as the input timestamp
        4. End of the current hour as the input timestamp
        5. End of the current minute as the input timestamp

    Example:
        Input: 1704067200 (2023-12-31 16:00:00 in seconds)
        Output: [
            "1704095999",  # End of year: 2023-12-31 23:59:59
            "1704095999",  # End of month: 2023-12-31 23:59:59
            "1704095999",  # End of day: 2023-12-31 23:59:59
            "1704070799",  # End of hour: 2023-12-31 16:59:59
            "1704067259"   # End of minute: 2023-12-31 16:00:59
        ]
    """
    if not isinstance(epoch_timestamp, int):
        raise ValueError(f"epoch_timestamp must be an integer, got {type(epoch_timestamp)}")

    # Detect timestamp precision by checking the magnitude
    # Current Unix epoch timestamps:
    # - Seconds: ~1.7e9 (2024)
    # - Milliseconds: ~1.7e12 (2024)
    # - Nanoseconds: ~1.7e18 (2024)

    timestamp_str = str(epoch_timestamp)
    if len(timestamp_str) >= 19:  # Likely nanoseconds (19 digits for 2024 timestamps)
        # Convert nanoseconds to seconds
        dt = datetime.fromtimestamp(epoch_timestamp / 1_000_000_000)
    elif len(timestamp_str) >= 13:  # Likely milliseconds (13 digits for 2024 timestamps)
        # Convert milliseconds to seconds
        dt = datetime.fromtimestamp(epoch_timestamp / 1_000)
    else:  # Likely seconds
        dt = datetime.fromtimestamp(epoch_timestamp)

    # Calculate end timestamps for each period
    partition_timestamps = []

    # 1. End of year
    year_end = datetime(dt.year, 12, 31, 23, 59, 59)
    partition_timestamps.append(str(int(year_end.timestamp())))

    # 2. End of month
    _, last_day = calendar.monthrange(dt.year, dt.month)
    month_end = datetime(dt.year, dt.month, last_day, 23, 59, 59)
    partition_timestamps.append(str(int(month_end.timestamp())))

    # 3. End of day
    day_end = datetime(dt.year, dt.month, dt.day, 23, 59, 59)
    partition_timestamps.append(str(int(day_end.timestamp())))

    # 4. End of hour
    hour_end = datetime(dt.year, dt.month, dt.day, dt.hour, 59, 59)
    partition_timestamps.append(str(int(hour_end.timestamp())))

    # 5. End of minute
    minute_end = datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, 59)
    partition_timestamps.append(str(int(minute_end.timestamp())))

    return partition_timestamps


def epoch_timestamp_partition_parser(dir_name: str, level: int) -> Optional[int]:
    """
    Parser function for validating epoch timestamp partition directory names.

    This function validates that a directory name is a valid epoch timestamp string
    and returns the timestamp value for lexicographic comparison.

    Args:
        dir_name: Directory name to validate (should be a string representation of an epoch timestamp)
        level: Partition level (0=year, 1=month, 2=day, 3=hour, 4=minute) - used for consistency

    Returns:
        The timestamp value as an integer if valid, or None if the directory name is not a valid timestamp.

    Example:
        >>> epoch_timestamp_partition_parser("1704095999", 0)
        1704095999
        >>> epoch_timestamp_partition_parser("invalid_name", 1)
        None
    """
    # Validate level parameter
    if not (0 <= level <= 4):
        raise ValueError(f"Invalid partition level {level}. Must be between 0-4 (year=0, month=1, day=2, hour=3, minute=4)")

    if not dir_name:
        return None

    try:
        # Validate that it's a numeric string
        timestamp_value = int(dir_name)

        # Basic validation: check that it's a reasonable timestamp
        # Current Unix epoch (seconds): ~1.7e9 (2024)
        # Allow some margin for future dates
        if timestamp_value < 0 or timestamp_value > 3_000_000_000:  # Up to ~2065
            return None

        return timestamp_value

    except ValueError:
        # Not a valid integer string
        return None
