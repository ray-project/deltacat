import re
from typing import Optional, Tuple, List, Union

from pyarrow.fs import FileSystem

from deltacat.utils import pyarrow
from ray.data.datasource.path_util import _resolve_paths_and_filesystem
from pyarrow.fs import FileInfo
from pyarrow.fs import FileType

# TODO this is copy pasted from https://github.com/ray-project/deltacat/pull/455/files
# Merge with utils pushed from that PR

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
                    "and https://docs.ray.io/en/latest/data/creating-datasets.html#reading-from-remote-storage "  # noqa
                    "for more information."
                )
            )
        else:
            raise error


def filesystem(
    path: str,
    filesystem: Optional[FileSystem] = None,
) -> Tuple[str, FileSystem]:
    """
    Normalizes the input path and resolves a corresponding file system.
    :param path: A file or directory path.
    :param filesystem: File system to use for path IO.
    :return: Normalized path and resolved file system for that path.
    """
    # TODO(pdames): resolve and cache filesystem at catalog root level
    #   ensure returned paths are normalized as posix paths
    paths, filesystem = _resolve_paths_and_filesystem(
        paths=path,
        filesystem=filesystem,
    )
    assert len(paths) == 1, len(paths)
    return paths[0], filesystem


def _expand_directory(
    path: str,
    filesystem: FileSystem,
    exclude_prefixes: Optional[List[str]] = None,
    ignore_missing_path: bool = False,
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
    Returns:
        An iterator of (file_path, file_size) tuples.
    """
    if exclude_prefixes is None:
        exclude_prefixes = [".", "_"]

    from pyarrow.fs import FileSelector

    selector = FileSelector(path, recursive=True, allow_not_found=ignore_missing_path)
    files = filesystem.get_file_info(selector)
    base_path = selector.base_dir
    out = []
    for file_ in files:
        if not file_.is_file:
            continue
        file_path = file_.path
        if not file_path.startswith(base_path):
            continue
        relative = file_path[len(base_path) :]
        if any(relative.startswith(prefix) for prefix in exclude_prefixes):
            continue
        out.append((file_path, file_.size))
    # We sort the paths to guarantee a stable order.
    return sorted(out)

def _get_file_infos(
    path: str,
    filesystem: FileSystem,
    ignore_missing_path: bool = False,
) -> List[Tuple[str, int]]:
    """Get the file info for all files at or under the provided path."""
    file_infos = []
    try:
        file_info = filesystem.get_file_info(path)
    except OSError as e:
        _handle_read_os_error(e, path)

    if file_info.type == FileType.Directory:
        for (file_path, file_size) in _expand_directory(path, filesystem):
            file_infos.append((file_path, file_size))
    elif file_info.type == FileType.File:
        file_infos.append((path, file_info.size))
    elif file_info.type == FileType.NotFound and ignore_missing_path:
        pass
    else:
        raise FileNotFoundError(path)

    return file_infos


def _get_file_info(
    path: str,
    filesystem: FileSystem,
    ignore_missing_path: bool = False,
) -> Union[FileInfo, List[FileInfo]]:
    """Get the file info or list of file infos for the provided path."""
    try:
        file_info = filesystem.get_file_info(path)
    except OSError as e:
        _handle_read_os_error(e, path)
    if file_info.type == FileType.NotFound and not ignore_missing_path:
        raise FileNotFoundError(path)

    return file_info