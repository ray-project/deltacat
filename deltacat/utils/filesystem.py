from __future__ import annotations

import re
from typing import Optional, Tuple, Union, List

import sys
import urllib
import pathlib

import pyarrow
import pyarrow as pa
from pyarrow.fs import (
    _resolve_filesystem_and_path,
    FileSelector,
    FileInfo,
    FileType,
    FileSystem,
    FSSpecHandler,
    PyFileSystem,
)

_LOCAL_SCHEME = "local"


def resolve_paths_and_filesystem(
    paths: Union[str, List[str]],
    filesystem: pyarrow.fs.FileSystem = None,
) -> Tuple[List[str], pyarrow.fs.FileSystem]:
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
            filesystem = resolved_filesystem
        elif need_unwrap_path_protocol:
            resolved_path = _unwrap_protocol(resolved_path)
        resolved_path = filesystem.normalize_path(resolved_path)
        resolved_paths.append(resolved_path)

    return resolved_paths, filesystem


def resolve_path_and_filesystem(
    path: str,
    filesystem: Optional[pyarrow.fs.FileSystem] = None,
) -> Tuple[str, pyarrow.fs.FileSystem]:
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
    filesystem: pyarrow.fs.FileSystem,
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
        if isinstance(e, FileNotFoundError):
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
        if any(relative.startswith(prefix) for prefix in exclude_prefixes):
            continue
        out.append((file_path, file_.size))
    # We sort the paths to guarantee a stable order.
    return sorted(out)


def get_file_info(
    path: str,
    filesystem: pyarrow.fs.FileSystem,
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
