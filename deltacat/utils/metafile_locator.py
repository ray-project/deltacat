import posixpath
import pyarrow.fs

from deltacat.storage.model.partition import PartitionLocator
from deltacat.utils.filesystem import resolve_path_and_filesystem

"""
Helper functions to work with deltacat metadata paths.
TODO: Replace with direct calls to Deltacat storage interface.
"""


def _find_first_child_with_rev(
    parent_path: str, filesystem: pyarrow.fs.FileSystem
) -> str:
    """
    Walks the filesystem to find the first child directory with a `rev/` folder.

    This is a temporary solution to locate the first Namespace and Table directories.
    The Deltacat Storage interface will provide a more robust way to locate these directories.

    param: parent_path: The parent directory to search for a child with a `rev/` folder.
    param: filesystem: The filesystem to search for the child directory.
    returns: The name of the first child directory with a `rev/` folder.
    """
    children = filesystem.get_file_info(
        pyarrow.fs.FileSelector(parent_path, allow_not_found=True)
    )
    for child in children:
        if child.type == pyarrow.fs.FileType.Directory:
            rev_path = posixpath.join(child.path, "rev")
            if filesystem.get_file_info(rev_path).type == pyarrow.fs.FileType.Directory:
                return child.base_name
    raise ValueError(f"No directory with 'rev/' found under {parent_path}")


def _find_table_path(root_path: str, filesystem: pyarrow.fs.FileSystem):
    """
    Finds a path with structure: root/namespace_id/table_id
    Uses _find_first_child_with_rev to determine the namespace and table ids.

    param: root_path: The root directory to search for the namespace and table directories.
    param: filesystem: The filesystem to search for the namespace and table directories.
    returns: The path to the table directory.
    raises: ValueError if the namespace or table directories are not found.
    """
    try:
        # Find Namespace (first directory under root with rev/)
        namespace_id = _find_first_child_with_rev(root_path, filesystem)
        namespace_path = posixpath.join(root_path, namespace_id)

        # Find Table (first directory under namespace with rev/)
        table_id = _find_first_child_with_rev(namespace_path, filesystem)
        return posixpath.join(namespace_path, table_id)

    except ValueError as e:
        raise ValueError(f"Failed to locate Namespace or Table: {e}") from e


def _find_partition_path(root_path: str, locator: PartitionLocator) -> str:
    """
    Finds the path to the partition directory for the specified locator.

    param: root_uri: The root URI of the dataset.
    param: locator: The DeltaLocator for the delta.
    returns: The path to the delta directory.
    """
    root_path, filesystem = resolve_path_and_filesystem(root_path)
    return posixpath.join(
        _find_table_path(root_path, filesystem),
        locator.table_version,
        locator.stream_id,
    )
