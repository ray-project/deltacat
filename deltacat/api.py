import functools
from typing import Any, Optional

import ray.data

import deltacat as dc
from deltacat.catalog import Catalog

from deltacat import DistributedDataset
from storage import StreamLocator
from deltacat.storage import metastore

"""
    # CLI Example of Copying from Source to Dest without file conversion
    # (i.e., register only - shallow copy):
    $ dcat cp json+s3://my_bucket/vpc_flow_logs/ dc://security_lake/vpc_flow_logs/json_table
    $ dcat cp json+s3://my_bucket/vpc_flow_logs/ dc://security_lake/vpc_flow_logs/json_table

    # CLI Example of Copying from Source to Dest without file conversion
    # (i.e., register only - deep copy):
    $ dcat cp json+s3://my_bucket/vpc_flow_logs/ dc://security_lake/vpc_flow_logs/json_table -r
    # The above command will make a deep copy of all JSON files found in the source
    # to the catalog data file directory in the destination.

    # CLI Example of Copying from Source to Dest with file conversion
    # (i.e., deep copy with file content type transformation):
    $ dcat convert json+s3://my_bucket/vpc_flow_logs/ dc://security_lake/vpc_flow_logs/ --type FEATHER
    # The above command will read JSON files found in the source, transform them to
    # Arrow Feather files, and register them in the destination.

    # Python Example of Copying from Source to Dest with file conversion
    # (i.e., deep copy with file content type transformation):
    >>> ds = dc.get("json+s3://my_bucket/vpc_flow_logs/")
    >>> dc.put("dc://security_lake/vpc_flow_logs/", dataset=ds, type=ContentType.FEATHER)
    # Or, equivalently, we can do the write directly from the dataset:
    >>> ds.write_deltacat("dc://security_lake/vpc_flow_logs/", type=ContentType.FEATHER)
"""



def copy(source, destination, recursive=False):
    src_obj = get(source) if not recursive else list(source, long=True)
    dc_dest_url = DcUrl(destination)
    # TODO(pdames): Add writer with support for Ray Dataset DeltaCAT Sink &
    #  Recursive DeltaCAT source object copies. Ideally, the Ray Dataset read
    #  is lazy, and only indexes metadata about the objects at source instead
    #  of eagerly converting them to PyArrow-based Blocks.
    dc_dest_url.writer(src_obj, recursive=recursive)

    src_parts = source.split("/")
    src_parts = [part for part in src_parts if part]
    dst_parts = destination.split("/")
    dst_parts = [part for part in dst_parts if part]
    if not dc.is_initialized():
        raise ValueError("Catalog not initialized.")
    if len(src_parts) != len(dst_parts) and len(src_parts) != len(dst_parts) + 1:
        # TODO(pdames): Better error message.
        raise ValueError(
            f"Cannot copy {source} to {destination}. "
            f"Source and destination must share the same type."
        )
    if len(src_parts) == 1:
        # copy the given catalog
        raise NotImplementedError
    elif len(src_parts) == 2:
        # TODO(pdames): Make catalog specification optional if there is only
        #  one catalog (e.g., auto-retrieve src_parts[0]/dst_parts[0])
        # copy the given namespace
        src_namespace_name = src_parts[1]
        dst_catalog_name = dst_parts[0]
        dst_namespace_name = dst_parts[1] if len(dst_parts) >= 2 else src_namespace_name
        new_namespace = dc.create_namespace(
            namespace=dst_namespace_name,
            properties=src_obj.properties,
            catalog=dst_catalog_name,
        )
        return new_namespace
    elif len(src_parts) == 3:
        # copy the given table
        raise NotImplementedError
    elif len(src_parts) == 4:
        # copy the given table version
        raise NotImplementedError
    elif len(src_parts) == 5:
        # copy the given stream
        raise NotImplementedError
    elif len(src_parts) == 6:
        # copy the given partition
        raise NotImplementedError
    elif len(src_parts) == 7:
        # copy the given partition delta
        raise NotImplementedError
    raise ValueError(f"Invalid path: {src_parts}")


def concat(source, destination):
    raise NotImplementedError


def delete(source):
    raise NotImplementedError


def move(source, destination):
    raise NotImplementedError


def list(path, long=False):
    raise NotImplementedError


def _ensure_dc_initialized():
    if not dc.is_initialized():
        # TODO(pdames): Re-initialize DeltaCAT with all catalogs from the
        #  last session.
        raise RuntimeError(
            "DeltaCAT is not initialized. Please call `dc.init()` and try again."
        )


def get(
    path,
    *args,
    **kwargs,
) -> Union[Metafile, DistributedDataset]:
    dc_url = DcUrl(path)
    return dc_url.reader(*args, **kwargs)


def put(path, *args, **kwargs) -> Any:
    parts = path.split("/")
    parts = [part for part in parts if part]
    if len(parts) == 1:
        # TODO(pdames): Save all catalogs registered from the last session on
        #  disk so that users don't need to re-initialize them every time.
        # register the given catalog
        catalog_name = parts[0]
        # Initialize default catalog using kwargs
        catalog = Catalog(**kwargs)
        return dc.put_catalog(catalog_name, catalog)
    elif len(parts) == 2:
        # register the given namespace
        catalog_name = parts[0]
        namespace_name = parts[1]
        if not dc.is_initialized():
            # TODO(pdames): Re-initialize DeltaCAT with all catalogs from the
            #  last session.
            raise ValueError("Catalog not initialized.")
        new_namespace = dc.create_namespace(
            namespace=namespace_name,
            catalog=catalog_name,
            *args,
            **kwargs,
        )
        return new_namespace
    elif len(parts) == 3:
        # register the given table
        raise NotImplementedError
    elif len(parts) == 4:
        # register the given table version
        raise NotImplementedError
    elif len(parts) == 5:
        # register the given stream
        raise NotImplementedError
    elif len(parts) == 6:
        # register the given partition
        raise NotImplementedError
    elif len(parts) == 7:
        # register the given partition delta
        raise NotImplementedError
    raise ValueError(f"Invalid path: {path}")


def touch(path):
    raise NotImplementedError


def exists(path):
    raise NotImplementedError


def query(expression):
    raise NotImplementedError


def tail(path):
    raise NotImplementedError


def head(path):
    raise NotImplementedError
