from typing import List, Optional, Union, Dict, Any

from ray.data import Dataset as RayDataset
from ray.data import read_datasource

from deltacat.io.datasource.deltacat_datasource import DeltaCatDatasource
from deltacat.io.dataset.deltacat_dataset import DeltaCatDataset
from deltacat.utils.common import ReadKwargsProvider
from deltacat.utils.url import DeltaCatUrl, DeltaCatUrlReader
from deltacat.io.datasource.deltacat_datasource import DeltacatReadType


class EmptyReadKwargsProvider(ReadKwargsProvider):
    def _get_kwargs(
        self,
        datasource_type: str,
        kwargs: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {}


def read_deltacat(
    urls: Union[DeltaCatUrl, List[DeltaCatUrl]],
    *,
    deltacat_read_type: DeltacatReadType = DeltacatReadType.DATA,
    timestamp_as_of: Optional[int] = None,
    merge_on_read: Optional[bool] = False,
    read_kwargs_provider: Optional[ReadKwargsProvider] = EmptyReadKwargsProvider(),
) -> DeltaCatDataset:
    """Reads the given DeltaCAT URLs into a Ray Dataset. DeltaCAT URLs can
    either reference objects registered in a DeltaCAT catalog, or unregistered
    external objects that are readable into a Ray Dataset.

    Unless `metadata_only` is `True`, all reads of registered DeltaCAT catalog
    object data must resolve to a single table version.

    When reading unregistered external objects, all additional keyword
    arguments specified are passed into the Ray Datasource resolved for the
    given DeltaCAT URLs.

    Examples:
        >>> # Read the latest active DeltaCAT table version:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("dc://my_catalog/my_namespace/my_table")
        >>> # If `my_catalog is the default catalog, this is equivalent to:
        >>> dc.io.read_deltacat("namespace://my_namespace/my_table")
        >>> # If `my_namespace` is the default namespace, this is equivalent to:
        >>> dc.io.read_deltacat("table://my_table")

        >>> # Read metadata from all partitions and deltas of the latest active
        >>> # DeltaCAT table version:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("dc://my_catalog/my_namespace/my_table", metadata_only=True)
        >>> # Since "default" always resolves to the latest active table version.
        >>> # This is equivalent to:
        >>> dc.io.read_deltacat("dc://my_catalog/my_namespace/my_table/default", metadata_only=True)

        >>> # Read only the latest active table version's top-level metadata:
        >>> dc.io.read_deltacat("dc://my_catalog/my_namespace/my_table/default", metadata_only=True, recursive=False)

        >>> # Read only top-level metadata from a DeltaCAT table:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("dc://my_catalog/my_namespace/my_table", metadata_only=True, recursive=False)

        >>> # Read top-level table metadata from all table versions:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("dc://my_catalog/my_namespace/my_table/*", metadata_only=True, recursive=False)

        >>> # Read metadata from all partitions and deltas of all table versions:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("dc://my_catalog/my_namespace/my_table/*", metadata_only=True)

        >>> # Read metadata from all tables and table versions of the namespace:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("dc://my_catalog/my_namespace/*", metadata_only=True)

        >>> # Read metadata from the latest active table version for each
        >>> # table in the namespace:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("dc://my_catalog/my_namespace", metadata_only=True)

        >>> # Read metadata from the latest active table version for each
        >>> # table in the namespace:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("dc://my_catalog/my_namespace", metadata_only=True)

        >>> # Read metadata from the latest active table version for each
        >>> # table in the catalog's default namespace:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("dc://my_catalog", metadata_only=True)

        >>> # Read metadata from all table versions for each table in each
        >>> # catalog namespace:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("dc://my_catalog/*", metadata_only=True)

        >>> # Read the Iceberg stream of the latest active DeltaCAT table version,
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("dc://my_catalog/my_namespace/my_table/default/iceberg")
        >>> # Or, if `my_catalog` is the default catalog, this is equivalent to:
        >>> dc.io.read_deltacat("namespace://my_namespace/my_table/default/iceberg")
        >>> # Or, if `my_namespace` is the default namespace, this is equivalent to:
        >>> dc.io.read_deltacat("table://my_table/default/iceberg")

        >>> # Read an external unregistered Iceberg table `my_db.my_table`:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("iceberg://my_db.my_table")

        >>> # Read an external unregistered audio file from /my/audio.mp4:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("audio+file:///my/audio.mp4")

        >>> # Read an external unregistered audio file from s3://my/audio.mp4:
        >>> import deltacat as dc
        >>> dc.io.read_deltacat("audio+s3://my/audio.mp4")

    Args:
        urls: The DeltaCAT URLs to read.
        deltacat_read_type: If METADATA, reads only DeltaCAT metadata for the
            given URL and skips both recursive metadata expansion and reads
            of the underlying data files. If METADATA_RECURSIVE then recursively
            expands child metadata but does not read underlying data files. If
            DATA then recursively expands child metadata to discover and read
            all underlying data files.
        timestamp_as_of: Reads a historic snapshot of the given paths as-of the
            given millisecond-precision epoch timestamp (only used when reading
            registered DeltaCAT catalog objects).
        merge_on_read: If True, merges all unmaterialized inserts, updates,
            and deletes in the registered DeltaCAT table version being read. Only
            applicable if `metadata_only` is False.
            ray_remote_args: kwargs passed to `ray.remote` in the read tasks.
        read_kwargs_provider: Resolves
            :class:`~deltacat.types.media.DatasourceType` string keys to
            kwarg dictionaries to pass to the resolved
            :class:`~ray.data.Datasource` implementation for each distinct
            DeltaCAT URL type.

    Returns:
        DeltacatDataset holding Arrow records read from the specified URL.
    """
    # TODO(pdames): The below implementation serializes reads of each URL and
    #   then unions their respective datasets together. While this was an easy
    #   starting point to implement, a more efficient implementation should push
    #   all URLs down into `DeltacatDatasource` to parallelize all reads
    #   (i.e., by returning the `ReadTask` for all datasources in
    #   `get_read_tasks()` and estimating the corresponding memory size across
    #   all datasources in `estimate_inmemory_data_size()`.
    dataset: RayDataset = None
    for url in urls:
        if not url.is_deltacat_catalog_url():
            # this URL points to an external unregistered Ray Datasource
            # TODO(pdames): Honor metadata only reads of external datasources
            #  by registering only file paths & metadata in delta manifests.
            reader = DeltaCatUrlReader(url)
            next_ds = reader.read(read_kwargs_provider(url.datastore_type, {}))
        else:
            # this URL points to a registered DeltaCAT object
            next_ds = read_datasource(
                DeltaCatDatasource(
                    url=url,
                    deltacat_read_type=deltacat_read_type,
                    timestamp_as_of=timestamp_as_of,
                    merge_on_read=merge_on_read,
                    read_kwargs_provider=read_kwargs_provider,
                )
            )
        # union the last dataset read into the result set
        if not dataset:
            dataset = next_ds
        else:
            dataset.union(next_ds)
    return DeltaCatDataset.from_dataset(dataset)
