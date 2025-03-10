import logging
from deltacat.compute.merge_on_read.model.merge_on_read_params import MergeOnReadParams
from deltacat.storage.model.types import DistributedDataset
from deltacat.types.media import TableType, DistributedDatasetType
from deltacat.compute.merge_on_read.utils.delta import create_df_from_all_deltas
from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def merge(params: MergeOnReadParams, **kwargs) -> DistributedDataset:
    """
    Merges the given deltas and returns the result as distributed dataframe.
    It reads the deltas into the Daft dataframe and leverages operations supported
    by Daft to perform an efficient merge using Ray cluster.

    TODO(raghumdani): Perform actual merge.
    """

    delta_dfs = create_df_from_all_deltas(
        deltas=params.deltas,
        table_type=TableType.PYARROW,
        distributed_dataset_type=DistributedDatasetType.DAFT,
        reader_kwargs=params.reader_kwargs,
        deltacat_storage=params.deltacat_storage,
        deltacat_storage_kwargs=params.deltacat_storage_kwargs,
        **kwargs,
    )

    logger.info(f"Merging {len(delta_dfs)} delta dfs...")

    # TODO: This code should be optimized from daft side
    result = None
    for df in delta_dfs:
        if result is None:
            result = df
        else:
            result = result.concat(df)

    return result
