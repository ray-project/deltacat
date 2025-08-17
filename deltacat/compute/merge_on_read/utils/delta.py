from typing import List, Dict, Any, Optional, Union
from deltacat.storage.model.delta import Delta, DeltaLocator
from deltacat.storage.model.types import DistributedDataset
from deltacat.storage import (
    interface as unimplemented_deltacat_storage,
)
from deltacat.types.media import TableType, StorageType, DistributedDatasetType


def create_df_from_all_deltas(
    deltas: List[Union[Delta, DeltaLocator]],
    table_type: TableType,
    distributed_dataset_type: DistributedDatasetType,
    reader_kwargs: Optional[Dict[Any, Any]] = None,
    deltacat_storage=unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Optional[Dict[Any, Any]] = None,
    *args,
    **kwargs
) -> List[DistributedDataset]:  # type: ignore
    """
    This method creates a distributed dataset for each delta and returns their references.
    """

    if reader_kwargs is None:
        reader_kwargs = {}
    if deltacat_storage_kwargs is None:
        deltacat_storage_kwargs = {}

    df_list = []

    for delta in deltas:
        df = deltacat_storage.download_delta(
            delta_like=delta,
            table_type=table_type,
            distributed_dataset_type=distributed_dataset_type,
            storage_type=StorageType.DISTRIBUTED,
            **reader_kwargs,
            **deltacat_storage_kwargs
        )
        df_list.append(df)

    return df_list
