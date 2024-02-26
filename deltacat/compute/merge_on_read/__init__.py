from deltacat.types.media import DistributedDatasetType
from deltacat.compute.merge_on_read.daft import merge as daft_merge

MERGE_FUNC_BY_DISTRIBUTED_DATASET_TYPE = {DistributedDatasetType.DAFT.value: daft_merge}
