import logging
from typing import Optional
from deltacat import logs
from deltacat.compute.compactor import RoundCompletionInfo
from deltacat.storage import PartitionLocator
from deltacat.storage.model.partition import Partition
from deltacat.utils.metrics import metrics

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


@metrics
def read_round_completion_info(
    source_partition_locator: PartitionLocator,
    destination_partition_locator: PartitionLocator,
    deltacat_storage,
    deltacat_storage_kwargs: Optional[dict] = None,
    destination_partition: Optional[Partition] = None,
) -> Optional[RoundCompletionInfo]:
    """
    Read round completion info from the partition metafile.

    Args:
        source_partition_locator: Source partition locator for validation
        destination_partition_locator: Destination partition locator
        deltacat_storage: Storage implementation
        deltacat_storage_kwargs: Optional storage kwargs
        destination_partition: Optional destination partition to avoid redundant get_partition calls

    Returns:
        RoundCompletionInfo if found in partition, None otherwise
    """
    if not destination_partition_locator:
        return None
        
    if deltacat_storage_kwargs is None:
        deltacat_storage_kwargs = {}
    
    try:
        # Use provided partition or get it from storage
        if destination_partition:
            partition = destination_partition
        else:
            # Try to get the partition from storage
            partition = deltacat_storage.get_partition(
                destination_partition_locator.stream_locator,
                destination_partition_locator.partition_values,
                **deltacat_storage_kwargs,
            )
        
        if partition:
            round_completion_info = partition.compaction_round_completion_info
            if round_completion_info:
                # Validate that prev_source_partition_locator matches current source
                if not source_partition_locator or not round_completion_info.prev_source_partition_locator:
                    raise ValueError(
                        f"Source partition locator ({source_partition_locator}) and "
                        f"prev_source_partition_locator ({round_completion_info.prev_source_partition_locator}) "
                        f"must both be provided."
                    )

                if (round_completion_info.prev_source_partition_locator.canonical_string() != source_partition_locator.canonical_string()):
                    logger.warning(
                        f"Previous source partition locator mismatch: "
                        f"expected {source_partition_locator.canonical_string()}, "
                        f"but found {round_completion_info.prev_source_partition_locator.canonical_string()} "
                        f"in round completion info. Ignoring cached round completion info."
                    )
                    return None
                
                logger.info(f"Read round completion info from partition metafile: {round_completion_info}")
                return round_completion_info
        
    except Exception as e:
        logger.debug(f"Failed to read round completion info from partition metafile: {e}")
        
    return None
