import logging
from typing import Optional
from deltacat import logs
from deltacat.compute.compactor import RoundCompletionInfo
from deltacat.storage import PartitionLocator
from deltacat.storage.model.partition import Partition
from deltacat.utils.metrics import metrics
from deltacat.exceptions import PartitionNotFoundError

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
            # First get the current partition to access its previous_partition_id
            current_partition: Partition = deltacat_storage.get_partition(
                destination_partition_locator.stream_locator,
                destination_partition_locator.partition_values,
                **deltacat_storage_kwargs,
            )

            # If current partition has round completion info, use it
            if current_partition.compaction_round_completion_info:
                partition = current_partition
            elif current_partition.previous_partition_id is not None:
                # For incremental compaction, we need to get the previous committed partition
                # that contains the round completion info.
                # Get the previous partition by ID - this is where the round completion info should be
                logger.info(
                    f"Current partition {destination_partition_locator} does not have round completion info, "
                    f"getting previous partition with ID: {current_partition.previous_partition_id}"
                )
                previous_partition = deltacat_storage.get_partition_by_id(
                    destination_partition_locator.stream_locator,
                    current_partition.previous_partition_id,
                    **deltacat_storage_kwargs,
                )
                if previous_partition is not None:
                    logger.info(
                        f"Found previous partition: {previous_partition.locator}"
                    )
                    partition = previous_partition
                else:
                    raise PartitionNotFoundError(
                        f"Previous partition with ID {current_partition.previous_partition_id} not found"
                    )
            else:
                logger.info(f"No previous partition ID found, using current partition")
                partition = current_partition

        if partition:
            round_completion_info = partition.compaction_round_completion_info
            if round_completion_info:
                # Validate that prev_source_partition_locator matches current source
                if (
                    not source_partition_locator
                    or not round_completion_info.prev_source_partition_locator
                ):
                    raise ValueError(
                        f"Source partition locator ({source_partition_locator}) and "
                        f"prev_source_partition_locator ({round_completion_info.prev_source_partition_locator}) "
                        f"must both be provided."
                    )

                if (
                    round_completion_info.prev_source_partition_locator.canonical_string()
                    != source_partition_locator.canonical_string()
                ):
                    logger.warning(
                        f"Previous source partition locator mismatch: "
                        f"expected {source_partition_locator.canonical_string()}, "
                        f"but found {round_completion_info.prev_source_partition_locator.canonical_string()} "
                        f"in round completion info. Ignoring cached round completion info."
                    )
                    return None

                logger.info(
                    f"Read round completion info from partition metafile: {round_completion_info}"
                )
                return round_completion_info

    except Exception as e:
        logger.debug(
            f"Failed to read round completion info from partition metafile: {e}"
        )

    return None
