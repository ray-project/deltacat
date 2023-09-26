from typing import Optional, Dict

from deltacat.aws.redshift import Manifest, ManifestMeta
from deltacat.storage import Partition, DeltaType, DeltaLocator, Delta
from deltacat.utils.common import current_time_ms


def create_empty_delta(
    partition: Partition,
    delta_type: DeltaType,
    author: Optional[str],
    properties: Optional[Dict[str, str]] = None,
    manifest_entry_id: Optional[str] = None,
) -> Delta:
    stream_position = current_time_ms()
    delta_locator = DeltaLocator.of(partition.locator, stream_position=stream_position)

    if manifest_entry_id:
        manifest = Manifest.of(
            entries=[],
            author=author,
            uuid=manifest_entry_id,
        )
    else:
        manifest = None

    return Delta.of(
        delta_locator,
        delta_type=delta_type,
        meta=ManifestMeta(),
        properties=properties,
        manifest=manifest,
        previous_stream_position=partition.stream_position,
    )
