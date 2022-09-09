import argparse
import base64
import gzip
import json
import logging
import pathlib
from io import BytesIO

import math
import yaml

from typing import Dict, Any, List, Tuple, Optional, Set, Union, TextIO

from deltacat.autoscaler.events.compaction.cluster import ClusterSizeSuggester
from deltacat.autoscaler.events.compaction.collections.partition_key_value import PartitionKeyValues, PartitionKeyValue
from deltacat.autoscaler.events.compaction.input import CompactionInput
from deltacat.autoscaler.events.session_manager import SessionManager, SESSION_ID_KEY
from deltacat.compute.compactor.utils import round_completion_file as rcf
from deltacat.compute.stats.models.delta_stats import DeltaStats
from deltacat.storage import interface as dcs
from deltacat import ContentType, logs, SortKey
from deltacat.compute.compactor import RoundCompletionInfo, compaction_session, PrimaryKeyIndexMeta, \
    PrimaryKeyIndexLocator
from deltacat.storage import PartitionLocator

_PRIMARY_KEY_INDEX_ALGORITHM_VERSION: str = "1.0"


logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def read_latest_round_completion_file(source_partition_locator,
                                      compacted_partition_locator,
                                      compaction_artifact_s3_bucket,
                                      primary_keys,
                                      sort_keys: List[SortKey] = None):
    if sort_keys is None:
        sort_keys = []
    # get the root path of a compatible primary key index for this round
    compatible_primary_key_index_meta = PrimaryKeyIndexMeta.of(
        compacted_partition_locator,
        primary_keys,
        sort_keys,
        _PRIMARY_KEY_INDEX_ALGORITHM_VERSION,
    )
    compatible_primary_key_index_locator = PrimaryKeyIndexLocator.of(
        compatible_primary_key_index_meta)
    compatible_primary_key_index_root_path = \
        compatible_primary_key_index_locator.primary_key_index_root_path

    # read the results from any previously completed compaction round that used
    # a compatible primary key index
    round_completion_info = rcf.read_round_completion_file(
        compaction_artifact_s3_bucket,
        source_partition_locator,
        compatible_primary_key_index_root_path,
    )
    return round_completion_info


def build_partition_locator(partition_data: Dict[str, Any]):
    return PartitionLocator.at(
        partition_data["owner"],
        partition_data["name"],
        partition_data.get("tableVersion"),
        partition_data["streamUUID"],
        None,  # storage type
        partition_data["partition_values"],  # partition values
        None  # partition ID
    )


def compact(compaction_input: CompactionInput,
            hash_bucket_count: Optional[int] = None,
            deltacat_storage=dcs,
            **kwargs):
    compaction_session.compact_partition(
        compaction_input.source_partition_locator,
        compaction_input.compacted_partition_locator,
        set(compaction_input.primary_keys),
        compaction_input.compaction_artifact_s3_bucket,
        compaction_input.last_stream_position_to_compact,
        schema_on_read=compaction_input.schema_on_read,
        input_deltas_stats=compaction_input.input_deltas_stats,
        hash_bucket_count=hash_bucket_count if hash_bucket_count else compaction_input.hash_bucket_count,
        deltacat_storage=deltacat_storage,
        **kwargs
    )


def calc_new_hash_bucket_count(cluster_memory_bytes: int,
                               max_memory_per_vcpu: int,
                               vcpu_per_node: int):
    new_hash_bucket_count = max(
        math.ceil(cluster_memory_bytes / max_memory_per_vcpu),
        min(vcpu_per_node, 256)  # Do not exceed 256 CPUs as a safety measure
    )

    return new_hash_bucket_count


def get_round_completion_file(
        source_partition_locator: PartitionLocator,
        compacted_partition_locator: PartitionLocator,
        primary_keys: Set[str],
        compaction_artifact_s3_bucket: str
):
    return read_latest_round_completion_file(source_partition_locator,
                                             compacted_partition_locator,
                                             compaction_artifact_s3_bucket,
                                             sorted(primary_keys))


def calc_compaction_cluster_memory_bytes(compaction_input: CompactionInput,
                                         new_uncompacted_deltas_bytes: int = 0) -> int:
    round_completion_file = get_round_completion_file(compaction_input.source_partition_locator,
                                                      compaction_input.compacted_partition_locator,
                                                      compaction_input.primary_keys,
                                                      compaction_input.compaction_artifact_s3_bucket)
    if round_completion_file is None:
        # if no previous compaction rounds exist, use the incoming delta size as a place to start for calculations
        est_incoming_delta_size = new_uncompacted_deltas_bytes * 1.3
        logger.warning(f"No previous round completion file found for {compaction_input}."
                       f"Using estimates: {est_incoming_delta_size}")
        return int(est_incoming_delta_size)

    old_num_records = round_completion_file.compacted_pyarrow_write_result.records
    sort_keys_bit_width = round_completion_file.sort_keys_bit_width
    pk_index_row_size = 32 + math.ceil(sort_keys_bit_width / 8)
    cluster_memory_bytes = max(
        old_num_records * pk_index_row_size * 1.3,  # object store memory (hash bucketing)
        round_completion_file.compacted_pyarrow_write_result.pyarrow_bytes + new_uncompacted_deltas_bytes  # dedupe
    )
    return int(cluster_memory_bytes)


def get_compaction_size_inputs(config: Dict[str, Any],
                               partition_key_values: PartitionKeyValues,
                               cluster_memory_bytes: int,
                               stats_metadata: Dict[int, DeltaStats] = None,
                               parent_session_id: str = None,
                               session_id: str = None) -> Tuple[int, TextIO]:
    suggester = ClusterSizeSuggester(cluster_memory_bytes=cluster_memory_bytes)
    new_hash_bucket_count = calc_new_hash_bucket_count(cluster_memory_bytes,
                                                       suggester.get_max_memory_per_vcpu(),
                                                       suggester.get_num_vcpu_per_node())
    cluster_cpus = max(
        new_hash_bucket_count,
        suggester.get_suggested_vcpu_count()
    )
    cluster_nodes = int(math.ceil(cluster_cpus / suggester.get_num_vcpu_per_node()))
    yaml_file = generate_compaction_session_yaml(config,
                                                 partition_key_values,
                                                 worker_node_count=cluster_nodes,
                                                 instance_type=suggester.instance_type,
                                                 stats_metadata=stats_metadata,
                                                 parent_session_id=parent_session_id,
                                                 session_id=session_id)
    return new_hash_bucket_count, yaml_file


def generate_compaction_session_yaml(config: Dict[str, Any],
                                     partition_key_values: PartitionKeyValues,
                                     head_node_count: int = 0,
                                     worker_node_count: int = 0,
                                     stats_metadata: Dict[int, DeltaStats] = None,
                                     instance_type: str = None,
                                     parent_session_id: str = None,
                                     session_id: str = None) -> TextIO:
    # TODO: Remove this workaround when custom AMIs are built with baked-in build files (i.e. wheels, jars)
    new_config = {**config}
    for local_path, _ in new_config["file_mounts"].items():
        new_config["file_mounts"][local_path] = local_path
    pkv_id = partition_key_values.id
    new_filename = f"compact.{pkv_id}.yaml"
    new_config["cluster_name"] = f"compaction-session-{pkv_id}"
    # Allow child clusters to re-use the same SSH key provided from the parent cluster
    new_config["auth"]["ssh_private_key"] = f"~/ray_bootstrap_key.pem"
    new_config["file_mounts"] = {
        **config["file_mounts"],
        f"~/{new_filename}": f"~/{new_filename}"
    }
    new_config["provider"]["use_internal_ips"] = True
    new_config["max_workers"] = worker_node_count
    # TODO: Determine optimal object store memory / worker heap memory allocation ratios?
    new_config["available_node_types"]["ray.worker.default"]["min_workers"] = \
        new_config["available_node_types"]["ray.worker.default"]["max_workers"] = worker_node_count
    new_config["available_node_types"]["ray.worker.default"]["node_config"]["InstanceType"] = instance_type
    new_config["available_node_types"]["ray.head.default"]["node_config"]["InstanceType"] = instance_type

    # TODO: Formalize supported parameter key/values after initial shadow compaction
    new_events = {
        **config["events"],
        "parameters": {
            **config["events"]["parameters"],
            SESSION_ID_KEY: session_id,
        },
        "metadata": {
            "partitionKeyValues": compress(partition_key_values).decode('utf-8')
        }
    }
    if stats_metadata:
        new_events["metadata"]["statsMetadata"] = compress(stats_metadata).decode('utf-8')
    new_config["events"] = new_events

    with open(new_filename, "w") as yaml_file:
        yaml.dump(new_config, yaml_file, default_flow_style=False)
        return yaml_file

def compress(serializable_obj: Union[Dict, Tuple, List]) -> bytes:
    json_dict = json.dumps(serializable_obj)
    out = BytesIO()
    with gzip.open(out, "wt", encoding="utf-8") as zipfile:
        zipfile.write(json_dict)

    return base64.b64encode(out.getvalue())
