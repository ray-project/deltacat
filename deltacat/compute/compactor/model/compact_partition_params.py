from __future__ import annotations

import copy
import json
from typing import Any, Dict, List, Optional

from deltacat.types.media import ContentType


class CompactPartitionParams(dict):
    """
    This class represents the parameters passed to compact_partition (deltacat/compute/compactor/compaction_session.py)
    """

    @staticmethod
    def of(params: Optional[Dict]) -> CompactPartitionParams:
        if params is None:
            params = {}
        compact_partition_params = CompactPartitionParams()
        compact_partition_params["destination_partition_locator"] = params.get(
            "destination_partition_locator"
        )
        compact_partition_params["last_stream_position_to_compact"] = params.get(
            "last_stream_position_to_compact"
        )
        compact_partition_params["source_partition_locator"] = params.get(
            "source_partition_locator"
        )
        compact_partition_params["primary_keys"] = params.get("primary_keys")
        compact_partition_params["rebase_source_partition_locator"] = params.get(
            "rebase_source_partition_locator"
        )
        compact_partition_params["rebase_source_partition_high_watermark"] = params.get(
            "rebase_source_partition_high_watermark"
        )
        compact_partition_params["hash_bucket_count"] = params.get("hash_bucket_count")
        compact_partition_params["deltacat_storage"] = params.get("deltacat_storage")
        compact_partition_params["compaction_artifact_s3_bucket"] = params.get(
            "compaction_artifact_s3_bucket"
        )
        compact_partition_params["properties"] = params.get("properties")
        compact_partition_params["compacted_file_content_type"] = params.get(
            "compacted_file_content_type"
        )
        compact_partition_params["list_deltas_kwargs"] = params.get(
            "list_deltas_kwargs"
        )
        compact_partition_params["pg_config"] = params.get("pg_config")
        compact_partition_params["read_kwargs_provider"] = params.get(
            "read_kwargs_provider"
        )
        compact_partition_params["s3_table_writer_kwargs"] = params.get(
            "s3_table_writer_kwargs"
        )
        return compact_partition_params

    @property
    def destination_partition_locator(self) -> Optional[dict]:
        return self["destination_partition_locator"]

    @property
    def last_stream_position_to_compact(self) -> Optional[int]:
        return self["last_stream_position_to_compact"]

    @property
    def source_partition_locator(self) -> Optional[dict]:
        return self["source_partition_locator"]

    @property
    def primary_keys(self) -> Optional[List[str]]:
        return list(self["primary_keys"])

    @property
    def rebase_source_partition_locator(self) -> Optional[dict]:
        return self["rebase_source_partition_locator"]

    @property
    def rebase_source_partition_high_watermark(self) -> Optional[int]:
        return self["rebase_source_partition_high_watermark"]

    @property
    def hash_bucket_count(self) -> Optional[int]:
        return self["hash_bucket_count"]

    @property
    def deltacat_storage(self) -> Optional[str]:
        return self["deltacat_storage"]

    @property
    def compaction_artifact_s3_bucket(self) -> Optional[str]:
        return self["compaction_artifact_s3_bucket"]

    @property
    def properties(self) -> Optional[Dict[str, str]]:
        return self["properties"]

    @property
    def compacted_file_content_type(self) -> Optional[ContentType]:
        return self["compacted_file_content_type"]

    @property
    def list_deltas_kwargs(self) -> Optional[dict]:
        return self["list_deltas_kwargs"]

    @property
    def pg_config(self) -> Optional[Any]:
        return self["pg_config"]

    @property
    def read_kwargs_provider(self) -> Optional[Any]:
        return self["read_kwargs_provider"]

    @property
    def s3_table_writer_kwargs(self) -> Optional[Any]:
        return self["s3_table_writer_kwargs"]

    @staticmethod
    def json_handler_for_compact_partition_params(obj):
        """
        A handler for the `json.dumps()` function that can be used to serialize sets to JSON.
        If the `set_default()` handler is passed as the `default` argument to the `json.dumps()` function, it will be called whenever a set object is encountered.
        The `set_default()` handler will then serialize the set as a list.
        """
        try:
            if isinstance(obj, set):
                return list(obj)
            elif hasattr(obj, "toJSON"):
                return obj.toJSON()
            else:
                return obj.__dict__
        except Exception:
            return obj.__class__.__name__

    def serialize(self) -> str:
        """
        Serializes itself to a json-formatted string

        Returns:
            The serialized object.

        """
        to_serialize: Dict[str, Any] = {}
        # individually try deepcopy the values from the self dictionary and just use the class name for the value when it is not possible to deepcopy
        for attr, value in self.items():
            try:
                to_serialize[attr] = copy.deepcopy(value)
            except Exception:  # if unable to deep copy the objects like module objects for example then just provide the class name at minimum
                to_serialize[attr] = value.__class__.__name__
        serialized_arguments_compact_partition_args: str = json.dumps(
            to_serialize,
            default=CompactPartitionParams.json_handler_for_compact_partition_params,
        )
        return serialized_arguments_compact_partition_args
