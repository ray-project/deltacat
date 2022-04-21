import argparse
import base64
import gzip
import json
import logging
import os
import pathlib
from typing import List

import boto3

import yaml

from deltacat import logs
from deltacat.autoscaler.events.dispatcher import EventDispatcher
from deltacat.autoscaler.events.event_store import EventStoreClient

from ray.autoscaler._private.aws.events import AwsEventManager

from deltacat.autoscaler.events.workflow import poll_events

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

COMPACTION_EVENT_SUMMARY_TABLE_NAME = "RAY_COMPACTION_EVENT_SUMMARY"
ONE_HOUR_MS = 60 * 1000 * 60


def partition_discovery() -> List[str]:
    return ["PLACEHOLDER_PARTITION"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_path", type=pathlib.Path, required=True)

    args = parser.parse_args()

    full_path = os.path.expanduser(args.config_path)
    with open(full_path, "r") as stream:
        config = yaml.safe_load(stream)

    events_config = config["events"]
    manager = AwsEventManager(events_config) if events_config else None
    source_table_dict = events_config["parameters"]["sourceTable"]
    b64_encoded_pkv = events_config["parameters"].get("partitionsToCompact")
    if b64_encoded_pkv:
        pkv = gzip.decompress(base64.b64decode(b64_encoded_pkv))
        pkv_json = json.loads(pkv)
        partition_values = [kv["value"] for kv in pkv_json[0]]
    else:
        logger.info(f"No partition key-values found to compact. Attempting partition discovery.")
        partition_values = partition_discovery()

    # TODO: pass partition_values to child jobs
    ddb_client = boto3.client("dynamodb", "us-east-1")
    event_store = EventStoreClient(ddb_client, COMPACTION_EVENT_SUMMARY_TABLE_NAME)
    event_dispatcher = EventDispatcher(manager)
    poll_events(manager, event_store, event_dispatcher)
