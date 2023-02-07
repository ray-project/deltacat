import errno
import logging
import os
import subprocess
from subprocess import run
from typing import Any

import ray
from tenacity import RetryError, Retrying, stop_after_attempt, wait_fixed

from deltacat import logs
from deltacat.compute.metastats.utils.constants import (
    MAX_WORKER_MULTIPLIER,
    WORKER_NODE_OBJECT_STORE_MEMORY_RESERVE_RATIO,
)

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

RAY_DOWN_DEFAULT_RETRY_ATTEMPTS = 3


def run_cmd_exit_code(cmd: str) -> int:
    logger.info(f"running command {cmd}")
    exit_code = int(os.system(cmd))
    logger.info(f"Got {exit_code} when running {cmd}")


def run_cmd_with_retry(cmd: str) -> None:
    retrying = Retrying(
        wait=wait_fixed(2), stop=stop_after_attempt(RAY_DOWN_DEFAULT_RETRY_ATTEMPTS)
    )
    try:
        retrying(run_cmd_exit_code(cmd))
    except RetryError:
        logger.info(f"{cmd} failed after {RAY_DOWN_DEFAULT_RETRY_ATTEMPTS} retries.")


def run_cmd(cmd: str) -> None:
    result = run(cmd, shell=True, capture_output=True)
    exit_code = int(result.returncode)
    assert exit_code == 0, (
        f"`{cmd}` failed. Exit code: {exit_code} " f"Error Trace: {result.stderr}"
    )


def ray_up(cluster_cfg: str) -> None:
    logger.info(f"Starting Ray cluster '{cluster_cfg}'")
    run_cmd(f"ray up {cluster_cfg} -y --no-config-cache --no-restart")
    logger.info(f"Started Ray cluster '{cluster_cfg}'")


def ray_down(cluster_cfg: str) -> None:
    logger.info(f"Destroying Ray cluster '{cluster_cfg}'")
    run_cmd_with_retry(f"ray down {cluster_cfg} -y")
    logger.info(f"Destroyed Ray cluster '{cluster_cfg}'")


def clean_up_cluster_cfg_file(cluster_cfg) -> None:
    logger.info(f"Removing stats cluster config at: '{cluster_cfg}'")
    run_cmd(f"rm -f {cluster_cfg}")
    logger.info(f"Removed stats cluster config at: '{cluster_cfg}'")


def get_head_node_ip(cluster_cfg: str) -> str:
    logger.info(f"Getting Ray cluster head node IP for '{cluster_cfg}'")
    proc = subprocess.run(
        f"ray get-head-ip {cluster_cfg}",
        shell=True,
        capture_output=True,
        text=True,
        check=True,
    )
    # the head node IP should be the last line printed to stdout
    head_node_ip = proc.stdout.splitlines()[-1]
    logger.info(f"Ray cluster head node IP for '{cluster_cfg}': {head_node_ip}")
    return head_node_ip


def ray_init(host, port) -> Any:
    ray_init_uri = f"ray://{host}:{port}"
    logger.info(f"Connecting Ray Client to '{ray_init_uri}'")
    client = ray.init(ray_init_uri, allow_multiple=True)
    logger.info(f"Connected Ray Client to '{ray_init_uri}'")
    return client


def replace_cluster_cfg_vars(
    partition_canonical_string: str,
    trace_id: str,
    file_path: str,
    min_workers: int,
    head_type: str,
    worker_type: str,
    head_object_store_memory_pct: int,
    worker_object_store_memory_pct: int,
) -> str:

    head_object_store_memory_pct = head_object_store_memory_pct if not None else 30
    worker_object_store_memory_pct = WORKER_NODE_OBJECT_STORE_MEMORY_RESERVE_RATIO * 100

    max_workers = int(min_workers * MAX_WORKER_MULTIPLIER)
    with open(file_path, "r+") as file:
        contents = file.read().replace("{{use-internal-ips}}", "True")
        contents = contents.replace(
            "{{partition_canonical_string}}", partition_canonical_string
        )
        contents = contents.replace("'{{trace_id}}'", trace_id)
        contents = contents.replace("'{{min-workers}}'", str(min_workers))
        contents = contents.replace("'{{max-workers}}'", str(max_workers))
        contents = contents.replace("'{{head-instance-type}}'", head_type)
        contents = contents.replace("'{{worker-instance-type}}'", worker_type)
        contents = contents.replace(
            "'{{head-object-store-memory-pct}}'", str(head_object_store_memory_pct)
        )
        contents = contents.replace(
            "'{{worker-object-store-memory-pct}}'", str(worker_object_store_memory_pct)
        )
    partition_id = partition_canonical_string.split("|")[-1]
    out_file_name = f"{trace_id}-{partition_id}.{os.path.basename(file_path)}"
    out_file_dir = os.path.join(os.path.dirname(file_path), "tmp")
    out_file_path = os.path.join(out_file_dir, out_file_name)
    try:
        os.makedirs(os.path.dirname(out_file_path))
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    with open(out_file_path, "w") as output:
        output.write(contents)
    return out_file_path
