import logging
from deltacat.compute.compactor_v2.model.merge_input import MergeInput
import numpy as np
import ray
from deltacat import logs
from deltacat.compute.compactor_v2.model.merge_result import MergeResult
from deltacat.utils.performance import timed_invocation
from deltacat.utils.metrics import emit_timer_metrics

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def _timed_merge(input: MergeInput) -> MergeResult:
    # TODO: Implementation goes here
    pass


@ray.remote
def merge(input: MergeInput) -> MergeResult:

    logger.info(f"Starting hash bucket task...")
    merge_result, duration = timed_invocation(func=_timed_merge, input=input)

    emit_metrics_time = 0.0
    if input.metrics_config:
        emit_result, latency = timed_invocation(
            func=emit_timer_metrics,
            metrics_name="hash_bucket",
            value=duration,
            metrics_config=input.metrics_config,
        )
        emit_metrics_time = latency

    logger.info(f"Finished hash bucket task...")
    return MergeResult(
        merge_result[0],
        merge_result[1],
        merge_result[2],
        np.double(emit_metrics_time),
        merge_result[4],
    )
