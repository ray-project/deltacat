# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from contextlib import AbstractContextManager
from types import TracebackType
import ray
import sys
import threading
import time
from typing import Dict, Any, Optional
from dataclasses import dataclass
from deltacat import logs
import logging
from resource import getrusage, RUSAGE_SELF
import platform
import psutil
import schedule
from deltacat.constants import BYTES_PER_GIBIBYTE


logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


@dataclass
class ClusterUtilization:
    def __init__(
        self, cluster_resources: Dict[str, Any], available_resources: Dict[str, Any]
    ):
        used_resources = {}

        for key in cluster_resources:
            if (
                isinstance(cluster_resources[key], float)
                or isinstance(cluster_resources[key], int)
            ) and key in available_resources:
                used_resources[key] = cluster_resources[key] - available_resources[key]

        self.total_memory_bytes = cluster_resources.get("memory")
        self.used_memory_bytes = used_resources.get("memory", 0.0)
        self.total_cpu = cluster_resources.get("CPU")
        self.used_cpu = used_resources.get("CPU", 0)
        self.total_object_store_memory_bytes = cluster_resources.get(
            "object_store_memory"
        )
        self.used_object_store_memory_bytes = used_resources.get(
            "object_store_memory", 0.0
        )
        self.used_memory_percent = (
            self.used_memory_bytes / self.total_memory_bytes
        ) * 100
        self.used_object_store_memory_percent = (
            self.used_object_store_memory_bytes / self.total_object_store_memory_bytes
        ) * 100
        self.used_cpu_percent = (self.used_cpu / self.total_cpu) * 100
        self.used_resources = used_resources

    @staticmethod
    def get_current_cluster_utilization() -> ClusterUtilization:
        cluster_resources = ray.cluster_resources()
        available_resources = ray.available_resources()

        return ClusterUtilization(
            cluster_resources=cluster_resources, available_resources=available_resources
        )


class ClusterUtilizationOverTimeRange(AbstractContextManager):
    """
    This class can be used to compute the cluster utilization metrics
    which requires us to compute it over time as they change on-demand.

    For example, in an autoscaling cluster, the vCPUs keep changing and hence
    more important metrics to capture in that scenario is vcpu-seconds.
    """

    def __init__(self) -> None:
        self.total_vcpu_seconds = 0.0
        self.used_vcpu_seconds = 0.0
        self.total_memory_gb_seconds = 0.0
        self.used_memory_gb_seconds = 0.0
        self.max_cpu = 0.0
        self.max_memory = 0.0

    def __enter__(self) -> Any:
        schedule.every().second.do(self._update_resources)
        self.stop_run_schedules = self._run_schedule()
        return super().__enter__()

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> bool | None:
        if __exc_value:
            logger.error(
                f"Error occurred while calculating cluster resources: {__exc_value}"
            )
        self.stop_run_schedules.set()
        return super().__exit__(__exc_type, __exc_value, __traceback)

    # It is not truely parallel(due to GIL Ref: https://wiki.python.org/moin/GlobalInterpreterLock)
    # even if we are using threading library. However, it averages out and gives a very good approximation.
    def _update_resources(self):
        cluster_resources = ray.cluster_resources()
        available_resources = ray.available_resources()
        if "CPU" not in cluster_resources:
            return

        if "CPU" in available_resources:
            self.used_vcpu_seconds = self.used_vcpu_seconds + float(
                str(cluster_resources["CPU"] - available_resources["CPU"])
            )

        self.total_vcpu_seconds = self.total_vcpu_seconds + float(
            str(cluster_resources["CPU"])
        )
        self.max_cpu = max(self.max_cpu, float(str(cluster_resources["CPU"])))

        if "memory" not in cluster_resources:
            return

        if "memory" in available_resources:
            self.used_memory_gb_seconds = (
                self.used_memory_gb_seconds
                + float(
                    str(cluster_resources["memory"] - available_resources["memory"])
                )
                / BYTES_PER_GIBIBYTE
            )

        self.total_memory_gb_seconds = (
            self.total_memory_gb_seconds
            + float(str(cluster_resources["memory"])) / BYTES_PER_GIBIBYTE
        )

        self.max_memory = max(
            self.max_memory,
            float(str(cluster_resources["memory"] - available_resources["memory"])),
        )

    def _run_schedule(self, interval: Optional[float] = 1.0):
        cease_continuous_run = threading.Event()

        class ScheduleThread(threading.Thread):
            @classmethod
            def run(cls):
                while not cease_continuous_run.is_set():
                    schedule.run_pending()
                    time.sleep(float(str(interval)))

        continuous_thread = ScheduleThread()
        continuous_thread.start()
        return cease_continuous_run


def get_current_process_peak_memory_usage_in_bytes():
    """
    Returns the peak memory usage of the process in bytes. This method works across
    Windows, Darwin and Linux platforms.
    """
    current_platform = platform.system()
    if current_platform != "Windows":
        usage = getrusage(RUSAGE_SELF).ru_maxrss
        if current_platform == "Linux":
            usage = usage * 1024
        return usage
    else:
        return psutil.Process().memory_info().peak_wset


def get_size_of_object_in_bytes(obj: object) -> float:
    size = sys.getsizeof(obj)
    if isinstance(obj, dict):
        return (
            size
            + sum(map(get_size_of_object_in_bytes, obj.keys()))
            + sum(map(get_size_of_object_in_bytes, obj.values()))
        )
    if isinstance(obj, (list, tuple, set, frozenset)):
        return size + sum(map(get_size_of_object_in_bytes, obj))
    return size


class ProcessUtilizationOverTimeRange(AbstractContextManager):
    """
    This class can be used to compute the process utilization metrics
    which requires us to compute it over time as memory utilization changes.
    """

    def __init__(self) -> None:
        self.max_memory = 0.0

    def __enter__(self) -> Any:
        schedule.every().second.do(self._update_resources)
        self.stop_run_schedules = self._run_schedule()
        return super().__enter__()

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> bool | None:
        if __exc_value:
            logger.error(
                f"Error occurred while calculating process resources: {__exc_value}"
            )
        self.stop_run_schedules.set()
        return super().__exit__(__exc_type, __exc_value, __traceback)

    def schedule_callback(self, callback, callback_frequency_in_seconds) -> None:
        schedule.every(callback_frequency_in_seconds).seconds.do(callback)

    # It is not truely parallel(due to GIL Ref: https://wiki.python.org/moin/GlobalInterpreterLock)
    # even if we are using threading library. However, it averages out and gives a very good approximation.
    def _update_resources(self):
        self.max_memory = get_current_process_peak_memory_usage_in_bytes()

    def _run_schedule(self, interval: Optional[float] = 1.0):
        cease_continuous_run = threading.Event()

        class ScheduleThread(threading.Thread):
            @classmethod
            def run(cls):
                while not cease_continuous_run.is_set():
                    schedule.run_pending()
                    time.sleep(float(str(interval)))

        continuous_thread = ScheduleThread()
        continuous_thread.start()
        return cease_continuous_run
