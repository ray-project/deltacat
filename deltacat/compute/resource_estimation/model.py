from __future__ import annotations
from enum import Enum
from typing import Optional


class ResourceEstimationMethod(str, Enum):
    """
    The default approach executes certain methods in a specific order until the size
    is estimated by any. The order is as follows:
    1. CONTENT_TYPE_META
    2. PREVIOUS_INFLATION
    This method expects previous inflation and average record bytes to be passed.
    """

    DEFAULT = "DEFAULT"

    """
    This approach combines intelligent estimation and inflation based methods
    and runs them in the order specified below:
    1. INTELLIGENT_ESTIMATION
    2. FILE_SAMPLING
    3. PREVIOUS_INFLATION
    """
    DEFAULT_V2 = "DEFAULT_V2"

    """
    This approach strictly uses previous inflation and average record size to arrive
    at a resource estimate. It requires users to pass in previous inflation and average
    record sizes.
    """
    PREVIOUS_INFLATION = "PREVIOUS_INFLATION"

    """
    This approach is similar to PREVIOUS_INFLATION, but it determines average record size
    and previous inflation by sampling few files in the given set of files.
    """
    FILE_SAMPLING = "FILE_SAMPLING"

    """
    This approach leverages metadata present in content type params.
    """
    CONTENT_TYPE_META = "CONTENT_TYPE_META"

    """
    This approach leverages parquet metadata and granularly estimate resources for each column and
    then aggregate to arrive at most accurate estimation.
    """
    INTELLIGENT_ESTIMATION = "INTELLIGENT_ESTIMATION"


class EstimateResourcesParams(dict):
    """
    This class represents the parameters required for estimating resources.
    """

    @staticmethod
    def of(
        resource_estimation_method: ResourceEstimationMethod = ResourceEstimationMethod.DEFAULT,
        previous_inflation: Optional[float] = None,
        parquet_to_pyarrow_inflation: Optional[float] = None,
        average_record_size_bytes: Optional[float] = None,
        max_files_to_sample: Optional[int] = None,
    ) -> EstimateResourcesParams:
        result = EstimateResourcesParams()
        result["previous_inflation"] = previous_inflation
        result["parquet_to_pyarrow_inflation"] = parquet_to_pyarrow_inflation
        result["resource_estimation_method"] = resource_estimation_method
        result["max_files_to_sample"] = max_files_to_sample
        result["average_record_size_bytes"] = average_record_size_bytes
        return result

    @property
    def resource_estimation_method(self) -> ResourceEstimationMethod:
        return self["resource_estimation_method"]

    @property
    def max_files_to_sample(self) -> Optional[int]:
        """
        Applicable only for FILE_SAMPLING method. This parameter controls the
        number of files to sample to arrive at average record sizes and previous inflation.
        """
        return self.get("max_files_to_sample")

    @property
    def previous_inflation(self) -> Optional[float]:
        """
        This parameter is required for PREVIOUS_INFLATION method. The inflation factor determines
        a ratio of in-memory size to the on-disk size.
        """
        return self.get("previous_inflation")

    @property
    def parquet_to_pyarrow_inflation(self) -> Optional[float]:
        """
        This parameter is required for INTELLIGENT_ESTIMATION or CONTENT_TYPE_META method.
        This determines inflation factor for parquet estimated size to pyarrow in-memory table size.
        """
        return self.get("parquet_to_pyarrow_inflation")

    @property
    def average_record_size_bytes(self) -> Optional[float]:
        """
        This parameter is required for PREVIOUS_INFLATION method. This determines average size of
        records in bytes in a given file or entity.
        """
        return self.get("average_record_size_bytes")


class OperationType(str, Enum):
    """
    This operation type is used when user would download the given entities using pyarrow library.
    """

    PYARROW_DOWNLOAD = "DOWNLOAD"


class EstimatedResources(dict):
    """
    This class represents the resource requirements for a certain type of operation.
    For example, downloading a delta requires certain amount of memory.
    """

    @staticmethod
    def of(memory_bytes: float, statistics: Statistics = None) -> EstimatedResources:
        result = EstimatedResources()
        result["memory_bytes"] = memory_bytes
        result["statistics"] = statistics
        return result

    @property
    def memory_bytes(self) -> float:
        return self["memory_bytes"]

    @property
    def statistics(self) -> Optional[Statistics]:
        return self.get("statistics")


class Statistics(dict):
    """
    This class represents the statistics of underlying objects that was used
    to estimate the resource required.
    """

    @staticmethod
    def of(
        in_memory_size_bytes: float, record_count: int, on_disk_size_bytes: float
    ) -> Statistics:
        result = Statistics()
        result["in_memory_size_bytes"] = in_memory_size_bytes
        result["record_count"] = record_count
        result["on_disk_size_bytes"] = on_disk_size_bytes
        return result

    @property
    def in_memory_size_bytes(self) -> float:
        return self["in_memory_size_bytes"]

    @property
    def record_count(self) -> int:
        return self["record_count"]

    @property
    def on_disk_size_bytes(self) -> float:
        return self["on_disk_size_bytes"]
