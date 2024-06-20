from __future__ import annotations
from typing import List
from enum import Enum


class TransformName(str, Enum):
    IDENTITY = "identity"
    BUCKET = "bucket"


class TransformParameters(dict):
    """
    This is a parent class that contains properties
    to be passed to the corresponding transform
    """

    pass


class IdentityTransformParameters(TransformParameters):
    """
    This class is used to pass parameters to the identity transform
    """

    @staticmethod
    def of(column_name: str) -> IdentityTransformParameters:
        identify_transform_parameters = IdentityTransformParameters()
        identify_transform_parameters["columnName"] = column_name
        return identify_transform_parameters

    @property
    def column_name(self) -> str:
        """
        The name of the column to use for identity transform
        """
        return self["columnName"]

    @column_name.setter
    def column_name(self, value: str) -> None:
        self["columnName"] = value


class BucketingStrategy(str, Enum):
    """
    A bucketing strategy for the transform
    """

    # Uses default deltacat bucketing strategy.
    # This strategy supports hashing on composite keys
    # and uses SHA1 hashing for determining the bucket.
    # If no columns passed, it will use a random UUID
    # for determining the bucket.
    DEFAULT = "default"

    # Uses iceberg compliant bucketing strategy.
    # As indicated in the iceberg spec, it does not support
    # composite keys and uses murmur3 hash for determining
    # the bucket.
    # See https://iceberg.apache.org/spec/#partitioning
    ICEBERG = "iceberg"


class BucketTransformParameters(TransformParameters):
    """
    Encapsulates parameters for the bucket transform.
    """

    def of(
        self,
        num_buckets: int,
        column_names: List[str],
        bucketing_strategy: BucketingStrategy,
    ) -> BucketTransformParameters:
        bucket_transform_parameters = BucketTransformParameters()
        bucket_transform_parameters["numBuckets"] = num_buckets
        bucket_transform_parameters["columnNames"] = column_names
        bucket_transform_parameters["bucketingStrategy"] = bucketing_strategy

        return bucket_transform_parameters

    @property
    def num_buckets(self) -> int:
        """
        The total number of buckets to create for values of the column
        """
        return self["numBuckets"]

    @property
    def column_names(self) -> List[str]:
        """
        An ordered list of unique column names from the table schema
        to use for bucketings.
        """
        return self["columnNames"]

    @property
    def bucketing_strategy(self) -> BucketingStrategy:
        """
        The bucketing strategy to used.
        """
        return self["bucketingStrategy"]


class Transform(dict):
    """
    A transform is represents how a particular column value can be
    transformed into a new value. This is mostly used in the context
    of partitioning the data files in a table.
    """

    @staticmethod
    def of(
        name: TransformName,
        parameters: TransformParameters,
    ) -> Transform:
        partition_transform = Transform()
        partition_transform["name"] = name
        partition_transform["parameters"] = parameters
        return partition_transform

    @property
    def name(self) -> TransformName:
        return self["name"]

    @property
    def parameters(self) -> TransformParameters:
        return self["parameters"]
