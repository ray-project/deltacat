from __future__ import annotations
from enum import Enum
from typing import Dict, Any, Optional


class TransformName(str, Enum):
    IDENTITY = "identity"
    BUCKET = "bucket"
    YEAR = "year"
    MONTH = "month"
    DAY = "day"
    HOUR = "hour"
    TRUNCATE = "truncate"
    VOID = "void"
    UNKNOWN = "unknown"


class TransformParameters(dict):
    """
    This is a parent class that contains properties
    to be passed to the corresponding transform
    """
    pass


class BucketingStrategy(str, Enum):
    """
    A bucketing strategy for the transform
    """

    # Default DeltaCAT SHA-1 based hash bucketing strategy.
    DEFAULT = "default"

    # Iceberg-compliant murmur3 based hash bucketing strategy.
    ICEBERG = "iceberg"


class BucketTransformParameters(TransformParameters):
    """
    Parameters for the bucket transform.
    """
    @staticmethod
    def of(
        num_buckets: int,
        bucketing_strategy: BucketingStrategy,
    ) -> BucketTransformParameters:
        bucket_transform_parameters = BucketTransformParameters()
        bucket_transform_parameters["numBuckets"] = num_buckets
        bucket_transform_parameters["bucketingStrategy"] = bucketing_strategy

        return bucket_transform_parameters

    @property
    def num_buckets(self) -> int:
        """
        The total number of buckets to create.
        """
        return self["numBuckets"]

    @property
    def bucketing_strategy(self) -> BucketingStrategy:
        """
        The bucketing strategy to use.
        """
        return BucketingStrategy(self["bucketingStrategy"])


class TruncateTransformParameters(TransformParameters):
    """
    Parameters for the truncate transform.
    """

    @staticmethod
    def of(width: int) -> TruncateTransformParameters:
        truncate_transform_parameters = TruncateTransformParameters()
        truncate_transform_parameters["width"] = width
        return truncate_transform_parameters

    @property
    def width(self) -> int:
        """
        The width to truncate the field to.
        """
        return self["width"]


class Transform(dict):
    """
    A transform represents how a particular column value can be
    transformed into a new value. For example, transforms may be used
    to determine partition or sort values for table records.
    """

    @property
    def name(self) -> TransformName:
        return TransformName(self["name"])

    @name.setter
    def name(self, name: TransformName) -> None:
        self["name"] = name

    @property
    def parameters(self) -> Optional[TransformParameters]:
        val: Dict[str, Any] = self.get("parameters")
        if val is not None and not isinstance(val, TransformParameters):
            self["parameters"] = val = TransformParameters(val)
        return val

    @parameters.setter
    def parameters(
            self,
            parameters: Optional[TransformParameters] = None,
    ) -> None:
        self["parameters"] = parameters


class BucketTransform(Transform):
    """
    A transform that hashes field values into a fixed number of buckets.
    """

    @staticmethod
    def of(parameters: BucketTransformParameters) -> BucketTransform:
        transform = BucketTransform()
        transform.name = TransformName.BUCKET
        super().parameters = parameters
        return transform

    @property
    def parameters(self) -> BucketTransformParameters:
        return BucketTransformParameters(super().parameters)


class TruncateTransform(Transform):
    """
    A transform that truncates field values to a fixed width.
    """

    @staticmethod
    def of(parameters: TruncateTransformParameters) -> TruncateTransform:
        transform = TruncateTransform()
        transform.name = TransformName.TRUNCATE
        super().parameters = parameters
        return transform

    @property
    def parameters(self) -> TruncateTransformParameters:
        return TruncateTransformParameters(super().parameters)


class IdentityTransform(Transform):
    """
    A no-op transform that returns unmodified field values.
    """

    @staticmethod
    def of() -> IdentityTransform:
        transform = IdentityTransform()
        transform.name = TransformName.IDENTITY
        return transform


class HourTransform(Transform):
    """
    A transform that returns the hour of a datetime value.
    """

    @staticmethod
    def of() -> HourTransform:
        transform = HourTransform()
        transform.name = TransformName.HOUR
        return transform


class DayTransform(Transform):
    """
    A transform that returns the day of a datetime value.
    """

    @staticmethod
    def of() -> DayTransform:
        transform = DayTransform()
        transform.name = TransformName.DAY
        return transform


class MonthTransform(Transform):
    """
    A transform that returns the month of a datetime value.
    """

    @staticmethod
    def of() -> MonthTransform:
        transform = MonthTransform()
        transform.name = TransformName.MONTH
        return transform


class YearTransform(Transform):
    """
    A transform that returns the year of a datetime value.
    """

    @staticmethod
    def of() -> YearTransform:
        transform = YearTransform()
        transform.name = TransformName.YEAR
        return transform


class VoidTransform(Transform):
    """
    A transform that coerces all field values to None.
    """

    @staticmethod
    def of() -> VoidTransform:
        transform = VoidTransform()
        transform.name = TransformName.VOID
        return transform


class UnknownTransform(Transform):
    """
    An unknown or invalid transform.
    """

    @staticmethod
    def of() -> UnknownTransform:
        transform = UnknownTransform()
        transform.name = TransformName.UNKNOWN
        return transform
