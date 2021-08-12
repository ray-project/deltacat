from enum import Enum


class DeltaType(Enum):
    UPSERT = "upsert"
    DELETE = "delete"


class LifecycleState(Enum):
    UNRELEASED = "unreleased"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    BETA = "beta"
    DELETED = "deleted"
