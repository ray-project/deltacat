from enum import Enum


class DeltaType(str, Enum):
    UPSERT = "upsert"
    DELETE = "delete"


class LifecycleState(str, Enum):
    UNRELEASED = "unreleased"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    BETA = "beta"
    DELETED = "deleted"


class SchemaConsistencyType(str, Enum):
    NONE = "none"
    COERCE = "coerce"
    VALIDATE = "validate"
