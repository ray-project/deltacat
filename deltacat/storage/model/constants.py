from deltacat.storage import StreamFormat

# These are the default hardcoded values for Rivulet-DeltaCAT storage catalog.
# These defaults should be applied in catalog interface implementations
# Storage interface implementations should be agnostic to defaults and require full information
DEFAULT_NAMESPACE = "DEFAULT"
DEFAULT_TABLE_VERSION = "1"
DEFAULT_STREAM_ID = "stream"
DEFAULT_STREAM_FORMAT = StreamFormat.DELTACAT
DEFAULT_PARTITION_ID = "partition"
DEFAULT_PARTITION_VALUES = ["default"]
