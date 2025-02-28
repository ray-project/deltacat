from deltacat.storage import StreamFormat

# These are the default hardcoded values for Rivulet-DeltaCAT storage catalog.
# TODO: Integrate these values into the storage interface dynamically.
#       Currently, these are defined as static defaults, but they should
#       be determined at runtime based on the storage catalog configuration.
#       This will ensure that they remain consistent across different storage
#       implementations and can be easily modified or overridden when needed.
DEFAULT_NAMESPACE = "DEFAULT"
DEFAULT_TABLE_VERSION = "1"
DEFAULT_STREAM_ID = "stream"
DEFAULT_STREAM_FORMAT = StreamFormat.DELTACAT
DEFAULT_PARTITION_ID = "partition"
DEFAULT_PARTITION_VALUES = ["default"]