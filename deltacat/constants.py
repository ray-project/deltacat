from __future__ import annotations


from deltacat.utils.common import env_string, env_bool

# Environment variables
DELTACAT_SYS_LOG_LEVEL = env_string("DELTACAT_SYS_LOG_LEVEL", "DEBUG")
DELTACAT_SYS_LOG_DIR = env_string(
    "DELTACAT_SYS_LOG_DIR",
    "/tmp/deltacat/var/output/logs/",
)
DELTACAT_SYS_INFO_LOG_BASE_FILE_NAME = env_string(
    "DELTACAT_SYS_INFO_LOG_BASE_FILE_NAME",
    "deltacat-python.info.log",
)
DELTACAT_SYS_DEBUG_LOG_BASE_FILE_NAME = env_string(
    "DELTACAT_SYS_DEBUG_LOG_BASE_FILE_NAME",
    "deltacat-python.debug.log",
)

DELTACAT_APP_LOG_LEVEL = env_string("DELTACAT_APP_LOG_LEVEL", "DEBUG")
DELTACAT_APP_LOG_DIR = env_string(
    "DELTACAT_APP_LOG_DIR",
    "/tmp/deltacat/var/output/logs/",
)
DELTACAT_APP_INFO_LOG_BASE_FILE_NAME = env_string(
    "DELTACAT_APP_INFO_LOG_BASE_FILE_NAME",
    "application.info.log",
)
DELTACAT_APP_DEBUG_LOG_BASE_FILE_NAME = env_string(
    "DELTACAT_APP_DEBUG_LOG_BASE_FILE_NAME",
    "application.debug.log",
)
# A json context which will be logged along with other context args.
DELTACAT_LOGGER_CONTEXT = env_string("DELTACAT_LOGGER_CONTEXT", None)
DELTACAT_LOGGER_USE_SINGLE_HANDLER = env_bool(
    "DELTACAT_LOGGER_USE_SINGLE_HANDLER",
    False,
)

DELTACAT_METAFILE_FORMAT = env_string("DELTACAT_METAFILE_FORMAT", "msgpack")


# Byte Units
BYTES_PER_KIBIBYTE = 2**10
BYTES_PER_MEBIBYTE = 2**20
BYTES_PER_GIBIBYTE = 2**30
BYTES_PER_TEBIBYTE = 2**40
BYTES_PER_PEBIBYTE = 2**50

SIGNED_INT64_MIN_VALUE = -(2**63)
SIGNED_INT64_MAX_VALUE = 2**63 - 1

# Time Units
NANOS_PER_SEC = 1_000_000_000
MICROS_PER_SEC = 1_000_000
MILLIS_PER_SEC = 1000

# Inflation multiplier from snappy-compressed parquet to pyarrow.
# This should be kept larger than actual average inflation multipliers.
# Note that this is a very rough guess since actual observed pyarrow
# inflation multiplier for snappy-compressed parquet is about 5.45X for
# all rows, but here we're trying to guess the inflation multipler for just
# a primary key SHA1 digest and sort key columns (which could be all columns
# of the table in the worst case, but here we're assuming that they
# represent no more than ~1/4th of the total table bytes)
PYARROW_INFLATION_MULTIPLIER = 2.5

# Inflation multiplier from snappy-compressed parquet to pyarrow for all columns.
PYARROW_INFLATION_MULTIPLIER_ALL_COLUMNS = 6

MEMORY_TO_HASH_BUCKET_COUNT_RATIO = 0.0512 * BYTES_PER_TEBIBYTE

# The number of bytes allocated to null values in string physical type in parquet
NULL_SIZE_BYTES = 4

# Metastore Constants
REVISION_DIR_NAME: str = "rev"
METAFILE_EXT = ".json" if DELTACAT_METAFILE_FORMAT == "json" else ".mpk"
TXN_DIR_NAME: str = "txn"
RUNNING_TXN_DIR_NAME: str = "running"
FAILED_TXN_DIR_NAME: str = "failed"
SUCCESS_TXN_DIR_NAME: str = "success"
TXN_PART_SEPARATOR = "_"
