from deltacat.utils.common import env_string

# Environment variables
DELTACAT_LOG_LEVEL = env_string(
    "DELTACAT_LOG_LEVEL",
    "DEBUG"
)
APPLICATION_LOG_LEVEL = env_string(
    "APPLICATION_LOG_LEVEL",
    "DEBUG"
)

# Byte Units
BYTES_PER_KIBIBYTE = 2**10
BYTES_PER_MEBIBYTE = 2**20
BYTES_PER_GIBIBYTE = 2**30
BYTES_PER_TEBIBYTE = 2**40
BYTES_PER_PEBIBYTE = 2**50

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

PRIMARY_KEY_INDEX_WRITE_BOTO3_CONFIG = {
   "retries": {
      'max_attempts': 25,
      'mode': 'standard'
   }
}