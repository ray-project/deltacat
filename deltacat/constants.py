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
