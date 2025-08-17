DEFAULT_CONVERTER_TASK_MAX_PARALLELISM = 4096

# Safe limit ONLY considering CPU limit, typically 32 for a 8x-large worker
DEFAULT_MAX_PARALLEL_DATA_FILE_DOWNLOAD = 30


# Unique identifier delimiter to ensure different primary key don't end up with same hash when concatenated.
# e.g.: pk column a with value: 1, 12; pk column b with value: 12, 1; Without delimiter will both become "121".
IDENTIFIER_FIELD_DELIMITER = "c303282d"
