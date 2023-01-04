from deltacat.constants import PYARROW_INFLATION_MULTIPLIER_ALL_COLUMNS

def estimation_function(content_length, content_type, content_encoding, *args, **kwargs):
    # TODO(zyiqin): update the estimation here to be consistent with number of required worker nodes estimate.
    #  Current implementation is only a rough guess using the PYARROW_INFLATION_MULTIPLIER(content_length to pyarrow butes(all columns).
    #  The full implementation logic should be:
    #      1. liner regression with a confidence level: pull metastats data for all deltas for this partition if len(datapoints) > 30.
    #      2. if not enough previous stats collected for same partition: Fall back to datapoints for all paritions for same table.
    #      3. If not enough stats collected for this table: use average content length to each content_type and content_encoding inflation rates
    #      4. If not enough stats for this content_type and content_encoding combination: use the basic PYARROW_INFLATION_MULTIPLIER instead.

    if content_length:
        return content_length * PYARROW_INFLATION_MULTIPLIER_ALL_COLUMNS
    else:
        return 0