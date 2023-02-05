from deltacat.compute.compactor import SortKey, SortOrder, \
    RoundCompletionInfo, PrimaryKeyIndexVersionLocator, DeltaFileEnvelope, \
    DeltaFileLocator, PyArrowWriteResult

def delta_file_locator_to_mat_bucket_index(
        df_locator: DeltaFileLocator,
        materialize_bucket_count: int) -> int:
    digest = df_locator.digest()
    return int.from_bytes(digest, "big") % materialize_bucket_count