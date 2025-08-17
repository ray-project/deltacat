import pyarrow as pa
import pyarrow.compute as pc
import deltacat.compute.converter.utils.iceberg_columns as sc
from deltacat.compute.converter.utils.io import (
    download_data_table_and_append_iceberg_columns,
)
from deltacat.compute.converter.utils.converter_session_utils import (
    sort_data_files_maintaining_order,
)
import logging
from deltacat import logs
from typing import List, Dict, Tuple, Optional, Any
from pyiceberg.manifest import DataFile

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def dedupe_data_files(
    data_file_to_dedupe: List[Tuple[int, DataFile]],
    identifier_columns: List[str],
    remaining_data_table_after_convert: Optional[pa.Table],
    merge_sort_column: str,
    s3_client_kwargs: Optional[Dict[str, Any]],
) -> Tuple[pa.Table, int, int]:
    data_file_table = []
    if remaining_data_table_after_convert:
        data_file_table.append(remaining_data_table_after_convert)

    data_file_to_dedupe = sort_data_files_maintaining_order(
        data_files=data_file_to_dedupe
    )
    downloaded_data_file_record_count = 0
    for file_tuple in data_file_to_dedupe:
        data_file = file_tuple[1]
        data_file_to_dedupe_table = download_data_table_and_append_iceberg_columns(
            file=data_file,
            columns_to_download=identifier_columns,
            additional_columns_to_append=[
                sc._FILE_PATH_COLUMN_NAME,
                sc._ORDERED_RECORD_IDX_COLUMN_NAME,
            ],
            s3_client_kwargs=s3_client_kwargs,
        )
        logger.info(
            f"Length of downloaded data file table: {len(data_file_to_dedupe_table)}"
        )
        downloaded_data_file_record_count += len(data_file_to_dedupe_table)
        data_file_table.append(data_file_to_dedupe_table)

    final_data_to_dedupe = pa.concat_tables(data_file_table)

    dedupe_input_record_count = downloaded_data_file_record_count
    if remaining_data_table_after_convert:
        dedupe_input_record_count += len(remaining_data_table_after_convert)
    assert len(final_data_to_dedupe) == dedupe_input_record_count, (
        f"Mismatch record count while performing table concat, Got {len(final_data_to_dedupe)} in final table, "
        f"while input table length is: {dedupe_input_record_count}"
    )

    logger.info(f"Length of pyarrow table to dedupe:{len(final_data_to_dedupe)}")

    record_idx_iterator = iter(range(len(final_data_to_dedupe)))

    # Append global record index to used as aggregate column
    final_data_to_dedupe = sc.append_global_record_idx_column(
        final_data_to_dedupe, record_idx_iterator
    )

    final_data_table_indices = final_data_to_dedupe.group_by(
        sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME, use_threads=False
    ).aggregate([(sc._GLOBAL_RECORD_IDX_COLUMN_NAME, "max")])

    pos_delete_indices = pc.invert(
        pc.is_in(
            final_data_to_dedupe[sc._GLOBAL_RECORD_IDX_COLUMN_NAME],
            value_set=final_data_table_indices[
                f"{sc._GLOBAL_RECORD_IDX_COLUMN_NAME}_max"
            ],
        )
    )

    final_data_table_to_delete = final_data_to_dedupe.filter(pos_delete_indices)

    final_data_table_to_delete = final_data_table_to_delete.drop(
        [sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME, sc._GLOBAL_RECORD_IDX_COLUMN_NAME]
    )
    logger.info(
        f"Deduped {len(final_data_table_to_delete)} Records based off identifier columns."
    )
    return (
        final_data_table_to_delete,
        len(final_data_to_dedupe),
        int(final_data_to_dedupe.nbytes),
    )
