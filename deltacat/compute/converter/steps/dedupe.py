import pyarrow as pa
import pyarrow.compute as pc
import deltacat.compute.converter.utils.iceberg_columns as sc
from deltacat.compute.converter.utils.io import (
    download_data_table_and_append_iceberg_columns,
)


def dedupe_data_files(
    data_file_to_dedupe,
    identify_column_name_concatenated,
    identifier_columns,
    merge_sort_column,
):
    data_file_table = []

    # Sort data files by file sequence number first
    data_file_to_dedupe = sorted(data_file_to_dedupe, key=lambda f: f[0])
    for file_tuple in data_file_to_dedupe:
        sequence_number = file_tuple[0]
        data_file = file_tuple[1]
        data_file_to_dedupe_table = download_data_table_and_append_iceberg_columns(
            file=data_file,
            columns_to_download=identifier_columns,
            additional_columns_to_append=[
                sc._FILE_PATH_COLUMN_NAME,
                sc._ORDERED_RECORD_IDX_COLUMN_NAME,
            ],
            sequence_number=sequence_number,
        )
        data_file_table.append(data_file_to_dedupe_table)

    final_data_to_dedupe = pa.concat_tables(data_file_table)

    record_idx_iterator = iter(range(len(final_data_to_dedupe)))

    # Append global record index to used as aggregate column
    final_data_to_dedupe = sc.append_global_record_idx_column(
        final_data_to_dedupe, record_idx_iterator
    )

    final_data_table_indices = final_data_to_dedupe.group_by(
        identify_column_name_concatenated, use_threads=False
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
        [identify_column_name_concatenated, sc._GLOBAL_RECORD_IDX_COLUMN_NAME]
    )
    return final_data_table_to_delete
