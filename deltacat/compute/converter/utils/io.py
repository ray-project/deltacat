import logging
from deltacat import logs
import deltacat.compute.converter.utils.iceberg_columns as sc
import daft
from deltacat.utils.daft import _get_s3_io_config
from daft import TimeUnit
import pyarrow as pa
from deltacat.utils.pyarrow import sliced_string_cast
from deltacat.compute.converter.constants import IDENTIFIER_FIELD_DELIMITER

import pyarrow.compute as pc

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def download_data_table_and_append_iceberg_columns(
    file,
    columns_to_download,
    additional_columns_to_append,
    sequence_number,
    s3_client_kwargs,
):
    table = download_parquet_with_daft_hash_applied(
        identifier_columns=columns_to_download,
        file=file,
        s3_client_kwargs=s3_client_kwargs,
    )

    if sc._FILE_PATH_COLUMN_NAME in additional_columns_to_append:
        table = sc.append_file_path_column(table, file.file_path)
    if sc._ORDERED_RECORD_IDX_COLUMN_NAME in additional_columns_to_append:
        record_idx_iterator = iter(range(len(table)))
        table = sc.append_record_idx_col(table, record_idx_iterator)

    return table


def download_parquet_with_daft_hash_applied(
    identifier_columns, file, s3_client_kwargs, **kwargs
):

    # TODO: Add correct read kwargs as in:
    #  https://github.com/ray-project/deltacat/blob/383855a4044e4dfe03cf36d7738359d512a517b4/deltacat/utils/daft.py#L97

    coerce_int96_timestamp_unit = TimeUnit.from_str(
        kwargs.get("coerce_int96_timestamp_unit", "ms")
    )

    # TODO: Use Daft SHA1 hash instead to minimize probably of data corruption
    io_config = _get_s3_io_config(s3_client_kwargs=s3_client_kwargs)
    df = daft_read_parquet(
        path=file.file_path,
        io_config=io_config,
        coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
    )

    hash_column = concatenate_hashed_identifier_columns(
        df=df, identifier_columns=identifier_columns
    )

    table = pa.Table.from_arrays(
        [hash_column], names=[sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME]
    )

    return table


def daft_read_parquet(path, io_config, coerce_int96_timestamp_unit):
    df = daft.read_parquet(
        path=path,
        io_config=io_config,
        coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
    )
    return df


def concatenate_hashed_identifier_columns(df, identifier_columns):
    pk_hash_columns = []
    previous_hash_column_length = None
    for i in range(len(identifier_columns)):
        pk_hash_column = df.select(daft.col(identifier_columns[i]).hash())
        pk_hash_column_arrow = pk_hash_column.to_arrow()

        # Assert that each hash column downloaded are same length to ensure we don't create mismatch between columns.
        if not previous_hash_column_length:
            previous_hash_column_length = len(pk_hash_column_arrow)
        else:
            assert previous_hash_column_length == len(pk_hash_column_arrow), (
                f"Identifier column Length mismatch: {identifier_columns[i]} has length {len(pk_hash_column_arrow)} "
                f"but expected {previous_hash_column_length}."
            )
            previous_hash_column_length = len(pk_hash_column_arrow)

        # Convert identifier from different datatypes to string here
        pk_hash_column_str = sliced_string_cast(
            pk_hash_column_arrow[identifier_columns[i]]
        )
        assert len(pk_hash_column_str) == previous_hash_column_length, (
            f"Casting column Length mismatch: {identifier_columns[i]} has length {len(pk_hash_column_str)} after casting, "
            f"before casting length: {previous_hash_column_length}."
        )

        pk_hash_columns.append(pk_hash_column_str)

    pk_hash_columns.append(IDENTIFIER_FIELD_DELIMITER)
    pk_hash_columns_concatenated = pc.binary_join_element_wise(
        *pk_hash_columns, null_handling="replace"
    )
    assert len(pk_hash_columns_concatenated) == previous_hash_column_length, (
        f"Concatenated column Length mismatch: Final concatenated identifier column has length {len(pk_hash_columns_concatenated)}, "
        f"before concatenating length: {previous_hash_column_length}."
    )

    return pk_hash_columns_concatenated
