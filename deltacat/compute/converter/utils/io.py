import deltacat.compute.converter.utils.iceberg_columns as sc
import daft


def download_data_table_and_append_iceberg_columns(
    file, columns_to_download, additional_columns_to_append, sequence_number
):
    # TODO; add S3 client kwargs
    table = download_parquet_with_daft_hash_applied(
        identify_columns=columns_to_download, file=file, s3_client_kwargs={}
    )
    if sc._FILE_PATH_COLUMN_NAME in additional_columns_to_append:
        table = sc.append_file_path_column(table, file.file_path)
    if sc._ORDERED_RECORD_IDX_COLUMN_NAME in additional_columns_to_append:
        record_idx_iterator = iter(range(len(table)))
        table = sc.append_record_idx_col(table, record_idx_iterator)
    return table


def download_parquet_with_daft_hash_applied(
    identify_columns, file, s3_client_kwargs, **kwargs
):
    from daft import TimeUnit

    # TODO: Add correct read kwargs as in:
    #  https://github.com/ray-project/deltacat/blob/383855a4044e4dfe03cf36d7738359d512a517b4/deltacat/utils/daft.py#L97

    coerce_int96_timestamp_unit = TimeUnit.from_str(
        kwargs.get("coerce_int96_timestamp_unit", "ms")
    )

    from deltacat.utils.daft import _get_s3_io_config

    # TODO: Use Daft SHA1 hash instead to minimize probably of data corruption
    io_config = _get_s3_io_config(s3_client_kwargs=s3_client_kwargs)
    df = daft.read_parquet(
        path=file.file_path,
        io_config=io_config,
        coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
    )
    df = df.select(daft.col(identify_columns[0]).hash())
    arrow_table = df.to_arrow()
    return arrow_table
