import uuid
import logging
from pyiceberg.exceptions import NoSuchTableError
from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def get_s3_file_system():
    import pyarrow

    return pyarrow.fs.S3FileSystem(
        access_key="admin",
        secret_key="password",
        endpoint_override="http://localhost:9000",
    )
    #        'region="us-east-1", proxy_options={'scheme': 'http', 'host': 'localhost',
    # 'port': 9000, 'username': 'admin',
    # 'password': 'password'})


def write_equality_data_table(
    file_link_prefix, table, partition_value, equality_delete_table
):
    import pyarrow.parquet as pq

    uuid_path = uuid.uuid4()
    deletes_file_path = f"{file_link_prefix}/{uuid_path}_deletes.parquet"
    file_system = get_s3_file_system()
    pq.write_table(equality_delete_table, deletes_file_path, filesystem=file_system)
    return f"s3://{deletes_file_path}"


def add_equality_data_files(file_paths, table, partition_value):
    with table.transaction() as tx:
        if table.metadata.name_mapping() is None:
            tx.set_properties(
                **{
                    "schema.name-mapping.default": table.metadata.schema().name_mapping.model_dump_json()
                }
            )
        with tx.update_snapshot().fast_append() as update_snapshot:
            data_files = parquet_files_to_equality_data_files(
                table_metadata=table.metadata,
                file_paths=file_paths,
                io=table.io,
                partition_value=partition_value,
            )
            for data_file in data_files:
                update_snapshot.append_data_file(data_file)


def parquet_files_to_equality_data_files(
    io, table_metadata, file_paths, partition_value
):
    from pyiceberg.io.pyarrow import (
        _check_pyarrow_schema_compatible,
        data_file_statistics_from_parquet_metadata,
        compute_statistics_plan,
        parquet_path_to_id_mapping,
    )
    from pyiceberg.manifest import (
        DataFile,
        DataFileContent,
        FileFormat,
    )
    import pyarrow.parquet as pq

    for file_path in file_paths:
        input_file = io.new_input(file_path)
        with input_file.open() as input_stream:
            parquet_metadata = pq.read_metadata(input_stream)

        schema = table_metadata.schema()
        _check_pyarrow_schema_compatible(
            schema, parquet_metadata.schema.to_arrow_schema()
        )

        statistics = data_file_statistics_from_parquet_metadata(
            parquet_metadata=parquet_metadata,
            stats_columns=compute_statistics_plan(schema, table_metadata.properties),
            parquet_column_mapping=parquet_path_to_id_mapping(schema),
        )
        data_file = DataFile(
            content=DataFileContent.EQUALITY_DELETES,
            file_path=file_path,
            file_format=FileFormat.PARQUET,
            partition=partition_value,
            file_size_in_bytes=len(input_file),
            sort_order_id=None,
            spec_id=table_metadata.default_spec_id,
            equality_ids=None,
            key_metadata=None,
            **statistics.to_serialized_dict(),
        )

        yield data_file


def commit_equality_delete_to_table(
    table, file_link_prefix, partition_value, equality_delete_table
):

    data_files = [
        write_equality_data_table(
            table=table,
            file_link_prefix=file_link_prefix,
            partition_value=partition_value,
            equality_delete_table=equality_delete_table,
        )
    ]

    add_equality_data_files(
        file_paths=data_files, partition_value=partition_value, table=table
    )
    return data_files


def drop_table_if_exists(table, catalog):
    try:
        catalog.drop_table(table)
    except NoSuchTableError:
        logger.warning(f"table:{table} doesn't exist, not dropping table.")
