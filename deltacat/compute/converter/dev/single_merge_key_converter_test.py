import pyarrow as pa
import uuid
import boto3

from typing import Optional

def get_s3_path(bucket_name: str, database_name: Optional[str] = None, table_name: Optional[str] = None) -> str:
    result_path = f"s3://{bucket_name}"
    if database_name is not None:
        result_path += f"/{database_name}.db"

    if table_name is not None:
        result_path += f"/{table_name}"
    return result_path

def get_bucket_name():
    return "metadata-py4j-zyiqin1"

def get_credential():
    boto3_session = boto3.Session()
    credentials = boto3_session.get_credentials()
    return credentials

def get_glue_catalog():
    from pyiceberg.catalog.glue import GLUE_CATALOG_ENDPOINT, GlueCatalog
    from pyiceberg.catalog import load_catalog
    credential = get_credential()
    access_key_id = credential.access_key
    secret_access_key = credential.secret_key
    session_token = credential.token
    s3_path = get_s3_path(get_bucket_name())
    glue_catalog = load_catalog("glue",
                                **{"warehouse": s3_path,
                                    "type": "glue",
                                    "aws_access_key_id": access_key_id,
                                    "aws_secret_access_key": secret_access_key,
                                    "aws_session_token": session_token,
                                    "region_name": "us-east-1",
                                    "s3.access-key-id": access_key_id,
                                    "s3.secret-access-key": secret_access_key,
                                    "s3.session-token": session_token,
                                    "s3.region": "us-east-1"})

    return glue_catalog

def get_table_schema():
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, StringType, DoubleType, DecimalType, IntegerType, BooleanType, LongType
    return Schema(
        NestedField(field_id=1, name="partitionkey", field_type=StringType(), required=False),
        NestedField(field_id=2, name="bucket", field_type=LongType(), required=False),
        NestedField(field_id=3, name="primarykey", field_type=StringType(), required=False),
        NestedField(field_id=2147483546, name='file_path', field_type=StringType(), required=False),
        NestedField(field_id=2147483545, name="pos", field_type=LongType(), require=False),
        schema_id=1
    )

def get_partition_spec():
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import BucketTransform, IdentityTransform
    NUM_OF_BUCKETS = 3
    partition_field_identity = PartitionField(
        source_id=1, field_id=101, transform=IdentityTransform(), name="partitionkey"
    )
    partition_spec = PartitionSpec(
        partition_field_identity
    )
    return partition_spec

def create_table_with_data_files_and_equality_deletes(table_version):
    glue_catalog = get_glue_catalog()
    schema = get_table_schema()
    ps = get_partition_spec()

    properties = dict()
    properties['write.format.default'] = 'parquet'
    properties['write.delete.mode']='merge-on-read'
    properties['write.update.mode']='merge-on-read'
    properties['write.merge.mode']='merge-on-read'
    properties['format-version'] = '2'
    glue_catalog.create_table(f"testio.example_{table_version}_partitioned", schema=schema, partition_spec=ps, properties=properties)
    loaded_table = glue_catalog.load_table(f"testio.example_{table_version}_partitioned")

def load_table(table_version):
    glue_catalog = get_glue_catalog()
    loaded_table = glue_catalog.load_table(f"testio.example_{table_version}_partitioned")
    return loaded_table

def get_s3_file_system():
    import pyarrow
    credential = get_credential()
    access_key_id = credential.access_key
    secret_access_key = credential.secret_key
    session_token = credential.token
    return pyarrow.fs.S3FileSystem(access_key=access_key_id, secret_key=secret_access_key, session_token=session_token)

def write_pos_delete_table(tmp_path: str, data_file_path) -> str:
    import pyarrow.parquet as pq
    uuid_path = uuid.uuid4()
    deletes_file_path = f"{tmp_path}/deletes_{uuid_path}.parquet"
    # Note: The following path should reference correct data file path to make sure positional delete are correctly applied
    # Hardcoded file path for quick POC purpose
    path = data_file_path
    # path = "s3://metadata-py4j-zyiqin1/data_a4f15d4a-20f6-4253-9926-d01c2cfbf884.parquet"
    table = pa.table({"partitionkey": ["1", "1"], "file_path": [path, "s3://metadata-py4j-zyiqin1/data_edde020f-f7d4-457d-9f9a-9331b7267860.parquet"], "pos": [1, 1]})
    file_system = get_s3_file_system()
    pq.write_table(table, deletes_file_path, filesystem=file_system)
    file_size_in_bytes = table.nbytes
    return build_delete_data_file(f"s3://{deletes_file_path}")

def write_data_table(tmp_path: str, batch_number, number_of_records, partition_value) -> str:
    import pyarrow.parquet as pq
    uuid_path = uuid.uuid4()
    deletes_file_path = f"{tmp_path}/data_{uuid_path}.parquet"
    table = generate_test_pyarrow_table(batch_number=batch_number,
                                        number_of_records=number_of_records,
                                        partition_value=partition_value)
    file_system = get_s3_file_system()
    pq.write_table(table, deletes_file_path, filesystem=file_system)
    file_size_in_bytes = table.nbytes
    return build_delete_data_file(f"s3://{deletes_file_path}")

def build_delete_data_file(file_path):
    from pyiceberg.manifest import DataFile, DataFileContent
    from pyiceberg.manifest import FileFormat
    print(f"build_delete_file_path:{file_path}")
    return file_path

def commit_pos_delete_to_table(table, data_file_paths):
    delete_s3_url = "metadata-py4j-zyiqin1"
    data_files = [write_pos_delete_table(delete_s3_url, data_file_paths)]
    add_delete_files(file_paths=data_files)


def commit_data_to_table(table, batch_number, number_of_records, partition_value):
    delete_s3_url = "metadata-py4j-zyiqin1"
    data_files = [write_data_table(delete_s3_url, batch_number, number_of_records, partition_value)]
    add_data_files(file_paths=data_files)
    return data_files

def commit_equality_delete_to_table(table, to_be_deleted_batch_number, partition_value, number_of_records):
    delete_s3_url = "metadata-py4j-zyiqin1"
    data_files = [write_equality_data_table(delete_s3_url=delete_s3_url,
                                            to_be_deleted_batch_number=to_be_deleted_batch_number,
                                            partition_value=partition_value,
                                            number_of_records=number_of_records)]
    add_equality_data_files(file_paths=data_files)
    return data_files

def write_equality_data_table(delete_s3_url, to_be_deleted_batch_number, partition_value, number_of_records):
    import pyarrow.parquet as pq
    uuid_path = uuid.uuid4()
    deletes_file_path = f"{delete_s3_url}/equality_delete_{uuid_path}.parquet"
    table = generate_test_pyarrow_table(batch_number=to_be_deleted_batch_number, partition_value=partition_value, number_of_records=number_of_records)
    file_system = get_s3_file_system()
    pq.write_table(table, deletes_file_path, filesystem=file_system)
    file_size_in_bytes = table.nbytes
    return build_delete_data_file(f"s3://{deletes_file_path}")

def scan_new_table(table):
    task = table.scan(snapshot_id=table.snapshot_id).plan_files()
    file = task.file
    path = file.file_path
    record_count = file.record_count
    file_format = file.file_format
    iceberg_delete_files = [f.file_path for f in task.delete_files]

def generate_test_pyarrow_table(batch_number, number_of_records, partition_value):
    primary_keys_iterables = range(1, number_of_records + 1, 1)
    primary_keys = list(f"pk_sequence{batch_number}_value{str(index)}" for index in primary_keys_iterables)
    print(f"primary_keys:{primary_keys}")
    test_table = pa.table({"partitionkey": [partition_value] * number_of_records, "primarykey": primary_keys, "bucket": [1] * number_of_records})
    return test_table


# commit to s3
def parquet_files_to_positional_delete_files(io, table_metadata, file_paths):
    from pyiceberg.io.pyarrow import (_check_pyarrow_schema_compatible, data_file_statistics_from_parquet_metadata,
                                      compute_statistics_plan, parquet_path_to_id_mapping)
    from pyiceberg.manifest import (
        DataFile,
        DataFileContent,
        FileFormat,
    )
    import pyarrow.parquet as pq
    from pyiceberg.typedef import Record
    for file_path in file_paths:
        input_file = io.new_input(file_path)
        with input_file.open() as input_stream:
            parquet_metadata = pq.read_metadata(input_stream)

        schema = table_metadata.schema()
        _check_pyarrow_schema_compatible(schema, parquet_metadata.schema.to_arrow_schema())

        statistics = data_file_statistics_from_parquet_metadata(
            parquet_metadata=parquet_metadata,
            stats_columns=compute_statistics_plan(schema, table_metadata.properties),
            parquet_column_mapping=parquet_path_to_id_mapping(schema),
        )
        data_file = DataFile(
            content=DataFileContent.POSITION_DELETES,
            file_path=file_path,
            file_format=FileFormat.PARQUET,
            partition=Record(partitionkey="111"),
            # partition=Record(**{"pk": "111", "bucket": 2}),
            file_size_in_bytes=len(input_file),
            sort_order_id=None,
            spec_id=table_metadata.default_spec_id,
            equality_ids=None,
            key_metadata=None,
            **statistics.to_serialized_dict(),
        )

        yield data_file

def produce_pos_delete_file(io, table_metadata, file_path):
    from pyiceberg.io.pyarrow import (_check_pyarrow_schema_compatible, data_file_statistics_from_parquet_metadata,
                                      compute_statistics_plan, parquet_path_to_id_mapping)
    from pyiceberg.manifest import (
        DataFile,
        DataFileContent,
        FileFormat,
    )
    import pyarrow.parquet as pq
    from pyiceberg.typedef import Record
    input_file = io.new_input(file_path)
    with input_file.open() as input_stream:
        parquet_metadata = pq.read_metadata(input_stream)

    schema = table_metadata.schema()
    _check_pyarrow_schema_compatible(schema, parquet_metadata.schema.to_arrow_schema())

    statistics = data_file_statistics_from_parquet_metadata(
        parquet_metadata=parquet_metadata,
        stats_columns=compute_statistics_plan(schema, table_metadata.properties),
        parquet_column_mapping=parquet_path_to_id_mapping(schema),
    )
    data_file = DataFile(
        content=DataFileContent.POSITION_DELETES,
        file_path=file_path,
        file_format=FileFormat.PARQUET,
        partition=Record(partitionkey="111"),
        # partition=Record(**{"pk": "111", "bucket": 2}),
        file_size_in_bytes=len(input_file),
        sort_order_id=None,
        spec_id=table_metadata.default_spec_id,
        equality_ids=None,
        key_metadata=None,
        **statistics.to_serialized_dict(),
    )

    return data_file

def parquet_files_to_data_files(io, table_metadata, file_paths):
    from pyiceberg.io.pyarrow import (_check_pyarrow_schema_compatible, data_file_statistics_from_parquet_metadata,
                                      compute_statistics_plan, parquet_path_to_id_mapping)
    from pyiceberg.manifest import (
        DataFile,
        DataFileContent,
        FileFormat,
    )
    from pyiceberg.schema import Schema
    from pyiceberg.types import StructType, NestedField, StringType, DoubleType, DecimalType, IntegerType, BooleanType, LongType
    import pyarrow.parquet as pq
    from pyiceberg.typedef import Record
    for file_path in file_paths:
        input_file = io.new_input(file_path)
        with input_file.open() as input_stream:
            parquet_metadata = pq.read_metadata(input_stream)

        schema = table_metadata.schema()
        _check_pyarrow_schema_compatible(schema, parquet_metadata.schema.to_arrow_schema())

        statistics = data_file_statistics_from_parquet_metadata(
            parquet_metadata=parquet_metadata,
            stats_columns=compute_statistics_plan(schema, table_metadata.properties),
            parquet_column_mapping=parquet_path_to_id_mapping(schema),
        )
        # pv = Record(**{"pk": "222", "bucket": 1})
        pv = Record(**{"partitionkey": "111"})
        data_file = DataFile(
            content=DataFileContent.DATA,
            file_path=file_path,
            file_format=FileFormat.PARQUET,
            partition=pv,
            file_size_in_bytes=len(input_file),
            sort_order_id=None,
            spec_id=table_metadata.default_spec_id,
            equality_ids=None,
            key_metadata=None,
            **statistics.to_serialized_dict(),
        )

        yield data_file


def add_delete_files(file_paths):
    table = load_table(TABLE_VERSION)

    with table.transaction() as tx:
        if table.metadata.name_mapping() is None:
            table.set_properties(**{
                "schema.name-mapping.default": table.table_metadata.schema().name_mapping.model_dump_json()
            })
        with tx.update_snapshot().fast_append() as update_snapshot:
            data_files = parquet_files_to_positional_delete_files(
                table_metadata=table.metadata, file_paths=file_paths, io=table.io
            )
            for data_file in data_files:
                update_snapshot.append_data_file(data_file)


def yield_position_delete_file(data_file_paths):
    delete_s3_url = "metadata-py4j-zyiqin1"
    data_files = [write_pos_delete_table(delete_s3_url, data_file_paths)]
    pos_delete_file = produce_pos_delete_file(
                table_metadata=table.metadata, file_path=data_files[0], io=table.io
            )
    return pos_delete_file

def add_data_files(file_paths):
    table = load_table(TABLE_VERSION)
    # table.refresh()
    with table.transaction() as tx:
        if table.metadata.name_mapping() is None:
            tx.set_properties(**{
                "schema.name-mapping.default": table.metadata.schema().name_mapping.model_dump_json()
            })
        with tx.update_snapshot().fast_append() as update_snapshot:
            data_files = parquet_files_to_data_files(
                table_metadata=table.metadata, file_paths=file_paths, io=table.io
            )
            for data_file in data_files:
                update_snapshot.append_data_file(data_file)

def add_equality_data_files(file_paths):
    table = load_table(TABLE_VERSION)
    with table.transaction() as tx:
        if table.metadata.name_mapping() is None:
            tx.set_properties(**{
                "schema.name-mapping.default": table.metadata.schema().name_mapping.model_dump_json()
            })
        with tx.update_snapshot().fast_append() as update_snapshot:
            data_files = parquet_files_to_equality_data_files(
                table_metadata=table.metadata, file_paths=file_paths, io=table.io
            )
            for data_file in data_files:
                update_snapshot.append_data_file(data_file)

def parquet_files_to_equality_data_files(io, table_metadata, file_paths):
    from pyiceberg.io.pyarrow import (_check_pyarrow_schema_compatible, data_file_statistics_from_parquet_metadata,
                                      compute_statistics_plan, parquet_path_to_id_mapping)
    from pyiceberg.manifest import (
        DataFile,
        DataFileContent,
        FileFormat,
    )
    import pyarrow.parquet as pq
    from pyiceberg.typedef import Record
    for file_path in file_paths:
        input_file = io.new_input(file_path)
        with input_file.open() as input_stream:
            parquet_metadata = pq.read_metadata(input_stream)

        schema = table_metadata.schema()
        _check_pyarrow_schema_compatible(schema, parquet_metadata.schema.to_arrow_schema())

        statistics = data_file_statistics_from_parquet_metadata(
            parquet_metadata=parquet_metadata,
            stats_columns=compute_statistics_plan(schema, table_metadata.properties),
            parquet_column_mapping=parquet_path_to_id_mapping(schema),
        )
        data_file = DataFile(
            content=DataFileContent.EQUALITY_DELETES,
            file_path=file_path,
            file_format=FileFormat.PARQUET,
            partition=Record(partitionkey="111"),
            file_size_in_bytes=len(input_file),
            sort_order_id=None,
            spec_id=table_metadata.default_spec_id,
            equality_ids=None,
            key_metadata=None,
            **statistics.to_serialized_dict(),
        )

        yield data_file

def scan_table(table):
    print(f"scan_table result:{table.scan().to_arrow().to_pydict()}")



# Test with creating a new iceberg table
TABLE_VERSION = "36"
create_table_with_data_files_and_equality_deletes(TABLE_VERSION)
table = load_table(TABLE_VERSION)

# Using batch_number to simulate the snapshot sequence
# 1. commit equality delete batch 1, which shouldn't take into any effect as no data files is committed yet
batch_number_1 = 1
data_file_paths = commit_equality_delete_to_table(table, to_be_deleted_batch_number=batch_number_1, partition_value="1", number_of_records=1)

# 2. commit 3 records for data table batch 2
batch_number_2 = 2
commit_data_to_table(table, batch_number=batch_number_2, partition_value="1", number_of_records=3)

# 3. Commit 1 equality delete record for data table 2
batch_number_3 = 3
commit_equality_delete_to_table(table, to_be_deleted_batch_number=batch_number_2, partition_value = "1", number_of_records=1)

# 4. commit 3 records for data table batch 4
batch_number_4 = 4
commit_data_to_table(table, batch_number=batch_number_4, partition_value="1", number_of_records=3)

# 5. Commit 1 equality delete record for data table batch 4
batch_number_5 = 5
commit_equality_delete_to_table(table, to_be_deleted_batch_number=batch_number_4, partition_value="1", number_of_records=1)

# 6. Commit 3 records for data table batch 6
batch_number_6 = 6
commit_data_to_table(table, batch_number=batch_number_6, partition_value="1", number_of_records=3)
# Total records remaining should be 3 - 1 + 3 - 1 + 3 = 7 when reading with Spark


from deltacat.compute.converter.equality_delete_to_position_delete_session import convert_equality_deletes_to_position_deletes
convert_equality_deletes_to_position_deletes(
        compact_small_files=False,
        position_delete_for_single_file=True,
        task_max_parallelism=4096,
        iceberg_table=table)
