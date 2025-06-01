from unittest import TestCase
from deltacat.utils.pyarrow import (
    s3_partial_parquet_file_to_table,
    pyarrow_read_csv,
    content_type_to_reader_kwargs,
    _add_column_kwargs,
    s3_file_to_table,
    ReadKwargsProviderPyArrowSchemaOverride,
    RAISE_ON_EMPTY_CSV_KWARG,
    table_to_file,
)
from deltacat.types.media import ContentEncoding, ContentType
from deltacat.types.partial_download import PartialParquetParameters
from pyarrow.parquet import ParquetFile
import tempfile
import pyarrow as pa
import fsspec
import gzip
import json
from pyarrow import (
    feather as paf,
    parquet as papq,
    orc as paorc,
)

PARQUET_FILE_PATH = "deltacat/tests/utils/data/test_file.parquet"
EMPTY_UTSV_PATH = "deltacat/tests/utils/data/empty.csv"
NON_EMPTY_VALID_UTSV_PATH = "deltacat/tests/utils/data/non_empty_valid.csv"
GZIP_COMPRESSED_FILE_UTSV_PATH = "deltacat/tests/utils/data/non_empty_compressed.gz"
BZ2_COMPRESSED_FILE_UTSV_PATH = "deltacat/tests/utils/data/non_empty_compressed.bz2"


class TestS3PartialParquetFileToTable(TestCase):
    def test_s3_partial_parquet_file_to_table_sanity(self):

        pq_file = ParquetFile(PARQUET_FILE_PATH)
        partial_parquet_params = PartialParquetParameters.of(
            pq_metadata=pq_file.metadata
        )

        self.assertEqual(
            partial_parquet_params.num_row_groups, 2, "test_file.parquet has changed."
        )

        # only first row group to be downloaded
        partial_parquet_params.row_groups_to_download.pop()

        result = s3_partial_parquet_file_to_table(
            PARQUET_FILE_PATH,
            include_columns=["n_legs"],
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            partial_file_download_params=partial_parquet_params,
        )

        self.assertEqual(len(result), 3)
        self.assertEqual(len(result.columns), 1)

    def test_s3_partial_parquet_file_to_table_when_schema_passed(self):

        pq_file = ParquetFile(PARQUET_FILE_PATH)
        partial_parquet_params = PartialParquetParameters.of(
            pq_metadata=pq_file.metadata
        )
        # only first row group to be downloaded
        partial_parquet_params.row_groups_to_download.pop()

        schema = pa.schema(
            [
                pa.field("n_legs", pa.string()),
                pa.field("animal", pa.string()),
                # NOTE: This field is not in the parquet file, but will be added on as an all-null column
                pa.field("MISSING", pa.int64()),
            ]
        )

        pa_kwargs_provider = lambda content_type, kwargs: {"schema": schema}

        result = s3_partial_parquet_file_to_table(
            PARQUET_FILE_PATH,
            ContentType.PARQUET.value,
            ContentEncoding.IDENTITY.value,
            pa_read_func_kwargs_provider=pa_kwargs_provider,
            partial_file_download_params=partial_parquet_params,
        )

        self.assertEqual(len(result), 3)
        self.assertEqual(len(result.column_names), 3)

        result_schema = result.schema
        self.assertEqual(result_schema.field(0).type, "string")
        self.assertEqual(result_schema.field(0).name, "n_legs")
        self.assertEqual(result_schema.field(1).type, "string")
        self.assertEqual(result_schema.field(1).name, "animal")
        self.assertEqual(result_schema.field(2).type, "int64")
        self.assertEqual(result_schema.field(2).name, "MISSING")

    def test_s3_partial_parquet_file_to_table_when_schema_missing_columns(self):

        pq_file = ParquetFile(PARQUET_FILE_PATH)
        partial_parquet_params = PartialParquetParameters.of(
            pq_metadata=pq_file.metadata
        )
        # only first row group to be downloaded
        partial_parquet_params.row_groups_to_download.pop()

        schema = pa.schema(
            [
                pa.field("n_legs", pa.string()),
                pa.field("animal", pa.string()),
                # NOTE: This field is not in the parquet file, but will be added on as an all-null column
                pa.field("MISSING", pa.int64()),
            ]
        )

        pa_kwargs_provider = lambda content_type, kwargs: {"schema": schema}

        result = s3_partial_parquet_file_to_table(
            PARQUET_FILE_PATH,
            ContentType.PARQUET.value,
            ContentEncoding.IDENTITY.value,
            pa_read_func_kwargs_provider=pa_kwargs_provider,
            partial_file_download_params=partial_parquet_params,
            column_names=["n_legs", "animal", "MISSING"],
            include_columns=["MISSING"],
        )

        self.assertEqual(len(result), 0)
        self.assertEqual(len(result.column_names), 1)

        result_schema = result.schema
        self.assertEqual(result_schema.field(0).type, "int64")
        self.assertEqual(result_schema.field(0).name, "MISSING")

    def test_s3_partial_parquet_file_to_table_when_schema_passed_with_include_columns(
        self,
    ):

        pq_file = ParquetFile(PARQUET_FILE_PATH)
        partial_parquet_params = PartialParquetParameters.of(
            pq_metadata=pq_file.metadata
        )
        # only first row group to be downloaded
        partial_parquet_params.row_groups_to_download.pop()

        schema = pa.schema(
            [pa.field("animal", pa.string()), pa.field("n_legs", pa.string())]
        )

        pa_kwargs_provider = lambda content_type, kwargs: {"schema": schema}

        result = s3_partial_parquet_file_to_table(
            PARQUET_FILE_PATH,
            ContentType.PARQUET.value,
            ContentEncoding.IDENTITY.value,
            ["n_legs", "animal"],
            pa_read_func_kwargs_provider=pa_kwargs_provider,
            partial_file_download_params=partial_parquet_params,
        )

        self.assertEqual(len(result), 3)
        self.assertEqual(len(result.column_names), 2)

        result_schema = result.schema
        self.assertEqual(result_schema.field(0).type, "string")
        self.assertEqual(result_schema.field(0).name, "n_legs")  # order doesn't change

    def test_s3_partial_parquet_file_to_table_when_multiple_row_groups(self):

        pq_file = ParquetFile(PARQUET_FILE_PATH)
        partial_parquet_params = PartialParquetParameters.of(
            pq_metadata=pq_file.metadata
        )

        self.assertEqual(
            partial_parquet_params.num_row_groups, 2, "test_file.parquet has changed."
        )

        result = s3_partial_parquet_file_to_table(
            PARQUET_FILE_PATH,
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            partial_file_download_params=partial_parquet_params,
        )

        self.assertEqual(len(result), 6)
        self.assertEqual(len(result.columns), 2)


class TestReadCSV(TestCase):
    def test_read_csv_sanity(self):

        schema = pa.schema(
            [("is_active", pa.string()), ("ship_datetime_utc", pa.timestamp("us"))]
        )
        kwargs = content_type_to_reader_kwargs(ContentType.UNESCAPED_TSV.value)
        _add_column_kwargs(
            ContentType.UNESCAPED_TSV.value,
            ["is_active", "ship_datetime_utc"],
            None,
            kwargs,
        )

        read_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(schema=schema)

        kwargs = read_kwargs_provider(ContentType.UNESCAPED_TSV.value, kwargs)

        result = pyarrow_read_csv(NON_EMPTY_VALID_UTSV_PATH, **kwargs)

        self.assertEqual(len(result), 3)
        self.assertEqual(len(result.column_names), 2)
        result_schema = result.schema
        for index, field in enumerate(result_schema):
            self.assertEqual(field.name, schema.field(index).name)

        self.assertEqual(result.schema.field(0).type, "string")

    def test_read_csv_when_column_order_changes(self):

        schema = pa.schema(
            [("is_active", pa.string()), ("ship_datetime_utc", pa.timestamp("us"))]
        )
        kwargs = content_type_to_reader_kwargs(ContentType.UNESCAPED_TSV.value)
        _add_column_kwargs(
            ContentType.UNESCAPED_TSV.value,
            ["is_active", "ship_datetime_utc"],
            ["ship_datetime_utc", "is_active"],
            kwargs,
        )

        read_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(schema=schema)

        kwargs = read_kwargs_provider(ContentType.UNESCAPED_TSV.value, kwargs)

        result = pyarrow_read_csv(NON_EMPTY_VALID_UTSV_PATH, **kwargs)

        self.assertEqual(len(result), 3)
        self.assertEqual(len(result.column_names), 2)
        result_schema = result.schema
        self.assertEqual(result_schema.field(1).type, "string")
        self.assertEqual(result_schema.field(0).type, "timestamp[us]")

    def test_read_csv_when_partial_columns_included(self):

        schema = pa.schema(
            [("is_active", pa.string()), ("ship_datetime_utc", pa.timestamp("us"))]
        )
        kwargs = content_type_to_reader_kwargs(ContentType.UNESCAPED_TSV.value)
        _add_column_kwargs(
            ContentType.UNESCAPED_TSV.value,
            ["is_active", "ship_datetime_utc"],
            ["is_active"],
            kwargs,
        )

        read_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(schema=schema)

        kwargs = read_kwargs_provider(ContentType.UNESCAPED_TSV.value, kwargs)

        result = pyarrow_read_csv(NON_EMPTY_VALID_UTSV_PATH, **kwargs)

        self.assertEqual(len(result), 3)
        self.assertEqual(len(result.column_names), 1)
        result_schema = result.schema
        self.assertEqual(result_schema.field(0).type, "string")

    def test_read_csv_when_column_names_partial(self):

        schema = pa.schema(
            [("is_active", pa.string()), ("ship_datetime_utc", pa.timestamp("us"))]
        )
        kwargs = content_type_to_reader_kwargs(ContentType.UNESCAPED_TSV.value)
        _add_column_kwargs(ContentType.UNESCAPED_TSV.value, ["is_active"], None, kwargs)

        read_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(schema=schema)

        kwargs = read_kwargs_provider(ContentType.UNESCAPED_TSV.value, kwargs)

        self.assertRaises(
            pa.lib.ArrowInvalid,
            lambda: pyarrow_read_csv(NON_EMPTY_VALID_UTSV_PATH, **kwargs),
        )

    def test_read_csv_when_excess_columns_included(self):

        schema = pa.schema(
            [
                ("is_active", pa.string()),
                ("ship_datetime_utc", pa.timestamp("us")),
                ("MISSING", pa.string()),
            ]
        )
        kwargs = content_type_to_reader_kwargs(ContentType.UNESCAPED_TSV.value)
        _add_column_kwargs(
            ContentType.UNESCAPED_TSV.value,
            ["is_active", "ship_datetime_utc", "MISSING"],
            ["is_active", "ship_datetime_utc", "MISSING"],
            kwargs,
        )

        read_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(schema=schema)

        kwargs = read_kwargs_provider(ContentType.UNESCAPED_TSV.value, kwargs)

        self.assertRaises(
            pa.lib.ArrowInvalid,
            lambda: pyarrow_read_csv(NON_EMPTY_VALID_UTSV_PATH, **kwargs),
        )

    def test_read_csv_when_empty_csv_sanity(self):

        schema = pa.schema(
            [("is_active", pa.string()), ("ship_datetime_utc", pa.timestamp("us"))]
        )
        kwargs = content_type_to_reader_kwargs(ContentType.UNESCAPED_TSV.value)
        _add_column_kwargs(
            ContentType.UNESCAPED_TSV.value,
            ["is_active", "ship_datetime_utc"],
            None,
            kwargs,
        )

        read_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(schema=schema)
        kwargs = read_kwargs_provider(ContentType.UNESCAPED_TSV.value, kwargs)
        result = pyarrow_read_csv(EMPTY_UTSV_PATH, **kwargs)

        self.assertEqual(len(result), 0)
        self.assertEqual(len(result.column_names), 2)
        result_schema = result.schema
        self.assertEqual(result_schema.field(0).type, "string")
        self.assertEqual(result_schema.field(1).type, "timestamp[us]")

    def test_read_csv_when_empty_csv_include_columns(self):

        schema = pa.schema(
            [("is_active", pa.string()), ("ship_datetime_utc", pa.timestamp("us"))]
        )
        kwargs = content_type_to_reader_kwargs(ContentType.UNESCAPED_TSV.value)
        _add_column_kwargs(
            ContentType.UNESCAPED_TSV.value,
            ["is_active", "ship_datetime_utc"],
            ["ship_datetime_utc", "is_active"],
            kwargs,
        )

        read_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(schema=schema)

        kwargs = read_kwargs_provider(ContentType.UNESCAPED_TSV.value, kwargs)

        result = pyarrow_read_csv(EMPTY_UTSV_PATH, **kwargs)

        self.assertEqual(len(result), 0)
        self.assertEqual(len(result.column_names), 2)
        result_schema = result.schema
        self.assertEqual(result_schema.field(1).type, "string")
        self.assertEqual(result_schema.field(0).type, "timestamp[us]")

    def test_read_csv_when_empty_csv_include_partial_columns(self):

        schema = pa.schema(
            [("is_active", pa.string()), ("ship_datetime_utc", pa.timestamp("us"))]
        )
        kwargs = content_type_to_reader_kwargs(ContentType.UNESCAPED_TSV.value)
        _add_column_kwargs(
            ContentType.UNESCAPED_TSV.value,
            ["is_active", "ship_datetime_utc"],
            ["is_active"],
            kwargs,
        )

        read_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(schema=schema)

        kwargs = read_kwargs_provider(ContentType.UNESCAPED_TSV.value, kwargs)

        result = pyarrow_read_csv(EMPTY_UTSV_PATH, **kwargs)

        self.assertEqual(len(result), 0)
        self.assertEqual(len(result.column_names), 1)
        result_schema = result.schema
        self.assertEqual(result_schema.field(0).type, "string")

    def test_read_csv_when_empty_csv_honors_column_names(self):

        schema = pa.schema(
            [("is_active", pa.string()), ("ship_datetime_utc", pa.timestamp("us"))]
        )
        kwargs = content_type_to_reader_kwargs(ContentType.UNESCAPED_TSV.value)
        _add_column_kwargs(ContentType.UNESCAPED_TSV.value, ["is_active"], None, kwargs)

        read_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(schema=schema)

        kwargs = read_kwargs_provider(ContentType.UNESCAPED_TSV.value, kwargs)

        result = pyarrow_read_csv(EMPTY_UTSV_PATH, **kwargs)

        self.assertEqual(len(result), 0)
        self.assertEqual(len(result.column_names), 1)
        result_schema = result.schema
        self.assertEqual(result_schema.field(0).type, "string")

    def test_read_csv_when_empty_csv_and_raise_on_empty_passed(self):

        schema = pa.schema(
            [("is_active", pa.string()), ("ship_datetime_utc", pa.timestamp("us"))]
        )
        kwargs = content_type_to_reader_kwargs(ContentType.UNESCAPED_TSV.value)
        _add_column_kwargs(ContentType.UNESCAPED_TSV.value, ["is_active"], None, kwargs)

        read_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(schema=schema)

        kwargs = read_kwargs_provider(ContentType.UNESCAPED_TSV.value, kwargs)

        self.assertRaises(
            pa.lib.ArrowInvalid,
            lambda: pyarrow_read_csv(
                EMPTY_UTSV_PATH, **{**kwargs, RAISE_ON_EMPTY_CSV_KWARG: True}
            ),
        )


class TestS3FileToTable(TestCase):
    def test_s3_file_to_table_identity_sanity(self):

        schema = pa.schema(
            [("is_active", pa.string()), ("ship_datetime_utc", pa.timestamp("us"))]
        )

        result = s3_file_to_table(
            NON_EMPTY_VALID_UTSV_PATH,
            ContentType.UNESCAPED_TSV.value,
            ContentEncoding.IDENTITY.value,
            ["is_active", "ship_datetime_utc"],
            None,
            pa_read_func_kwargs_provider=ReadKwargsProviderPyArrowSchemaOverride(
                schema=schema
            ),
        )

        self.assertEqual(len(result), 3)
        self.assertEqual(len(result.column_names), 2)
        result_schema = result.schema
        for index, field in enumerate(result_schema):
            self.assertEqual(field.name, schema.field(index).name)

        self.assertEqual(result.schema.field(0).type, "string")

    def test_s3_file_to_table_gzip_compressed_sanity(self):

        schema = pa.schema(
            [("is_active", pa.string()), ("ship_datetime_utc", pa.timestamp("us"))]
        )

        result = s3_file_to_table(
            GZIP_COMPRESSED_FILE_UTSV_PATH,
            ContentType.UNESCAPED_TSV.value,
            ContentEncoding.GZIP.value,
            ["is_active", "ship_datetime_utc"],
            None,
            pa_read_func_kwargs_provider=ReadKwargsProviderPyArrowSchemaOverride(
                schema=schema
            ),
        )

        self.assertEqual(len(result), 3)
        self.assertEqual(len(result.column_names), 2)
        result_schema = result.schema
        for index, field in enumerate(result_schema):
            self.assertEqual(field.name, schema.field(index).name)

        self.assertEqual(result.schema.field(0).type, "string")

    def test_s3_file_to_table_bz2_compressed_sanity(self):

        schema = pa.schema(
            [("is_active", pa.string()), ("ship_datetime_utc", pa.timestamp("us"))]
        )

        result = s3_file_to_table(
            BZ2_COMPRESSED_FILE_UTSV_PATH,
            ContentType.UNESCAPED_TSV.value,
            ContentEncoding.BZIP2.value,
            ["is_active", "ship_datetime_utc"],
            None,
            pa_read_func_kwargs_provider=ReadKwargsProviderPyArrowSchemaOverride(
                schema=schema
            ),
        )

        self.assertEqual(len(result), 3)
        self.assertEqual(len(result.column_names), 2)
        result_schema = result.schema
        for index, field in enumerate(result_schema):
            self.assertEqual(field.name, schema.field(index).name)

        self.assertEqual(result.schema.field(0).type, "string")

    def test_s3_file_to_table_when_parquet_sanity(self):

        pa_kwargs_provider = lambda content_type, kwargs: {
            "reader_type": "pyarrow",
            **kwargs,
        }

        result = s3_file_to_table(
            PARQUET_FILE_PATH,
            ContentType.PARQUET.value,
            ContentEncoding.IDENTITY.value,
            ["n_legs", "animal"],
            ["n_legs"],
            pa_read_func_kwargs_provider=pa_kwargs_provider,
        )

        self.assertEqual(len(result), 6)
        self.assertEqual(len(result.column_names), 1)
        schema = result.schema
        schema_index = schema.get_field_index("n_legs")
        self.assertEqual(schema.field(schema_index).type, "int64")

    def test_s3_file_to_table_when_parquet_schema_overridden(self):

        schema = pa.schema(
            [pa.field("animal", pa.string()), pa.field("n_legs", pa.string())]
        )

        pa_kwargs_provider = lambda content_type, kwargs: {
            "schema": schema,
            "reader_type": "pyarrow",
            **kwargs,
        }

        result = s3_file_to_table(
            PARQUET_FILE_PATH,
            ContentType.PARQUET.value,
            ContentEncoding.IDENTITY.value,
            ["n_legs", "animal"],
            pa_read_func_kwargs_provider=pa_kwargs_provider,
        )

        self.assertEqual(len(result), 6)
        self.assertEqual(len(result.column_names), 2)

        result_schema = result.schema
        for index, field in enumerate(result_schema):
            self.assertEqual(field.name, schema.field(index).name)

        self.assertEqual(result.schema.field(1).type, "string")


class TestWriters(TestCase):
    def setUp(self):
        self.table = pa.table({
            'col1': ['a,b\tc|d', 'e,f\tg|h'],
            'col2': [1, 2]
        })
        self.fs = fsspec.filesystem("file")
        self.base_path = tempfile.mkdtemp()
        self.fs.makedirs(self.base_path, exist_ok=True)

    def tearDown(self):
        self.fs.rm(self.base_path, recursive=True)

    def test_write_feather(self):
        path = f"{self.base_path}/test.feather"
        
        table_to_file(
            self.table,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.FEATHER.value
        )
        assert self.fs.exists(path), "file was not written"
        
        # Verify content
        result = paf.read_table(path)
        assert result.equals(self.table)

    def test_write_csv(self):
        path = f"{self.base_path}/test.csv"
        
        table_to_file(
            self.table,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.CSV.value
        )
        assert self.fs.exists(path), "file was not written"
        
        # Verify content (should be GZIP compressed)
        with self.fs.open(path, "rb") as f:
            with gzip.GzipFile(fileobj=f) as gz:
                content = gz.read().decode('utf-8')
                # Should be quoted due to commas in data
                assert '"a,b\tc|d",1' in content
                assert '"e,f\tg|h",2' in content

    def test_write_tsv(self):
        path = f"{self.base_path}/test.tsv"
        
        table_to_file(
            self.table,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.TSV.value,
        )
        assert self.fs.exists(path), "file was not written"
        
        # Verify content (should be GZIP compressed)
        with self.fs.open(path, "rb") as f:
            with gzip.GzipFile(fileobj=f) as gz:
                content = gz.read().decode('utf-8')
                # Should be quoted due to tabs in data
                assert '"a,b\tc|d"\t1' in content
                assert '"e,f\tg|h"\t2' in content

    def test_write_psv(self):
        path = f"{self.base_path}/test.psv"
        
        table_to_file(
            self.table,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.PSV.value,
        )
        assert self.fs.exists(path), "file was not written"
        
        # Verify content (should be GZIP compressed)
        with self.fs.open(path, "rb") as f:
            with gzip.GzipFile(fileobj=f) as gz:
                content = gz.read().decode('utf-8')
                # Should be quoted due to pipes in data
                assert '"a,b\tc|d"|1' in content
                assert '"e,f\tg|h"|2' in content

    def test_write_unescaped_tsv(self):
        # Create table without delimiters for unescaped TSV
        table = pa.table({
            'col1': ['abc', 'def'],
            'col2': [1, 2]
        })
        path = f"{self.base_path}/test.tsv"
        
        table_to_file(
            table,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.UNESCAPED_TSV.value,
        )
        assert self.fs.exists(path), "file was not written"
        
        # Verify content (should be GZIP compressed)
        with self.fs.open(path, "rb") as f:
            with gzip.GzipFile(fileobj=f) as gz:
                content = gz.read().decode('utf-8')
                # With quoting_style="none", strings should not be quoted
                assert 'abc\t1' in content
                assert 'def\t2' in content

    def test_write_orc(self):
        path = f"{self.base_path}/test.orc"
        
        table_to_file(
            self.table,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.ORC.value
        )
        assert self.fs.exists(path), "file was not written"
        
        # Verify content
        result = paorc.read_table(path)
        assert result.equals(self.table)

    def test_write_parquet(self):
        path = f"{self.base_path}/test.parquet"
        
        table_to_file(
            self.table,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.PARQUET.value
        )
        assert self.fs.exists(path), "file was not written"
        
        # Verify content
        result = papq.read_table(path)
        assert result.equals(self.table)

    def test_write_json(self):
        path = f"{self.base_path}/test.json"
        
        table_to_file(
            self.table,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.JSON.value
        )
        assert self.fs.exists(path), "file was not written"
        
        # Verify content (should be GZIP compressed)
        with self.fs.open(path, "rb") as f:
            with gzip.GzipFile(fileobj=f) as gz:
                content = gz.read().decode('utf-8')
                # Each line should be a valid JSON object
                lines = [line for line in content.split('\n') if line]  # Skip empty lines
                assert len(lines) == 2  # 2 records
                assert json.loads(lines[0]) == {"col1": "a,b\tc|d", "col2": 1}
                assert json.loads(lines[1]) == {"col1": "e,f\tg|h", "col2": 2}

    def test_write_avro(self):
        import polars as pl
        path = f"{self.base_path}/test.avro"
        
        table_to_file(
            self.table,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.AVRO.value
        )
        assert self.fs.exists(path), "file was not written"
        
        # Verify content by reading with polars
        result = pl.read_avro(path).to_arrow()
        # Cast the result to match the original table's schema
        # (the round-trip from arrow->polars->arrow casts string to large string)
        result = result.cast(self.table.schema)
        assert result.equals(self.table)
