from unittest import TestCase
from deltacat.utils.pyarrow import (
    partial_parquet_file_to_table,
    pyarrow_read_csv,
    ContentTypeValidationError,
    content_type_to_reader_kwargs,
    _add_column_kwargs,
    file_to_table,
    file_to_parquet,
    table_to_file,
    ReadKwargsProviderPyArrowSchemaOverride,
    ReadKwargsProviderPyArrowCsvPureUtf8,
    RAISE_ON_DECIMAL_OVERFLOW,
    RAISE_ON_EMPTY_CSV_KWARG,
)
import decimal
from deltacat.types.media import ContentEncoding, ContentType
from deltacat.types.partial_download import PartialParquetParameters
from pyarrow.parquet import ParquetFile
import tempfile
import pyarrow as pa
from pyarrow import csv as pacsv
import fsspec
import gzip
import json
from pyarrow import (
    feather as paf,
    parquet as papq,
    orc as paorc,
)

PARQUET_FILE_PATH = "deltacat/tests/utils/data/test_file.parquet"
PARQUET_GZIP_COMPRESSED_FILE_PATH = "deltacat/tests/utils/data/test_file.parquet.gz"
EMPTY_UTSV_PATH = "deltacat/tests/utils/data/empty.csv"
NON_EMPTY_VALID_UTSV_PATH = "deltacat/tests/utils/data/non_empty_valid.csv"
OVERFLOWING_DECIMAL_PRECISION_UTSV_PATH = (
    "deltacat/tests/utils/data/overflowing_decimal_precision.csv"
)
OVERFLOWING_DECIMAL_SCALE_UTSV_PATH = (
    "deltacat/tests/utils/data/overflowing_decimal_scale.csv"
)
GZIP_COMPRESSED_FILE_UTSV_PATH = "deltacat/tests/utils/data/non_empty_compressed.gz"
BZ2_COMPRESSED_FILE_UTSV_PATH = "deltacat/tests/utils/data/non_empty_compressed.bz2"


class TestPartialParquetFileToTable(TestCase):
    def test_partial_parquet_file_to_table_sanity(self):

        pq_file = ParquetFile(PARQUET_FILE_PATH)
        partial_parquet_params = PartialParquetParameters.of(
            pq_metadata=pq_file.metadata
        )

        self.assertEqual(
            partial_parquet_params.num_row_groups, 2, "test_file.parquet has changed."
        )

        # only first row group to be downloaded
        partial_parquet_params.row_groups_to_download.pop()

        result = partial_parquet_file_to_table(
            PARQUET_FILE_PATH,
            include_columns=["n_legs"],
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            partial_file_download_params=partial_parquet_params,
        )

        self.assertEqual(len(result), 3)
        self.assertEqual(len(result.columns), 1)

    def test_partial_parquet_file_to_table_when_schema_passed(self):

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

        result = partial_parquet_file_to_table(
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

    def test_partial_parquet_file_to_table_when_schema_missing_columns(self):

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

        result = partial_parquet_file_to_table(
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

    def test_partial_parquet_file_to_table_when_schema_passed_with_include_columns(
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

        result = partial_parquet_file_to_table(
            PARQUET_FILE_PATH,
            ContentType.PARQUET.value,
            ContentEncoding.IDENTITY.value,
            column_names=["n_legs", "animal"],
            pa_read_func_kwargs_provider=pa_kwargs_provider,
            partial_file_download_params=partial_parquet_params,
        )

        self.assertEqual(len(result), 3)
        self.assertEqual(len(result.column_names), 2)

        result_schema = result.schema
        self.assertEqual(result_schema.field(0).type, "string")
        self.assertEqual(result_schema.field(0).name, "n_legs")  # order doesn't change

    def test_partial_parquet_file_to_table_when_multiple_row_groups(self):

        pq_file = ParquetFile(PARQUET_FILE_PATH)
        partial_parquet_params = PartialParquetParameters.of(
            pq_metadata=pq_file.metadata
        )

        self.assertEqual(
            partial_parquet_params.num_row_groups, 2, "test_file.parquet has changed."
        )

        result = partial_parquet_file_to_table(
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

    def test_read_csv_when_decimal_precision_overflows_and_raise_kwarg_specified(self):
        schema = pa.schema(
            [("is_active", pa.string()), ("decimal_value", pa.decimal128(4, 2))]
        )
        kwargs = content_type_to_reader_kwargs(ContentType.UNESCAPED_TSV.value)
        _add_column_kwargs(
            ContentType.UNESCAPED_TSV.value,
            ["is_active", "decimal_value"],
            ["is_active", "decimal_value"],
            kwargs,
        )
        read_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(schema=schema)

        kwargs = read_kwargs_provider(ContentType.UNESCAPED_TSV.value, kwargs)
        self.assertRaises(
            pa.lib.ArrowInvalid,
            lambda: pyarrow_read_csv(
                OVERFLOWING_DECIMAL_PRECISION_UTSV_PATH,
                **{**kwargs, RAISE_ON_DECIMAL_OVERFLOW: True},
            ),
        )

    def test_read_csv_when_decimal_precision_overflows_sanity(self):
        schema = pa.schema(
            [("is_active", pa.string()), ("decimal_value", pa.decimal128(4, 2))]
        )
        kwargs = content_type_to_reader_kwargs(ContentType.UNESCAPED_TSV.value)
        _add_column_kwargs(
            ContentType.UNESCAPED_TSV.value,
            ["is_active", "decimal_value"],
            ["is_active", "decimal_value"],
            kwargs,
        )

        read_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(schema=schema)

        kwargs = read_kwargs_provider(ContentType.UNESCAPED_TSV.value, kwargs)

        self.assertRaises(
            pa.lib.ArrowInvalid,
            lambda: pyarrow_read_csv(OVERFLOWING_DECIMAL_PRECISION_UTSV_PATH, **kwargs),
        )

    def test_read_csv_when_decimal_scale_overflows_and_raise_kwarg_specified(self):
        schema = pa.schema(
            [("is_active", pa.string()), ("decimal_value", pa.decimal128(20, 2))]
        )
        kwargs = content_type_to_reader_kwargs(ContentType.UNESCAPED_TSV.value)
        _add_column_kwargs(
            ContentType.UNESCAPED_TSV.value,
            ["is_active", "decimal_value"],
            ["is_active", "decimal_value"],
            kwargs,
        )
        read_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(schema=schema)

        kwargs = read_kwargs_provider(ContentType.UNESCAPED_TSV.value, kwargs)

        self.assertRaises(
            pa.lib.ArrowInvalid,
            lambda: pyarrow_read_csv(
                OVERFLOWING_DECIMAL_SCALE_UTSV_PATH,
                **{**kwargs, RAISE_ON_DECIMAL_OVERFLOW: True},
            ),
        )

    def test_read_csv_when_decimal_scale_overflows_sanity(self):
        schema = pa.schema(
            [("is_active", pa.string()), ("decimal_value", pa.decimal128(20, 2))]
        )
        kwargs = content_type_to_reader_kwargs(ContentType.UNESCAPED_TSV.value)
        _add_column_kwargs(
            ContentType.UNESCAPED_TSV.value,
            ["is_active", "decimal_value"],
            ["is_active", "decimal_value"],
            kwargs,
        )

        read_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(schema=schema)

        kwargs = read_kwargs_provider(ContentType.UNESCAPED_TSV.value, kwargs)

        result = pyarrow_read_csv(OVERFLOWING_DECIMAL_SCALE_UTSV_PATH, **kwargs)

        self.assertEqual(len(result), 3)
        self.assertEqual(
            result[1][0].as_py(), decimal.Decimal("322236.66")
        )  # rounding decimal
        self.assertEqual(result[1][1].as_py(), decimal.Decimal("32.33"))  # not rounded
        self.assertEqual(len(result.column_names), 2)
        result_schema = result.schema
        self.assertEqual(result_schema.field(0).type, "string")
        self.assertEqual(result_schema.field(1).type, pa.decimal128(20, 2))

    def test_read_csv_when_decimal_scale_overflows_and_negative_scale(self):
        schema = pa.schema(
            [("is_active", pa.string()), ("decimal_value", pa.decimal128(20, -2))]
        )
        kwargs = content_type_to_reader_kwargs(ContentType.UNESCAPED_TSV.value)
        _add_column_kwargs(
            ContentType.UNESCAPED_TSV.value,
            ["is_active", "decimal_value"],
            ["is_active", "decimal_value"],
            kwargs,
        )

        read_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(schema=schema)

        kwargs = read_kwargs_provider(ContentType.UNESCAPED_TSV.value, kwargs)

        result = pyarrow_read_csv(OVERFLOWING_DECIMAL_SCALE_UTSV_PATH, **kwargs)

        self.assertEqual(len(result), 3)
        self.assertEqual(
            result[1][0].as_py(),
            decimal.Decimal("322200"),  # consequence of negative scale
        )  # rounding decimal
        self.assertEqual(result[1][1].as_py(), decimal.Decimal("00"))
        self.assertEqual(len(result.column_names), 2)
        result_schema = result.schema
        self.assertEqual(result_schema.field(0).type, "string")
        self.assertEqual(result_schema.field(1).type, pa.decimal128(20, -2))

    def test_read_csv_when_decimal_scale_overflows_with_decimal256(self):
        schema = pa.schema(
            [("is_active", pa.string()), ("decimal_value", pa.decimal256(20, 2))]
        )
        kwargs = content_type_to_reader_kwargs(ContentType.UNESCAPED_TSV.value)
        _add_column_kwargs(
            ContentType.UNESCAPED_TSV.value,
            ["is_active", "decimal_value"],
            ["is_active", "decimal_value"],
            kwargs,
        )

        read_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(schema=schema)

        kwargs = read_kwargs_provider(ContentType.UNESCAPED_TSV.value, kwargs)

        result = pyarrow_read_csv(OVERFLOWING_DECIMAL_SCALE_UTSV_PATH, **kwargs)

        self.assertEqual(len(result), 3)
        self.assertEqual(
            result[1][0].as_py(), decimal.Decimal("322236.66")
        )  # rounding decimal
        self.assertEqual(result[1][1].as_py(), decimal.Decimal("32.33"))  # not rounded
        self.assertEqual(len(result.column_names), 2)
        result_schema = result.schema
        self.assertEqual(result_schema.field(0).type, "string")
        self.assertEqual(result_schema.field(1).type, pa.decimal256(20, 2))

    def test_read_csv_when_decimal_scale_overflows_with_decimal256_and_raise_on_overflow(
        self,
    ):
        schema = pa.schema(
            [("is_active", pa.string()), ("decimal_value", pa.decimal256(20, 2))]
        )
        kwargs = content_type_to_reader_kwargs(ContentType.UNESCAPED_TSV.value)
        _add_column_kwargs(
            ContentType.UNESCAPED_TSV.value,
            ["is_active", "decimal_value"],
            ["is_active", "decimal_value"],
            kwargs,
        )

        read_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(schema=schema)

        kwargs = read_kwargs_provider(ContentType.UNESCAPED_TSV.value, kwargs)

        self.assertRaises(
            pa.lib.ArrowNotImplementedError,
            lambda: pyarrow_read_csv(
                OVERFLOWING_DECIMAL_SCALE_UTSV_PATH,
                **{**kwargs, RAISE_ON_DECIMAL_OVERFLOW: True},
            ),
        )

    def test_read_csv_when_decimal_scale_overflows_without_any_schema_then_infers(self):
        kwargs = content_type_to_reader_kwargs(ContentType.UNESCAPED_TSV.value)
        read_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(schema=None)
        kwargs = read_kwargs_provider(ContentType.UNESCAPED_TSV.value, kwargs)

        result = pyarrow_read_csv(OVERFLOWING_DECIMAL_SCALE_UTSV_PATH, **kwargs)

        # The default behavior of pyarrow is to invalid skip rows
        self.assertEqual(len(result), 2)
        self.assertEqual(result[1][0].as_py(), 32.33)  # rounding decimal
        self.assertEqual(result[1][1].as_py(), 0.4)  # not rounded
        self.assertEqual(len(result.column_names), 2)
        result_schema = result.schema
        self.assertEqual(result_schema.field(0).type, "string")
        self.assertEqual(result_schema.field(1).type, pa.float64())

    def test_read_csv_when_decimal_scale_and_precision_overflow_and_raise_on_overflow(
        self,
    ):
        schema = pa.schema(
            [("is_active", pa.string()), ("decimal_value", pa.decimal128(5, 2))]
        )
        kwargs = content_type_to_reader_kwargs(ContentType.UNESCAPED_TSV.value)
        _add_column_kwargs(
            ContentType.UNESCAPED_TSV.value,
            ["is_active", "decimal_value"],
            ["is_active", "decimal_value"],
            kwargs,
        )

        read_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(schema=schema)

        kwargs = read_kwargs_provider(ContentType.UNESCAPED_TSV.value, kwargs)

        self.assertRaises(
            pa.lib.ArrowInvalid,
            lambda: pyarrow_read_csv(OVERFLOWING_DECIMAL_SCALE_UTSV_PATH, **kwargs),
        )

    def test_read_csv_when_decimal_scale_overflow_and_file_like_obj_passed(self):
        schema = pa.schema(
            [("is_active", pa.string()), ("decimal_value", pa.decimal128(15, 2))]
        )
        kwargs = content_type_to_reader_kwargs(ContentType.UNESCAPED_TSV.value)
        _add_column_kwargs(
            ContentType.UNESCAPED_TSV.value,
            ["is_active", "decimal_value"],
            ["is_active", "decimal_value"],
            kwargs,
        )

        read_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(schema=schema)

        kwargs = read_kwargs_provider(ContentType.UNESCAPED_TSV.value, kwargs)

        with open(OVERFLOWING_DECIMAL_SCALE_UTSV_PATH, "rb") as file:
            result = pyarrow_read_csv(file, **kwargs)

            self.assertEqual(len(result), 3)
            self.assertEqual(
                result[1][0].as_py(), decimal.Decimal("322236.66")
            )  # rounding decimal
            self.assertEqual(
                result[1][1].as_py(), decimal.Decimal("32.33")
            )  # not rounded
            self.assertEqual(len(result.column_names), 2)
            result_schema = result.schema
            self.assertEqual(result_schema.field(0).type, "string")
            self.assertEqual(result_schema.field(1).type, pa.decimal128(15, 2))


class TestWriters(TestCase):
    def setUp(self):
        self.table = pa.table({"col1": ["a,b\tc|d", "e,f\tg|h"], "col2": [1, 2]})
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
            content_type=ContentType.FEATHER.value,
        )
        assert self.fs.exists(path), "file was not written"

        # Verify content
        result = paf.read_table(path)
        assert result.equals(self.table)

    def test_write_csv(self):
        path = f"{self.base_path}/test.csv.gz"

        table_to_file(
            self.table,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.CSV.value,
        )
        assert self.fs.exists(path), "file was not written"

        # Verify content (should be GZIP compressed)
        with self.fs.open(path, "rb") as f:
            with gzip.GzipFile(fileobj=f) as gz:
                content = gz.read().decode("utf-8")
                # Should be quoted due to commas in data
                assert '"a,b\tc|d",1' in content
                assert '"e,f\tg|h",2' in content

    def test_write_tsv(self):
        path = f"{self.base_path}/test.tsv.gz"

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
                content = gz.read().decode("utf-8")
                # Should be quoted due to tabs in data
                assert '"a,b\tc|d"\t1' in content
                assert '"e,f\tg|h"\t2' in content

    def test_write_psv(self):
        path = f"{self.base_path}/test.psv.gz"

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
                content = gz.read().decode("utf-8")
                # Should be quoted due to pipes in data
                assert '"a,b\tc|d"|1' in content
                assert '"e,f\tg|h"|2' in content

    def test_write_unescaped_tsv(self):
        # Create table without delimiters for unescaped TSV
        table = pa.table({"col1": ["abc", "def"], "col2": [1, 2]})
        path = f"{self.base_path}/test.tsv.gz"

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
                content = gz.read().decode("utf-8")
                # With quoting_style="none", strings should not be quoted
                assert "abc\t1" in content
                assert "def\t2" in content

    def test_write_orc(self):
        path = f"{self.base_path}/test.orc"

        table_to_file(
            self.table,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.ORC.value,
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
            content_type=ContentType.PARQUET.value,
        )
        assert self.fs.exists(path), "file was not written"

        # Verify content
        result = papq.read_table(path)
        assert result.equals(self.table)

    def test_write_json(self):
        path = f"{self.base_path}/test.json.gz"

        table_to_file(
            self.table,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.JSON.value,
        )
        assert self.fs.exists(path), "file was not written"

        # Verify content (should be GZIP compressed)
        with self.fs.open(path, "rb") as f:
            with gzip.GzipFile(fileobj=f) as gz:
                content = gz.read().decode("utf-8")
                # Each line should be a valid JSON object
                lines = [
                    line for line in content.split("\n") if line
                ]  # Skip empty lines
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
            content_type=ContentType.AVRO.value,
        )
        assert self.fs.exists(path), "file was not written"

        # Verify content by reading with polars
        result = pl.read_avro(path).to_arrow()
        # Cast the result to match the original table's schema
        # (the round-trip from arrow->polars->arrow casts string to large string)
        result = result.cast(self.table.schema)
        assert result.equals(self.table)


class TestPyArrowReaders(TestCase):
    def setUp(self):
        # Create test data files for reading
        self.fs = fsspec.filesystem("file")
        self.base_path = tempfile.mkdtemp()
        self.fs.makedirs(self.base_path, exist_ok=True)

        # Create test Table
        self.table = pa.Table.from_pylist(
            [
                {"col1": "a,b\tc|d", "col2": 1, "col3": 1.1},
                {"col1": "e,f\tg|h", "col2": 2, "col3": 2.2},
                {"col1": "test", "col2": 3, "col3": 3.3},
            ]
        )

        # Write test files in different formats
        self._create_test_files()

    def tearDown(self):
        self.fs.rm(self.base_path, recursive=True)

    def _create_test_files(self):
        # Create CSV file (GZIP compressed)
        csv_path = f"{self.base_path}/test.csv"
        with self.fs.open(csv_path, "wb") as f:
            with gzip.GzipFile(fileobj=f, mode="wb") as gz:
                content = '"a,b\tc|d",1,1.1\n"e,f\tg|h",2,2.2\ntest,3,3.3\n'
                gz.write(content.encode("utf-8"))

        # Create TSV file (GZIP compressed)
        tsv_path = f"{self.base_path}/test.tsv"
        with self.fs.open(tsv_path, "wb") as f:
            with gzip.GzipFile(fileobj=f, mode="wb") as gz:
                content = '"a,b\tc|d"\t1\t1.1\n"e,f\tg|h"\t2\t2.2\ntest\t3\t3.3\n'
                gz.write(content.encode("utf-8"))

        # Create PSV file (GZIP compressed)
        psv_path = f"{self.base_path}/test.psv"
        with self.fs.open(psv_path, "wb") as f:
            with gzip.GzipFile(fileobj=f, mode="wb") as gz:
                content = '"a,b\tc|d"|1|1.1\n"e,f\tg|h"|2|2.2\ntest|3|3.3\n'
                gz.write(content.encode("utf-8"))

        # Create unescaped TSV file (GZIP compressed)
        unescaped_tsv_path = f"{self.base_path}/test_unescaped.tsv"
        pa.Table.from_pylist(
            [
                {"col1": "abc", "col2": 1, "col3": 1.1},
                {"col1": "def", "col2": 2, "col3": 2.2},
                {"col1": "ghi", "col2": 3, "col3": 3.3},
            ]
        )
        with self.fs.open(unescaped_tsv_path, "wb") as f:
            with gzip.GzipFile(fileobj=f, mode="wb") as gz:
                content = "abc\t1\t1.1\ndef\t2\t2.2\nghi\t3\t3.3\n"
                gz.write(content.encode("utf-8"))

        # Create Parquet file
        parquet_path = f"{self.base_path}/test.parquet"
        with self.fs.open(parquet_path, "wb") as f:
            papq.write_table(self.table, f)

        # Create Feather file
        feather_path = f"{self.base_path}/test.feather"
        with self.fs.open(feather_path, "wb") as f:
            paf.write_feather(self.table, f)

        # Create JSON file (GZIP compressed)
        json_path = f"{self.base_path}/test.json"
        with self.fs.open(json_path, "wb") as f:
            with gzip.GzipFile(fileobj=f, mode="wb") as gz:
                # Create NDJSON format - one JSON object per line
                lines = []
                for row in self.table.to_pylist():
                    lines.append(json.dumps(row))
                content = "\n".join(lines) + "\n"
                gz.write(content.encode("utf-8"))

        # Create Avro file using polars (since pyarrow delegates to polars for Avro)
        avro_path = f"{self.base_path}/test.avro"
        import polars as pl

        pl_df = pl.from_arrow(self.table)
        pl_df.write_avro(avro_path)

        # Create ORC file
        orc_path = f"{self.base_path}/test.orc"
        with self.fs.open(orc_path, "wb") as f:
            paorc.write_table(self.table, f)

    def test_content_type_to_reader_kwargs(self):
        # Test CSV kwargs
        csv_kwargs = content_type_to_reader_kwargs(ContentType.CSV.value)
        expected_csv = {"parse_options": pacsv.ParseOptions(delimiter=",")}
        assert (
            csv_kwargs["parse_options"].delimiter
            == expected_csv["parse_options"].delimiter
        )

        # Test TSV kwargs
        tsv_kwargs = content_type_to_reader_kwargs(ContentType.TSV.value)
        expected_tsv = {"parse_options": pacsv.ParseOptions(delimiter="\t")}
        assert (
            tsv_kwargs["parse_options"].delimiter
            == expected_tsv["parse_options"].delimiter
        )

        # Test PSV kwargs
        psv_kwargs = content_type_to_reader_kwargs(ContentType.PSV.value)
        expected_psv = {"parse_options": pacsv.ParseOptions(delimiter="|")}
        assert (
            psv_kwargs["parse_options"].delimiter
            == expected_psv["parse_options"].delimiter
        )

        # Test unescaped TSV kwargs
        unescaped_kwargs = content_type_to_reader_kwargs(
            ContentType.UNESCAPED_TSV.value
        )
        assert unescaped_kwargs["parse_options"].delimiter == "\t"
        assert unescaped_kwargs["parse_options"].quote_char is False
        assert unescaped_kwargs["convert_options"].null_values == [""]

        # Test Parquet kwargs (should be empty)
        parquet_kwargs = content_type_to_reader_kwargs(ContentType.PARQUET.value)
        assert parquet_kwargs == {}

        # Test ORC kwargs (should be empty)
        orc_kwargs = content_type_to_reader_kwargs(ContentType.ORC.value)
        assert orc_kwargs == {}

        # Test Avro kwargs (should be empty)
        avro_kwargs = content_type_to_reader_kwargs(ContentType.AVRO.value)
        assert avro_kwargs == {}

    def test_add_column_kwargs(self):
        kwargs = {}
        column_names = ["col1", "col2", "col3"]
        include_columns = ["col1", "col2"]

        # Test CSV column kwargs
        _add_column_kwargs(ContentType.CSV.value, column_names, include_columns, kwargs)
        assert kwargs["read_options"].column_names == column_names
        assert kwargs["convert_options"].include_columns == include_columns

        # Test Parquet column kwargs
        kwargs = {}
        _add_column_kwargs(
            ContentType.PARQUET.value, column_names, include_columns, kwargs
        )
        assert kwargs["columns"] == include_columns

    def test_file_to_table_csv(self):
        # Test reading CSV with file_to_table
        csv_path = f"{self.base_path}/test.csv"

        result = file_to_table(
            csv_path,
            ContentType.CSV.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs,
            column_names=["col1", "col2", "col3"],
        )

        assert len(result) == 3
        assert result.column_names == ["col1", "col2", "col3"]
        assert result.column("col1").to_pylist() == ["a,b\tc|d", "e,f\tg|h", "test"]

    def test_file_to_table_tsv(self):
        # Test reading TSV with file_to_table
        tsv_path = f"{self.base_path}/test.tsv"

        result = file_to_table(
            tsv_path,
            ContentType.TSV.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs,
            column_names=["col1", "col2", "col3"],
        )

        assert len(result) == 3
        assert result.column_names == ["col1", "col2", "col3"]
        assert result.column("col1").to_pylist() == ["a,b\tc|d", "e,f\tg|h", "test"]

    def test_file_to_table_psv(self):
        # Test reading PSV with file_to_table
        psv_path = f"{self.base_path}/test.psv"

        result = file_to_table(
            psv_path,
            ContentType.PSV.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs,
            column_names=["col1", "col2", "col3"],
        )

        assert len(result) == 3
        assert result.column_names == ["col1", "col2", "col3"]
        assert result.column("col1").to_pylist() == ["a,b\tc|d", "e,f\tg|h", "test"]

    def test_file_to_table_unescaped_tsv(self):
        # Test reading unescaped TSV with file_to_table
        unescaped_tsv_path = f"{self.base_path}/test_unescaped.tsv"

        result = file_to_table(
            unescaped_tsv_path,
            ContentType.UNESCAPED_TSV.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs,
            column_names=["col1", "col2", "col3"],
        )

        assert len(result) == 3
        assert result.column_names == ["col1", "col2", "col3"]
        assert result.column("col1").to_pylist() == ["abc", "def", "ghi"]

    def test_file_to_table_parquet(self):
        # Test reading Parquet with file_to_table
        parquet_path = f"{self.base_path}/test.parquet"

        result = file_to_table(
            parquet_path, ContentType.PARQUET.value, filesystem=self.fs
        )

        assert len(result) == 3
        assert result.column_names == ["col1", "col2", "col3"]
        assert result.equals(self.table)

    def test_file_to_table_feather(self):
        # Test reading Feather with file_to_table
        feather_path = f"{self.base_path}/test.feather"

        result = file_to_table(
            feather_path, ContentType.FEATHER.value, filesystem=self.fs
        )

        assert len(result) == 3
        assert result.column_names == ["col1", "col2", "col3"]
        assert result.equals(self.table)

    def test_file_to_table_json(self):
        # Test reading JSON with file_to_table
        json_path = f"{self.base_path}/test.json"

        result = file_to_table(
            json_path,
            ContentType.JSON.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs,
        )

        assert len(result) == 3
        assert set(result.column_names) == {"col1", "col2", "col3"}
        assert result.column("col1").to_pylist() == ["a,b\tc|d", "e,f\tg|h", "test"]

    def test_file_to_table_avro(self):
        # Test reading Avro with file_to_table
        avro_path = f"{self.base_path}/test.avro"

        result = file_to_table(avro_path, ContentType.AVRO.value, filesystem=self.fs)

        assert len(result) == 3
        assert result.column_names == ["col1", "col2", "col3"]
        # Avro may have different dtypes, so compare values
        assert result.column("col1").to_pylist() == ["a,b\tc|d", "e,f\tg|h", "test"]

    def test_file_to_table_orc(self):
        # Test reading ORC with file_to_table
        orc_path = f"{self.base_path}/test.orc"

        result = file_to_table(orc_path, ContentType.ORC.value, filesystem=self.fs)

        assert len(result) == 3
        assert result.column_names == ["col1", "col2", "col3"]
        assert result.equals(self.table)

    def test_file_to_table_with_column_selection(self):
        # Test reading with column selection
        csv_path = f"{self.base_path}/test.csv"

        result = file_to_table(
            csv_path,
            ContentType.CSV.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs,
            column_names=["col1", "col2", "col3"],
            include_columns=["col1", "col2"],
        )

        assert len(result) == 3
        assert len(result.column_names) == 2  # Should only have 2 columns
        assert result.column_names == ["col1", "col2"]

    def test_file_to_table_with_kwargs_provider(self):
        # Test reading with kwargs provider
        csv_path = f"{self.base_path}/test.csv"
        provider = ReadKwargsProviderPyArrowCsvPureUtf8(
            include_columns=["col1", "col2", "col3"]
        )

        result = file_to_table(
            csv_path,
            ContentType.CSV.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs,
            column_names=["col1", "col2", "col3"],
            pa_read_func_kwargs_provider=provider,
        )

        assert len(result) == 3
        assert result.column_names == ["col1", "col2", "col3"]
        # With string types provider, all columns should be strings
        for col_name in result.column_names:
            assert result.schema.field(col_name).type == pa.string()

    def test_file_to_table_filesystem_inference(self):
        # Test filesystem inference when no filesystem is provided
        # Use JSON file since it should work well with inference
        json_path = f"{self.base_path}/test.json"

        result = file_to_table(
            json_path,
            ContentType.JSON.value,
            ContentEncoding.GZIP.value
            # No filesystem provided - should be inferred
        )

        assert len(result) == 3
        assert set(result.column_names) == {"col1", "col2", "col3"}
        assert result.column("col1").to_pylist() == ["a,b\tc|d", "e,f\tg|h", "test"]

    def test_file_to_table_unsupported_content_type(self):
        # Test error handling for unsupported content type
        parquet_path = f"{self.base_path}/test.parquet"

        with self.assertRaises(NotImplementedError) as context:
            file_to_table(parquet_path, "unsupported/content-type", filesystem=self.fs)

        assert "not implemented" in str(context.exception)

    def test_file_to_table_bzip2_compression(self):
        # Test BZIP2 compression handling
        import bz2

        # Create a BZIP2 compressed CSV file
        csv_content = '"a,b\tc|d",1,1.1\n"e,f\tg|h",2,2.2\ntest,3,3.3\n'
        compressed_content = bz2.compress(csv_content.encode("utf-8"))

        bz2_path = f"{self.base_path}/test.csv.bz2"
        with self.fs.open(bz2_path, "wb") as f:
            f.write(compressed_content)

        result = file_to_table(
            bz2_path,
            ContentType.CSV.value,
            ContentEncoding.BZIP2.value,
            filesystem=self.fs,
            column_names=["col1", "col2", "col3"],
        )

        assert len(result) == 3
        assert result.column_names == ["col1", "col2", "col3"]
        assert result.column("col1").to_pylist() == ["a,b\tc|d", "e,f\tg|h", "test"]


class TestFileToParquet(TestCase):
    def setUp(self):
        # Create test data files for reading
        self.fs = fsspec.filesystem("file")
        self.base_path = tempfile.mkdtemp()
        self.fs.makedirs(self.base_path, exist_ok=True)

        # Create test Table
        self.table = pa.Table.from_pylist(
            [
                {"col1": "a,b\tc|d", "col2": 1, "col3": 1.1},
                {"col1": "e,f\tg|h", "col2": 2, "col3": 2.2},
                {"col1": "test", "col2": 3, "col3": 3.3},
            ]
        )

        # Write test parquet files
        self._create_test_files()

    def tearDown(self):
        self.fs.rm(self.base_path, recursive=True)

    def _create_test_files(self):
        # Create basic Parquet file
        parquet_path = f"{self.base_path}/test.parquet"
        with self.fs.open(parquet_path, "wb") as f:
            papq.write_table(self.table, f)

        # Create larger Parquet file with multiple row groups
        large_table = pa.Table.from_pylist(
            [{"col1": f"row_{i}", "col2": i, "col3": float(i)} for i in range(1000)]
        )
        large_parquet_path = f"{self.base_path}/test_large.parquet"
        with self.fs.open(large_parquet_path, "wb") as f:
            papq.write_table(
                large_table, f, row_group_size=100
            )  # Create multiple row groups

    def test_file_to_parquet_basic(self):
        # Test basic parquet file reading
        parquet_path = f"{self.base_path}/test.parquet"

        result = file_to_parquet(parquet_path, filesystem=self.fs)

        assert isinstance(result, papq.ParquetFile)
        assert result.num_row_groups > 0
        assert result.metadata.num_rows == 3
        assert result.metadata.num_columns == 3

        # Verify we can read the data
        table = result.read()
        assert len(table) == 3
        assert table.column_names == ["col1", "col2", "col3"]

    def test_file_to_parquet_with_schema_provider(self):
        # Test with schema override provider
        parquet_path = f"{self.base_path}/test.parquet"

        schema = pa.schema(
            [
                pa.field("col1", pa.string()),
                pa.field("col2", pa.string()),  # Override to string
                pa.field("col3", pa.string()),  # Override to string
            ]
        )

        provider = ReadKwargsProviderPyArrowSchemaOverride(schema=schema)

        result = file_to_parquet(
            parquet_path, filesystem=self.fs, pa_read_func_kwargs_provider=provider
        )

        assert isinstance(result, papq.ParquetFile)
        # Note: schema override might not affect ParquetFile metadata,
        # but should work when reading the table
        table = result.read()
        assert len(table) == 3

    def test_file_to_parquet_with_custom_kwargs(self):
        # Test with custom ParquetFile kwargs
        parquet_path = f"{self.base_path}/test.parquet"

        result = file_to_parquet(
            parquet_path,
            filesystem=self.fs,
            validate_schema=True,  # Custom kwarg for ParquetFile
            memory_map=True,  # Another custom kwarg
        )

        assert isinstance(result, papq.ParquetFile)
        assert result.metadata.num_rows == 3

    def test_file_to_parquet_filesystem_inference(self):
        # Test filesystem inference when no filesystem is provided
        parquet_path = f"{self.base_path}/test.parquet"

        result = file_to_parquet(
            parquet_path
            # No filesystem provided - should be inferred
        )

        assert isinstance(result, papq.ParquetFile)
        assert result.metadata.num_rows == 3
        assert result.metadata.num_columns == 3

    def test_file_to_parquet_large_file(self):
        # Test with larger parquet file (multiple row groups)
        large_parquet_path = f"{self.base_path}/test_large.parquet"

        result = file_to_parquet(large_parquet_path, filesystem=self.fs)

        assert isinstance(result, papq.ParquetFile)
        assert result.metadata.num_rows == 1000
        assert result.num_row_groups > 1  # Should have multiple row groups

        # Test reading specific row groups
        first_row_group = result.read_row_group(0)
        assert len(first_row_group) <= 100  # Based on row_group_size=100

    def test_file_to_parquet_metadata_access(self):
        # Test accessing various metadata properties
        parquet_path = f"{self.base_path}/test.parquet"

        result = file_to_parquet(parquet_path, filesystem=self.fs)

        # Test metadata access
        metadata = result.metadata
        assert metadata.num_rows == 3
        assert metadata.num_columns == 3
        assert metadata.num_row_groups >= 1

        # Test schema access
        schema = result.schema
        assert len(schema) == 3
        assert "col1" in schema.names
        assert "col2" in schema.names
        assert "col3" in schema.names

        # Test schema_arrow property
        schema_arrow = result.schema_arrow
        assert isinstance(schema_arrow, pa.Schema)
        assert len(schema_arrow) == 3

    def test_file_to_parquet_column_selection(self):
        # Test reading specific columns
        parquet_path = f"{self.base_path}/test.parquet"

        result = file_to_parquet(parquet_path, filesystem=self.fs)

        # Read only specific columns
        table = result.read(columns=["col1", "col2"])
        assert len(table.column_names) == 2
        assert table.column_names == ["col1", "col2"]
        assert len(table) == 3

    def test_file_to_parquet_invalid_content_type(self):
        # Test error handling for invalid content type
        parquet_path = f"{self.base_path}/test.parquet"

        with self.assertRaises(ContentTypeValidationError) as context:
            file_to_parquet(
                parquet_path,
                content_type=ContentType.CSV.value,  # Invalid content type
                filesystem=self.fs,
            )

        assert "cannot be read into pyarrow.parquet.ParquetFile" in str(
            context.exception
        )

    def test_file_to_parquet_invalid_content_encoding(self):
        # Test error handling for invalid content encoding
        parquet_path = f"{self.base_path}/test.parquet"

        with self.assertRaises(ContentTypeValidationError) as context:
            file_to_parquet(
                parquet_path,
                content_encoding=ContentEncoding.GZIP.value,  # Invalid encoding
                filesystem=self.fs,
            )

        assert "cannot be read into pyarrow.parquet.ParquetFile" in str(
            context.exception
        )

    def test_file_to_parquet_different_filesystems(self):
        # Test with different filesystem implementations
        parquet_path = f"{self.base_path}/test.parquet"

        # Test with fsspec filesystem
        result_fsspec = file_to_parquet(parquet_path, filesystem=self.fs)
        assert isinstance(result_fsspec, papq.ParquetFile)
        assert result_fsspec.metadata.num_rows == 3

        # Test with None filesystem (inferred)
        result_inferred = file_to_parquet(parquet_path, filesystem=None)
        assert isinstance(result_inferred, papq.ParquetFile)
        assert result_inferred.metadata.num_rows == 3

    def test_file_to_parquet_lazy_loading(self):
        # Test that ParquetFile provides lazy loading capabilities
        large_parquet_path = f"{self.base_path}/test_large.parquet"

        result = file_to_parquet(large_parquet_path, filesystem=self.fs)

        # ParquetFile should be created without loading all data
        assert isinstance(result, papq.ParquetFile)
        assert result.metadata.num_rows == 1000

        # Test reading only specific columns (lazy loading)
        partial_table = result.read(columns=["col1", "col2"])
        assert len(partial_table) == 1000  # All rows but only 2 columns
        assert partial_table.column_names == ["col1", "col2"]

        # Test reading specific row group (lazy loading)
        row_group_table = result.read_row_group(0)
        assert len(row_group_table) <= 100  # Based on row_group_size

    def test_file_to_parquet_performance_timing(self):
        # Test that performance timing is logged (basic functionality test)
        parquet_path = f"{self.base_path}/test.parquet"

        # This should complete without error and log timing
        result = file_to_parquet(parquet_path, filesystem=self.fs)

        assert isinstance(result, papq.ParquetFile)
        assert result.metadata.num_rows == 3


class TestFileToTableFilesystems(TestCase):
    """Test file_to_table with different filesystem implementations across all content types."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self._create_test_files()

    def tearDown(self):
        import shutil

        shutil.rmtree(self.tmpdir)

    def _create_test_files(self):
        """Create test files for all supported content types."""
        # Test data
        test_data = pa.table(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
                "age": [25, 30, 35, 28, 32],
                "score": [85.5, 92.0, 78.5, 88.0, 95.5],
            }
        )

        # File paths
        self.csv_file = f"{self.tmpdir}/test.csv"
        self.tsv_file = f"{self.tmpdir}/test.tsv"
        self.psv_file = f"{self.tmpdir}/test.psv"
        self.unescaped_tsv_file = f"{self.tmpdir}/test_unescaped.tsv"
        self.parquet_file = f"{self.tmpdir}/test.parquet"
        self.feather_file = f"{self.tmpdir}/test.feather"
        self.json_file = f"{self.tmpdir}/test.json"
        self.orc_file = f"{self.tmpdir}/test.orc"
        self.avro_file = f"{self.tmpdir}/test.avro"

        # Create CSV file
        pacsv.write_csv(
            test_data,
            self.csv_file,
            write_options=pacsv.WriteOptions(delimiter=",", include_header=False),
        )

        # Create TSV file
        pacsv.write_csv(
            test_data,
            self.tsv_file,
            write_options=pacsv.WriteOptions(delimiter="\t", include_header=False),
        )

        # Create PSV file
        pacsv.write_csv(
            test_data,
            self.psv_file,
            write_options=pacsv.WriteOptions(delimiter="|", include_header=False),
        )

        # Create unescaped TSV file
        pacsv.write_csv(
            test_data,
            self.unescaped_tsv_file,
            write_options=pacsv.WriteOptions(
                delimiter="\t", include_header=False, quoting_style="none"
            ),
        )

        # Create Parquet file
        papq.write_table(test_data, self.parquet_file)

        # Create Feather file
        paf.write_feather(test_data, self.feather_file)

        # Create JSON file (write as JSONL format)
        df = test_data.to_pandas()
        with open(self.json_file, "w") as f:
            for _, row in df.iterrows():
                json.dump(row.to_dict(), f)
                f.write("\n")

        # Create ORC file
        paorc.write_table(test_data, self.orc_file)

        # Create Avro file
        try:
            import polars as pl

            pl_df = pl.from_arrow(test_data)
            pl_df.write_avro(self.avro_file)
        except ImportError:
            # Skip Avro file creation if polars is not available
            self.avro_file = None

    def _get_filesystems(self, file_path):
        """Get different filesystem implementations for testing."""
        # fsspec AbstractFileSystem
        fsspec_fs = fsspec.filesystem("file")

        # PyArrow filesystem
        import pyarrow.fs as pafs

        pyarrow_fs = pafs.LocalFileSystem()

        # None for automatic inference
        auto_infer_fs = None

        return [
            ("fsspec", fsspec_fs),
            ("pyarrow", pyarrow_fs),
            ("auto_infer", auto_infer_fs),
        ]

    def _assert_table_content(self, table, content_type):
        """Assert that the loaded table has expected content."""
        self.assertEqual(len(table), 5, f"Expected 5 rows for {content_type}")
        self.assertEqual(
            len(table.columns), 4, f"Expected 4 columns for {content_type}"
        )

        # Check column names exist (order might vary for some formats)
        column_names = set(table.column_names)
        expected_columns = {"id", "name", "age", "score"}
        self.assertEqual(
            column_names, expected_columns, f"Column names mismatch for {content_type}"
        )

    def test_csv_all_filesystems(self):
        """Test CSV reading with all filesystem types."""
        for fs_name, filesystem in self._get_filesystems(self.csv_file):
            with self.subTest(filesystem=fs_name):
                table = file_to_table(
                    self.csv_file,
                    ContentType.CSV.value,
                    ContentEncoding.IDENTITY.value,
                    filesystem=filesystem,
                    column_names=["id", "name", "age", "score"],
                )
                self._assert_table_content(table, f"CSV with {fs_name}")

    def test_tsv_all_filesystems(self):
        """Test TSV reading with all filesystem types."""
        for fs_name, filesystem in self._get_filesystems(self.tsv_file):
            with self.subTest(filesystem=fs_name):
                table = file_to_table(
                    self.tsv_file,
                    ContentType.TSV.value,
                    ContentEncoding.IDENTITY.value,
                    filesystem=filesystem,
                    column_names=["id", "name", "age", "score"],
                )
                self._assert_table_content(table, f"TSV with {fs_name}")

    def test_psv_all_filesystems(self):
        """Test PSV reading with all filesystem types."""
        for fs_name, filesystem in self._get_filesystems(self.psv_file):
            with self.subTest(filesystem=fs_name):
                table = file_to_table(
                    self.psv_file,
                    ContentType.PSV.value,
                    ContentEncoding.IDENTITY.value,
                    filesystem=filesystem,
                    column_names=["id", "name", "age", "score"],
                )
                self._assert_table_content(table, f"PSV with {fs_name}")

    def test_unescaped_tsv_all_filesystems(self):
        """Test unescaped TSV reading with all filesystem types."""
        for fs_name, filesystem in self._get_filesystems(self.unescaped_tsv_file):
            with self.subTest(filesystem=fs_name):
                table = file_to_table(
                    self.unescaped_tsv_file,
                    ContentType.UNESCAPED_TSV.value,
                    ContentEncoding.IDENTITY.value,
                    filesystem=filesystem,
                    column_names=["id", "name", "age", "score"],
                )
                self._assert_table_content(table, f"UNESCAPED_TSV with {fs_name}")

    def test_parquet_all_filesystems(self):
        """Test Parquet reading with all filesystem types."""
        for fs_name, filesystem in self._get_filesystems(self.parquet_file):
            with self.subTest(filesystem=fs_name):
                table = file_to_table(
                    self.parquet_file,
                    ContentType.PARQUET.value,
                    ContentEncoding.IDENTITY.value,
                    filesystem=filesystem,
                )
                self._assert_table_content(table, f"PARQUET with {fs_name}")

    def test_feather_all_filesystems(self):
        """Test Feather reading with all filesystem types."""
        for fs_name, filesystem in self._get_filesystems(self.feather_file):
            with self.subTest(filesystem=fs_name):
                table = file_to_table(
                    self.feather_file,
                    ContentType.FEATHER.value,
                    ContentEncoding.IDENTITY.value,
                    filesystem=filesystem,
                )
                self._assert_table_content(table, f"FEATHER with {fs_name}")

    def test_json_all_filesystems(self):
        """Test JSON reading with all filesystem types."""
        for fs_name, filesystem in self._get_filesystems(self.json_file):
            with self.subTest(filesystem=fs_name):
                table = file_to_table(
                    self.json_file,
                    ContentType.JSON.value,
                    ContentEncoding.IDENTITY.value,
                    filesystem=filesystem,
                )
                self._assert_table_content(table, f"JSON with {fs_name}")

    def test_orc_all_filesystems(self):
        """Test ORC reading with all filesystem types."""
        for fs_name, filesystem in self._get_filesystems(self.orc_file):
            with self.subTest(filesystem=fs_name):
                table = file_to_table(
                    self.orc_file,
                    ContentType.ORC.value,
                    ContentEncoding.IDENTITY.value,
                    filesystem=filesystem,
                )
                self._assert_table_content(table, f"ORC with {fs_name}")

    def test_avro_all_filesystems(self):
        """Test Avro reading with all filesystem types."""
        if self.avro_file is None:
            self.skipTest("Avro file creation skipped (polars not available)")

        for fs_name, filesystem in self._get_filesystems(self.avro_file):
            with self.subTest(filesystem=fs_name):
                table = file_to_table(
                    self.avro_file,
                    ContentType.AVRO.value,
                    ContentEncoding.IDENTITY.value,
                    filesystem=filesystem,
                )
                self._assert_table_content(table, f"AVRO with {fs_name}")

    def test_column_selection_all_filesystems(self):
        """Test column selection works with all filesystem types."""
        for fs_name, filesystem in self._get_filesystems(self.parquet_file):
            with self.subTest(filesystem=fs_name):
                table = file_to_table(
                    self.parquet_file,
                    ContentType.PARQUET.value,
                    ContentEncoding.IDENTITY.value,
                    filesystem=filesystem,
                    include_columns=["name", "age"],
                )
                self.assertEqual(
                    len(table.columns), 2, f"Expected 2 columns with {fs_name}"
                )
                self.assertEqual(
                    set(table.column_names),
                    {"name", "age"},
                    f"Column selection failed with {fs_name}",
                )

    def test_kwargs_provider_all_filesystems(self):
        """Test that kwargs providers work with all filesystem types."""

        def schema_provider(content_type, kwargs):
            if content_type == ContentType.CSV.value:
                # Force all columns to be strings
                kwargs["convert_options"] = pacsv.ConvertOptions(
                    column_types={
                        "id": pa.string(),
                        "name": pa.string(),
                        "age": pa.string(),
                        "score": pa.string(),
                    }
                )
            return kwargs

        for fs_name, filesystem in self._get_filesystems(self.csv_file):
            with self.subTest(filesystem=fs_name):
                table = file_to_table(
                    self.csv_file,
                    ContentType.CSV.value,
                    ContentEncoding.IDENTITY.value,
                    filesystem=filesystem,
                    column_names=["id", "name", "age", "score"],
                    pa_read_func_kwargs_provider=schema_provider,
                )
                # Check that all columns are strings
                for field in table.schema:
                    self.assertEqual(
                        field.type,
                        pa.string(),
                        f"Column {field.name} should be string with {fs_name}",
                    )

    def test_filesystem_auto_inference_consistency(self):
        """Test that auto-inferred filesystem produces same results as explicit filesystems."""
        # Use Parquet as it's most reliable across filesystem types

        # Read with auto-inference
        auto_table = file_to_table(
            self.parquet_file,
            ContentType.PARQUET.value,
            ContentEncoding.IDENTITY.value,
            filesystem=None,  # Auto-infer
        )

        # Read with explicit fsspec filesystem
        fsspec_fs = fsspec.filesystem("file")
        fsspec_table = file_to_table(
            self.parquet_file,
            ContentType.PARQUET.value,
            ContentEncoding.IDENTITY.value,
            filesystem=fsspec_fs,
        )

        # Read with explicit PyArrow filesystem
        import pyarrow.fs as pafs

        pyarrow_fs = pafs.LocalFileSystem()
        pyarrow_table = file_to_table(
            self.parquet_file,
            ContentType.PARQUET.value,
            ContentEncoding.IDENTITY.value,
            filesystem=pyarrow_fs,
        )

        # All should produce equivalent results
        self.assertTrue(
            auto_table.equals(fsspec_table),
            "Auto-inferred result should match fsspec result",
        )
        self.assertTrue(
            auto_table.equals(pyarrow_table),
            "Auto-inferred result should match PyArrow result",
        )

    def test_error_handling_all_filesystems(self):
        """Test error handling works consistently across filesystem types."""
        for fs_name, filesystem in self._get_filesystems(self.parquet_file):
            with self.subTest(filesystem=fs_name):
                # Test unsupported content type
                with self.assertRaises(NotImplementedError):
                    file_to_table(
                        self.parquet_file,
                        "UNSUPPORTED_TYPE",
                        ContentEncoding.IDENTITY.value,
                        filesystem=filesystem,
                    )

                # Test non-existent file
                with self.assertRaises((FileNotFoundError, OSError)):
                    file_to_table(
                        f"{self.tmpdir}/non_existent.parquet",
                        ContentType.PARQUET.value,
                        ContentEncoding.IDENTITY.value,
                        filesystem=filesystem,
                    )
