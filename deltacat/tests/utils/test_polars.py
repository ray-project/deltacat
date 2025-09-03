from unittest import TestCase
import polars as pl
import pandas as pd
import tempfile
import fsspec
import gzip
import json
import io
from deltacat.types.media import ContentType, ContentEncoding
from deltacat.utils.polars import (
    dataframe_to_file,
    file_to_dataframe,
    content_type_to_reader_kwargs,
    _add_column_kwargs,
    ReadKwargsProviderPolarsStringTypes,
    concat_dataframes,
)


class TestPolarsWriters(TestCase):
    def setUp(self):
        # Create a test DataFrame with data that includes delimiters
        self.df = pl.DataFrame({"col1": ["a,b\tc|d", "e,f\tg|h"], "col2": [1, 2]})
        self.fs = fsspec.filesystem("file")
        self.base_path = tempfile.mkdtemp()
        self.fs.makedirs(self.base_path, exist_ok=True)

    def tearDown(self):
        self.fs.rm(self.base_path, recursive=True)

    def test_write_feather(self):
        path = f"{self.base_path}/test.feather"

        dataframe_to_file(
            self.df,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.FEATHER.value,
        )
        assert self.fs.exists(path), "file was not written"

        # Verify content
        result = pl.read_ipc(path)
        assert result.equals(self.df)

    def test_write_csv(self):
        path = f"{self.base_path}/test.csv.gz"

        dataframe_to_file(
            self.df, path, self.fs, lambda x: path, content_type=ContentType.CSV.value
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

        dataframe_to_file(
            self.df,
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
                # Polars writes TSV with tab separators
                assert '"a,b\tc|d"\t1' in content
                assert '"e,f\tg|h"\t2' in content

    def test_write_psv(self):
        path = f"{self.base_path}/test.psv.gz"

        dataframe_to_file(
            self.df,
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
                # Polars writes PSV with pipe separators
                assert '"a,b\tc|d"|1' in content
                assert '"e,f\tg|h"|2' in content

    def test_write_unescaped_tsv(self):
        # Create DataFrame without delimiters for unescaped TSV
        df = pl.DataFrame({"col1": ["abc", "def"], "col2": [1, 2]})
        path = f"{self.base_path}/test.tsv.gz"

        dataframe_to_file(
            df,
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
                # With quote_char=None for unescaped TSV, should use tab separators
                assert "abc\t1" in content
                assert "def\t2" in content

    def test_write_orc(self):
        path = f"{self.base_path}/test.orc"

        dataframe_to_file(
            self.df, path, self.fs, lambda x: path, content_type=ContentType.ORC.value
        )
        assert self.fs.exists(path), "file was not written"

        # Verify content by reading with pandas (since polars delegates to PyArrow)
        result = pd.read_orc(path)
        expected = self.df.to_pandas()
        pd.testing.assert_frame_equal(result, expected)

    def test_write_parquet(self):
        path = f"{self.base_path}/test.parquet"

        dataframe_to_file(
            self.df,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.PARQUET.value,
        )
        assert self.fs.exists(path), "file was not written"

        # Verify content
        result = pl.read_parquet(path)
        assert result.equals(self.df)

    def test_write_json(self):
        path = f"{self.base_path}/test.json.gz"

        dataframe_to_file(
            self.df, path, self.fs, lambda x: path, content_type=ContentType.JSON.value
        )
        assert self.fs.exists(path), "file was not written"

        # Verify content (should be GZIP compressed, newline-delimited JSON)
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
        path = f"{self.base_path}/test.avro"

        dataframe_to_file(
            self.df, path, self.fs, lambda x: path, content_type=ContentType.AVRO.value
        )
        assert self.fs.exists(path), "file was not written"

        # Verify content by reading with polars
        result = pl.read_avro(path)
        assert result.equals(self.df)


class TestPolarsReaders(TestCase):
    def setUp(self):
        # Create test data files for reading
        self.fs = fsspec.filesystem("file")
        self.base_path = tempfile.mkdtemp()
        self.fs.makedirs(self.base_path, exist_ok=True)

        # Create test DataFrame
        self.df = pl.DataFrame(
            {
                "col1": ["a,b\tc|d", "e,f\tg|h", "test"],
                "col2": [1, 2, 3],
                "col3": [1.1, 2.2, 3.3],
            }
        )

        # Write test files in different formats
        self._create_test_files()

    def tearDown(self):
        self.fs.rm(self.base_path, recursive=True)

    def _create_test_files(self):
        """Create test files for reading tests with the original test data structure."""
        import gzip
        import bz2

        # Create CSV file (GZIP compressed) with the original test data
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
        with self.fs.open(unescaped_tsv_path, "wb") as f:
            with gzip.GzipFile(fileobj=f, mode="wb") as gz:
                content = "abc\t1\t1.1\ndef\t2\t2.2\nghi\t3\t3.3\n"
                gz.write(content.encode("utf-8"))

        # Create Parquet file
        parquet_path = f"{self.base_path}/test.parquet"
        self.df.write_parquet(parquet_path)

        # Create Feather file
        feather_path = f"{self.base_path}/test.feather"
        self.df.write_ipc(feather_path)

        # Create JSON file (GZIP compressed, NDJSON format)
        json_path = f"{self.base_path}/test.json"
        with self.fs.open(json_path, "wb") as f:
            with gzip.GzipFile(fileobj=f, mode="wb") as gz:
                # Use proper NDJSON format - one JSON object per line
                lines = []
                for i in range(len(self.df)):
                    row = self.df.row(i)
                    json_obj = {"col1": row[0], "col2": row[1], "col3": row[2]}
                    lines.append(json.dumps(json_obj))
                content = "\n".join(lines) + "\n"
                gz.write(content.encode("utf-8"))

        # Create Avro file
        avro_path = f"{self.base_path}/test.avro"
        self.df.write_avro(avro_path)

        # Create ORC file using pandas (since polars delegates to pandas for ORC)
        orc_path = f"{self.base_path}/test.orc"
        self.df.to_pandas().to_orc(orc_path)

        # Create BZIP2 compressed CSV for compression tests
        bzip2_path = f"{self.base_path}/test_bzip2.csv.bz2"
        with bz2.open(bzip2_path, "wt") as f:
            f.write('"a,b\tc|d",1,1.1\n"e,f\tg|h",2,2.2\ntest,3,3.3\n')

    def test_content_type_to_reader_kwargs(self):
        # Test CSV kwargs
        csv_kwargs = content_type_to_reader_kwargs(ContentType.CSV.value)
        expected_csv = {"separator": ",", "has_header": False}
        assert csv_kwargs == expected_csv

        # Test TSV kwargs
        tsv_kwargs = content_type_to_reader_kwargs(ContentType.TSV.value)
        expected_tsv = {"separator": "\t", "has_header": False}
        assert tsv_kwargs == expected_tsv

        # Test PSV kwargs
        psv_kwargs = content_type_to_reader_kwargs(ContentType.PSV.value)
        expected_psv = {"separator": "|", "has_header": False}
        assert psv_kwargs == expected_psv

        # Test unescaped TSV kwargs
        unescaped_kwargs = content_type_to_reader_kwargs(
            ContentType.UNESCAPED_TSV.value
        )
        expected_unescaped = {
            "separator": "\t",
            "has_header": False,
            "null_values": [""],
            "quote_char": None,
        }
        assert unescaped_kwargs == expected_unescaped

        # Test Parquet kwargs (should be empty)
        parquet_kwargs = content_type_to_reader_kwargs(ContentType.PARQUET.value)
        assert parquet_kwargs == {}

    def test_add_column_kwargs(self):
        kwargs = {}
        column_names = ["col1", "col2", "col3"]
        include_columns = ["col1", "col2"]

        # Test CSV column kwargs
        _add_column_kwargs(ContentType.CSV.value, column_names, include_columns, kwargs)
        assert kwargs["new_columns"] == column_names
        assert kwargs["columns"] == include_columns

        # Test Parquet column kwargs
        kwargs = {}
        _add_column_kwargs(
            ContentType.PARQUET.value, column_names, include_columns, kwargs
        )
        assert kwargs["columns"] == include_columns
        assert "new_columns" not in kwargs

    def test_read_csv_from_file(self):
        csv_path = f"{self.base_path}/test.csv"

        # Read using polars directly to test our reader logic
        with self.fs.open(csv_path, "rb") as f:
            with gzip.GzipFile(fileobj=f) as gz:
                source = io.BytesIO(gz.read())

        kwargs = content_type_to_reader_kwargs(ContentType.CSV.value)
        kwargs["new_columns"] = ["col1", "col2", "col3"]

        result = pl.read_csv(source, **kwargs)

        # Verify basic structure
        assert len(result) == 3
        assert list(result.columns) == ["col1", "col2", "col3"]
        assert result["col1"].to_list() == ["a,b\tc|d", "e,f\tg|h", "test"]

    def test_read_tsv_from_file(self):
        tsv_path = f"{self.base_path}/test.tsv"

        with self.fs.open(tsv_path, "rb") as f:
            with gzip.GzipFile(fileobj=f) as gz:
                source = io.BytesIO(gz.read())

        kwargs = content_type_to_reader_kwargs(ContentType.TSV.value)
        kwargs["new_columns"] = ["col1", "col2", "col3"]

        result = pl.read_csv(source, **kwargs)

        assert len(result) == 3
        assert list(result.columns) == ["col1", "col2", "col3"]
        assert result["col1"].to_list() == ["a,b\tc|d", "e,f\tg|h", "test"]

    def test_read_psv_from_file(self):
        psv_path = f"{self.base_path}/test.psv"

        with self.fs.open(psv_path, "rb") as f:
            with gzip.GzipFile(fileobj=f) as gz:
                source = io.BytesIO(gz.read())

        kwargs = content_type_to_reader_kwargs(ContentType.PSV.value)
        kwargs["new_columns"] = ["col1", "col2", "col3"]

        result = pl.read_csv(source, **kwargs)

        assert len(result) == 3
        assert list(result.columns) == ["col1", "col2", "col3"]
        assert result["col1"].to_list() == ["a,b\tc|d", "e,f\tg|h", "test"]

    def test_read_parquet_from_file(self):
        parquet_path = f"{self.base_path}/test.parquet"
        result = pl.read_parquet(parquet_path)

        assert len(result) == 3
        assert list(result.columns) == ["col1", "col2", "col3"]
        assert result.equals(self.df)

    def test_read_feather_from_file(self):
        feather_path = f"{self.base_path}/test.feather"
        result = pl.read_ipc(feather_path)

        assert len(result) == 3
        assert list(result.columns) == ["col1", "col2", "col3"]
        assert result.equals(self.df)

    def test_read_json_from_file(self):
        json_path = f"{self.base_path}/test.json"

        with self.fs.open(json_path, "rb") as f:
            with gzip.GzipFile(fileobj=f) as gz:
                source = io.BytesIO(gz.read())

        result = pl.read_ndjson(source)

        assert len(result) == 3
        assert set(result.columns) == {"col1", "col2", "col3"}
        assert result["col1"].to_list() == ["a,b\tc|d", "e,f\tg|h", "test"]

    def test_read_avro_from_file(self):
        avro_path = f"{self.base_path}/test.avro"
        result = pl.read_avro(avro_path)

        assert len(result) == 3
        assert list(result.columns) == ["col1", "col2", "col3"]
        assert result.equals(self.df)

    def test_read_orc_from_file(self):
        # Test ORC reading via pandas conversion
        orc_path = f"{self.base_path}/test.orc"

        # Read with pandas and convert to polars (mimicking our ORC handling)
        import pandas as pd

        pd_df = pd.read_orc(orc_path)
        result = pl.from_pandas(pd_df)

        assert len(result) == 3
        assert list(result.columns) == ["col1", "col2", "col3"]
        # Convert both to pandas for comparison due to potential type differences
        pd.testing.assert_frame_equal(result.to_pandas(), self.df.to_pandas())

    def test_read_kwargs_provider_string_types(self):
        # Test the string types provider
        provider = ReadKwargsProviderPolarsStringTypes()
        kwargs = {"separator": ",", "has_header": False}

        # Apply string types
        result_kwargs = provider._get_kwargs(ContentType.CSV.value, kwargs)

        # Should add infer_schema=False for string type inference
        assert "infer_schema" in result_kwargs
        assert result_kwargs["infer_schema"] is False

    def test_concat_dataframes(self):
        # Test concatenation of multiple dataframes
        df1 = pl.DataFrame({"col1": ["a"], "col2": [1]})
        df2 = pl.DataFrame({"col1": ["b"], "col2": [2]})
        df3 = pl.DataFrame({"col1": ["c"], "col2": [3]})

        # Test normal concatenation
        result = concat_dataframes([df1, df2, df3])
        assert len(result) == 3
        assert result["col1"].to_list() == ["a", "b", "c"]

        # Test single dataframe
        result = concat_dataframes([df1])
        assert result.equals(df1)

        # Test empty list
        result = concat_dataframes([])
        assert result is None

        # Test None input
        result = concat_dataframes(None)
        assert result is None

    def test_file_to_dataframe_csv(self):
        # Test reading CSV with file_to_dataframe
        csv_path = f"{self.base_path}/test.csv"

        result = file_to_dataframe(
            csv_path,
            ContentType.CSV.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs,
            column_names=["col1", "col2", "col3"],
        )

        assert len(result) == 3
        assert list(result.columns) == ["col1", "col2", "col3"]
        assert result["col1"].to_list() == ["a,b\tc|d", "e,f\tg|h", "test"]

    def test_file_to_dataframe_tsv(self):
        # Test reading TSV with file_to_dataframe
        tsv_path = f"{self.base_path}/test.tsv"

        result = file_to_dataframe(
            tsv_path,
            ContentType.TSV.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs,
            column_names=["col1", "col2", "col3"],
        )

        assert len(result) == 3
        assert list(result.columns) == ["col1", "col2", "col3"]
        assert result["col1"].to_list() == ["a,b\tc|d", "e,f\tg|h", "test"]

    def test_file_to_dataframe_parquet(self):
        # Test reading Parquet with file_to_dataframe
        parquet_path = f"{self.base_path}/test.parquet"

        result = file_to_dataframe(
            parquet_path, ContentType.PARQUET.value, filesystem=self.fs
        )

        assert len(result) == 3
        assert list(result.columns) == ["col1", "col2", "col3"]
        assert result.equals(self.df)

    def test_file_to_dataframe_feather(self):
        # Test reading Feather with file_to_dataframe
        feather_path = f"{self.base_path}/test.feather"

        result = file_to_dataframe(
            feather_path, ContentType.FEATHER.value, filesystem=self.fs
        )

        assert len(result) == 3
        assert list(result.columns) == ["col1", "col2", "col3"]
        assert result.equals(self.df)

    def test_file_to_dataframe_json(self):
        # Test reading JSON with file_to_dataframe
        json_path = f"{self.base_path}/test.json"

        result = file_to_dataframe(
            json_path,
            ContentType.JSON.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs,
        )

        assert len(result) == 3
        assert set(result.columns) == {"col1", "col2", "col3"}
        assert result["col1"].to_list() == ["a,b\tc|d", "e,f\tg|h", "test"]

    def test_file_to_dataframe_avro(self):
        # Test reading Avro with file_to_dataframe
        avro_path = f"{self.base_path}/test.avro"

        result = file_to_dataframe(
            avro_path, ContentType.AVRO.value, filesystem=self.fs
        )

        assert len(result) == 3
        assert list(result.columns) == ["col1", "col2", "col3"]
        assert result.equals(self.df)

    def test_file_to_dataframe_orc(self):
        # Test reading ORC with file_to_dataframe
        orc_path = f"{self.base_path}/test.orc"

        result = file_to_dataframe(orc_path, ContentType.ORC.value, filesystem=self.fs)

        assert len(result) == 3
        assert list(result.columns) == ["col1", "col2", "col3"]
        # Convert both to pandas for comparison due to potential type differences
        pd.testing.assert_frame_equal(result.to_pandas(), self.df.to_pandas())

    def test_file_to_dataframe_with_column_selection(self):
        # Test reading with column selection
        csv_path = f"{self.base_path}/test.csv"

        # When has_header=False and we specify columns, we need to use column indices or
        # not provide new_columns. Let's test by just specifying the first 2 columns by index
        result = file_to_dataframe(
            csv_path,
            ContentType.CSV.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs,
            include_columns=[0, 1],  # Select first two columns by index
        )

        assert len(result) == 3
        assert len(result.columns) == 2  # Should only have 2 columns
        # With auto-generated column names when has_header=False
        assert list(result.columns) == ["column_1", "column_2"]

    def test_file_to_dataframe_with_kwargs_provider(self):
        # Test reading with kwargs provider
        csv_path = f"{self.base_path}/test.csv"
        provider = ReadKwargsProviderPolarsStringTypes(
            include_columns=["column_1", "column_2", "column_3"]
        )

        result = file_to_dataframe(
            csv_path,
            ContentType.CSV.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs,
            pl_read_func_kwargs_provider=provider,
        )

        assert len(result) == 3
        assert list(result.columns) == ["column_1", "column_2", "column_3"]
        # With string types provider, all columns should be strings
        assert all(result[col].dtype == pl.Utf8 for col in result.columns)

    def test_file_to_dataframe_filesystem_inference(self):
        # Test filesystem inference when no filesystem is provided
        parquet_path = f"{self.base_path}/test.parquet"

        result = file_to_dataframe(
            parquet_path,
            ContentType.PARQUET.value
            # No filesystem provided - should be inferred
        )

        assert len(result) == 3
        assert list(result.columns) == ["col1", "col2", "col3"]
        assert result.equals(self.df)

    def test_file_to_dataframe_unsupported_content_type(self):
        # Test error handling for unsupported content type
        parquet_path = f"{self.base_path}/test.parquet"

        with self.assertRaises(NotImplementedError) as context:
            file_to_dataframe(
                parquet_path, "unsupported/content-type", filesystem=self.fs
            )

        assert "not implemented" in str(context.exception)

    def test_file_to_dataframe_bzip2_compression(self):
        # Test BZIP2 compression handling
        import bz2

        # Create a BZIP2 compressed CSV file
        csv_content = '"a,b\tc|d",1,1.1\n"e,f\tg|h",2,2.2\ntest,3,3.3\n'
        compressed_content = bz2.compress(csv_content.encode("utf-8"))

        bz2_path = f"{self.base_path}/test.csv.bz2"
        with self.fs.open(bz2_path, "wb") as f:
            f.write(compressed_content)

        result = file_to_dataframe(
            bz2_path,
            ContentType.CSV.value,
            ContentEncoding.BZIP2.value,
            filesystem=self.fs,
            column_names=["col1", "col2", "col3"],
        )

        assert len(result) == 3
        assert list(result.columns) == ["col1", "col2", "col3"]
        assert result["col1"].to_list() == ["a,b\tc|d", "e,f\tg|h", "test"]


class TestPolarsFileSystemSupport(TestCase):
    """
    Comprehensive tests for encoding-aware reader functions with different filesystem types.
    Tests fsspec AbstractFileSystem, PyArrow FileSystem, and auto-inferred filesystem.
    """

    def setUp(self):
        import pyarrow.fs as pafs

        # Create test data
        self.test_data = pl.DataFrame(
            {
                "col1": ["value1", "value2", "value3"],
                "col2": [1, 2, 3],
                "col3": [1.1, 2.2, 3.3],
            }
        )

        # Set up temporary directory
        self.temp_dir = tempfile.mkdtemp()

        # Set up different filesystem types
        self.fsspec_fs = fsspec.filesystem("file")
        self.pyarrow_fs = pafs.LocalFileSystem()

        # Create test files for each content type
        self._create_test_files()

    def tearDown(self):
        import shutil

        shutil.rmtree(self.temp_dir)

    def _create_test_files(self):
        """Create test files in different formats with different compression types."""
        import gzip
        import bz2

        # CSV files
        csv_data = "col1,col2,col3\nvalue1,1,1.1\nvalue2,2,2.2\nvalue3,3,3.3\n"

        # Create uncompressed CSV
        with open(f"{self.temp_dir}/test.csv", "w") as f:
            f.write(csv_data)

        # Create GZIP compressed CSV (fix: properly close the file)
        with gzip.open(f"{self.temp_dir}/test_gzip.csv.gz", "wt") as f:
            f.write(csv_data)

        # Create BZIP2 compressed CSV (fix: properly close the file)
        with bz2.open(f"{self.temp_dir}/test_bzip2.csv.bz2", "wt") as f:
            f.write(csv_data)

        # Parquet file
        self.test_data.write_parquet(f"{self.temp_dir}/test.parquet")

        # Feather/IPC file
        self.test_data.write_ipc(f"{self.temp_dir}/test.feather")

        # JSON file (NDJSON)
        json_data = '{"col1":"value1","col2":1,"col3":1.1}\n{"col1":"value2","col2":2,"col3":2.2}\n{"col1":"value3","col2":3,"col3":3.3}\n'
        with open(f"{self.temp_dir}/test.json", "w") as f:
            f.write(json_data)

        # AVRO file
        self.test_data.write_avro(f"{self.temp_dir}/test.avro")

        # ORC file (via pandas since polars delegates to pandas for ORC)
        self.test_data.to_pandas().to_orc(f"{self.temp_dir}/test.orc")

    def _assert_dataframes_equal(self, result, expected):
        """Helper to assert polars dataframes are equal."""
        # Convert to pandas for comparison since polars equality can be tricky with floating point
        pd.testing.assert_frame_equal(
            result.to_pandas().reset_index(drop=True),
            expected.to_pandas().reset_index(drop=True),
            check_dtype=False,  # Allow minor type differences
        )

    def test_csv_with_fsspec_filesystem(self):
        """Test CSV reading with fsspec AbstractFileSystem."""
        from deltacat.utils.polars import read_csv

        # Test uncompressed CSV
        result = read_csv(
            f"{self.temp_dir}/test.csv",
            filesystem=self.fsspec_fs,
            content_encoding=ContentEncoding.IDENTITY.value,
            has_header=True,
        )
        self._assert_dataframes_equal(result, self.test_data)

        # Test GZIP compressed CSV
        result = read_csv(
            f"{self.temp_dir}/test_gzip.csv.gz",
            filesystem=self.fsspec_fs,
            content_encoding=ContentEncoding.GZIP.value,
            has_header=True,
        )
        self._assert_dataframes_equal(result, self.test_data)

        # Test BZIP2 compressed CSV
        result = read_csv(
            f"{self.temp_dir}/test_bzip2.csv.bz2",
            filesystem=self.fsspec_fs,
            content_encoding=ContentEncoding.BZIP2.value,
            has_header=True,
        )
        self._assert_dataframes_equal(result, self.test_data)

    def test_csv_with_pyarrow_filesystem(self):
        """Test CSV reading with PyArrow FileSystem."""
        from deltacat.utils.polars import read_csv

        # Test uncompressed CSV
        result = read_csv(
            f"{self.temp_dir}/test.csv",
            filesystem=self.pyarrow_fs,
            content_encoding=ContentEncoding.IDENTITY.value,
            has_header=True,
        )
        self._assert_dataframes_equal(result, self.test_data)

        # Test GZIP compressed CSV
        result = read_csv(
            f"{self.temp_dir}/test_gzip.csv.gz",
            filesystem=self.pyarrow_fs,
            content_encoding=ContentEncoding.GZIP.value,
            has_header=True,
        )
        self._assert_dataframes_equal(result, self.test_data)

    def test_csv_with_auto_inferred_filesystem(self):
        """Test CSV reading with automatically inferred filesystem."""
        from deltacat.utils.polars import read_csv

        # Test uncompressed CSV (filesystem=None, should auto-infer)
        result = read_csv(
            f"{self.temp_dir}/test.csv",
            filesystem=None,
            content_encoding=ContentEncoding.IDENTITY.value,
            has_header=True,
        )
        self._assert_dataframes_equal(result, self.test_data)

    def test_parquet_with_different_filesystems(self):
        """Test Parquet reading with different filesystem types."""
        from deltacat.utils.polars import read_parquet

        # Test with fsspec
        result = read_parquet(
            f"{self.temp_dir}/test.parquet",
            filesystem=self.fsspec_fs,
            content_encoding=ContentEncoding.IDENTITY.value,
        )
        self._assert_dataframes_equal(result, self.test_data)

        # Test with PyArrow
        result = read_parquet(
            f"{self.temp_dir}/test.parquet",
            filesystem=self.pyarrow_fs,
            content_encoding=ContentEncoding.IDENTITY.value,
        )
        self._assert_dataframes_equal(result, self.test_data)

        # Test with auto-inferred
        result = read_parquet(
            f"{self.temp_dir}/test.parquet",
            filesystem=None,
            content_encoding=ContentEncoding.IDENTITY.value,
        )
        self._assert_dataframes_equal(result, self.test_data)

    def test_feather_with_different_filesystems(self):
        """Test Feather/IPC reading with different filesystem types."""
        from deltacat.utils.polars import read_ipc

        # Test with fsspec
        result = read_ipc(
            f"{self.temp_dir}/test.feather",
            filesystem=self.fsspec_fs,
            content_encoding=ContentEncoding.IDENTITY.value,
        )
        self._assert_dataframes_equal(result, self.test_data)

        # Test with PyArrow
        result = read_ipc(
            f"{self.temp_dir}/test.feather",
            filesystem=self.pyarrow_fs,
            content_encoding=ContentEncoding.IDENTITY.value,
        )
        self._assert_dataframes_equal(result, self.test_data)

        # Test with auto-inferred
        result = read_ipc(
            f"{self.temp_dir}/test.feather",
            filesystem=None,
            content_encoding=ContentEncoding.IDENTITY.value,
        )
        self._assert_dataframes_equal(result, self.test_data)

    def test_json_with_different_filesystems(self):
        """Test JSON reading with different filesystem types."""
        from deltacat.utils.polars import read_ndjson

        # Test with fsspec
        result = read_ndjson(
            f"{self.temp_dir}/test.json",
            filesystem=self.fsspec_fs,
            content_encoding=ContentEncoding.IDENTITY.value,
        )
        self._assert_dataframes_equal(result, self.test_data)

        # Test with PyArrow
        result = read_ndjson(
            f"{self.temp_dir}/test.json",
            filesystem=self.pyarrow_fs,
            content_encoding=ContentEncoding.IDENTITY.value,
        )
        self._assert_dataframes_equal(result, self.test_data)

        # Test with auto-inferred
        result = read_ndjson(
            f"{self.temp_dir}/test.json",
            filesystem=None,
            content_encoding=ContentEncoding.IDENTITY.value,
        )
        self._assert_dataframes_equal(result, self.test_data)

    def test_avro_with_different_filesystems(self):
        """Test AVRO reading with different filesystem types."""
        from deltacat.utils.polars import read_avro

        # Test with fsspec
        result = read_avro(
            f"{self.temp_dir}/test.avro",
            filesystem=self.fsspec_fs,
            content_encoding=ContentEncoding.IDENTITY.value,
        )
        self._assert_dataframes_equal(result, self.test_data)

        # Test with PyArrow
        result = read_avro(
            f"{self.temp_dir}/test.avro",
            filesystem=self.pyarrow_fs,
            content_encoding=ContentEncoding.IDENTITY.value,
        )
        self._assert_dataframes_equal(result, self.test_data)

        # Test with auto-inferred
        result = read_avro(
            f"{self.temp_dir}/test.avro",
            filesystem=None,
            content_encoding=ContentEncoding.IDENTITY.value,
        )
        self._assert_dataframes_equal(result, self.test_data)

    def test_orc_with_different_filesystems(self):
        """Test ORC reading with different filesystem types."""
        from deltacat.utils.polars import read_orc

        # Test with fsspec
        result = read_orc(
            f"{self.temp_dir}/test.orc",
            filesystem=self.fsspec_fs,
            content_encoding=ContentEncoding.IDENTITY.value,
        )
        self._assert_dataframes_equal(result, self.test_data)

        # Test with PyArrow
        result = read_orc(
            f"{self.temp_dir}/test.orc",
            filesystem=self.pyarrow_fs,
            content_encoding=ContentEncoding.IDENTITY.value,
        )
        self._assert_dataframes_equal(result, self.test_data)

        # Test with auto-inferred
        result = read_orc(
            f"{self.temp_dir}/test.orc",
            filesystem=None,
            content_encoding=ContentEncoding.IDENTITY.value,
        )
        self._assert_dataframes_equal(result, self.test_data)

    def test_file_to_dataframe_with_different_filesystems(self):
        """Test file_to_dataframe with different filesystem types for all content types."""
        test_cases = [
            (
                f"{self.temp_dir}/test.csv",
                ContentType.CSV.value,
                ContentEncoding.IDENTITY.value,
                {"has_header": True},
            ),
            (
                f"{self.temp_dir}/test_gzip.csv.gz",
                ContentType.CSV.value,
                ContentEncoding.GZIP.value,
                {"has_header": True},
            ),
            (
                f"{self.temp_dir}/test.parquet",
                ContentType.PARQUET.value,
                ContentEncoding.IDENTITY.value,
                {},
            ),
            (
                f"{self.temp_dir}/test.feather",
                ContentType.FEATHER.value,
                ContentEncoding.IDENTITY.value,
                {},
            ),
            (
                f"{self.temp_dir}/test.json",
                ContentType.JSON.value,
                ContentEncoding.IDENTITY.value,
                {},
            ),
            (
                f"{self.temp_dir}/test.avro",
                ContentType.AVRO.value,
                ContentEncoding.IDENTITY.value,
                {},
            ),
            (
                f"{self.temp_dir}/test.orc",
                ContentType.ORC.value,
                ContentEncoding.IDENTITY.value,
                {},
            ),
        ]

        filesystems = [
            ("fsspec", self.fsspec_fs),
            ("pyarrow", self.pyarrow_fs),
            ("auto-inferred", None),
        ]

        for path, content_type, content_encoding, extra_kwargs in test_cases:
            for fs_name, filesystem in filesystems:
                with self.subTest(
                    content_type=content_type,
                    filesystem=fs_name,
                    encoding=content_encoding,
                ):
                    result = file_to_dataframe(
                        path=path,
                        content_type=content_type,
                        content_encoding=content_encoding,
                        filesystem=filesystem,
                        **extra_kwargs,
                    )
                    self._assert_dataframes_equal(result, self.test_data)

    def test_compression_encoding_with_different_filesystems(self):
        """Test that compression encoding works correctly with different filesystem types."""
        test_cases = [
            (f"{self.temp_dir}/test.csv", ContentEncoding.IDENTITY.value),
            (f"{self.temp_dir}/test_gzip.csv.gz", ContentEncoding.GZIP.value),
            (f"{self.temp_dir}/test_bzip2.csv.bz2", ContentEncoding.BZIP2.value),
        ]

        filesystems = [
            ("fsspec", self.fsspec_fs),
            ("pyarrow", self.pyarrow_fs),
            ("auto-inferred", None),
        ]

        for path, content_encoding in test_cases:
            for fs_name, filesystem in filesystems:
                with self.subTest(encoding=content_encoding, filesystem=fs_name):
                    result = file_to_dataframe(
                        path=path,
                        content_type=ContentType.CSV.value,
                        content_encoding=content_encoding,
                        filesystem=filesystem,
                        has_header=True,
                    )
                    self._assert_dataframes_equal(result, self.test_data)

    def test_filesystem_open_kwargs(self):
        """Test that filesystem open kwargs are properly passed through."""
        from deltacat.utils.polars import read_csv

        # Test with custom fs_open_kwargs
        result = read_csv(
            f"{self.temp_dir}/test.csv",
            filesystem=self.fsspec_fs,
            content_encoding=ContentEncoding.IDENTITY.value,
            fs_open_kwargs={
                "encoding": "utf-8"
            },  # This should be passed to filesystem.open()
            has_header=True,
        )
        self._assert_dataframes_equal(result, self.test_data)
