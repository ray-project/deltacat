from unittest import TestCase
import pandas as pd
import tempfile
import fsspec
import gzip
import json
import io
import polars as pl
from deltacat.types.media import ContentType, ContentEncoding
from deltacat.utils.pandas import (
    dataframe_to_file,
    file_to_dataframe,
    content_type_to_reader_kwargs,
    _add_column_kwargs,
    ReadKwargsProviderPandasCsvPureUtf8,
    concat_dataframes,
)


class TestPandasWriters(TestCase):
    def setUp(self):
        # Create a test DataFrame with data that includes delimiters
        self.df = pd.DataFrame({
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
        
        dataframe_to_file(
            self.df,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.FEATHER.value
        )
        assert self.fs.exists(path), "file was not written"
        
        # Verify content
        result = pd.read_feather(path)
        pd.testing.assert_frame_equal(result, self.df)

    def test_write_csv(self):
        path = f"{self.base_path}/test.csv"
        
        dataframe_to_file(
            self.df,
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
                content = gz.read().decode('utf-8')
                # Should be quoted due to tabs in data
                assert '"a,b\tc|d"\t1' in content
                assert '"e,f\tg|h"\t2' in content

    def test_write_psv(self):
        path = f"{self.base_path}/test.psv"
        
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
                content = gz.read().decode('utf-8')
                # Should be quoted due to pipes in data
                assert '"a,b\tc|d"|1' in content
                assert '"e,f\tg|h"|2' in content

    def test_write_unescaped_tsv(self):
        # Create DataFrame without delimiters for unescaped TSV
        df = pd.DataFrame({
            'col1': ['abc', 'def'],
            'col2': [1, 2]
        })
        path = f"{self.base_path}/test.tsv"
        
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
                content = gz.read().decode('utf-8')
                # With quoting_style="none", strings should not be quoted
                assert 'abc\t1' in content
                assert 'def\t2' in content

    def test_write_orc(self):
        path = f"{self.base_path}/test.orc"
        
        dataframe_to_file(
            self.df,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.ORC.value
        )
        assert self.fs.exists(path), "file was not written"
        
        # Verify content
        result = pd.read_orc(path)
        pd.testing.assert_frame_equal(result, self.df)

    def test_write_parquet(self):
        path = f"{self.base_path}/test.parquet"
        
        dataframe_to_file(
            self.df,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.PARQUET.value
        )
        assert self.fs.exists(path), "file was not written"
        
        # Verify content
        result = pd.read_parquet(path)
        pd.testing.assert_frame_equal(result, self.df)

    def test_write_json(self):
        path = f"{self.base_path}/test.json"
        
        dataframe_to_file(
            self.df,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.JSON.value,
            orient='records'  # Write each record as a separate JSON object
        )
        assert self.fs.exists(path), "file was not written"
        
        # Verify content (should be GZIP compressed)
        with self.fs.open(path, "rb") as f:
            with gzip.GzipFile(fileobj=f) as gz:
                content = gz.read().decode('utf-8')
                # Each line should be a valid JSON object
                data = json.loads(content)
                assert len(data) == 2  # 2 records
                assert data[0] == {"col1": "a,b\tc|d", "col2": 1}
                assert data[1] == {"col1": "e,f\tg|h", "col2": 2}

    def test_write_avro(self):
        path = f"{self.base_path}/test.avro"
        
        dataframe_to_file(
            self.df,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.AVRO.value
        )
        assert self.fs.exists(path), "file was not written"
        
        # Verify content by reading with polars
        result = pl.read_avro(path).to_pandas()
        pd.testing.assert_frame_equal(result, self.df)


class TestPandasReaders(TestCase):
    def setUp(self):
        # Create test data files for reading
        self.fs = fsspec.filesystem("file")
        self.base_path = tempfile.mkdtemp()
        self.fs.makedirs(self.base_path, exist_ok=True)
        
        # Create test DataFrame
        self.df = pd.DataFrame({
            'col1': ['a,b\tc|d', 'e,f\tg|h', 'test'],
            'col2': [1, 2, 3],
            'col3': [1.1, 2.2, 3.3]
        })
        
        # Write test files in different formats
        self._create_test_files()

    def tearDown(self):
        self.fs.rm(self.base_path, recursive=True)

    def _create_test_files(self):
        # Create CSV file (GZIP compressed)
        csv_path = f"{self.base_path}/test.csv"
        with self.fs.open(csv_path, "wb") as f:
            with gzip.GzipFile(fileobj=f, mode='wb') as gz:
                content = '"a,b\tc|d",1,1.1\n"e,f\tg|h",2,2.2\ntest,3,3.3\n'
                gz.write(content.encode('utf-8'))
        
        # Create TSV file (GZIP compressed)  
        tsv_path = f"{self.base_path}/test.tsv"
        with self.fs.open(tsv_path, "wb") as f:
            with gzip.GzipFile(fileobj=f, mode='wb') as gz:
                content = '"a,b\tc|d"\t1\t1.1\n"e,f\tg|h"\t2\t2.2\ntest\t3\t3.3\n'
                gz.write(content.encode('utf-8'))
        
        # Create PSV file (GZIP compressed)
        psv_path = f"{self.base_path}/test.psv"
        with self.fs.open(psv_path, "wb") as f:
            with gzip.GzipFile(fileobj=f, mode='wb') as gz:
                content = '"a,b\tc|d"|1|1.1\n"e,f\tg|h"|2|2.2\ntest|3|3.3\n'
                gz.write(content.encode('utf-8'))
        
        # Create unescaped TSV file (GZIP compressed)
        unescaped_tsv_path = f"{self.base_path}/test_unescaped.tsv"
        simple_df = pd.DataFrame({
            'col1': ['abc', 'def', 'ghi'],
            'col2': [1, 2, 3],
            'col3': [1.1, 2.2, 3.3]
        })
        with self.fs.open(unescaped_tsv_path, "wb") as f:
            with gzip.GzipFile(fileobj=f, mode='wb') as gz:
                content = 'abc\t1\t1.1\ndef\t2\t2.2\nghi\t3\t3.3\n'
                gz.write(content.encode('utf-8'))
        
        # Create Parquet file
        parquet_path = f"{self.base_path}/test.parquet"
        self.df.to_parquet(parquet_path, index=False)
        
        # Create Feather file
        feather_path = f"{self.base_path}/test.feather"
        self.df.to_feather(feather_path)
        
        # Create JSON file (GZIP compressed, orient=records format)
        json_path = f"{self.base_path}/test.json"
        with self.fs.open(json_path, "wb") as f:
            with gzip.GzipFile(fileobj=f, mode='wb') as gz:
                json_str = self.df.to_json(orient='records')
                gz.write(json_str.encode('utf-8'))
        
        # Create Avro file using polars (since pandas delegates to polars for Avro)
        avro_path = f"{self.base_path}/test.avro"
        pl_df = pl.from_pandas(self.df)
        pl_df.write_avro(avro_path)
        
        # Create ORC file
        orc_path = f"{self.base_path}/test.orc"
        self.df.to_orc(orc_path, index=False)

    def test_content_type_to_reader_kwargs(self):
        # Test CSV kwargs
        csv_kwargs = content_type_to_reader_kwargs(ContentType.CSV.value)
        expected_csv = {"sep": ",", "header": None}
        assert csv_kwargs == expected_csv
        
        # Test TSV kwargs
        tsv_kwargs = content_type_to_reader_kwargs(ContentType.TSV.value)
        expected_tsv = {"sep": "\t", "header": None}
        assert tsv_kwargs == expected_tsv
        
        # Test PSV kwargs
        psv_kwargs = content_type_to_reader_kwargs(ContentType.PSV.value)
        expected_psv = {"sep": "|", "header": None}
        assert psv_kwargs == expected_psv
        
        # Test unescaped TSV kwargs
        unescaped_kwargs = content_type_to_reader_kwargs(ContentType.UNESCAPED_TSV.value)
        expected_unescaped = {"sep": "\t", "header": None, "na_values": [""], "keep_default_na": False}
        assert unescaped_kwargs == expected_unescaped
        
        # Test Parquet kwargs (should be empty)
        parquet_kwargs = content_type_to_reader_kwargs(ContentType.PARQUET.value)
        assert parquet_kwargs == {}
        
        # Test Avro kwargs (should be empty)
        avro_kwargs = content_type_to_reader_kwargs(ContentType.AVRO.value)
        assert avro_kwargs == {}

    def test_add_column_kwargs(self):
        kwargs = {}
        column_names = ["col1", "col2", "col3"]
        include_columns = ["col1", "col2"]
        
        # Test CSV column kwargs
        _add_column_kwargs(ContentType.CSV.value, column_names, include_columns, kwargs)
        assert kwargs["names"] == column_names
        assert kwargs["usecols"] == include_columns
        
        # Test Parquet column kwargs
        kwargs = {}
        _add_column_kwargs(ContentType.PARQUET.value, column_names, include_columns, kwargs)
        assert kwargs["columns"] == include_columns
        assert "names" not in kwargs

    def test_file_to_dataframe_csv(self):
        # Test reading CSV with file_to_dataframe
        csv_path = f"{self.base_path}/test.csv"
        
        result = file_to_dataframe(
            csv_path,
            ContentType.CSV.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs,
            column_names=['col1', 'col2', 'col3']
        )
        
        assert len(result) == 3
        assert list(result.columns) == ['col1', 'col2', 'col3']
        assert result['col1'].tolist() == ['a,b\tc|d', 'e,f\tg|h', 'test']

    def test_file_to_dataframe_tsv(self):
        # Test reading TSV with file_to_dataframe
        tsv_path = f"{self.base_path}/test.tsv"
        
        result = file_to_dataframe(
            tsv_path,
            ContentType.TSV.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs,
            column_names=['col1', 'col2', 'col3']
        )
        
        assert len(result) == 3
        assert list(result.columns) == ['col1', 'col2', 'col3']
        assert result['col1'].tolist() == ['a,b\tc|d', 'e,f\tg|h', 'test']

    def test_file_to_dataframe_psv(self):
        # Test reading PSV with file_to_dataframe
        psv_path = f"{self.base_path}/test.psv"
        
        result = file_to_dataframe(
            psv_path,
            ContentType.PSV.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs,
            column_names=['col1', 'col2', 'col3']
        )
        
        assert len(result) == 3
        assert list(result.columns) == ['col1', 'col2', 'col3']
        assert result['col1'].tolist() == ['a,b\tc|d', 'e,f\tg|h', 'test']

    def test_file_to_dataframe_unescaped_tsv(self):
        # Test reading unescaped TSV with file_to_dataframe
        unescaped_tsv_path = f"{self.base_path}/test_unescaped.tsv"
        
        result = file_to_dataframe(
            unescaped_tsv_path,
            ContentType.UNESCAPED_TSV.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs,
            column_names=['col1', 'col2', 'col3']
        )
        
        assert len(result) == 3
        assert list(result.columns) == ['col1', 'col2', 'col3']
        assert result['col1'].tolist() == ['abc', 'def', 'ghi']

    def test_file_to_dataframe_parquet(self):
        # Test reading Parquet with file_to_dataframe
        parquet_path = f"{self.base_path}/test.parquet"
        
        result = file_to_dataframe(
            parquet_path,
            ContentType.PARQUET.value,
            filesystem=self.fs
        )
        
        assert len(result) == 3
        assert list(result.columns) == ['col1', 'col2', 'col3']
        pd.testing.assert_frame_equal(result, self.df)

    def test_file_to_dataframe_feather(self):
        # Test reading Feather with file_to_dataframe
        feather_path = f"{self.base_path}/test.feather"
        
        result = file_to_dataframe(
            feather_path,
            ContentType.FEATHER.value,
            filesystem=self.fs
        )
        
        assert len(result) == 3
        assert list(result.columns) == ['col1', 'col2', 'col3']
        pd.testing.assert_frame_equal(result, self.df)

    def test_file_to_dataframe_json(self):
        # Test reading JSON with file_to_dataframe
        json_path = f"{self.base_path}/test.json"
        
        result = file_to_dataframe(
            json_path,
            ContentType.JSON.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs
        )
        
        assert len(result) == 3
        assert set(result.columns) == {'col1', 'col2', 'col3'}
        assert result['col1'].tolist() == ['a,b\tc|d', 'e,f\tg|h', 'test']

    def test_file_to_dataframe_avro(self):
        # Test reading Avro with file_to_dataframe
        avro_path = f"{self.base_path}/test.avro"
        
        result = file_to_dataframe(
            avro_path,
            ContentType.AVRO.value,
            filesystem=self.fs
        )
        
        assert len(result) == 3
        assert list(result.columns) == ['col1', 'col2', 'col3']
        # Avro may have different dtypes, so compare values
        assert result['col1'].tolist() == ['a,b\tc|d', 'e,f\tg|h', 'test']

    def test_file_to_dataframe_orc(self):
        # Test reading ORC with file_to_dataframe
        orc_path = f"{self.base_path}/test.orc"
        
        result = file_to_dataframe(
            orc_path,
            ContentType.ORC.value,
            filesystem=self.fs
        )
        
        assert len(result) == 3
        assert list(result.columns) == ['col1', 'col2', 'col3']
        pd.testing.assert_frame_equal(result, self.df)

    def test_file_to_dataframe_with_column_selection(self):
        # Test reading with column selection
        csv_path = f"{self.base_path}/test.csv"
        
        result = file_to_dataframe(
            csv_path,
            ContentType.CSV.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs,
            column_names=['col1', 'col2', 'col3'],
            include_columns=['col1', 'col2']
        )
        
        assert len(result) == 3
        assert len(result.columns) == 2  # Should only have 2 columns
        assert list(result.columns) == ['col1', 'col2']

    def test_file_to_dataframe_with_kwargs_provider(self):
        # Test reading with kwargs provider
        csv_path = f"{self.base_path}/test.csv"
        provider = ReadKwargsProviderPandasCsvPureUtf8(include_columns=['col1', 'col2', 'col3'])
        
        result = file_to_dataframe(
            csv_path,
            ContentType.CSV.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs,
            column_names=['col1', 'col2', 'col3'],
            pd_read_func_kwargs_provider=provider
        )
        
        assert len(result) == 3
        assert list(result.columns) == ['col1', 'col2', 'col3']
        # With string types provider, all columns should be strings
        assert all(result[col].dtype == 'object' for col in result.columns)

    def test_file_to_dataframe_filesystem_inference(self):
        # Test filesystem inference when no filesystem is provided
        # Use JSON file since Parquet requires seekable files
        json_path = f"{self.base_path}/test.json"
        
        result = file_to_dataframe(
            json_path,
            ContentType.JSON.value,
            ContentEncoding.GZIP.value
            # No filesystem provided - should be inferred
        )
        
        assert len(result) == 3
        assert set(result.columns) == {'col1', 'col2', 'col3'}
        assert result['col1'].tolist() == ['a,b\tc|d', 'e,f\tg|h', 'test']

    def test_file_to_dataframe_unsupported_content_type(self):
        # Test error handling for unsupported content type
        parquet_path = f"{self.base_path}/test.parquet"
        
        with self.assertRaises(NotImplementedError) as context:
            file_to_dataframe(
                parquet_path,
                "unsupported/content-type",
                filesystem=self.fs
            )
        
        assert "not implemented" in str(context.exception)

    def test_file_to_dataframe_bzip2_compression(self):
        # Test BZIP2 compression handling
        import bz2
        
        # Create a BZIP2 compressed CSV file
        csv_content = '"a,b\tc|d",1,1.1\n"e,f\tg|h",2,2.2\ntest,3,3.3\n'
        compressed_content = bz2.compress(csv_content.encode('utf-8'))
        
        bz2_path = f"{self.base_path}/test.csv.bz2"
        with self.fs.open(bz2_path, "wb") as f:
            f.write(compressed_content)
        
        result = file_to_dataframe(
            bz2_path,
            ContentType.CSV.value,
            ContentEncoding.BZIP2.value,
            filesystem=self.fs,
            column_names=['col1', 'col2', 'col3']
        )
        
        assert len(result) == 3
        assert list(result.columns) == ['col1', 'col2', 'col3']
        assert result['col1'].tolist() == ['a,b\tc|d', 'e,f\tg|h', 'test']

    def test_concat_dataframes(self):
        # Test concatenation of multiple dataframes
        df1 = pd.DataFrame({"col1": ["a"], "col2": [1]})
        df2 = pd.DataFrame({"col1": ["b"], "col2": [2]})
        df3 = pd.DataFrame({"col1": ["c"], "col2": [3]})
        
        # Test normal concatenation
        result = concat_dataframes([df1, df2, df3])
        assert len(result) == 3
        assert result["col1"].tolist() == ["a", "b", "c"]
        
        # Test single dataframe
        result = concat_dataframes([df1])
        pd.testing.assert_frame_equal(result, df1)
        
        # Test empty list
        result = concat_dataframes([])
        assert result is None
        
        # Test None input
        result = concat_dataframes(None)
        assert result is None 