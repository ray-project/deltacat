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
    s3_file_to_dataframe,
    content_type_to_reader_kwargs,
    _add_column_kwargs,
    ReadKwargsProviderPolarsStringTypes,
    concat_dataframes,
)

class TestPolarsWriters(TestCase):
    def setUp(self):
        # Create a test DataFrame with data that includes delimiters
        self.df = pl.DataFrame({
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
        result = pl.read_ipc(path)
        assert result.equals(self.df)

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
                # Polars writes TSV with tab separators
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
                # Polars writes PSV with pipe separators
                assert '"a,b\tc|d"|1' in content
                assert '"e,f\tg|h"|2' in content

    def test_write_unescaped_tsv(self):
        # Create DataFrame without delimiters for unescaped TSV
        df = pl.DataFrame({
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
                # With quote_char=None for unescaped TSV, should use tab separators
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
            content_type=ContentType.PARQUET.value
        )
        assert self.fs.exists(path), "file was not written"
        
        # Verify content
        result = pl.read_parquet(path)
        assert result.equals(self.df)

    def test_write_json(self):
        path = f"{self.base_path}/test.json"
        
        dataframe_to_file(
            self.df,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.JSON.value
        )
        assert self.fs.exists(path), "file was not written"
        
        # Verify content (should be GZIP compressed, newline-delimited JSON)
        with self.fs.open(path, "rb") as f:
            with gzip.GzipFile(fileobj=f) as gz:
                content = gz.read().decode('utf-8')
                # Each line should be a valid JSON object
                lines = [line for line in content.split('\n') if line]  # Skip empty lines
                assert len(lines) == 2  # 2 records
                assert json.loads(lines[0]) == {"col1": "a,b\tc|d", "col2": 1}
                assert json.loads(lines[1]) == {"col1": "e,f\tg|h", "col2": 2}

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
        result = pl.read_avro(path)
        assert result.equals(self.df)

class TestPolarsReaders(TestCase):
    def setUp(self):
        # Create test data files for reading
        self.fs = fsspec.filesystem("file")
        self.base_path = tempfile.mkdtemp()
        self.fs.makedirs(self.base_path, exist_ok=True)
        
        # Create test DataFrame
        self.df = pl.DataFrame({
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
        simple_df = pl.DataFrame({
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
        self.df.write_parquet(parquet_path)
        
        # Create Feather file
        feather_path = f"{self.base_path}/test.feather"
        self.df.write_ipc(feather_path)
        
        # Create JSON file (GZIP compressed, NDJSON format)
        json_path = f"{self.base_path}/test.json"
        with self.fs.open(json_path, "wb") as f:
            with gzip.GzipFile(fileobj=f, mode='wb') as gz:
                # Use proper NDJSON format - one JSON object per line
                lines = []
                for i in range(len(self.df)):
                    row = self.df.row(i)
                    json_obj = {
                        "col1": row[0],
                        "col2": row[1], 
                        "col3": row[2]
                    }
                    lines.append(json.dumps(json_obj))
                content = '\n'.join(lines) + '\n'
                gz.write(content.encode('utf-8'))
        
        # Create Avro file
        avro_path = f"{self.base_path}/test.avro"
        self.df.write_avro(avro_path)
        
        # Create ORC file using pandas (since polars delegates to pandas for ORC)
        orc_path = f"{self.base_path}/test.orc"
        self.df.to_pandas().to_orc(orc_path)

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
        unescaped_kwargs = content_type_to_reader_kwargs(ContentType.UNESCAPED_TSV.value)
        expected_unescaped = {"separator": "\t", "has_header": False, "null_values": [""]}
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
        _add_column_kwargs(ContentType.PARQUET.value, column_names, include_columns, kwargs)
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
        assert result["col1"].to_list() == ['a,b\tc|d', 'e,f\tg|h', 'test']

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
        assert result["col1"].to_list() == ['a,b\tc|d', 'e,f\tg|h', 'test']

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
        assert result["col1"].to_list() == ['a,b\tc|d', 'e,f\tg|h', 'test']

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
        assert result["col1"].to_list() == ['a,b\tc|d', 'e,f\tg|h', 'test']

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
        
        # Should add dtypes for string type inference
        assert "dtypes" in result_kwargs
        assert result_kwargs["dtypes"] == pl.Utf8

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

    def test_s3_file_to_dataframe_mock(self):
        # Test the s3_file_to_dataframe function by mocking S3 response
        import unittest.mock
        from deltacat.aws import s3u as s3_utils
        
        # Create test data for CSV
        csv_content = '"a,b\tc|d",1,1.1\n"e,f\tg|h",2,2.2\ntest,3,3.3\n'
        compressed_content = gzip.compress(csv_content.encode('utf-8'))
        
        # Mock S3 response
        mock_s3_response = {
            'Body': unittest.mock.MagicMock()
        }
        mock_s3_response['Body'].read.return_value = compressed_content
        
        with unittest.mock.patch.object(s3_utils, 'get_object_at_url', return_value=mock_s3_response):
            # Test CSV reading
            result = s3_file_to_dataframe(
                's3://test-bucket/test.csv',
                ContentType.CSV.value,
                ContentEncoding.GZIP.value,
                column_names=['col1', 'col2', 'col3']
            )
            
            assert len(result) == 3
            assert list(result.columns) == ['col1', 'col2', 'col3']
            assert result['col1'].to_list() == ['a,b\tc|d', 'e,f\tg|h', 'test']

    def test_s3_file_to_dataframe_orc_mock(self):
        # Test ORC handling via pandas conversion
        import unittest.mock
        import pandas as pd
        from deltacat.aws import s3u as s3_utils
        
        # Create test ORC data
        test_df = pd.DataFrame({
            'col1': ['test1', 'test2'],
            'col2': [1, 2],
            'col3': [1.1, 2.2]
        })
        
        # Mock S3 response (content doesn't matter since we're mocking pd.read_orc)
        mock_s3_response = {
            'Body': unittest.mock.MagicMock()
        }
        mock_s3_response['Body'].read.return_value = b'fake_orc_content'
        
        # Mock both S3 read and pandas ORC read
        with unittest.mock.patch.object(s3_utils, 'get_object_at_url', return_value=mock_s3_response):
            with unittest.mock.patch('pandas.read_orc', return_value=test_df):
                result = s3_file_to_dataframe(
                    's3://test-bucket/test.orc',
                    ContentType.ORC.value,
                    ContentEncoding.IDENTITY.value
                )
                
                assert len(result) == 2
                assert list(result.columns) == ['col1', 'col2', 'col3']
                assert result['col1'].to_list() == ['test1', 'test2'] 