from unittest import TestCase
import pandas as pd
import tempfile
import fsspec
import gzip
import json
import polars as pl
from deltacat.types.media import ContentType, ContentEncoding
from deltacat.utils.pandas import dataframe_to_file


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