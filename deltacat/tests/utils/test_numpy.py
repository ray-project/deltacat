from unittest import TestCase
import numpy as np
import tempfile
import fsspec
import gzip
import polars as pl
from deltacat.types.media import ContentType, ContentEncoding
from deltacat.utils.numpy import (
    file_to_ndarray,
    slice_ndarray,
    ndarray_size,
    ndarray_to_file,
)
from deltacat.utils.pandas import ReadKwargsProviderPandasCsvPureUtf8


class TestNumpyReaders(TestCase):
    def setUp(self):
        # Create test data files for reading
        self.fs = fsspec.filesystem("file")
        self.base_path = tempfile.mkdtemp()
        self.fs.makedirs(self.base_path, exist_ok=True)

        # Create test data as 2D array (3 rows, 3 columns)
        self.expected_data = np.array(
            [["a,b\tc|d", "1", "1.1"], ["e,f\tg|h", "2", "2.2"], ["test", "3", "3.3"]]
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
        with self.fs.open(unescaped_tsv_path, "wb") as f:
            with gzip.GzipFile(fileobj=f, mode="wb") as gz:
                content = "abc\t1\t1.1\ndef\t2\t2.2\nghi\t3\t3.3\n"
                gz.write(content.encode("utf-8"))

        # Create Parquet file
        parquet_path = f"{self.base_path}/test.parquet"
        import pandas as pd

        df = pd.DataFrame(
            {
                "col1": ["a,b\tc|d", "e,f\tg|h", "test"],
                "col2": [1, 2, 3],
                "col3": [1.1, 2.2, 3.3],
            }
        )
        df.to_parquet(parquet_path, index=False)

        # Create Feather file
        feather_path = f"{self.base_path}/test.feather"
        df.to_feather(feather_path)

        # Create JSON file (GZIP compressed, NDJSON format)
        json_path = f"{self.base_path}/test.json"
        with self.fs.open(json_path, "wb") as f:
            with gzip.GzipFile(fileobj=f, mode="wb") as gz:
                json_str = df.to_json(orient="records", lines=True)
                gz.write(json_str.encode("utf-8"))

        # Create Avro file using polars
        avro_path = f"{self.base_path}/test.avro"
        pl_df = pl.from_pandas(df)
        pl_df.write_avro(avro_path)

        # Create ORC file
        orc_path = f"{self.base_path}/test.orc"
        df.to_orc(orc_path, index=False)

    def test_file_to_ndarray_csv(self):
        # Test reading CSV with file_to_ndarray
        csv_path = f"{self.base_path}/test.csv"

        result = file_to_ndarray(
            csv_path,
            ContentType.CSV.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs,
            column_names=["col1", "col2", "col3"],
        )

        assert result.shape == (3, 3)
        assert result[0, 0] == "a,b\tc|d"
        assert result[1, 0] == "e,f\tg|h"
        assert result[2, 0] == "test"

    def test_file_to_ndarray_tsv(self):
        # Test reading TSV with file_to_ndarray
        tsv_path = f"{self.base_path}/test.tsv"

        result = file_to_ndarray(
            tsv_path,
            ContentType.TSV.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs,
            column_names=["col1", "col2", "col3"],
        )

        assert result.shape == (3, 3)
        assert result[0, 0] == "a,b\tc|d"
        assert result[1, 0] == "e,f\tg|h"
        assert result[2, 0] == "test"

    def test_file_to_ndarray_psv(self):
        # Test reading PSV with file_to_ndarray
        psv_path = f"{self.base_path}/test.psv"

        result = file_to_ndarray(
            psv_path,
            ContentType.PSV.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs,
            column_names=["col1", "col2", "col3"],
        )

        assert result.shape == (3, 3)
        assert result[0, 0] == "a,b\tc|d"
        assert result[1, 0] == "e,f\tg|h"
        assert result[2, 0] == "test"

    def test_file_to_ndarray_unescaped_tsv(self):
        # Test reading unescaped TSV with file_to_ndarray
        unescaped_tsv_path = f"{self.base_path}/test_unescaped.tsv"

        result = file_to_ndarray(
            unescaped_tsv_path,
            ContentType.UNESCAPED_TSV.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs,
            column_names=["col1", "col2", "col3"],
        )

        assert result.shape == (3, 3)
        assert result[0, 0] == "abc"
        assert result[1, 0] == "def"
        assert result[2, 0] == "ghi"

    def test_file_to_ndarray_parquet(self):
        # Test reading Parquet with file_to_ndarray
        parquet_path = f"{self.base_path}/test.parquet"

        result = file_to_ndarray(
            parquet_path, ContentType.PARQUET.value, filesystem=self.fs
        )

        assert result.shape == (3, 3)
        assert result[0, 0] == "a,b\tc|d"
        assert result[1, 0] == "e,f\tg|h"
        assert result[2, 0] == "test"

    def test_file_to_ndarray_feather(self):
        # Test reading Feather with file_to_ndarray
        feather_path = f"{self.base_path}/test.feather"

        result = file_to_ndarray(
            feather_path, ContentType.FEATHER.value, filesystem=self.fs
        )

        assert result.shape == (3, 3)
        assert result[0, 0] == "a,b\tc|d"
        assert result[1, 0] == "e,f\tg|h"
        assert result[2, 0] == "test"

    def test_file_to_ndarray_json(self):
        # Test reading JSON with file_to_ndarray
        json_path = f"{self.base_path}/test.json"

        result = file_to_ndarray(
            json_path,
            ContentType.JSON.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs,
        )

        assert result.shape == (3, 3)
        # Note: JSON column order might differ, so check by value presence
        assert "a,b\tc|d" in result.flatten()
        assert "e,f\tg|h" in result.flatten()
        assert "test" in result.flatten()

    def test_file_to_ndarray_avro(self):
        # Test reading Avro with file_to_ndarray
        avro_path = f"{self.base_path}/test.avro"

        result = file_to_ndarray(avro_path, ContentType.AVRO.value, filesystem=self.fs)

        assert result.shape == (3, 3)
        assert result[0, 0] == "a,b\tc|d"
        assert result[1, 0] == "e,f\tg|h"
        assert result[2, 0] == "test"

    def test_file_to_ndarray_orc(self):
        # Test reading ORC with file_to_ndarray
        orc_path = f"{self.base_path}/test.orc"

        result = file_to_ndarray(orc_path, ContentType.ORC.value, filesystem=self.fs)

        assert result.shape == (3, 3)
        assert result[0, 0] == "a,b\tc|d"
        assert result[1, 0] == "e,f\tg|h"
        assert result[2, 0] == "test"

    def test_file_to_ndarray_with_column_selection(self):
        # Test reading with column selection
        csv_path = f"{self.base_path}/test.csv"

        result = file_to_ndarray(
            csv_path,
            ContentType.CSV.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs,
            column_names=["col1", "col2", "col3"],
            include_columns=["col1", "col2"],
        )

        assert result.shape == (3, 2)  # Should only have 2 columns
        assert result[0, 0] == "a,b\tc|d"
        assert result[1, 0] == "e,f\tg|h"
        assert result[2, 0] == "test"

    def test_file_to_ndarray_with_kwargs_provider(self):
        # Test reading with kwargs provider (forces string types)
        csv_path = f"{self.base_path}/test.csv"
        provider = ReadKwargsProviderPandasCsvPureUtf8(
            include_columns=["col1", "col2", "col3"]
        )

        result = file_to_ndarray(
            csv_path,
            ContentType.CSV.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fs,
            column_names=["col1", "col2", "col3"],
            pd_read_func_kwargs_provider=provider,
        )

        assert result.shape == (3, 3)
        assert result[0, 0] == "a,b\tc|d"
        # With string types provider, numbers should also be strings
        assert result[0, 1] == "1"
        assert result[0, 2] == "1.1"

    def test_file_to_ndarray_filesystem_inference(self):
        # Test filesystem inference when no filesystem is provided
        json_path = f"{self.base_path}/test.json"

        result = file_to_ndarray(
            json_path,
            ContentType.JSON.value,
            ContentEncoding.GZIP.value
            # No filesystem provided - should be inferred
        )

        assert result.shape == (3, 3)
        # JSON might have different column ordering
        assert "a,b\tc|d" in result.flatten()
        assert "e,f\tg|h" in result.flatten()
        assert "test" in result.flatten()

    def test_file_to_ndarray_bzip2_compression(self):
        # Test BZIP2 compression handling
        import bz2

        # Create a BZIP2 compressed CSV file
        csv_content = '"a,b\tc|d",1,1.1\n"e,f\tg|h",2,2.2\ntest,3,3.3\n'
        compressed_content = bz2.compress(csv_content.encode("utf-8"))

        bz2_path = f"{self.base_path}/test.csv.bz2"
        with self.fs.open(bz2_path, "wb") as f:
            f.write(compressed_content)

        result = file_to_ndarray(
            bz2_path,
            ContentType.CSV.value,
            ContentEncoding.BZIP2.value,
            filesystem=self.fs,
            column_names=["col1", "col2", "col3"],
        )

        assert result.shape == (3, 3)
        assert result[0, 0] == "a,b\tc|d"
        assert result[1, 0] == "e,f\tg|h"
        assert result[2, 0] == "test"

    def test_slice_ndarray(self):
        # Test slicing functionality
        arr = np.arange(10).reshape(10, 1)

        # Test without max_len (should return original array)
        result = slice_ndarray(arr, None)
        assert len(result) == 1
        np.testing.assert_array_equal(result[0], arr)

        # Test with max_len
        result = slice_ndarray(arr, 3)
        assert len(result) == 4  # 10 rows / 3 = 3 full slices + 1 remainder
        assert result[0].shape == (3, 1)
        assert result[1].shape == (3, 1)
        assert result[2].shape == (3, 1)
        assert result[3].shape == (1, 1)  # remainder

        # Verify data integrity
        np.testing.assert_array_equal(result[0], arr[:3])
        np.testing.assert_array_equal(result[1], arr[3:6])
        np.testing.assert_array_equal(result[2], arr[6:9])
        np.testing.assert_array_equal(result[3], arr[9:])

    def test_ndarray_size(self):
        # Test size calculation
        arr = np.array([[1, 2, 3], [4, 5, 6]], dtype=np.float64)
        size = ndarray_size(arr)
        expected_size = arr.nbytes
        assert size == expected_size

    def test_ndarray_to_file(self):
        # Test writing ndarray to file
        arr = np.array([1, 2, 3, 4, 5])
        path = f"{self.base_path}/test_output.parquet"

        ndarray_to_file(
            arr, path, self.fs, lambda x: path, content_type=ContentType.PARQUET.value
        )

        assert self.fs.exists(path), "file was not written"

        # Verify we can read it back (though this tests the write functionality)
        import pandas as pd

        result_df = pd.read_parquet(path)
        assert len(result_df) == 5
        assert "0" in result_df.columns

    def test_ndarray_to_file_different_content_types(self):
        # Test writing ndarray to different file formats
        arr = np.array([1, 2, 3, 4, 5])

        # Test Parquet
        parquet_path = f"{self.base_path}/test_output.parquet"
        ndarray_to_file(
            arr,
            parquet_path,
            self.fs,
            lambda x: parquet_path,
            content_type=ContentType.PARQUET.value,
        )
        assert self.fs.exists(parquet_path)

        # Test Feather
        feather_path = f"{self.base_path}/test_output.feather"
        ndarray_to_file(
            arr,
            feather_path,
            self.fs,
            lambda x: feather_path,
            content_type=ContentType.FEATHER.value,
        )
        assert self.fs.exists(feather_path)

        # Test CSV (compressed)
        csv_path = f"{self.base_path}/test_output.csv"
        ndarray_to_file(
            arr,
            csv_path,
            self.fs,
            lambda x: csv_path,
            content_type=ContentType.CSV.value,
        )
        assert self.fs.exists(csv_path)

        # Test JSON (compressed)
        json_path = f"{self.base_path}/test_output.json"
        ndarray_to_file(
            arr,
            json_path,
            self.fs,
            lambda x: json_path,
            content_type=ContentType.JSON.value,
        )
        assert self.fs.exists(json_path)

    def test_ndarray_to_file_different_dtypes(self):
        # Test writing arrays with different data types

        # Integer array
        int_arr = np.array([1, 2, 3, 4, 5], dtype=np.int64)
        int_path = f"{self.base_path}/test_int.parquet"
        ndarray_to_file(
            int_arr,
            int_path,
            self.fs,
            lambda x: int_path,
            content_type=ContentType.PARQUET.value,
        )
        assert self.fs.exists(int_path)

        # Float array
        float_arr = np.array([1.1, 2.2, 3.3, 4.4, 5.5], dtype=np.float64)
        float_path = f"{self.base_path}/test_float.parquet"
        ndarray_to_file(
            float_arr,
            float_path,
            self.fs,
            lambda x: float_path,
            content_type=ContentType.PARQUET.value,
        )
        assert self.fs.exists(float_path)

        # String array (object dtype)
        str_arr = np.array(["a", "b", "c", "d", "e"], dtype=object)
        str_path = f"{self.base_path}/test_str.parquet"
        ndarray_to_file(
            str_arr,
            str_path,
            self.fs,
            lambda x: str_path,
            content_type=ContentType.PARQUET.value,
        )
        assert self.fs.exists(str_path)

    def test_ndarray_to_file_2d_array(self):
        # Test writing 2D arrays
        arr_2d = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
        path = f"{self.base_path}/test_2d.parquet"

        ndarray_to_file(
            arr_2d,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.PARQUET.value,
        )

        assert self.fs.exists(path)

        # Verify the file structure
        import pandas as pd

        result_df = pd.read_parquet(path)
        assert len(result_df) == 3  # 3 rows (first dimension)
        # 2D array should have columns "0", "1", "2"
        assert list(result_df.columns) == ["0", "1", "2"]
        # Verify the data values are correct (convert back to numpy for comparison)
        result_array = result_df.to_numpy()
        np.testing.assert_array_equal(result_array, arr_2d)

    def test_ndarray_to_file_empty_array(self):
        # Test writing empty arrays
        empty_arr = np.array([])
        path = f"{self.base_path}/test_empty.parquet"

        ndarray_to_file(
            empty_arr,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.PARQUET.value,
        )

        assert self.fs.exists(path)

        # Verify the file structure
        import pandas as pd

        result_df = pd.read_parquet(path)
        assert len(result_df) == 0  # Empty DataFrame
        assert "0" in result_df.columns

    def test_ndarray_to_file_large_array(self):
        # Test writing larger arrays
        large_arr = np.arange(1000)
        path = f"{self.base_path}/test_large.parquet"

        ndarray_to_file(
            large_arr,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.PARQUET.value,
        )

        assert self.fs.exists(path)

        # Verify the file can be read and has correct size
        import pandas as pd

        result_df = pd.read_parquet(path)
        assert len(result_df) == 1000
        assert "0" in result_df.columns

    def test_ndarray_to_file_with_custom_kwargs(self):
        # Test writing with custom kwargs
        arr = np.array([1, 2, 3, 4, 5])
        path = f"{self.base_path}/test_kwargs.parquet"

        # Add some custom write kwargs (these will be passed to PyArrow)
        ndarray_to_file(
            arr,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.PARQUET.value,
            compression="snappy",  # Custom compression
        )

        assert self.fs.exists(path)

        # Verify the file was written (basic check)
        import pandas as pd

        result_df = pd.read_parquet(path)
        assert len(result_df) == 5
        assert "0" in result_df.columns

    def test_ndarray_to_file_readback_verification(self):
        # Test that we can read back the exact data we wrote
        original_arr = np.array([1.1, 2.2, 3.3, 4.4, 5.5])
        path = f"{self.base_path}/test_readback.parquet"

        # Write the array
        ndarray_to_file(
            original_arr,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.PARQUET.value,
        )

        # Read it back using pandas and verify content
        import pandas as pd

        result_df = pd.read_parquet(path)
        readback_arr = np.array(result_df["0"].tolist())

        # Check that the data matches
        np.testing.assert_array_almost_equal(original_arr, readback_arr)

    def test_ndarray_to_file_different_filesystems(self):
        # Test with different filesystem implementations
        arr = np.array([1, 2, 3, 4, 5])

        # Test with fsspec filesystem (already used in other tests)
        fsspec_path = f"{self.base_path}/test_fsspec.parquet"
        ndarray_to_file(
            arr,
            fsspec_path,
            self.fs,
            lambda x: fsspec_path,
            content_type=ContentType.PARQUET.value,
        )
        assert self.fs.exists(fsspec_path)

        # Test with None filesystem (should infer local filesystem)
        local_path = f"{self.base_path}/test_local.parquet"
        ndarray_to_file(
            arr,
            local_path,
            None,  # No filesystem specified
            lambda x: local_path,
            content_type=ContentType.PARQUET.value,
        )
        # Check if file exists using the fsspec filesystem
        assert self.fs.exists(local_path)

    def test_ndarray_to_file_boolean_array(self):
        # Test writing boolean arrays
        bool_arr = np.array([True, False, True, False, True])
        path = f"{self.base_path}/test_bool.parquet"

        ndarray_to_file(
            bool_arr,
            path,
            self.fs,
            lambda x: path,
            content_type=ContentType.PARQUET.value,
        )

        assert self.fs.exists(path)

        # Verify the file structure and content
        import pandas as pd

        result_df = pd.read_parquet(path)
        assert len(result_df) == 5
        assert "0" in result_df.columns

        # Check that boolean values are preserved
        readback_arr = np.array(result_df["0"].tolist())
        np.testing.assert_array_equal(bool_arr, readback_arr)

    def test_ndarray_to_file_complex_dtypes(self):
        # Test writing arrays with complex dtypes
        complex_arr = np.array([1 + 2j, 3 + 4j, 5 + 6j])
        path = f"{self.base_path}/test_complex.parquet"

        # Note: Complex numbers might not be directly supported by all formats
        # This test may need to handle conversion or errors gracefully
        try:
            ndarray_to_file(
                complex_arr,
                path,
                self.fs,
                lambda x: path,
                content_type=ContentType.PARQUET.value,
            )
            assert self.fs.exists(path)
        except (TypeError, ValueError, NotImplementedError):
            # Complex dtypes might not be supported by PyArrow/Parquet
            # This is acceptable behavior
            pass


class TestNumpyFileSystemSupport(TestCase):
    """
    Comprehensive tests for numpy file operations with different filesystem types.
    Tests fsspec AbstractFileSystem, PyArrow FileSystem, and auto-inferred filesystem.
    """

    def setUp(self):
        import pyarrow.fs as pafs

        # Create test data as numpy array
        # All formats preserve mixed types when converted to numpy, so use object dtype for all
        self.test_data = np.array(
            [["value1", 1, 1.1], ["value2", 2, 2.2], ["value3", 3, 3.3]], dtype=object
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
        import pandas as pd

        # Create pandas DataFrame for file creation
        df = pd.DataFrame(
            {
                "col1": ["value1", "value2", "value3"],
                "col2": [1, 2, 3],
                "col3": [1.1, 2.2, 3.3],
            }
        )

        # CSV files without headers to match test data structure
        csv_data = "value1,1,1.1\nvalue2,2,2.2\nvalue3,3,3.3\n"

        # Create uncompressed CSV
        with open(f"{self.temp_dir}/test.csv", "w") as f:
            f.write(csv_data)

        # Create GZIP compressed CSV
        with gzip.open(f"{self.temp_dir}/test_gzip.csv.gz", "wt") as f:
            f.write(csv_data)

        # Create BZIP2 compressed CSV
        with bz2.open(f"{self.temp_dir}/test_bzip2.csv.bz2", "wt") as f:
            f.write(csv_data)

        # Parquet file
        df.to_parquet(f"{self.temp_dir}/test.parquet", index=False)

        # Feather file
        df.to_feather(f"{self.temp_dir}/test.feather")

        # JSON file (NDJSON format)
        json_str = df.to_json(orient="records", lines=True)
        with open(f"{self.temp_dir}/test.json", "w") as f:
            f.write(json_str)

        # AVRO file (using polars since pandas delegates to polars for AVRO)
        import polars as pl

        pl_df = pl.from_pandas(df)
        pl_df.write_avro(f"{self.temp_dir}/test.avro")

        # ORC file
        df.to_orc(f"{self.temp_dir}/test.orc")

    def _assert_arrays_equal(self, result, expected):
        """Helper to assert numpy arrays are equal."""
        assert (
            result.shape == expected.shape
        ), f"Shape mismatch: {result.shape} vs {expected.shape}"
        np.testing.assert_array_equal(result, expected)

    def test_csv_with_fsspec_filesystem(self):
        """Test CSV reading with fsspec AbstractFileSystem."""
        # Test uncompressed CSV
        result = file_to_ndarray(
            f"{self.temp_dir}/test.csv",
            ContentType.CSV.value,
            ContentEncoding.IDENTITY.value,
            filesystem=self.fsspec_fs,
            column_names=["col1", "col2", "col3"],
        )
        self._assert_arrays_equal(result, self.test_data)

        # Test GZIP compressed CSV
        result = file_to_ndarray(
            f"{self.temp_dir}/test_gzip.csv.gz",
            ContentType.CSV.value,
            ContentEncoding.GZIP.value,
            filesystem=self.fsspec_fs,
            column_names=["col1", "col2", "col3"],
        )
        self._assert_arrays_equal(result, self.test_data)

        # Test BZIP2 compressed CSV
        result = file_to_ndarray(
            f"{self.temp_dir}/test_bzip2.csv.bz2",
            ContentType.CSV.value,
            ContentEncoding.BZIP2.value,
            filesystem=self.fsspec_fs,
            column_names=["col1", "col2", "col3"],
        )
        self._assert_arrays_equal(result, self.test_data)

    def test_csv_with_pyarrow_filesystem(self):
        """Test CSV reading with PyArrow FileSystem."""
        # Test uncompressed CSV
        result = file_to_ndarray(
            f"{self.temp_dir}/test.csv",
            ContentType.CSV.value,
            ContentEncoding.IDENTITY.value,
            filesystem=self.pyarrow_fs,
            column_names=["col1", "col2", "col3"],
        )
        self._assert_arrays_equal(result, self.test_data)

        # Test GZIP compressed CSV
        result = file_to_ndarray(
            f"{self.temp_dir}/test_gzip.csv.gz",
            ContentType.CSV.value,
            ContentEncoding.GZIP.value,
            filesystem=self.pyarrow_fs,
            column_names=["col1", "col2", "col3"],
        )
        self._assert_arrays_equal(result, self.test_data)

    def test_csv_with_auto_inferred_filesystem(self):
        """Test CSV reading with automatically inferred filesystem."""
        # Test uncompressed CSV (filesystem=None, should auto-infer)
        result = file_to_ndarray(
            f"{self.temp_dir}/test.csv",
            ContentType.CSV.value,
            ContentEncoding.IDENTITY.value,
            filesystem=None,
            column_names=["col1", "col2", "col3"],
        )
        self._assert_arrays_equal(result, self.test_data)

    def test_parquet_with_different_filesystems(self):
        """Test Parquet reading with different filesystem types."""
        # Test with fsspec
        result = file_to_ndarray(
            f"{self.temp_dir}/test.parquet",
            ContentType.PARQUET.value,
            ContentEncoding.IDENTITY.value,
            filesystem=self.fsspec_fs,
        )
        self._assert_arrays_equal(result, self.test_data)

        # Test with PyArrow
        result = file_to_ndarray(
            f"{self.temp_dir}/test.parquet",
            ContentType.PARQUET.value,
            ContentEncoding.IDENTITY.value,
            filesystem=self.pyarrow_fs,
        )
        self._assert_arrays_equal(result, self.test_data)

        # Test with auto-inferred
        result = file_to_ndarray(
            f"{self.temp_dir}/test.parquet",
            ContentType.PARQUET.value,
            ContentEncoding.IDENTITY.value,
            filesystem=None,
        )
        self._assert_arrays_equal(result, self.test_data)

    def test_feather_with_different_filesystems(self):
        """Test Feather reading with different filesystem types."""
        # Test with fsspec
        result = file_to_ndarray(
            f"{self.temp_dir}/test.feather",
            ContentType.FEATHER.value,
            ContentEncoding.IDENTITY.value,
            filesystem=self.fsspec_fs,
        )
        self._assert_arrays_equal(result, self.test_data)

        # Test with PyArrow
        result = file_to_ndarray(
            f"{self.temp_dir}/test.feather",
            ContentType.FEATHER.value,
            ContentEncoding.IDENTITY.value,
            filesystem=self.pyarrow_fs,
        )
        self._assert_arrays_equal(result, self.test_data)

        # Test with auto-inferred
        result = file_to_ndarray(
            f"{self.temp_dir}/test.feather",
            ContentType.FEATHER.value,
            ContentEncoding.IDENTITY.value,
            filesystem=None,
        )
        self._assert_arrays_equal(result, self.test_data)

    def test_json_with_different_filesystems(self):
        """Test JSON reading with different filesystem types."""
        # Test with fsspec
        result = file_to_ndarray(
            f"{self.temp_dir}/test.json",
            ContentType.JSON.value,
            ContentEncoding.IDENTITY.value,
            filesystem=self.fsspec_fs,
        )
        self._assert_arrays_equal(result, self.test_data)

        # Test with PyArrow
        result = file_to_ndarray(
            f"{self.temp_dir}/test.json",
            ContentType.JSON.value,
            ContentEncoding.IDENTITY.value,
            filesystem=self.pyarrow_fs,
        )
        self._assert_arrays_equal(result, self.test_data)

        # Test with auto-inferred
        result = file_to_ndarray(
            f"{self.temp_dir}/test.json",
            ContentType.JSON.value,
            ContentEncoding.IDENTITY.value,
            filesystem=None,
        )
        self._assert_arrays_equal(result, self.test_data)

    def test_avro_with_different_filesystems(self):
        """Test AVRO reading with different filesystem types."""
        # Test with fsspec
        result = file_to_ndarray(
            f"{self.temp_dir}/test.avro",
            ContentType.AVRO.value,
            ContentEncoding.IDENTITY.value,
            filesystem=self.fsspec_fs,
        )
        self._assert_arrays_equal(result, self.test_data)

        # Test with PyArrow
        result = file_to_ndarray(
            f"{self.temp_dir}/test.avro",
            ContentType.AVRO.value,
            ContentEncoding.IDENTITY.value,
            filesystem=self.pyarrow_fs,
        )
        self._assert_arrays_equal(result, self.test_data)

        # Test with auto-inferred
        result = file_to_ndarray(
            f"{self.temp_dir}/test.avro",
            ContentType.AVRO.value,
            ContentEncoding.IDENTITY.value,
            filesystem=None,
        )
        self._assert_arrays_equal(result, self.test_data)

    def test_orc_with_different_filesystems(self):
        """Test ORC reading with different filesystem types."""
        # Test with fsspec
        result = file_to_ndarray(
            f"{self.temp_dir}/test.orc",
            ContentType.ORC.value,
            ContentEncoding.IDENTITY.value,
            filesystem=self.fsspec_fs,
        )
        self._assert_arrays_equal(result, self.test_data)

        # Test with PyArrow
        result = file_to_ndarray(
            f"{self.temp_dir}/test.orc",
            ContentType.ORC.value,
            ContentEncoding.IDENTITY.value,
            filesystem=self.pyarrow_fs,
        )
        self._assert_arrays_equal(result, self.test_data)

        # Test with auto-inferred
        result = file_to_ndarray(
            f"{self.temp_dir}/test.orc",
            ContentType.ORC.value,
            ContentEncoding.IDENTITY.value,
            filesystem=None,
        )
        self._assert_arrays_equal(result, self.test_data)

    def test_file_to_ndarray_with_different_filesystems(self):
        """Test file_to_ndarray with different filesystem types for all content types."""
        test_cases = [
            (
                f"{self.temp_dir}/test.csv",
                ContentType.CSV.value,
                ContentEncoding.IDENTITY.value,
                {"column_names": ["col1", "col2", "col3"]},
                self.test_data,
            ),
            (
                f"{self.temp_dir}/test_gzip.csv.gz",
                ContentType.CSV.value,
                ContentEncoding.GZIP.value,
                {"column_names": ["col1", "col2", "col3"]},
                self.test_data,
            ),
            (
                f"{self.temp_dir}/test.parquet",
                ContentType.PARQUET.value,
                ContentEncoding.IDENTITY.value,
                {},
                self.test_data,
            ),
            (
                f"{self.temp_dir}/test.feather",
                ContentType.FEATHER.value,
                ContentEncoding.IDENTITY.value,
                {},
                self.test_data,
            ),
            (
                f"{self.temp_dir}/test.json",
                ContentType.JSON.value,
                ContentEncoding.IDENTITY.value,
                {},
                self.test_data,
            ),
            (
                f"{self.temp_dir}/test.avro",
                ContentType.AVRO.value,
                ContentEncoding.IDENTITY.value,
                {},
                self.test_data,
            ),
            (
                f"{self.temp_dir}/test.orc",
                ContentType.ORC.value,
                ContentEncoding.IDENTITY.value,
                {},
                self.test_data,
            ),
        ]

        filesystems = [
            ("fsspec", self.fsspec_fs),
            ("pyarrow", self.pyarrow_fs),
            ("auto-inferred", None),
        ]

        for (
            path,
            content_type,
            content_encoding,
            extra_kwargs,
            expected_data,
        ) in test_cases:
            for fs_name, filesystem in filesystems:
                with self.subTest(
                    content_type=content_type,
                    filesystem=fs_name,
                    encoding=content_encoding,
                ):
                    result = file_to_ndarray(
                        path=path,
                        content_type=content_type,
                        content_encoding=content_encoding,
                        filesystem=filesystem,
                        **extra_kwargs,
                    )
                    self._assert_arrays_equal(result, expected_data)

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
                    result = file_to_ndarray(
                        path=path,
                        content_type=ContentType.CSV.value,
                        content_encoding=content_encoding,
                        filesystem=filesystem,
                        column_names=["col1", "col2", "col3"],
                    )
                    self._assert_arrays_equal(result, self.test_data)

    def test_filesystem_open_kwargs(self):
        """Test that filesystem open kwargs are properly passed through."""
        # Test with custom fs_open_kwargs
        result = file_to_ndarray(
            f"{self.temp_dir}/test.csv",
            ContentType.CSV.value,
            ContentEncoding.IDENTITY.value,
            filesystem=self.fsspec_fs,
            fs_open_kwargs={
                "encoding": "utf-8"
            },  # This should be passed to filesystem.open()
            column_names=["col1", "col2", "col3"],
        )
        self._assert_arrays_equal(result, self.test_data)

    def test_delimited_formats_with_different_filesystems(self):
        """Test delimited formats (TSV, PSV, etc.) with different filesystem types."""
        # Create TSV test file without headers to match test data structure
        tsv_data = "value1\t1\t1.1\nvalue2\t2\t2.2\nvalue3\t3\t3.3\n"
        with open(f"{self.temp_dir}/test.tsv", "w") as f:
            f.write(tsv_data)

        # Create PSV test file without headers to match test data structure
        psv_data = "value1|1|1.1\nvalue2|2|2.2\nvalue3|3|3.3\n"
        with open(f"{self.temp_dir}/test.psv", "w") as f:
            f.write(psv_data)

        delimited_test_cases = [
            (
                f"{self.temp_dir}/test.tsv",
                ContentType.TSV.value,
                {"sep": "\t", "column_names": ["col1", "col2", "col3"]},
            ),
            (
                f"{self.temp_dir}/test.psv",
                ContentType.PSV.value,
                {"sep": "|", "column_names": ["col1", "col2", "col3"]},
            ),
        ]

        filesystems = [
            ("fsspec", self.fsspec_fs),
            ("pyarrow", self.pyarrow_fs),
            ("auto-inferred", None),
        ]

        for path, content_type, extra_kwargs in delimited_test_cases:
            for fs_name, filesystem in filesystems:
                with self.subTest(content_type=content_type, filesystem=fs_name):
                    result = file_to_ndarray(
                        path=path,
                        content_type=content_type,
                        content_encoding=ContentEncoding.IDENTITY.value,
                        filesystem=filesystem,
                        **extra_kwargs,
                    )
                    self._assert_arrays_equal(result, self.test_data)

    def test_numpy_array_conversion_consistency(self):
        """Test that numpy array conversion is consistent across filesystem types."""
        # Test that the same data produces the same numpy array regardless of filesystem
        filesystems = [
            ("fsspec", self.fsspec_fs),
            ("pyarrow", self.pyarrow_fs),
            ("auto-inferred", None),
        ]

        # Use Parquet as it preserves data types well
        parquet_path = f"{self.temp_dir}/test.parquet"

        results = []
        for fs_name, filesystem in filesystems:
            result = file_to_ndarray(
                parquet_path,
                ContentType.PARQUET.value,
                ContentEncoding.IDENTITY.value,
                filesystem=filesystem,
            )
            results.append((fs_name, result))

        # All results should be identical
        reference_result = results[0][1]
        for fs_name, result in results[1:]:
            with self.subTest(filesystem=fs_name):
                self._assert_arrays_equal(result, reference_result)

    def test_dtype_preservation_across_filesystems(self):
        """Test that data types are preserved across different filesystem types."""
        import pandas as pd

        # Create a DataFrame with mixed data types
        df = pd.DataFrame(
            {
                "int_col": [1, 2, 3],
                "float_col": [1.1, 2.2, 3.3],
                "str_col": ["a", "b", "c"],
            }
        )

        # Save as Parquet (preserves types best)
        parquet_path = f"{self.temp_dir}/test_dtypes.parquet"
        df.to_parquet(parquet_path, index=False)

        filesystems = [
            ("fsspec", self.fsspec_fs),
            ("pyarrow", self.pyarrow_fs),
            ("auto-inferred", None),
        ]

        # Test that data types are consistent across filesystems
        dtypes = []
        for fs_name, filesystem in filesystems:
            result = file_to_ndarray(
                parquet_path,
                ContentType.PARQUET.value,
                ContentEncoding.IDENTITY.value,
                filesystem=filesystem,
            )
            dtypes.append((fs_name, result.dtype))

        # All dtypes should be the same (object type for mixed data)
        reference_dtype = dtypes[0][1]
        for fs_name, dtype in dtypes[1:]:
            with self.subTest(filesystem=fs_name):
                assert (
                    dtype == reference_dtype
                ), f"Dtype mismatch for {fs_name}: {dtype} vs {reference_dtype}"

    def test_error_handling_across_filesystems(self):
        """Test that error handling is consistent across filesystem types."""
        filesystems = [
            ("fsspec", self.fsspec_fs),
            ("pyarrow", self.pyarrow_fs),
            ("auto-inferred", None),
        ]

        # Test with non-existent file
        for fs_name, filesystem in filesystems:
            with self.subTest(filesystem=fs_name):
                with self.assertRaises(
                    Exception
                ):  # Should raise some kind of file not found error
                    file_to_ndarray(
                        f"{self.temp_dir}/nonexistent.csv",
                        ContentType.CSV.value,
                        ContentEncoding.IDENTITY.value,
                        filesystem=filesystem,
                        column_names=["col1", "col2", "col3"],
                    )
