from ray.data import from_items
from typing import Any
import pytest
import fsspec
from fsspec import AbstractFileSystem
from ray.data.datasource import FilenameProvider
from deltacat.types.media import ContentType
import ray
import gzip
import json


class TestDatasetToFile:

    BASE_PATH = "/tmp"
    SUB_PATH = "abcd"

    @pytest.fixture(autouse=True, scope="module")
    def ensure_ray_down(self):
        # ray.data fails when ray is instantiated in local mode
        ray.shutdown()

    @pytest.fixture(scope="module")
    def mock_dataset(self):
        # Include data that would need escaping to test quoting behavior
        return from_items([{"col1": "a,b\tc|d", "col2": 0} for _ in range(5)])

    @pytest.fixture(scope="module")
    def mock_unescaped_dataset(self):
        # Use data without delimiters for unescaped TSV test
        return from_items([{"col1": "abc", "col2": 0} for _ in range(5)])

    @pytest.fixture(scope="module")
    def mock_filename_provider(self):
        class MockFilenameProvider(FilenameProvider):
            def get_filename_for_block(
                self, block: Any, task_index: int, block_index: int
            ) -> str:
                return TestDatasetToFile.SUB_PATH

        return MockFilenameProvider()

    def test_parquet_sanity(self, mock_dataset, mock_filename_provider):
        from deltacat.utils.ray_utils.dataset import dataset_to_file

        fs: AbstractFileSystem = fsspec.filesystem("file")

        dataset_to_file(
            mock_dataset,
            self.BASE_PATH,
            filesystem=fs,
            block_path_provider=mock_filename_provider,
        )

        file_expected_at = f"{self.BASE_PATH}/{self.SUB_PATH}"
        assert fs.exists(file_expected_at), "file was not written"
        fs.delete(file_expected_at)

    def test_csv_sanity(self, mock_dataset, mock_filename_provider):
        from deltacat.utils.ray_utils.dataset import dataset_to_file

        fs: AbstractFileSystem = fsspec.filesystem("file")

        dataset_to_file(
            mock_dataset,
            self.BASE_PATH,
            filesystem=fs,
            block_path_provider=mock_filename_provider,
            content_type=ContentType.CSV.value,
        )

        file_expected_at = f"{self.BASE_PATH}/{self.SUB_PATH}"
        assert fs.exists(file_expected_at), "file was not written"

        # Verify CSV format and content
        with fs.open(file_expected_at, "rb") as f:
            with gzip.GzipFile(fileobj=f) as gz:
                content = gz.read().decode("utf-8")
                # Should be quoted due to commas in data
                assert '"a,b\tc|d",0' in content

        fs.delete(file_expected_at)

    def test_tsv_sanity(self, mock_dataset, mock_filename_provider):
        from deltacat.utils.ray_utils.dataset import dataset_to_file

        fs: AbstractFileSystem = fsspec.filesystem("file")

        dataset_to_file(
            mock_dataset,
            self.BASE_PATH,
            filesystem=fs,
            block_path_provider=mock_filename_provider,
            content_type=ContentType.TSV.value,
        )

        file_expected_at = f"{self.BASE_PATH}/{self.SUB_PATH}"
        assert fs.exists(file_expected_at), "file was not written"

        # Verify TSV format and content
        with fs.open(file_expected_at, "rb") as f:
            with gzip.GzipFile(fileobj=f) as gz:
                content = gz.read().decode("utf-8")
                # Should be quoted due to tabs in data
                assert '"a,b\tc|d"\t0' in content

        fs.delete(file_expected_at)

    def test_psv_sanity(self, mock_dataset, mock_filename_provider):
        from deltacat.utils.ray_utils.dataset import dataset_to_file

        fs: AbstractFileSystem = fsspec.filesystem("file")

        dataset_to_file(
            mock_dataset,
            self.BASE_PATH,
            filesystem=fs,
            block_path_provider=mock_filename_provider,
            content_type=ContentType.PSV.value,
        )

        file_expected_at = f"{self.BASE_PATH}/{self.SUB_PATH}"
        assert fs.exists(file_expected_at), "file was not written"

        # Verify PSV format and content
        with fs.open(file_expected_at, "rb") as f:
            with gzip.GzipFile(fileobj=f) as gz:
                content = gz.read().decode("utf-8")
                # Should be quoted due to pipes in data
                assert '"a,b\tc|d"|0' in content

        fs.delete(file_expected_at)

    def test_unescaped_tsv_sanity(self, mock_unescaped_dataset, mock_filename_provider):
        from deltacat.utils.ray_utils.dataset import dataset_to_file

        fs: AbstractFileSystem = fsspec.filesystem("file")

        dataset_to_file(
            mock_unescaped_dataset,
            self.BASE_PATH,
            filesystem=fs,
            block_path_provider=mock_filename_provider,
            content_type=ContentType.UNESCAPED_TSV.value,
        )

        file_expected_at = f"{self.BASE_PATH}/{self.SUB_PATH}"
        assert fs.exists(file_expected_at), "file was not written"

        # Verify UNESCAPED_TSV format and content
        with fs.open(file_expected_at, "rb") as f:
            with gzip.GzipFile(fileobj=f) as gz:
                content = gz.read().decode("utf-8")
                # Should NOT be quoted since data has no delimiters
                assert "abc\t0" in content

        fs.delete(file_expected_at)

    def test_json_sanity(self, mock_dataset, mock_filename_provider):
        from deltacat.utils.ray_utils.dataset import dataset_to_file

        fs: AbstractFileSystem = fsspec.filesystem("file")

        dataset_to_file(
            mock_dataset,
            self.BASE_PATH,
            filesystem=fs,
            block_path_provider=mock_filename_provider,
            content_type=ContentType.JSON.value,
        )

        file_expected_at = f"{self.BASE_PATH}/{self.SUB_PATH}"
        assert fs.exists(file_expected_at), "file was not written"

        # Verify JSON format and content
        with fs.open(file_expected_at, "rb") as f:
            with gzip.GzipFile(fileobj=f) as gz:
                content = gz.read().decode("utf-8")
                # Each line should be a valid JSON object
                first_line = content.split("\n")[0]
                record = json.loads(first_line)
                assert record == {"col1": "a,b\tc|d", "col2": 0}

        fs.delete(file_expected_at)
