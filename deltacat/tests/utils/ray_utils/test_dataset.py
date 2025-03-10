from ray.data import from_items
from typing import Any
import pytest
import fsspec
from fsspec import AbstractFileSystem
from ray.data.datasource import FilenameProvider
from deltacat.types.media import ContentType
import ray


class TestDatasetToFile:

    BASE_PATH = "/tmp"
    SUB_PATH = "abcd"

    @pytest.fixture(autouse=True, scope="module")
    def ensure_ray_down(self):
        # ray.data fails when ray is instantiated in local mode
        ray.shutdown()

    @pytest.fixture(scope="module")
    def mock_dataset(self):
        return from_items([{"col1": i, "col2": i * 2} for i in range(1000)])

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

        fs: AbstractFileSystem = fsspec.filesystem("local")

        dataset_to_file(
            mock_dataset,
            self.BASE_PATH,
            file_system=fs,
            block_path_provider=mock_filename_provider,
        )

        file_expected_at = f"{self.BASE_PATH}/{self.SUB_PATH}"
        assert fs.exists(file_expected_at), "file was not written"
        fs.delete(file_expected_at)

    def test_csv_sanity(self, mock_dataset, mock_filename_provider):
        from deltacat.utils.ray_utils.dataset import dataset_to_file

        fs: AbstractFileSystem = fsspec.filesystem("local")

        dataset_to_file(
            mock_dataset,
            self.BASE_PATH,
            file_system=fs,
            block_path_provider=mock_filename_provider,
            content_type=ContentType.CSV.value,
        )

        file_expected_at = f"{self.BASE_PATH}/{self.SUB_PATH}"
        assert fs.exists(file_expected_at), "file was not written"
        fs.delete(file_expected_at)
