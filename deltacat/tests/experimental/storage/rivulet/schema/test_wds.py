import os
import pytest
import shutil
import json

import tempfile
from pathlib import Path
import webdataset as wds

from deltacat.experimental.storage.rivulet.dataset import Dataset
from deltacat.experimental.storage.rivulet import Field, Datatype


@pytest.fixture(scope="class")
def temp_dir(tmp_path_factory) -> Path:
    # One directory for the whole class
    return tmp_path_factory.mktemp("rivulet_suite")


def _add_txt_files_and_wds_tar(files, base_dir, name):
    for _, content in files.items():
        rel_path = content["filename"]
        full_path = os.path.join(base_dir, rel_path)

        # Ensure the directory exists
        os.makedirs(os.path.dirname(full_path), exist_ok=True)

        with open(full_path, "w") as f:
            f.write("Test .txt content.")
    # write shard with WebDataset
    shard_path_pattern = str(base_dir / f"{name}-%06d.tar")
    with wds.ShardWriter(shard_path_pattern, maxcount=1000) as sink:
        for i, (json_name, content) in enumerate(files.items()):
            txt_path = os.path.join(base_dir, content["filename"])
            with open(txt_path, "rb") as f:
                txt_bytes = f.read()

            sample = {
                "__key__": f"{i:06d}",  # still needed for WDS, but won't prefix files
                os.path.basename(content["filename"]): txt_bytes,
                json_name: json.dumps(content).encode("utf-8"),
            }
            sink.write(sample)

    return [str(p) for p in Path(base_dir).glob(f"{name}-*.tar")]


@pytest.fixture
def sample_wds_simple(temp_dir: Path):
    """Create a simple WebDataset shard using webdataset.ShardWriter."""
    name = "simple"
    files = {
        f"{name}_first.json": {
            "label": 1,
            "width": 500,
            "height": 429,
            "filename": "n01443537/n01443537_14753.TXT",
            "extra": 101,
        },
        f"{name}_second.json": {
            "label": 2,
            "width": 200,
            "height": 300,
            "filename": "n01443538/n01443538_14754.TXT",
            "extra": 102,
        },
    }

    # create corresponding dummy .txt files on disk
    return _add_txt_files_and_wds_tar(files, temp_dir, name)


@pytest.fixture
def sample_wds_simple_2(temp_dir):
    """Create a simple WebDataset shard using webdataset.ShardWriter."""
    name = "simple_2"
    files = {
        f"{name}_first.json": {
            "label": 1,
            "width": 500,
            "height": 429,
            "filename": "n01443537/n01443537_14753.TXT",
            "extra": 101,
        },
        f"{name}_second.json": {
            "label": 2,
            "width": 200,
            "height": 300,
            "filename": "n01443538/n01443538_14754.TXT",
            "extra": 102,
        },
    }
    # create corresponding dummy .txt files on disk
    return _add_txt_files_and_wds_tar(files, temp_dir, name)


@pytest.fixture
def sample_wds_long(temp_dir):
    name = "long"
    files = {}
    for i in range(6):
        files[f"long_{i}.json"] = {
            "label": i,
            "width": 100 + i * 50,
            "height": 200 + i * 50,
            "filename": f"n0144353{i}/n0144353{i}_1475{i}.TXT",
            "extra": 100 + i,
        }
    return _add_txt_files_and_wds_tar(files, temp_dir, name)


@pytest.fixture
def sample_wds_inconsistent(temp_dir):
    name = "inconsistent"
    files = {
        f"{name}_first.json": {
            "label": 1,
            "width": 500,
            "height": 429,
            "filename": "n01443537/n01443537_14753.TXT",
            "extra": 101,
        },
        f"{name}_second.json": {
            "label": 2,
            "width": 200,
            "height": 300,
            "filename": "n01443538/n01443538_14754.TXT",
        },
    }
    return _add_txt_files_and_wds_tar(files, temp_dir, name)


class TestFromWebDataset:
    @classmethod
    def setup_class(cls):
        cls.temp_dir = tempfile.mkdtemp()

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.temp_dir)

    def test_consistent_schema_handling(self, temp_dir, sample_wds_simple):
        """Test that from_webdataset correctly creates a dataset from a WebDataset with consistent JSON schemas and one merge key."""
        dataset = Dataset.from_webdataset(
            name="test_dataset",
            file_uri=sample_wds_simple,
            metadata_uri=temp_dir,
            merge_keys="filename",
        )

        # Verify schema fields
        assert len(dataset.fields) == 6
        assert "label" in dataset.fields
        assert "width" in dataset.fields
        assert "height" in dataset.fields
        assert "filename" in dataset.fields
        assert "extra" in dataset.fields
        assert "media_binary" in dataset.fields

        assert dataset.fields["filename"].is_merge_key

        # Verify datatypes inferred correctly
        assert dataset.fields["label"].datatype == Datatype.int64()
        assert dataset.fields["width"].datatype == Datatype.int64()
        assert dataset.fields["height"].datatype == Datatype.int64()
        assert dataset.fields["filename"].datatype == Datatype.string()
        assert dataset.fields["extra"].datatype == Datatype.int64()

        # Verify data can be read
        records = list(dataset.scan().to_pydict())

        # Check first record
        first_record = records[0]
        assert first_record["label"] == 1
        assert first_record["width"] == 500
        assert first_record["height"] == 429
        assert first_record["filename"] == "n01443537/n01443537_14753.TXT"
        assert "media_binary" in first_record
        assert isinstance(first_record["media_binary"], bytes)
        assert len(first_record["media_binary"]) > 0

        # Verify all fields are Field objects
        for _, field in dataset.fields:
            assert isinstance(field, Field)
            assert hasattr(field, "name")
            assert hasattr(field, "datatype")
            assert hasattr(field, "is_merge_key")

        # Verify media_binary field exists
        assert "media_binary" in dataset.fields
        assert dataset.fields["media_binary"].datatype == Datatype.binary("binary")

    def test_inconsistent_schema_handling(self, temp_dir, sample_wds_inconsistent):
        """Test that from_webdataset correctly handles inconsistent JSON schemas."""
        dataset = Dataset.from_webdataset(
            name="test_dataset",
            file_uri=sample_wds_inconsistent,
            metadata_uri=temp_dir,
            merge_keys="filename",
        )

        # Should include all fields from both schemas
        assert len(dataset.fields) == 6
        assert "label" in dataset.fields
        assert "width" in dataset.fields
        assert "height" in dataset.fields
        assert "filename" in dataset.fields
        assert "extra" in dataset.fields
        assert "media_binary" in dataset.fields

    def test_multiple_merge_key_handling(self, temp_dir, sample_wds_simple):
        """Test that specifying more than 1 merge key raises an error."""
        with pytest.raises(ValueError):
            Dataset.from_webdataset(
                name="test_multiple_merge_key_handling",
                file_uri=sample_wds_simple,
                metadata_uri=temp_dir,
                merge_keys=["label", "filename"],
            )

    def test_invalid_merge_key_handling(self, temp_dir, sample_wds_simple):
        """Test that specifying a non-existent field as merge key raises an error."""
        with pytest.raises(ValueError):
            Dataset.from_webdataset(
                name="test_invalid_merge_key_handling",
                file_uri=sample_wds_simple,
                metadata_uri=temp_dir,
                merge_keys="nonexistent_field",
            )

    def test_batch_reading_functionality(self, temp_dir, sample_wds_long):
        """Test that batch reading works correctly with different batch sizes."""
        # Test with batch_size=1
        dataset1 = Dataset.from_webdataset(
            name="test_batch1",
            file_uri=sample_wds_long,
            metadata_uri=temp_dir,
            merge_keys="filename",
            batch_size=1,
        )

        # Test with batch_size=2
        dataset2 = Dataset.from_webdataset(
            name="test_batch2",
            file_uri=sample_wds_long,
            metadata_uri=temp_dir,
            merge_keys="filename",
            batch_size=2,
        )

        # Test with batch_size=3
        dataset3 = Dataset.from_webdataset(
            name="test_batch3",
            file_uri=sample_wds_long,
            metadata_uri=temp_dir,
            merge_keys="filename",
            batch_size=3,
        )

        # Both should produce the same data
        records1 = list(dataset1.scan().to_pydict())
        records2 = list(dataset2.scan().to_pydict())
        records3 = list(dataset3.scan().to_pydict())

        assert len(records1) == len(records2) == len(records3) == 6
        assert records1 == records2 == records3
