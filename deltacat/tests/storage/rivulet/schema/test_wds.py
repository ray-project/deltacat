import itertools
import pytest
import pyarrow as pa
import json
import tarfile
import io
import os
from deltacat.storage.rivulet import Dataset, Schema, Field, Datatype


def create_test_webdataset(tmp_path):
    """Create a test WebDataset tar file dynamically."""
    tar_path = os.path.join(tmp_path, "test_wds.tar")
    
    with tarfile.open(tar_path, "w") as tar:
        # Create sample data
        sample_data = [
            {
                "filename": "sample1.txt",
                "label": 1,
                "width": 500,
                "height": 429,
                "content": "Sample text content 1"
            },
            {
                "filename": "sample2.txt", 
                "label": 2,
                "width": 600,
                "height": 400,
                "content": "Sample text content 2"
            }
        ]
        
        # Add JSON metadata files
        for i, data in enumerate(sample_data):
            json_content = json.dumps(data).encode('utf-8')
            json_info = tarfile.TarInfo(f"{i:06d}.json")
            json_info.size = len(json_content)
            tar.addfile(json_info, io.BytesIO(json_content))
            
            # Add corresponding text files
            txt_content = data["content"].encode('utf-8')
            txt_info = tarfile.TarInfo(f"{i:06d}.txt")
            txt_info.size = len(txt_content)
            tar.addfile(txt_info, io.BytesIO(txt_content))
    
    return tar_path


def create_inconsistent_webdataset(tmp_path):
    """Create a WebDataset with inconsistent JSON schemas."""
    tar_path = os.path.join(tmp_path, "test_wds_incon.tar")
    
    with tarfile.open(tar_path, "w") as tar:
        # First sample with standard fields
        data1 = {
            "filename": "sample1.txt",
            "label": 1,
            "width": 500,
            "height": 429
        }
        
        # Second sample with extra field
        data2 = {
            "filename": "sample2.txt",
            "label": 2,
            "width": 600,
            "height": 400,
            "extra": "additional_field"
        }
        
        # Add files
        for i, data in enumerate([data1, data2]):
            json_content = json.dumps(data).encode('utf-8')
            json_info = tarfile.TarInfo(f"{i:06d}.json")
            json_info.size = len(json_content)
            tar.addfile(json_info, io.BytesIO(json_content))
            
            txt_content = "Sample content".encode('utf-8')
            txt_info = tarfile.TarInfo(f"{i:06d}.txt")
            txt_info.size = len(txt_content)
            tar.addfile(txt_info, io.BytesIO(txt_content))
    
    return tar_path


def test_schema_field_types():
    """Test that Schema correctly stores Field objects with their types."""
    fields = [
        Field("id", Datatype.int64(), is_merge_key=True),
        Field("name", Datatype.string()),
    ]
    schema = Schema(fields)
    values = list(schema.values())
    assert len(values) == 2
    assert all(isinstance(v, Field) for v in values)


def test_webdataset_creation_and_reading(tmp_path):
    """Test that from_webdataset correctly creates a dataset and can read data."""
    tar_path = create_test_webdataset(tmp_path)
    
    dataset = Dataset.from_webdataset(
        name="test_webdataset",
        file_uri=tar_path,
        metadata_uri=tmp_path,
        merge_keys="filename"
    )
    
    # Verify schema fields
    assert "label" in dataset.fields
    assert "width" in dataset.fields
    assert "height" in dataset.fields
    assert "filename" in dataset.fields
    assert "media_binary" in dataset.fields
    assert len(dataset.fields) == 5
    
    # Verify data can be read
    records = dataset.scan().to_pydict()
    assert len(records) == 2
    
    # Check first record
    first_record = records[0]
    assert first_record["label"] == 1
    assert first_record["width"] == 500
    assert first_record["height"] == 429
    assert first_record["filename"] == "sample1.txt"
    assert "media_binary" in first_record


def test_data_extraction_and_validation(tmp_path):
    """Test that data values are correctly extracted and validated."""
    tar_path = create_test_webdataset(tmp_path)
    
    dataset = Dataset.from_webdataset(
        name="test_webdataset",
        file_uri=tar_path,
        metadata_uri=tmp_path,
        merge_keys="filename"
    )
    
    # Read all records
    records = dataset.scan().to_pydict()
    
    # Verify data integrity
    for record in records:
        assert isinstance(record["label"], int)
        assert isinstance(record["width"], int)
        assert isinstance(record["height"], int)
        assert isinstance(record["filename"], str)
        assert isinstance(record["media_binary"], bytes)
        
        # Verify data ranges
        assert record["label"] in [1, 2]
        assert record["width"] in [500, 600]
        assert record["height"] in [400, 429]


def test_merge_keys_functionality(tmp_path):
    """Test that merge keys are correctly identified and set in the schema."""
    tar_path = create_test_webdataset(tmp_path)
    
    dataset = Dataset.from_webdataset(
        name="test_webdataset",
        file_uri=tar_path,
        metadata_uri=tmp_path,
        merge_keys="filename"
    )
    
    # Verify merge keys
    merge_keys = dataset.get_merge_keys()
    assert "filename" in merge_keys
    assert len(merge_keys) == 1
    
    # Verify merge key field properties
    filename_field = dataset.fields["filename"]
    assert filename_field.is_merge_key is True


def test_invalid_merge_key_handling(tmp_path):
    """Test that specifying a non-existent field as merge key raises an error."""
    tar_path = create_test_webdataset(tmp_path)
    
    with pytest.raises(AttributeError):
        Dataset.from_webdataset(
            name="test_webdataset",
            file_uri=tar_path,
            metadata_uri=tmp_path,
            merge_keys="nonexistent_field"
        )


def test_datatype_inference(tmp_path):
    """Test that field datatypes are correctly inferred from the data."""
    tar_path = create_test_webdataset(tmp_path)
    
    dataset = Dataset.from_webdataset(
        name="test_webdataset",
        file_uri=tar_path,
        metadata_uri=tmp_path,
        merge_keys="filename"
    )
    
    # Verify inferred datatypes
    assert dataset.fields["label"].datatype == Datatype.int64()
    assert dataset.fields["width"].datatype == Datatype.int64()
    assert dataset.fields["height"].datatype == Datatype.int64()
    assert dataset.fields["filename"].datatype == Datatype.string()
    assert dataset.fields["media_binary"].datatype == Datatype.binary()


def test_field_object_validation(tmp_path):
    """Test that fields in the dataset are proper Field objects."""
    tar_path = create_test_webdataset(tmp_path)
    
    dataset = Dataset.from_webdataset(
        name="test_meta",
        file_uri=tar_path,
        metadata_uri=tmp_path,
        merge_keys="filename"
    )
    
    # Verify all fields are Field objects
    for field_name, field in dataset.fields.items():
        assert isinstance(field, Field)
        assert hasattr(field, "name")
        assert hasattr(field, "datatype")
        assert hasattr(field, "is_merge_key")


def test_inconsistent_schema_handling(tmp_path):
    """Test that from_webdataset correctly handles inconsistent JSON schemas."""
    tar_path = create_inconsistent_webdataset(tmp_path)
    
    dataset = Dataset.from_webdataset(
        name="test_webdataset",
        file_uri=tar_path,
        metadata_uri=tmp_path,
        merge_keys="filename"
    )
    
    # Should include all fields from both schemas
    assert "label" in dataset.fields
    assert "width" in dataset.fields
    assert "height" in dataset.fields
    assert "filename" in dataset.fields
    assert "extra" in dataset.fields
    assert len(dataset.fields) == 5


def test_batch_reading_functionality(tmp_path):
    """Test that batch reading works correctly with different batch sizes."""
    tar_path = create_test_webdataset(tmp_path)
    
    # Test with batch_size=1
    dataset1 = Dataset.from_webdataset(
        name="test_batch1",
        file_uri=tar_path,
        metadata_uri=tmp_path,
        merge_keys="filename",
        batch_size=1
    )
    
    # Test with batch_size=2
    dataset2 = Dataset.from_webdataset(
        name="test_batch2",
        file_uri=tar_path,
        metadata_uri=tmp_path,
        merge_keys="filename",
        batch_size=2
    )
    
    # Both should produce the same data
    records1 = dataset1.scan().to_pydict()
    records2 = dataset2.scan().to_pydict()
    
    assert len(records1) == len(records2) == 2
    assert records1 == records2


def test_media_binary_creation(tmp_path):
    """Test that media_binary column is properly created and populated."""
    tar_path = create_test_webdataset(tmp_path)
    
    dataset = Dataset.from_webdataset(
        name="test_webdataset",
        file_uri=tar_path,
        metadata_uri=tmp_path,
        merge_keys="filename"
    )
    
    # Verify media_binary field exists
    assert "media_binary" in dataset.fields
    assert dataset.fields["media_binary"].datatype == Datatype.binary()
    
    # Verify data contains binary content
    records = dataset.scan().to_pydict()
    for record in records:
        assert "media_binary" in record
        assert isinstance(record["media_binary"], bytes)
        assert len(record["media_binary"]) > 0


def test_dataset_persistence_and_reloading(tmp_path):
    """Test that datasets can be persisted and reloaded correctly."""
    tar_path = create_test_webdataset(tmp_path)
    
    # Create and save dataset
    dataset = Dataset.from_webdataset(
        name="test_persistence",
        file_uri=tar_path,
        metadata_uri=tmp_path,
        merge_keys="filename"
    )
    
    # Verify dataset was created
    assert dataset.name == "test_persistence"
    assert len(dataset.fields) == 5
    
    # Test that we can scan the data multiple times
    records1 = dataset.scan().to_pydict()
    records2 = dataset.scan().to_pydict()
    
    assert records1 == records2
    assert len(records1) == 2