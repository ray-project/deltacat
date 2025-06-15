import itertools
import pytest
import pyarrow as pa
import json
import tarfile
from deltacat.storage.rivulet import Dataset, Schema, Field, Datatype


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


def test_schema_fields(tmp_path):
    """Test that from_webdataset correctly identifies all fields in the tar file."""
    tar_path = "../../../test_utils/resources/test_wds.tar"
    dataset = Dataset.from_webdataset(
        name="test_webdataset",
        file_uri=tar_path,
        metadata_uri=tmp_path,
        merge_keys="filename"
    )
    assert "label" in dataset.fields
    assert "width" in dataset.fields
    assert "height" in dataset.fields
    assert "filename" in dataset.fields
    assert "media_binary" in dataset.fields
    assert len(dataset.fields) == 5


def test_schema_data(tmp_path):
    """Test that data values are correctly extracted from the tar file."""
    tar_path = "../../../test_utils/resources/test_wds.tar"
    dataset = Dataset.from_webdataset(
        name="test_webdataset",
        file_uri=tar_path,
        metadata_uri=tmp_path,
        merge_keys="filename"
    )
    records = dataset.scan().to_pydict()
    for record in itertools.islice(records, 1):
        assert record["label"] == 1
        assert record["width"] == 500
        assert record["height"] == 429
        assert record["filename"] == "n01443537/n01443537_14753.TXT"


def test_merge_keys_are_properly_set(tmp_path):
    """Test that merge keys are correctly identified and set in the schema."""
    tar_path = "../../../test_utils/resources/test_wds.tar"
    dataset = Dataset.from_webdataset(
        name="test_webdataset",
        file_uri=tar_path,
        metadata_uri=tmp_path,
        merge_keys="filename"
    )
    assert "filename" in dataset.get_merge_keys()
    assert len(dataset.get_merge_keys()) == 1


def test_invalid_merge_key_raises_error(tmp_path):
    """Test that specifying a non-existent field as merge key raises an error."""
    tar_path = "../../../test_utils/resources/test_wds.tar"
    with pytest.raises(AttributeError):
        Dataset.from_webdataset(
            name="test_webdataset",
            file_uri=tar_path,
            metadata_uri=tmp_path,
            merge_keys="nonexistent_field"
        )


def test_schema_datatypes(tmp_path):
    """Test that field datatypes are correctly inferred from the data."""
    tar_path = "../../../test_utils/resources/test_wds.tar"
    dataset = Dataset.from_webdataset(
        name="test_webdataset",
        file_uri=tar_path,
        metadata_uri=tmp_path,
        merge_keys="filename"
    )
    assert dataset.fields["label"].datatype == Datatype.int64()
    assert dataset.fields["width"].datatype == Datatype.int64()
    assert dataset.fields["height"].datatype == Datatype.int64()
    assert dataset.fields["filename"].datatype == Datatype.string()


def test_metadata_directory_creation(tmp_path):
    """Test that metadata directory is properly initialized."""
    tar_path = "../../../test_utils/resources/test_wds.tar"
    dataset = Dataset.from_webdataset(
        name="test_meta",
        file_uri=tar_path,
        metadata_uri=tmp_path,
        merge_keys="filename"
    )
    assert hasattr(dataset, "_metadata_path")
    assert dataset._metadata_path is not None


def test_field_is_field_object(tmp_path):
    """Test that fields in the dataset are proper Field objects."""
    tar_path = "../../../test_utils/resources/test_wds.tar"
    dataset = Dataset.from_webdataset(
        name="test_meta",
        file_uri=tar_path,
        metadata_uri=tmp_path,
        merge_keys="filename"
    )
    assert isinstance(dataset.fields["filename"], Field)

def test_inconsistent_tar_fields(tmp_path):
    """Test that from_webdataset correctly identifies all fields in the tar file if the jsons are inconsistent."""
    tar_path = "../../../test_utils/resources/test_wds_incon.tar"
    dataset = Dataset.from_webdataset(
        name="test_webdataset",
        file_uri=tar_path,
        metadata_uri=tmp_path,
        merge_keys="filename"
    )
    assert "label" in dataset.fields
    assert "width" in dataset.fields
    assert "height" in dataset.fields
    assert "filename" in dataset.fields
    assert "extra" in dataset.fields
    assert len(dataset.fields) == 5

def test_media_binary_with_txt(tmp_path):
    """Test that media_binary is an added column after Dataset is created from webdataset,
    where media files for this example are .TXT files."""
    tar_path = "../../../test_utils/resources/test_wds.tar" # media files for this example are .TXT files
    dataset = Dataset.from_webdataset(
        name="test_webdataset",
        file_uri=tar_path,
        metadata_uri=tmp_path,
        merge_keys="filename"
    )
    assert "label" in dataset.fields
    assert "width" in dataset.fields
    assert "height" in dataset.fields
    assert "filename" in dataset.fields
    assert "media_binary" in dataset.fields

def test_media_binary_with_jpg(tmp_path):
    """Test that media_binary is an added column after Dataset is created from webdataset,
    where media files for this example are .JPG files."""
    tar_path = "../../../test_utils/resources/test_wds_jpg.tar"
    dataset = Dataset.from_webdataset(
        name="test_webdataset",
        file_uri=tar_path,
        metadata_uri=tmp_path,
        merge_keys="filename"
    )
    assert "label" in dataset.fields
    assert "width" in dataset.fields
    assert "height" in dataset.fields
    assert "filename" in dataset.fields
    print(dataset.fields["media_binary"].datatype)
    assert "media_binary" in dataset.fields