import posixpath
from deltacat.utils.metafile_locator import _find_partition_path
import pytest

import pyarrow as pa
from deltacat.storage.rivulet import Schema, Field, Datatype
from deltacat.storage.rivulet.dataset import Dataset
from deltacat.storage.rivulet.reader.query_expression import QueryExpression


@pytest.fixture
def sample_schema():
    return Schema(
        fields=[
            Field("id", Datatype.int32(), is_merge_key=True),
            Field("name", Datatype.string()),
            Field("age", Datatype.int32()),
        ]
    )


@pytest.fixture
def sample_pydict():
    return {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}


@pytest.fixture
def sample_parquet_data(tmp_path, sample_pydict):
    parquet_path = tmp_path / "test.parquet"
    table = pa.Table.from_pydict(sample_pydict)
    pa.parquet.write_table(table, parquet_path)
    return parquet_path


# Updated Tests


def test_dataset_creation_with_schema(tmp_path, sample_schema):
    dataset = Dataset(dataset_name="test_dataset", schema=sample_schema)
    assert len(dataset.fields) == 3
    assert "id" in dataset.fields
    assert dataset.fields["id"].is_merge_key


def test_dataset_initialization_with_metadata(tmp_path):
    dataset = Dataset(dataset_name="test_dataset", metadata_uri=str(tmp_path))
    assert dataset.dataset_name == "test_dataset"
    assert dataset._metadata_folder.startswith(".riv-meta")


def test_invalid_dataset_initialization():
    with pytest.raises(ValueError, match="Name must be a non-empty string"):
        Dataset(dataset_name="")


def test_dataset_creation_metadata_structure(tmp_path):
    dataset = Dataset(dataset_name="test_dataset", metadata_uri=str(tmp_path))

    assert dataset._metadata_folder.startswith(".riv-meta")
    assert dataset._namespace == "DEFAULT"
    assert dataset.dataset_name == "test_dataset"
    assert dataset._metadata_path == str(tmp_path / ".riv-meta-test_dataset")

    locator = dataset._locator
    root_uri = dataset._metadata_path

    partition_path = _find_partition_path(root_uri, locator)

    # Ensures that directory structure for namespace -> table -> table_version -> stream_id -> partition_id exists
    assert posixpath.exists(partition_path)


def test_fields_accessor_add_field(tmp_path, sample_schema):
    dataset = Dataset(dataset_name="test_dataset", schema=sample_schema)
    dataset.fields.add("new_field", Datatype.float())
    assert "new_field" in dataset.fields
    assert dataset.fields["new_field"].datatype == Datatype.float()

    dataset.fields["new_field2"] = Field("new_field2", Datatype.int32())
    assert "new_field2" in dataset.fields
    assert "new_field2" in dataset.schemas["all"]
    with pytest.raises(TypeError):
        dataset.fields["new_field3"] = 2


def test_field_removal(tmp_path, sample_schema):
    dataset = Dataset(dataset_name="test_dataset", schema=sample_schema)
    del dataset.fields["age"]
    assert "age" not in dataset.fields
    with pytest.raises(ValueError):
        del dataset.fields["age"]
    with pytest.raises(KeyError):
        _ = dataset.fields["age"]


def test_fields_accessor_repr(tmp_path, sample_schema):
    dataset = Dataset(dataset_name="test_dataset", schema=sample_schema)
    repr_output = repr(dataset.fields)
    for field_name in ["id", "name", "age"]:
        assert field_name in repr_output, f"Field '{field_name}' missing in repr output"


def test_schemas_accessor_add_group(tmp_path, sample_schema):
    dataset = Dataset(dataset_name="test_dataset", schema=sample_schema)
    dataset.schemas["analytics"] = ["id", "name"]
    assert "analytics" in dataset.schemas
    assert len(dataset.schemas["analytics"]) == 2


def test_schema_removal(tmp_path, sample_schema):
    dataset = Dataset(dataset_name="test_dataset", schema=sample_schema)
    with pytest.raises(ValueError):
        del dataset.schemas["all"]
    with pytest.raises(ValueError):
        del dataset.schemas["does_not_exist"]
    dataset.schemas["new"] = ["id", "name"]
    del dataset.schemas["new"]
    with pytest.raises(KeyError):
        _ = dataset.schemas["new"]


def test_dataset_from_parquet(tmp_path, sample_parquet_data):
    dataset = Dataset.from_parquet(
        name="test_dataset",
        file_uri=str(sample_parquet_data),
        metadata_uri=str(tmp_path),
        merge_keys="id",
    )
    assert len(dataset.fields) == 3
    assert "id" in dataset.fields
    assert dataset.fields["id"].is_merge_key


def test_parquet_schema_modes(tmp_path, sample_pydict):
    # Create two parquet files with overlapping and unique schemas
    data_1 = {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}
    data_2 = {"id": [4, 5, 6], "age": [25, 30, 35]}

    path_1 = tmp_path / "data1.parquet"
    path_2 = tmp_path / "data2.parquet"
    pa.parquet.write_table(pa.Table.from_pydict(data_1), path_1)
    pa.parquet.write_table(pa.Table.from_pydict(data_2), path_2)

    dataset_union = Dataset.from_parquet(
        name="test_dataset_union",
        file_uri=str(tmp_path),
        merge_keys="id",
        schema_mode="union",
    )
    assert len(dataset_union.fields) == 3  # id, name, age

    dataset_intersect = Dataset.from_parquet(
        name="test_dataset_intersect",
        file_uri=str(tmp_path),
        merge_keys="id",
        schema_mode="intersect",
    )
    assert len(dataset_intersect.fields) == 1  # Only id


def test_merge_all_schemas():
    schema1 = Schema(
        fields=[
            Field("id", Datatype.int32(), is_merge_key=True),
            Field("name", Datatype.string()),
        ]
    )
    schema2 = Schema(
        fields=[
            Field("id", Datatype.int32(), is_merge_key=True),
            Field("age", Datatype.int32()),
        ]
    )
    merged_schema = Schema.merge_all([schema1, schema2])
    assert len(merged_schema) == 3
    assert "id" in merged_schema
    assert "name" in merged_schema
    assert "age" in merged_schema


def test_writer_creation_with_custom_format(tmp_path, sample_schema):
    dataset = Dataset(dataset_name="test_dataset", schema=sample_schema)
    writer = dataset.writer(file_format="feather")
    assert writer is not None


def test_scan_with_query(tmp_path, sample_schema):
    dataset = Dataset(dataset_name="test_dataset", schema=sample_schema)
    query = QueryExpression()  # Placeholder query
    scan = dataset.scan(query)
    assert scan is not None


def test_add_schema_to_new_schemas(tmp_path):
    """Test adding a schema to a new field group."""
    base_uri = str(tmp_path / "test_dataset")
    dataset = Dataset(dataset_name=base_uri)

    schema = Schema(
        [
            ("id", Datatype.int32()),
            ("name", Datatype.string()),
            ("age", Datatype.int32()),
        ],
        merge_keys=["id"],
    )

    dataset.add_schema(schema, schema_name="new_group")

    # Verify the field group is added
    assert "new_group" in dataset.schemas
    assert len(dataset.schemas["new_group"]) == 3
    assert dataset.schemas["new_group"]["id"].datatype == Datatype.int32()
    assert dataset.schemas["new_group"]["name"].datatype == Datatype.string()
    assert dataset.schemas["new_group"]["age"].datatype == Datatype.int32()


def test_add_schema_to_existing_schemas(tmp_path):
    """Test merging a schema into an existing field group."""
    base_uri = str(tmp_path / "test_dataset")
    dataset = Dataset(dataset_name=base_uri)

    schema_1 = Schema(
        [
            ("id", Datatype.int32()),
            ("name", Datatype.string()),
        ],
        merge_keys=["id"],
    )

    dataset.add_schema(schema_1, schema_name="existing_group")

    schema_2 = Schema(
        [
            ("age", Datatype.int32()),
            ("email", Datatype.string()),
        ],
        merge_keys=["id"],
    )

    dataset.add_schema(schema_2, schema_name="existing_group")

    # Verify the merged schema
    assert "existing_group" in dataset.schemas
    assert len(dataset.schemas["existing_group"]) == 4
    assert dataset.schemas["existing_group"]["id"].datatype == Datatype.int32()
    assert dataset.schemas["existing_group"]["name"].datatype == Datatype.string()
    assert dataset.schemas["existing_group"]["age"].datatype == Datatype.int32()
    assert dataset.schemas["existing_group"]["email"].datatype == Datatype.string()


def test_add_schema_conflicting_fields(tmp_path):
    """Test adding a schema with conflicting fields."""
    base_uri = str(tmp_path / "test_dataset")
    dataset = Dataset(dataset_name=base_uri)

    schema_1 = Schema(
        [
            ("id", Datatype.int32()),
            ("name", Datatype.string()),
        ],
        merge_keys=["id"],
    )

    dataset.add_schema(schema_1, schema_name="conflicting_group")

    schema_2 = Schema(
        [
            ("id", Datatype.string()),  # Conflict: datatype mismatch
            ("age", Datatype.int32()),
        ],
        merge_keys=["id"],
    )

    with pytest.raises(ValueError, match="already exists"):
        dataset.add_schema(schema_2, schema_name="conflicting_group")

    schema_3 = Schema(
        [
            ("id", Datatype.int32()),  # Conflict: datatype mismatch
            ("age", Datatype.int32()),
        ],
        merge_keys=["id"],
    )

    dataset.add_schema(schema_3, schema_name="conflicting_group")
    assert "conflicting_group" in dataset.schemas
    assert len(dataset.schemas["conflicting_group"]) == 3
    assert dataset.schemas["conflicting_group"]["id"].datatype == Datatype.int32()
    assert dataset.schemas["conflicting_group"]["name"].datatype == Datatype.string()
    assert dataset.schemas["conflicting_group"]["age"].datatype == Datatype.int32()


def test_add_fields_with_merge_key_field(tmp_path):
    base_uri = str(tmp_path / "test_dataset")
    dataset = Dataset(dataset_name=base_uri)
    dataset.add_fields([Field("my_merge_key", Datatype.string(), True)])
    assert dataset.schemas["default"].get_merge_key() == "my_merge_key"


def test_add_schema_to_nonexistent_schemas(tmp_path):
    """Test adding a schema to a nonexistent field group."""
    base_uri = str(tmp_path / "test_dataset")
    dataset = Dataset(dataset_name=base_uri)

    schema = Schema(
        [
            ("id", Datatype.int32()),
            ("name", Datatype.string()),
        ],
        merge_keys=["id"],
    )

    # Add to a non-existent field group
    dataset.add_schema(schema, schema_name="nonexistent_group")

    # Verify the field group is created
    assert "nonexistent_group" in dataset.schemas
    assert len(dataset.schemas["nonexistent_group"]) == 2


def test_add_missing_field_to_schema_raises_error(tmp_path, sample_schema):
    """
    Test that attempting to add a missing field to the 'all' schema raises a ValueError.
    """
    dataset = Dataset(dataset_name="test_dataset", schema=sample_schema)

    # Attempt to add a non-existent field to the 'all' schema
    with pytest.raises(
        ValueError, match="Field 'missing_field' does not exist in the dataset."
    ):
        dataset.schemas["all"] = [
            "missing_field"
        ]  # Attempt to set a list with a missing field


def test_schemas_accessor_methods(tmp_path, sample_schema):
    """
    Test the __iter__, __len__, and __repr__ methods of SchemasAccessor.
    """
    dataset = Dataset(
        dataset_name="test_dataset", schema=sample_schema
    )  # Default schema is defined automatically
    dataset.schemas["schema_1"] = ["id", "name"]
    dataset.schemas["schema_2"] = ["age"]

    # Test __iter__
    schema_names = list(iter(dataset.schemas))
    assert set(schema_names) == {
        "schema_1",
        "schema_2",
        "all",
        "default",
    }, "Schema names do not match expected values"

    # Test __len__
    assert len(dataset.schemas) == 4, "Length of schemas accessor is incorrect"

    # Test __repr__
    repr_output = repr(dataset.schemas)
    for schema_name in ["schema_1", "schema_2", "all"]:
        assert (
            schema_name in repr_output
        ), f"Schema '{schema_name}' missing in repr output"


def test_get_merge_keys(tmp_path, sample_schema):
    """
    Test the get_merge_keys method to ensure it returns all merge keys in the dataset.
    """
    dataset = Dataset(dataset_name="test_dataset", schema=sample_schema)

    # Add fields with additional merge key to the dataset
    other_schema = Schema(
        [("id2", Datatype.int32()), ("zip", Datatype.string())], merge_keys=["id2"]
    )

    dataset.add_schema(other_schema, "id2+zip")

    # Call get_merge_keys and validate the result
    merge_keys = dataset.get_merge_keys()
    assert merge_keys == [
        "id",
        "id2",
    ], f"Expected merge keys ['id', 'id2'], got {merge_keys}"


def test_add_fields_no_fields_raises_error(tmp_path, sample_schema):
    dataset = Dataset(dataset_name="test_dataset")
    with pytest.raises(ValueError):
        dataset.add_fields(fields=[])


def test_add_fields_mismatched_merge_keys_raises_error(tmp_path, sample_schema):
    dataset = Dataset(dataset_name="test_dataset")
    with pytest.raises(
        ValueError,
        match="The following merge keys were not found in the provided fields: does_not_exist",
    ):
        dataset.add_fields(fields=sample_schema.values(), merge_keys=["does_not_exist"])

    with pytest.raises(TypeError, match="Merge key status conflict"):
        dataset.add_fields(
            fields=[Field("id", Datatype.int32()), Field("name", Datatype.string())],
            merge_keys=["id"],
        )
