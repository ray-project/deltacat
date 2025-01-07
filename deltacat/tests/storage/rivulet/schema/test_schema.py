import pytest
import pyarrow as pa
from deltacat.storage.rivulet import Schema, Field, Datatype


def test_field_initialization():
    field = Field(name="test_field", datatype=Datatype.string(), is_merge_key=True)
    assert field.name == "test_field"
    assert field.datatype == Datatype.string()
    assert field.is_merge_key


def test_schema_initialization():
    fields = [("id", Datatype.int64()), ("name", Datatype.string())]
    schema = Schema(fields, merge_keys=["id"])
    assert len(schema) == 2
    assert "id" in schema.keys()
    assert schema["id"].datatype == Datatype.int64()
    assert "name" in schema.keys()
    assert schema["name"].datatype == Datatype.string()


def test_merge_key_conflict_on_init():
    fields = [
        Field("id", Datatype.int64(), is_merge_key=False),  # Merge key off here
        ("name", Datatype.string()),
    ]
    with pytest.raises(TypeError):
        Schema(fields, merge_keys=["id"])  # Merge key on here


def test_simultaneous_duplicate_field():
    with pytest.raises(ValueError):
        Schema(
            [
                ("id", Datatype.int32()),
                ("name", Datatype.string()),
                ("age", Datatype.int32()),
                ("age", Datatype.string()),
            ],
            merge_keys=["id"],
        )


def test_add_field():
    schema = Schema()
    field = Field("new_field", Datatype.float(), True)
    schema.add_field(field)
    assert len(schema) == 1
    assert "new_field" in schema.keys()
    assert schema["new_field"].datatype == Datatype.float()

    field2 = Field("another_field", Datatype.string(), True)
    schema.add_field(field2)
    assert len(schema) == 2
    assert "another_field" in schema.keys()
    assert schema["another_field"].datatype == Datatype.string()

    with pytest.raises(ValueError):
        schema.add_field(field2)


def test_setitem_field():
    schema = Schema()
    field = Field("test_field", Datatype.int64(), is_merge_key=True)
    schema["test_field"] = field
    assert schema["test_field"] == field


def test_setitem_datatype():
    schema = Schema()
    schema["id"] = (Datatype.int64(), True)
    schema["test_field"] = Datatype.int64()
    assert schema["test_field"].name == "test_field"
    assert schema["test_field"].datatype == Datatype.int64()
    assert not schema["test_field"].is_merge_key


def test_setitem_tuple_with_merge_key():
    schema = Schema()
    schema["test_field"] = (Datatype.int64(), True)
    assert schema["test_field"].name == "test_field"
    assert schema["test_field"].datatype == Datatype.int64()
    assert schema["test_field"].is_merge_key


def test_setitem_invalid_type():
    schema = Schema()
    with pytest.raises(TypeError):
        schema["test_field"] = "invalid"


def test_non_empty_merge_key():
    with pytest.raises(TypeError):
        _ = Schema([], merge_keys=["id"])


def test_merge_schemas():
    schema1 = Schema([("id", Datatype.int64())], merge_keys=["id"])
    schema2 = Schema(
        [("other_id", Datatype.string()), ("name", Datatype.string())],
        merge_keys="other_id",
    )
    schema1.merge(schema2)
    assert len(schema1) == 3
    assert "id" in schema1.keys()
    assert "name" in schema1.keys()
    assert "other_id" in schema1.keys()


def test_merge_schemas_same_merge_key():
    schema1 = Schema(
        [("id", Datatype.int64()), ("name", Datatype.string())], merge_keys=["id"]
    )
    schema2 = Schema(
        [("id", Datatype.int64()), ("other_name", Datatype.string())],
        merge_keys="id",
    )
    schema1.merge(schema2)
    assert len(schema1) == 3
    assert "id" in schema1.keys()
    assert "name" in schema1.keys()
    assert "other_name" in schema1.keys()


def test_merge_schema_conflict():
    schema1 = Schema([("id", Datatype.int64())], merge_keys=["id"])
    schema1_dup = Schema([("id", Datatype.int64())], merge_keys=["id"])
    schema2 = Schema([("id", Datatype.string())], merge_keys=["id"])

    with pytest.raises(ValueError):
        schema1.merge(schema2)

    schema1.merge(
        schema1_dup
    )  # Merging the same field is allowed (unlike using add_field)
    assert schema1["id"].datatype == Datatype.int64()
    assert len(schema1) == 1


def test_to_pyarrow_schema():
    fields = [("id", Datatype.int64()), ("name", Datatype.string())]
    schema = Schema(fields, merge_keys=["id"])
    pa_schema = schema.to_pyarrow()
    assert isinstance(pa_schema, pa.Schema)
    assert len(pa_schema) == 2
    assert pa_schema.field("id").type == pa.int64()
    assert pa_schema.field("name").type == pa.string()


def test_from_pyarrow_schema():
    pa_schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    schema = Schema.from_pyarrow(pa_schema, merge_keys=["id"])
    assert len(schema) == 2
    assert schema["id"].is_merge_key


def test_from_pyarrow_schema_invalid_merge_keys():
    pa_schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    with pytest.raises(ValueError):
        Schema.from_pyarrow(pa_schema, merge_keys=["bad_key"])


def test_get_field():
    schema = Schema([("id", Datatype.int64())], merge_keys=["id"])
    field = schema["id"]
    assert field.name == "id"
    assert field.datatype == Datatype.int64()


def test_set_field():
    schema = Schema([("id", Datatype.int64())], merge_keys=["id"])
    schema["name"] = Field("name", Datatype.string())
    assert len(schema) == 2
    assert "name" in schema.keys()
    assert schema["name"].datatype == Datatype.string()


def test_delete_field():
    schema = Schema(
        [("name", Datatype.string()), ("zip", Datatype.int32())], merge_keys=["name"]
    )
    del schema["zip"]
    assert "zip" not in schema.keys()
    assert "name" in schema.keys()


def test_delete_merge_key_field():
    schema = Schema([("id", Datatype.int64())], merge_keys=["id"])
    with pytest.raises(ValueError):
        del schema["id"]


def test_schema_iter():
    fields = [
        Field("id", Datatype.int32(), is_merge_key=True),
        Field("name", Datatype.string()),
    ]
    schema = Schema(fields)
    iter_result = list(iter(schema))
    assert len(iter_result) == 2
    assert all(isinstance(item, str) for item in iter_result)


def test_merge_all():
    schema1 = Schema(
        [
            Field("id", Datatype.int64(), is_merge_key=True),
            Field("name", Datatype.string()),
        ]
    )
    schema2 = Schema(
        [
            Field("age", Datatype.int32()),
            Field("email", Datatype.string(), is_merge_key=True),
        ]
    )
    merged_schema = Schema.merge_all([schema1, schema2])
    assert len(merged_schema) == 4


def test_schema_values():
    fields = [
        Field("id", Datatype.int64(), is_merge_key=True),
        Field("name", Datatype.string()),
    ]
    schema = Schema(fields)
    values = list(schema.values())
    assert len(values) == 2
    assert all(isinstance(v, Field) for v in values)


def test_schema_items():
    fields = [
        Field("id", Datatype.int64(), is_merge_key=True),
        Field("name", Datatype.string()),
    ]
    schema = Schema(fields)
    items = list(schema.items())
    assert len(items) == 2
    assert all(isinstance(k, str) and isinstance(v, Field) for k, v in items)
