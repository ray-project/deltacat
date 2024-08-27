from typing import List, Any
import pyarrow as pa


def glue_type_to_arrow(glue_type: str) -> pa.DataType:
    if glue_type == "byte":
        return pa.int8()
    elif glue_type == "short":
        return pa.int16()
    elif glue_type == "int":
        return pa.int32()
    elif glue_type == "bigint":
        return pa.int64()
    elif glue_type == "double":
        return pa.float64()
    elif glue_type == "float":
        return pa.float32()
    elif glue_type == "boolean":
        return pa.bool_()
    elif glue_type == "decimal":
        return pa.decimal128(38, 18)
    elif glue_type == "timestamp":
        return pa.timestamp("ms")
    elif glue_type == "date":
        return pa.date64()
    elif glue_type == "string":
        return pa.string()
    else:
        raise ValueError(f"Unsupported Glue type: {glue_type}")


def glue_columns_to_arrow_schema(columns: List[Any]):
    fields = [pa.field(col["Name"], glue_type_to_arrow(col["Type"])) for col in columns]
    return pa.schema(fields)


def pyarrow_to_glue_type(pa_type: pa.DataType):
    if pa.types.is_int8(pa_type):
        return "byte"
    elif pa.types.is_int16(pa_type):
        return "short"
    elif pa.types.is_int32(pa_type):
        return "int"
    elif pa.types.is_int64(pa_type):
        return "bigint"
    elif pa.types.is_float64(pa_type):
        return "double"
    elif pa.types.is_float32(pa_type):
        return "float"
    elif pa.types.is_boolean(pa_type):
        return "boolean"
    elif pa.types.is_decimal(pa_type):
        return "decimal"
    elif pa.types.is_timestamp(pa_type):
        return "timestamp"
    elif pa.types.is_date(pa_type):
        return "date"
    elif pa.types.is_string(pa_type):
        return "string"
    else:
        raise ValueError(f"Unsupported PyArrow type: {pa_type}")
