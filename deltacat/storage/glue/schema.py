from typing import List, Any
import pyarrow as pa


def glue_type_to_arrow(glue_type: str):
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
