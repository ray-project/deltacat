import daft
from daft.logical.schema import Schema as DaftSchema
import pyarrow as pa


def pyarrow_to_daft_schema(pa_schema: pa.Schema) -> DaftSchema:
    """Converts a PyArrow schema to Daft schema

    TODO: This should be upstreamed into Daft's Schema class

    Args:
        pa_schema: PyArrow schema to conver

    Returns:
        DaftSchema: Converted PyArrow schema
    """
    return DaftSchema._from_field_name_and_types(
        [
            (pa_field.name, daft.DataType.from_arrow_type(pa_field.type))
            for pa_field in pa_schema
        ]
    )
