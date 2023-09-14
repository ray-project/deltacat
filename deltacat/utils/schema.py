import pyarrow as pa


def coerce_pyarrow_table_to_schema(
    pa_table: pa.Table, input_schema: pa.Schema
) -> pa.Table:
    """Coerces a PyArrow table to the supplied schema

    1. For each field in `pa_table`, cast it to the field in `input_schema` if one with a matching name
        is available
    2. Reorder the fields in the casted table to the supplied schema, dropping any fields in `pa_table`
        that do not exist in the supplied schema
    3. If any fields in the supplied schema are not present, add a null array of the correct type

    Args:
        pa_table (pa.Table): Table to coerce
        input_schema (pa.Schema): Schema to coerce to

    Returns:
        pa.Table: Table with schema == `input_schema`
    """
    input_schema_names = set(input_schema.names)

    # Perform casting of types to provided schema's types
    cast_to_schema = [
        input_schema.field(inferred_field.name)
        if inferred_field.name in input_schema_names
        else inferred_field
        for inferred_field in pa_table.schema
    ]
    casted_table = pa_table.cast(pa.schema(cast_to_schema))

    # Reorder and pad columns with a null column where necessary
    pa_table_column_names = set(casted_table.column_names)
    columns = []
    for name in input_schema.names:
        if name in pa_table_column_names:
            columns.append(casted_table[name])
        else:
            columns.append(
                pa.nulls(len(casted_table), type=input_schema.field(name).type)
            )
    return pa.table(columns, schema=input_schema)
