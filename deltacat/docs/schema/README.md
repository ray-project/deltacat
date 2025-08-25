# Schemas

DeltaCAT tables may either be schemaless or backed by a schema based on the [Arrow type system](https://arrow.apache.org/docs/python/api/datatypes.html).

## Schemaless Tables
A schemaless table is created via `dc.create_table(new_table_name)` (schema omitted) or
`dc.write_to_table(data, new_table_name, schema=None)` (schema explicitly set to `None` when writing
to a new table). Schemaless tables only save a record of files written to them over time without schema
inference, data validation, or data coercion. Since it may not be possible to derive a unified schema on
read, data returned via `dc.read_table(table_name)` is always an ordered list of files written to the
table and their manifest entry info (e.g., size, content type, content encoding, etc.) referred to as a
**Manifest Table**. For example:

| Column                     | Value                     | Type     | Description                                          |
|----------------------------|---------------------------|----------|------------------------------------------------------|
| author_name                | "deltacat.write_to_table" | str      | Manifest producer name                               |
| author_version             | "2.0.0b12"                | str      | Manifest producer version                            |
| id                         | None                      | str      | Manifest entry ID (can be None)                      |
| mandatory                  | True                      | bool     | Raise error if file is missing (True/False)          |
| meta_content_encoding      | "identity"                | str      | File content encoding (identity = no encoding)       |
| meta_content_length        | 2413                      | int64    | File size in bytes (2.4 KB)                          |
| meta_content_type          | "application/parquet"     | str      | File format (Parquet)                                |
| meta_record_count          | 2                         | int64    | Number of records in this file                       |
| meta_source_content_length | 176                       | int64    | Original data size in memory (176 bytes)             |
| previous_stream_position   | 1                         | int64    | Previous delta stream position                       |
| stream_position            | 2                         | int64    | This delta's stream position                         |
| path                       | /my_catalog/data/file.pq  | str      | File path relative to catalog root                   |

If you know that this data can be read into a standard DeltaCAT dataset type (e.g., Daft, Ray Data, PyArrow,
Pandas, Polars), then you can materialize the manifest table via a call to
`dc.from_manifest_table(manifest_table)`.

Once created, schemaless tables cannot be altered to have a schema.

## Standard Tables
Tables with schemas have their data validation and schema evolution behavior governed by **Schema
Consistency Types** and **Schema Evolution Modes** to ensure that the table can always be materialized
with a unified schema at read time. By default, a DeltaCAT table created via `dc.create_table(table_name)`
infers a unified Arrow schema on write and rejects writes that would break reads for one or more supported
dataset types. Once created, standard tables cannot be altered to be schemaless.

## Schema Consistency Types
DeltaCAT table schemas can either be **inferred** to follow the shape of written data or **enforced**
to define the shape of written data. The default schema consistency type of all fields in a DeltaCAT
table schema is configured by setting the `DEFAULT_SCHEMA_CONSISTENCY_TYPE` table property to one
of the following values:



**NONE** (default): No data consistency checks are run. The schema field's type will be automatically
promoted to the most permissive Arrow data type that all values can be safely cast to using
`pyarrow.unify_schemas(schemas, promote_options="permissive")`. If safe casting is impossible,
then a `SchemaValidationError` will be raised.



**COERCE**: Coerce fields to fit the schema whenever possible, even if data truncation is required. Fields
will be coerced using either `pyarrow.compute.cast` or `daft.expression.cast` with default options. If the
field cannot be coerced to fit the given type, then a `SchemaValidationError` will be raised.



**VALIDATE**: Strict data consistency checks. An error is raised for any field that doesn't fit the schema.

A field's Schema Consistency Type can only be updated from least to most permissive (VALIDATE -> COERCE -> NONE).

## Schema Evolution Modes
Schema evolution modes control how schema changes are handled when writing to a table.
A table's schema evolution mode is configured by setting the `SCHEMA_EVOLUTION_MODE`
table property to one of the following values:



**AUTO** (default): Schema changes are automatically handled. New fields are added to
the schema with their Schema Consistency Type determined by the
`DEFAULT_SCHEMA_CONSISTENCY_TYPE` table property.



**MANUAL**: Schema changes must be made explicitly via `dc.alter_table()`. Attempts to
write data with fields not in the existing schema will raise a `SchemaValidationError`.



**DISABLED**: Schema changes are disabled. The schema that the table was first
created with is immutable.

A table's Schema Evolution Mode can be updated at any time.

## Arrow to File Format Type Mappings
The tables below show DeltaCAT's actual Arrow write type mappings across all supported dataset and content types.
These mappings are generated by:

1. Creating a PyArrow table with the target PyArrow data type via `pa.Table.from_arrays([pa.array(test_data, type=arrow_type)])`.
2. Casting to the target dataset type via `data = dc.from_pyarrow(pyarrow_table, target_dataset_type)`.
3. Writing to the target content type via `dc.write_to_table(data, table_name, content_type=target_content_type)`.

More details are available in the [type mapping generation script](../../deltacat/docs/autogen/schema/inference/generate_type_mappings.py).

### Runtime Environment
**Generation Date:** 2025-08-25

**PyArrow Version:** 16.0.0

**DeltaCAT Version:** 2.0.0b12

**Pandas Version:** 2.2.3

**Polars Version:** 1.28.1

**Daft Version:** 0.4.15

**Ray Version:** 2.46.0

### Type Mapping Tables

#### **binary**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ✅ | ✅ | Parquet:`BYTE_ARRAY`; Feather:`binary`; Avro:`bytes`; Orc:`binary` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`BYTE_ARRAY`; Feather:`binary`; Avro:`bytes`; Orc:`binary` |
| `polars` | ✅ | ✅ | ✅ | ✅ | Parquet:`BYTE_ARRAY`; Feather:`binary_view`; Avro:`bytes`; Orc:`binary` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`BYTE_ARRAY` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`BYTE_ARRAY` |

#### **bool**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ✅ | ✅ | Parquet:`BOOLEAN`; Feather:`bool`; Avro:`boolean`; Orc:`bool` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`BOOLEAN`; Feather:`bool`; Avro:`boolean`; Orc:`bool` |
| `polars` | ✅ | ✅ | ✅ | ✅ | Parquet:`BOOLEAN`; Feather:`bool`; Avro:`boolean`; Orc:`bool` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`BOOLEAN` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`BOOLEAN` |

#### **date32[day]**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ✅ | ✅ | Parquet:`INT32 (Date)`; Feather:`date32[day]`; Avro:`{'logicalType': 'date', 'type': 'int'}`; Orc:`date32[day]` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`INT32 (Date)`; Feather:`date32[day]`; Avro:`{'logicalType': 'date', 'type': 'int'}`; Orc:`date32[day]` |
| `polars` | ✅ | ✅ | ✅ | ✅ | Parquet:`INT32 (Date)`; Feather:`date32[day]`; Avro:`{'logicalType': 'date', 'type': 'int'}`; Orc:`date32[day]` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT32 (Date)` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT32 (Date)` |

#### **date64[ms]**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ✅ | ✅ | Parquet:`INT32 (Date)`; Feather:`date64[ms]`; Avro:`{'logicalType': 'local-timestamp-millis', 'type': 'long'}`; Orc:`timestamp[ns]` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`INT32 (Date)`; Feather:`date32[day]`; Avro:`{'logicalType': 'date', 'type': 'int'}`; Orc:`date32[day]` |
| `polars` | ✅ | ✅ | ✅ | ✅ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=false, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false))`; Feather:`timestamp[ms]`; Avro:`{'logicalType': 'local-timestamp-millis', 'type': 'long'}`; Orc:`timestamp[ns]` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=false, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false))` |
| `ray_dataset` | ❌ | ❌ | ❌ | ❌ |  |

#### **decimal128(1, 0)**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ✅ | ✅ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=1, scale=0))`; Feather:`decimal128(1, 0)`; Avro:`{'logicalType': 'decimal', 'precision': 1, 'type': 'bytes'}`; Orc:`decimal128` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=1, scale=0))`; Feather:`decimal128(1, 0)`; Avro:`{'logicalType': 'decimal', 'precision': 1, 'type': 'bytes'}`; Orc:`decimal128` |
| `polars` | ✅ | ✅ | ✅ | ✅ | Parquet:`INT32 (Decimal(precision=1, scale=0))`; Feather:`decimal128(1, 0)`; Avro:`{'logicalType': 'decimal', 'precision': 1, 'type': 'bytes'}`; Orc:`decimal128` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=1, scale=0))` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=1, scale=0))` |

#### **decimal128(38, 0)**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ✅ | ✅ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=38, scale=0))`; Feather:`decimal128(38, 0)`; Avro:`{'logicalType': 'decimal', 'precision': 38, 'type': 'bytes'}`; Orc:`decimal128` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=38, scale=0))`; Feather:`decimal128(38, 0)`; Avro:`{'logicalType': 'decimal', 'precision': 38, 'type': 'bytes'}`; Orc:`decimal128` |
| `polars` | ✅ | ✅ | ✅ | ✅ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=38, scale=0))`; Feather:`decimal128(38, 0)`; Avro:`{'logicalType': 'decimal', 'precision': 38, 'type': 'bytes'}`; Orc:`decimal128` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=38, scale=0))` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=38, scale=0))` |

#### **decimal128(38, 10)**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ✅ | ✅ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=38, scale=10))`; Feather:`decimal128(38, 10)`; Avro:`{'logicalType': 'decimal', 'precision': 38, 'scale': 10, 'type': 'bytes'}`; Orc:`decimal128` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=38, scale=10))`; Feather:`decimal128(38, 10)`; Avro:`{'logicalType': 'decimal', 'precision': 38, 'scale': 10, 'type': 'bytes'}`; Orc:`decimal128` |
| `polars` | ✅ | ✅ | ✅ | ✅ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=38, scale=10))`; Feather:`decimal128(38, 10)`; Avro:`{'logicalType': 'decimal', 'precision': 38, 'scale': 10, 'type': 'bytes'}`; Orc:`decimal128` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=38, scale=10))` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=38, scale=10))` |

#### **decimal128(5, 2)**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ✅ | ✅ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=5, scale=2))`; Feather:`decimal128(5, 2)`; Avro:`{'logicalType': 'decimal', 'precision': 5, 'scale': 2, 'type': 'bytes'}`; Orc:`decimal128` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=5, scale=2))`; Feather:`decimal128(5, 2)`; Avro:`{'logicalType': 'decimal', 'precision': 5, 'scale': 2, 'type': 'bytes'}`; Orc:`decimal128` |
| `polars` | ✅ | ✅ | ✅ | ✅ | Parquet:`INT32 (Decimal(precision=5, scale=2))`; Feather:`decimal128(5, 2)`; Avro:`{'logicalType': 'decimal', 'precision': 5, 'scale': 2, 'type': 'bytes'}`; Orc:`decimal128` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=5, scale=2))` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=5, scale=2))` |

#### **decimal256(1, 0)**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ❌ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=1, scale=0))`; Feather:`decimal256(1, 0)` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=1, scale=0))`; Feather:`decimal128(1, 0)`; Avro:`{'logicalType': 'decimal', 'precision': 1, 'type': 'bytes'}`; Orc:`decimal128` |
| `polars` | ❌ | ❌ | ❌ | ❌ |  |
| `daft` | ❌ | ❌ | ❌ | ❌ |  |
| `ray_dataset` | ❌ | ❌ | ❌ | ❌ |  |

#### **decimal256(5, 2)**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ❌ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=5, scale=2))`; Feather:`decimal256(5, 2)` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=5, scale=2))`; Feather:`decimal128(5, 2)`; Avro:`{'logicalType': 'decimal', 'precision': 5, 'scale': 2, 'type': 'bytes'}`; Orc:`decimal128` |
| `polars` | ❌ | ❌ | ❌ | ❌ |  |
| `daft` | ❌ | ❌ | ❌ | ❌ |  |
| `ray_dataset` | ❌ | ❌ | ❌ | ❌ |  |

#### **decimal256(76, 0)**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ❌ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=76, scale=0))`; Feather:`decimal256(76, 0)` |
| `pandas` | ✅ | ✅ | ❌ | ❌ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=76, scale=0))`; Feather:`decimal256(76, 0)` |
| `polars` | ❌ | ❌ | ❌ | ❌ |  |
| `daft` | ❌ | ❌ | ❌ | ❌ |  |
| `ray_dataset` | ❌ | ❌ | ❌ | ❌ |  |

#### **decimal256(76, 38)**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ❌ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=76, scale=38))`; Feather:`decimal256(76, 38)` |
| `pandas` | ✅ | ✅ | ❌ | ❌ | Parquet:`FIXED_LEN_BYTE_ARRAY (Decimal(precision=76, scale=38))`; Feather:`decimal256(76, 38)` |
| `polars` | ❌ | ❌ | ❌ | ❌ |  |
| `daft` | ❌ | ❌ | ❌ | ❌ |  |
| `ray_dataset` | ❌ | ❌ | ❌ | ❌ |  |

#### **dictionary<values=string, indices=int32, ordered=0>**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ❌ | Parquet:`BYTE_ARRAY (String)`; Feather:`dictionary<values=string, indices=int32, ordered=0>` |
| `pandas` | ✅ | ✅ | ❌ | ❌ | Parquet:`BYTE_ARRAY (String)`; Feather:`dictionary<values=string, indices=int8, ordered=0>` |
| `polars` | ✅ | ✅ | ❌ | ❌ | Parquet:`BYTE_ARRAY (String)`; Feather:`dictionary<values=string_view, indices=uint32, ordered=0>` |
| `daft` | ❌ | ❌ | ❌ | ❌ |  |
| `ray_dataset` | ❌ | ❌ | ❌ | ❌ |  |

#### **duration[ms]**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT64`; Feather:`duration[ms]` |
| `pandas` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT64`; Feather:`duration[ms]` |
| `polars` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT64`; Feather:`duration[ms]` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64` |

#### **duration[ns]**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT64`; Feather:`duration[ns]` |
| `pandas` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT64`; Feather:`duration[ns]` |
| `polars` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT64`; Feather:`duration[ns]` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64` |

#### **duration[s]**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT64`; Feather:`duration[s]` |
| `pandas` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT64`; Feather:`duration[s]` |
| `polars` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT64`; Feather:`duration[ms]` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64` |

#### **duration[us]**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT64`; Feather:`duration[us]` |
| `pandas` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT64`; Feather:`duration[us]` |
| `polars` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT64`; Feather:`duration[us]` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64` |

#### **extension<arrow.fixed_shape_tensor[value_type=int32, shape=[3,3]]>**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ❌ | Parquet:`3-Level List (LIST)`; Feather:`extension<arrow.fixed_shape_tensor[value_type=int32, shape=[3,3]]>` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`3-Level List (LIST)`; Feather:`list<item: int32>`; Avro:`{'type': 'array', 'items': ['null', 'int']}`; Orc:`list<item: int32>` |
| `polars` | ❌ | ❌ | ❌ | ❌ |  |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`3-Level List (LIST)` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`3-Level List (LIST)` |

#### **fixed_size_list<item: int32>[3]**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ✅ | Parquet:`3-Level List (LIST)`; Feather:`fixed_size_list<item: int32>[3]`; Orc:`list<item: int32>` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`3-Level List (LIST)`; Feather:`list<item: int32>`; Avro:`{'type': 'array', 'items': ['null', 'int']}`; Orc:`list<item: int32>` |
| `polars` | ✅ | ✅ | ❌ | ✅ | Parquet:`3-Level List (LIST)`; Feather:`fixed_size_list<item: int32>[3]`; Orc:`list<item: int32>` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`3-Level List (LIST)` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`3-Level List (LIST)` |

#### **halffloat**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ✅ | ❌ | Parquet:`FIXED_LEN_BYTE_ARRAY (Float16)`; Feather:`halffloat`; Avro:`float` |
| `pandas` | ✅ | ✅ | ✅ | ❌ | Parquet:`FIXED_LEN_BYTE_ARRAY (Float16)`; Feather:`halffloat`; Avro:`float` |
| `polars` | ✅ | ✅ | ✅ | ✅ | Parquet:`FLOAT`; Feather:`float`; Avro:`float`; Orc:`float` |
| `daft` | ❌ | ❌ | ❌ | ❌ |  |
| `ray_dataset` | ❌ | ❌ | ❌ | ❌ |  |

#### **float**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ✅ | ✅ | Parquet:`FLOAT`; Feather:`float`; Avro:`float`; Orc:`float` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`FLOAT`; Feather:`float`; Avro:`float`; Orc:`float` |
| `polars` | ✅ | ✅ | ✅ | ✅ | Parquet:`FLOAT`; Feather:`float`; Avro:`float`; Orc:`float` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`FLOAT` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`FLOAT` |

#### **double**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ✅ | ✅ | Parquet:`DOUBLE`; Feather:`double`; Avro:`double`; Orc:`double` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`DOUBLE`; Feather:`double`; Avro:`double`; Orc:`double` |
| `polars` | ✅ | ✅ | ✅ | ✅ | Parquet:`DOUBLE`; Feather:`double`; Avro:`double`; Orc:`double` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`DOUBLE` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`DOUBLE` |

#### **int16**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ✅ | Parquet:`INT32 (Int(bitWidth=16, isSigned=true))`; Feather:`int16`; Orc:`int16` |
| `pandas` | ✅ | ✅ | ❌ | ✅ | Parquet:`INT32 (Int(bitWidth=16, isSigned=true))`; Feather:`int16`; Orc:`int16` |
| `polars` | ✅ | ✅ | ❌ | ✅ | Parquet:`INT32 (Int(bitWidth=16, isSigned=true))`; Feather:`int16`; Orc:`int16` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT32 (Int(bitWidth=16, isSigned=true))` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT32 (Int(bitWidth=16, isSigned=true))` |

#### **int32**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ✅ | ✅ | Parquet:`INT32`; Feather:`int32`; Avro:`int`; Orc:`int32` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`INT32`; Feather:`int32`; Avro:`int`; Orc:`int32` |
| `polars` | ✅ | ✅ | ✅ | ✅ | Parquet:`INT32`; Feather:`int32`; Avro:`int`; Orc:`int32` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT32` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT32` |

#### **int64**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ✅ | ✅ | Parquet:`INT64`; Feather:`int64`; Avro:`long`; Orc:`int64` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`INT64`; Feather:`int64`; Avro:`long`; Orc:`int64` |
| `polars` | ✅ | ✅ | ✅ | ✅ | Parquet:`INT64`; Feather:`int64`; Avro:`long`; Orc:`int64` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64` |

#### **int8**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ✅ | Parquet:`INT32 (Int(bitWidth=8, isSigned=true))`; Feather:`int8`; Orc:`int8` |
| `pandas` | ✅ | ✅ | ❌ | ✅ | Parquet:`INT32 (Int(bitWidth=8, isSigned=true))`; Feather:`int8`; Orc:`int8` |
| `polars` | ✅ | ✅ | ❌ | ✅ | Parquet:`INT32 (Int(bitWidth=8, isSigned=true))`; Feather:`int8`; Orc:`int8` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT32 (Int(bitWidth=8, isSigned=true))` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT32 (Int(bitWidth=8, isSigned=true))` |

#### **large_binary**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ✅ | ✅ | Parquet:`BYTE_ARRAY`; Feather:`large_binary`; Avro:`bytes`; Orc:`binary` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`BYTE_ARRAY`; Feather:`binary`; Avro:`bytes`; Orc:`binary` |
| `polars` | ✅ | ✅ | ✅ | ✅ | Parquet:`BYTE_ARRAY`; Feather:`binary_view`; Avro:`bytes`; Orc:`binary` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`BYTE_ARRAY` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`BYTE_ARRAY` |

#### **large_list<item: int32>**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ✅ | ✅ | Parquet:`3-Level List (LIST)`; Feather:`large_list<item: int32>`; Avro:`{'type': 'array', 'items': ['null', 'int']}`; Orc:`list<item: int32>` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`3-Level List (LIST)`; Feather:`list<item: int32>`; Avro:`{'type': 'array', 'items': ['null', 'int']}`; Orc:`list<item: int32>` |
| `polars` | ✅ | ✅ | ✅ | ✅ | Parquet:`3-Level List (LIST)`; Feather:`large_list<item: int32>`; Avro:`{'type': 'array', 'items': ['null', 'int']}`; Orc:`list<item: int32>` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`3-Level List (LIST)` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`3-Level List (LIST)` |

#### **large_list_view<item: int32>**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ❌ | ❌ | ❌ | ❌ |  |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`3-Level List (LIST)`; Feather:`list<item: int32>`; Avro:`{'type': 'array', 'items': ['null', 'int']}`; Orc:`list<item: int32>` |
| `polars` | ❌ | ❌ | ❌ | ❌ |  |
| `daft` | ❌ | ❌ | ❌ | ❌ |  |
| `ray_dataset` | ❌ | ❌ | ❌ | ❌ |  |

#### **large_string**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ✅ | ✅ | Parquet:`BYTE_ARRAY (String)`; Feather:`large_string`; Avro:`string`; Orc:`string` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`BYTE_ARRAY (String)`; Feather:`string`; Avro:`string`; Orc:`string` |
| `polars` | ✅ | ✅ | ✅ | ✅ | Parquet:`BYTE_ARRAY (String)`; Feather:`string_view`; Avro:`string`; Orc:`string` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`BYTE_ARRAY (String)` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`BYTE_ARRAY (String)` |

#### **list<item: int32>**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ✅ | ✅ | Parquet:`3-Level List (LIST)`; Feather:`list<item: int32>`; Avro:`{'type': 'array', 'items': ['null', 'int']}`; Orc:`list<item: int32>` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`3-Level List (LIST)`; Feather:`list<item: int32>`; Avro:`{'type': 'array', 'items': ['null', 'int']}`; Orc:`list<item: int32>` |
| `polars` | ✅ | ✅ | ✅ | ✅ | Parquet:`3-Level List (LIST)`; Feather:`large_list<item: int32>`; Avro:`{'type': 'array', 'items': ['null', 'int']}`; Orc:`list<item: int32>` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`3-Level List (LIST)` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`3-Level List (LIST)` |

#### **list<item: string>**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ✅ | ✅ | Parquet:`3-Level List (LIST)`; Feather:`list<item: string>`; Avro:`{'type': 'array', 'items': ['null', 'string']}`; Orc:`list<item: string>` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`3-Level List (LIST)`; Feather:`list<item: string>`; Avro:`{'type': 'array', 'items': ['null', 'string']}`; Orc:`list<item: string>` |
| `polars` | ✅ | ✅ | ✅ | ✅ | Parquet:`3-Level List (LIST)`; Feather:`large_list<item: string_view>`; Avro:`{'type': 'array', 'items': ['null', 'string']}`; Orc:`list<item: string>` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`3-Level List (LIST)` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`3-Level List (LIST)` |

#### **list_view<item: int32>**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ❌ | ❌ | ❌ | ❌ |  |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`3-Level List (LIST)`; Feather:`list<item: int32>`; Avro:`{'type': 'array', 'items': ['null', 'int']}`; Orc:`list<item: int32>` |
| `polars` | ❌ | ❌ | ❌ | ❌ |  |
| `daft` | ❌ | ❌ | ❌ | ❌ |  |
| `ray_dataset` | ❌ | ❌ | ❌ | ❌ |  |

#### **map<string, int32>**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ✅ | ✅ | Parquet:`2-Level Map (MAP)`; Feather:`map<string, int32>`; Avro:`{'type': 'array', 'items': ['null', {'type': 'record', 'name': 'r1', 'fields': [{'name': 'key', 'type': ['null', 'string']}, {'name': 'value', 'type': ['null', 'int']}]}]}`; Orc:`map<string, int32>` |
| `pandas` | ❌ | ❌ | ❌ | ❌ |  |
| `polars` | ✅ | ✅ | ✅ | ✅ | Parquet:`4-Level List (LIST)`; Feather:`large_list<item: struct<key: string_view, value: int32>>`; Avro:`{'type': 'array', 'items': ['null', {'type': 'record', 'name': 'r1', 'fields': [{'name': 'key', 'type': ['null', 'string']}, {'name': 'value', 'type': ['null', 'int']}]}]}`; Orc:`list<item: struct<key: string, value: int32>>` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`2-Level Map (MAP)` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`2-Level Map (MAP)` |

#### **month_day_nano_interval**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ❌ | ✅ | ❌ | ❌ | Feather:`month_day_nano_interval` |
| `pandas` | ❌ | ✅ | ❌ | ❌ | Feather:`month_day_nano_interval` |
| `polars` | ❌ | ❌ | ❌ | ❌ |  |
| `daft` | ❌ | ❌ | ❌ | ❌ |  |
| `ray_dataset` | ❌ | ❌ | ❌ | ❌ |  |

#### **null**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT32 (Null)`; Feather:`null` |
| `pandas` | ✅ | ✅ | ✅ | ❌ | Parquet:`INT32 (Null)`; Feather:`null`; Avro:`string` |
| `polars` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT32 (Null)`; Feather:`null` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT32 (Null)` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT32 (Null)` |

#### **string**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ✅ | ✅ | Parquet:`BYTE_ARRAY (String)`; Feather:`string`; Avro:`string`; Orc:`string` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`BYTE_ARRAY (String)`; Feather:`string`; Avro:`string`; Orc:`string` |
| `polars` | ✅ | ✅ | ✅ | ✅ | Parquet:`BYTE_ARRAY (String)`; Feather:`string_view`; Avro:`string`; Orc:`string` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`BYTE_ARRAY (String)` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`BYTE_ARRAY (String)` |

#### **struct<name: string, age: int32>**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ✅ | ✅ | Parquet:`2-Level Struct`; Feather:`struct<name: string, age: int32>`; Avro:`{'type': 'record', 'name': 'r1', 'fields': [{'name': 'name', 'type': ['null', 'string']}, {'name': 'age', 'type': ['null', 'int']}]}`; Orc:`struct<name: string, age: int32>` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`2-Level Struct`; Feather:`struct<age: int64, name: string>`; Avro:`{'type': 'record', 'name': 'r1', 'fields': [{'name': 'age', 'type': ['null', 'long']}, {'name': 'name', 'type': ['null', 'string']}]}`; Orc:`struct<age: int64, name: string>` |
| `polars` | ✅ | ✅ | ✅ | ✅ | Parquet:`2-Level Struct`; Feather:`struct<name: string_view, age: int32>`; Avro:`{'type': 'record', 'name': 'r1', 'fields': [{'name': 'name', 'type': ['null', 'string']}, {'name': 'age', 'type': ['null', 'int']}]}`; Orc:`struct<name: string, age: int32>` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`2-Level Struct` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`2-Level Struct` |

#### **time32[ms]**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT32 (Time(isAdjustedToUTC=true, timeUnit=milliseconds))`; Feather:`time32[ms]` |
| `pandas` | ❌ | ❌ | ❌ | ❌ |  |
| `polars` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT64 (Time(isAdjustedToUTC=false, timeUnit=nanoseconds))`; Feather:`time64[ns]` |
| `daft` | ❌ | ❌ | ❌ | ❌ |  |
| `ray_dataset` | ❌ | ❌ | ❌ | ❌ |  |

#### **time32[s]**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT32 (Time(isAdjustedToUTC=true, timeUnit=milliseconds))`; Feather:`time32[s]` |
| `pandas` | ❌ | ❌ | ❌ | ❌ |  |
| `polars` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT64 (Time(isAdjustedToUTC=false, timeUnit=nanoseconds))`; Feather:`time64[ns]` |
| `daft` | ❌ | ❌ | ❌ | ❌ |  |
| `ray_dataset` | ❌ | ❌ | ❌ | ❌ |  |

#### **time64[ns]**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT64 (Time(isAdjustedToUTC=true, timeUnit=nanoseconds))`; Feather:`time64[ns]` |
| `pandas` | ❌ | ❌ | ❌ | ❌ |  |
| `polars` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT64 (Time(isAdjustedToUTC=false, timeUnit=nanoseconds))`; Feather:`time64[ns]` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64 (Time(isAdjustedToUTC=true, timeUnit=nanoseconds))` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64 (Time(isAdjustedToUTC=true, timeUnit=nanoseconds))` |

#### **time64[us]**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT64 (Time(isAdjustedToUTC=true, timeUnit=microseconds))`; Feather:`time64[us]` |
| `pandas` | ❌ | ❌ | ❌ | ❌ |  |
| `polars` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT64 (Time(isAdjustedToUTC=false, timeUnit=nanoseconds))`; Feather:`time64[ns]` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64 (Time(isAdjustedToUTC=true, timeUnit=microseconds))` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64 (Time(isAdjustedToUTC=true, timeUnit=microseconds))` |

#### **timestamp[ms]**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ✅ | ✅ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=false, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false))`; Feather:`timestamp[ms]`; Avro:`{'logicalType': 'local-timestamp-millis', 'type': 'long'}`; Orc:`timestamp[ns]` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=false, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false))`; Feather:`timestamp[ms]`; Avro:`{'logicalType': 'local-timestamp-millis', 'type': 'long'}`; Orc:`timestamp[ns]` |
| `polars` | ✅ | ✅ | ✅ | ✅ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=false, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false))`; Feather:`timestamp[ms]`; Avro:`{'logicalType': 'local-timestamp-millis', 'type': 'long'}`; Orc:`timestamp[ns]` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=false, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false))` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=false, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false))` |

#### **timestamp[ms, tz=UTC]**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ✅ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=true, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false))`; Feather:`timestamp[ms, tz=UTC]`; Orc:`timestamp[ns, tz=UTC]` |
| `pandas` | ✅ | ✅ | ❌ | ✅ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=true, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false))`; Feather:`timestamp[ms, tz=UTC]`; Orc:`timestamp[ns, tz=UTC]` |
| `polars` | ✅ | ✅ | ❌ | ✅ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=true, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false))`; Feather:`timestamp[ms, tz=UTC]`; Orc:`timestamp[ns, tz=UTC]` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=true, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false))` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=true, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false))` |

#### **timestamp[ns]**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ✅ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=false, timeUnit=nanoseconds, is_from_converted_type=false, force_set_converted_type=false))`; Feather:`timestamp[ns]`; Orc:`timestamp[ns]` |
| `pandas` | ✅ | ✅ | ❌ | ✅ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=false, timeUnit=nanoseconds, is_from_converted_type=false, force_set_converted_type=false))`; Feather:`timestamp[ns]`; Orc:`timestamp[ns]` |
| `polars` | ✅ | ✅ | ❌ | ✅ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=false, timeUnit=nanoseconds, is_from_converted_type=false, force_set_converted_type=false))`; Feather:`timestamp[ns]`; Orc:`timestamp[ns]` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=false, timeUnit=nanoseconds, is_from_converted_type=false, force_set_converted_type=false))` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=false, timeUnit=nanoseconds, is_from_converted_type=false, force_set_converted_type=false))` |

#### **timestamp[ns, tz=UTC]**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ✅ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=true, timeUnit=nanoseconds, is_from_converted_type=false, force_set_converted_type=false))`; Feather:`timestamp[ns, tz=UTC]`; Orc:`timestamp[ns, tz=UTC]` |
| `pandas` | ✅ | ✅ | ❌ | ✅ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=true, timeUnit=nanoseconds, is_from_converted_type=false, force_set_converted_type=false))`; Feather:`timestamp[ns, tz=UTC]`; Orc:`timestamp[ns, tz=UTC]` |
| `polars` | ✅ | ✅ | ❌ | ✅ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=true, timeUnit=nanoseconds, is_from_converted_type=false, force_set_converted_type=false))`; Feather:`timestamp[ns, tz=UTC]`; Orc:`timestamp[ns, tz=UTC]` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=true, timeUnit=nanoseconds, is_from_converted_type=false, force_set_converted_type=false))` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=true, timeUnit=nanoseconds, is_from_converted_type=false, force_set_converted_type=false))` |

#### **timestamp[s]**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ✅ | ✅ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=false, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false))`; Feather:`timestamp[s]`; Avro:`{'logicalType': 'local-timestamp-millis', 'type': 'long'}`; Orc:`timestamp[ns]` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=false, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false))`; Feather:`timestamp[s]`; Avro:`{'logicalType': 'local-timestamp-millis', 'type': 'long'}`; Orc:`timestamp[ns]` |
| `polars` | ✅ | ✅ | ✅ | ✅ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=false, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false))`; Feather:`timestamp[ms]`; Avro:`{'logicalType': 'local-timestamp-millis', 'type': 'long'}`; Orc:`timestamp[ns]` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=false, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false))` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=false, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false))` |

#### **timestamp[s, tz=UTC]**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ✅ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=true, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false))`; Feather:`timestamp[s, tz=UTC]`; Orc:`timestamp[ns, tz=UTC]` |
| `pandas` | ✅ | ✅ | ❌ | ✅ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=true, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false))`; Feather:`timestamp[s, tz=UTC]`; Orc:`timestamp[ns, tz=UTC]` |
| `polars` | ✅ | ✅ | ❌ | ✅ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=true, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false))`; Feather:`timestamp[ms, tz=UTC]`; Orc:`timestamp[ns, tz=UTC]` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=true, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false))` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=true, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false))` |

#### **timestamp[us]**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ✅ | ✅ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false))`; Feather:`timestamp[us]`; Avro:`{'logicalType': 'local-timestamp-micros', 'type': 'long'}`; Orc:`timestamp[ns]` |
| `pandas` | ✅ | ✅ | ✅ | ✅ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false))`; Feather:`timestamp[us]`; Avro:`{'logicalType': 'local-timestamp-micros', 'type': 'long'}`; Orc:`timestamp[ns]` |
| `polars` | ✅ | ✅ | ✅ | ✅ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false))`; Feather:`timestamp[us]`; Avro:`{'logicalType': 'local-timestamp-micros', 'type': 'long'}`; Orc:`timestamp[ns]` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false))` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false))` |

#### **timestamp[us, tz=UTC]**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ✅ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=true, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false))`; Feather:`timestamp[us, tz=UTC]`; Orc:`timestamp[ns, tz=UTC]` |
| `pandas` | ✅ | ✅ | ❌ | ✅ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=true, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false))`; Feather:`timestamp[us, tz=UTC]`; Orc:`timestamp[ns, tz=UTC]` |
| `polars` | ✅ | ✅ | ❌ | ✅ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=true, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false))`; Feather:`timestamp[us, tz=UTC]`; Orc:`timestamp[ns, tz=UTC]` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=true, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false))` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64 (Timestamp(isAdjustedToUTC=true, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false))` |

#### **uint16**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT32 (Int(bitWidth=16, isSigned=false))`; Feather:`uint16` |
| `pandas` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT32 (Int(bitWidth=16, isSigned=false))`; Feather:`uint16` |
| `polars` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT32 (Int(bitWidth=16, isSigned=false))`; Feather:`uint16` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT32 (Int(bitWidth=16, isSigned=false))` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT32 (Int(bitWidth=16, isSigned=false))` |

#### **uint32**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT32 (Int(bitWidth=32, isSigned=false))`; Feather:`uint32` |
| `pandas` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT32 (Int(bitWidth=32, isSigned=false))`; Feather:`uint32` |
| `polars` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT32 (Int(bitWidth=32, isSigned=false))`; Feather:`uint32` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT32 (Int(bitWidth=32, isSigned=false))` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT32 (Int(bitWidth=32, isSigned=false))` |

#### **uint64**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT64 (Int(bitWidth=64, isSigned=false))`; Feather:`uint64` |
| `pandas` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT64 (Int(bitWidth=64, isSigned=false))`; Feather:`uint64` |
| `polars` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT64 (Int(bitWidth=64, isSigned=false))`; Feather:`uint64` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64 (Int(bitWidth=64, isSigned=false))` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT64 (Int(bitWidth=64, isSigned=false))` |

#### **uint8**
| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |
|--------------|---------|---------|------|-----|---------------|
| `pyarrow` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT32 (Int(bitWidth=8, isSigned=false))`; Feather:`uint8` |
| `pandas` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT32 (Int(bitWidth=8, isSigned=false))`; Feather:`uint8` |
| `polars` | ✅ | ✅ | ❌ | ❌ | Parquet:`INT32 (Int(bitWidth=8, isSigned=false))`; Feather:`uint8` |
| `daft` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT32 (Int(bitWidth=8, isSigned=false))` |
| `ray_dataset` | ✅ | ❌ | ❌ | ❌ | Parquet:`INT32 (Int(bitWidth=8, isSigned=false))` |

## Read Compatibility Tables


The following tables show read compatibility for each Arrow type across available writer/reader combinations.


This information is automatically used by DeltaCAT at write time to ensure that data written in one format can be
read by all supported reader types defined in a table's `SUPPORTED_READER_TYPES` table property. If data to be
written cannot be read by one or more supported reader types, then a `TableValidationError` will be raised.

### binary


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ❌ | ❌ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### bool


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### date32[day]


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### date64[ms]


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ❌ | ❌ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ❌ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ❌ | ❌ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ❌ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


### decimal128(1, 0)


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### decimal128(38, 0)


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### decimal128(38, 10)


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### decimal128(5, 2)


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### decimal256(1, 0)


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | ❌ | ✅ | ❌ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | ✅ | ✅ | ❌ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


### decimal256(5, 2)


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | ❌ | ✅ | ❌ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | ✅ | ✅ | ❌ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


### decimal256(76, 0)


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ❌ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | ❌ | ✅ | ❌ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ✅ | ✅ | ❌ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | ✅ | ✅ | ❌ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


### decimal256(76, 38)


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ❌ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | ❌ | ✅ | ❌ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ✅ | ✅ | ❌ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | ✅ | ✅ | ❌ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


### dictionary<values=string, indices=int32, ordered=0>


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ❌ | ✅ | ❌ | ❌ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ❌ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


### double


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### duration[ms]


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### duration[ns]


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### duration[s]


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### duration[us]


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### extension<arrow.fixed_shape_tensor[value_type=int32, shape=[3,3]]>


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | ❌ | ✅ | ❌ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ❌ | ❌ | ❌ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | ✅ | ✅ | ❌ | ✅ | ✅ |
| **ray_dataset** | ✅ | ❌ | ❌ | ❌ | ✅ |


### fixed_size_list<item: int32>[3]


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### float


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### halffloat


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


### int16


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### int32


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### int64


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### int8


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### large_binary


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ❌ | ❌ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### large_list<item: int32>


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### large_list_view<item: int32>


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


### large_string


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ❌ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### list<item: int32>


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### list<item: string>


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ❌ | ✅ | ❌ | ❌ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### list_view<item: int32>


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


### map<string, int32>


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ❌ | ❌ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | ❌ | ✅ | ✅ | ❌ | ❌ |
| **pyarrow** | ❌ | ✅ | ❌ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ❌ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ❌ | ✅ | ✅ |
| **pandas** | — | — | — | — | — |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ❌ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ❌ | ✅ | ✅ |


### month_day_nano_interval


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ❌ | ✅ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | ❌ | ✅ | ❌ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


### null


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ❌ | ❌ | ✅ |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### string


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ❌ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### struct<name: string, age: int32>


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ❌ | ❌ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ❌ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ❌ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### time32[ms]


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ❌ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ❌ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


### time32[s]


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ❌ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ❌ | ✅ | ❌ | ✅ |
| **ray_dataset** | — | — | — | — | — |


### time64[ns]


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ❌ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ❌ | ✅ | ✅ | ✅ |
| **pandas** | — | — | — | — | — |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ❌ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ❌ | ✅ | ✅ | ✅ |


### time64[us]


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ❌ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ❌ | ✅ | ✅ | ✅ |
| **pandas** | — | — | — | — | — |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ❌ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ❌ | ✅ | ✅ | ✅ |


### timestamp[ms, tz=UTC]


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### timestamp[ms]


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### timestamp[ns, tz=UTC]


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### timestamp[ns]


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### timestamp[s, tz=UTC]


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### timestamp[s]


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### timestamp[us, tz=UTC]


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### timestamp[us]


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### uint16


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### uint32


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### uint64


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |


### uint8


#### application/avro

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/feather

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | — | — | — | — | — |


#### application/orc

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | — | — | — | — | — |
| **pandas** | — | — | — | — | — |
| **polars** | — | — | — | — | — |
| **pyarrow** | — | — | — | — | — |
| **ray_dataset** | — | — | — | — | — |


#### application/parquet

| Writer \ Reader | daft | pandas | polars | pyarrow | ray_dataset |
|---|---|---|---|---|---|
| **daft** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pandas** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **polars** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **pyarrow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **ray_dataset** | ✅ | ✅ | ✅ | ✅ | ✅ |
