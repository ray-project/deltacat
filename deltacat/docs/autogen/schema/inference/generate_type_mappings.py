"""
Proper approach: Use dc.list with recursive=True and table-specific URLs
to correctly map files to the tests that wrote them.
"""

import os
import json
import tempfile
import uuid
from polars.exceptions import PanicException
from datetime import datetime, date
from decimal import Decimal
from typing import List, Dict, Any, Tuple
import numpy as np

import deltacat as dc
from deltacat import Catalog
from deltacat.catalog import CatalogProperties
from deltacat.types.media import ContentType, DatasetType
from deltacat.types.tables import from_pyarrow, TableWriteMode
from deltacat.storage import Metafile, Delta

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.orc as orc
import pyarrow.feather as feather


def get_version_info():
    """Capture version information for all libraries."""
    version_info = {
        "test_date": datetime.now().isoformat(),
        "pyarrow_version": pa.__version__,
    }

    # Get DeltaCAT version
    try:
        version_info["deltacat_version"] = dc.__version__
    except AttributeError:
        # Fallback if __version__ not available
        try:
            import pkg_resources

            version_info["deltacat_version"] = pkg_resources.get_distribution(
                "deltacat"
            ).version
        except Exception:
            version_info["deltacat_version"] = "unknown"

    # Get Pandas version
    try:
        import pandas as pd

        version_info["pandas_version"] = pd.__version__
    except ImportError:
        version_info["pandas_version"] = "not_available"

    # Get Polars version
    try:
        import polars as pl

        version_info["polars_version"] = pl.__version__
    except ImportError:
        version_info["polars_version"] = "not_available"

    # Get Daft version
    try:
        import daft

        version_info["daft_version"] = daft.__version__
    except (ImportError, AttributeError):
        version_info["daft_version"] = "not_available"

    # Get Ray version
    try:
        import ray

        version_info["ray_version"] = ray.__version__
    except (ImportError, AttributeError):
        version_info["ray_version"] = "not_available"

    return version_info


def get_comprehensive_test_types() -> List[Tuple[str, str, List[Any]]]:
    """Get comprehensive Arrow types for testing."""
    return [
        # Integer types
        ("int8", "pa.int8()", [127, -128, 0]),
        ("int16", "pa.int16()", [32767, -32768, 1000]),
        ("int32", "pa.int32()", [2147483647, -2147483648, 1000]),
        ("int64", "pa.int64()", [9223372036854775807, -9223372036854775808, 1000]),
        ("uint8", "pa.uint8()", [255, 0, 128]),
        ("uint16", "pa.uint16()", [65535, 0, 1000]),
        ("uint32", "pa.uint32()", [4294967295, 0, 1000]),
        ("uint64", "pa.uint64()", [18446744073709551615, 0, 1000]),
        # Float types
        ("float16", "pa.float16()", np.array([1.5, np.nan], dtype=np.float16)),
        ("float32", "pa.float32()", [3.14159, -2.71828, 1.41421]),
        ("float64", "pa.float64()", [1.123456789, -2.987654321, 3.141592653589793]),
        # Boolean and null
        ("bool_", "pa.bool_()", [True, False, True]),
        ("null", "pa.null()", [None, None, None]),
        # String types
        ("string", "pa.string()", ["hello", "world", "test"]),
        (
            "large_string",
            "pa.large_string()",
            ["large hello", "large world", "large test"],
        ),
        # Binary types
        ("binary", "pa.binary()", [b"hello", b"world", b"test"]),
        (
            "large_binary",
            "pa.large_binary()",
            [b"large hello", b"large world", b"large test"],
        ),
        # Date and time types
        (
            "date32",
            "pa.date32()",
            [date(2023, 1, 1), date(2023, 12, 31), date(2024, 6, 15)],
        ),
        (
            "date64",
            "pa.date64()",
            [date(2023, 1, 1), date(2023, 12, 31), date(2024, 6, 15)],
        ),
        ("time32_s", "pa.time32('s')", [1754962113, 1754962114, 1754962115]),
        ("time32_ms", "pa.time32('ms')", [1754962113, 1754962114, 1754962115]),
        (
            "time64_us",
            "pa.time64('us')",
            [1754962113000000, 1754962114000000, 1754962115000000],
        ),
        (
            "time64_ns",
            "pa.time64('ns')",
            [1754962113000000000, 1754962114000000000, 1754962115000000000],
        ),
        (
            "timestamp_us",
            "pa.timestamp('us')",
            [
                datetime(2023, 1, 1, 12, 0, 0),
                datetime(2023, 12, 31, 23, 59, 59),
                datetime(2024, 6, 15, 10, 30, 45),
            ],
        ),
        (
            "timestamp_s_utc",
            "pa.timestamp('s', tz='UTC')",
            [
                datetime(2023, 1, 1, 12, 0, 0),
                datetime(2023, 12, 31, 23, 59, 59),
                datetime(2024, 6, 15, 10, 30, 45),
            ],
        ),
        ("duration_s", "pa.duration('s')", [1754962113, 1754962114, 1754962115]),
        (
            "duration_ms",
            "pa.duration('ms')",
            [1754962113000, 1754962114000, 1754962115000],
        ),
        (
            "duration_us",
            "pa.duration('us')",
            [1754962113000000, 1754962114000000, 1754962115000000],
        ),
        (
            "duration_ns",
            "pa.duration('ns')",
            [1754962113000000000, 1754962114000000000, 1754962115000000000],
        ),
        (
            "month_day_nano",
            "pa.month_day_nano_interval()",
            [
                pa.scalar((1, 15, -30), type=pa.month_day_nano_interval()),
                pa.scalar((2, 15, -30), type=pa.month_day_nano_interval()),
                pa.scalar((3, 15, -30), type=pa.month_day_nano_interval()),
            ],
        ),
        # Decimal
        (
            "decimal128_5_2",
            "pa.decimal128(5, 2)",
            [Decimal("123.45"), Decimal("-67.89"), Decimal("999.99")],
        ),
        (
            "decimal128_38_0",
            "pa.decimal128(38, 0)",
            [Decimal("12345678901234567890123456789012345678"), Decimal("-12345678901234567890123456789012345678"), Decimal("0")],
        ),
        (
            "decimal128_1_0",
            "pa.decimal128(1, 0)",
            [Decimal("1"), Decimal("2"), Decimal("3")],
        ),
        (
            "decimal128_38_10",
            "pa.decimal128(38, 10)",
            [Decimal("1234567890123456789012345678.9012345678"), Decimal("-1234567890123456789012345678.9012345678"), Decimal("0.0000000000")],
        ),
        (
            "decimal256_76_0",
            "pa.decimal256(76, 0)",
            [Decimal("1234567890123456789012345678901234567812345678901234567890123456789012345678"), Decimal("-0"), Decimal("0")],
        ),
        (
            "decimal256_1_0",
            "pa.decimal256(1, 0)",
            [Decimal("1"), Decimal("2"), Decimal("3")],
        ),
        (
            "decimal256_5_2",
            "pa.decimal256(5, 2)",
            [Decimal("123.45"), Decimal("-67.89"), Decimal("999.99")],
        ),
        (
            "decimal256_76_38",
            "pa.decimal256(76, 38)",
            [Decimal("12345678901234567890123456789012345678.12345678901234567890123456789012345678"), Decimal("-0.00000000000000000000000000000000000000"), Decimal("0.00000000000000000000000000000000000000")],
        ),
        # List types
        ("list_int32", "pa.list_(pa.int32())", [[1, 2, 3], [4, 5], [6, 7, 8, 9]]),
        ("list_string", "pa.list_(pa.string())", [["a", "b"], ["c", "d", "e"], ["f"]]),
        # Struct type
        (
            "struct_simple",
            "pa.struct([('name', pa.string()), ('age', pa.int32())])",
            [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25},
                {"name": "Charlie", "age": 35},
            ],
        ),
        # Dictionary type
        (
            "dictionary_string",
            "pa.dictionary(pa.int32(), pa.string())",
            ["apple", "banana", "apple"],
        ),
        # Map type
        (
            "map_string_int32",
            "pa.map_(pa.string(), pa.int32())",
            [{"a": 1, "b": 2}, {"c": 3, "d": 4}, {"e": 5}],
        ),
    ]


def extract_file_paths_from_deltas(all_objects: List[Any]) -> List[str]:
    """Extract file paths from Delta objects by parsing manifest entries."""
    file_paths = []

    for obj in all_objects:
        obj_type = Metafile.get_class(obj)

        if obj_type == Delta:
            delta_obj = obj
            # Access manifest entries to get file paths
            if hasattr(delta_obj, "manifest") and delta_obj.manifest:
                manifest = delta_obj.manifest
                if hasattr(manifest, "entries") and manifest.entries:
                    for entry in manifest.entries:
                        file_url = entry.uri or entry.url

                        # Convert file:// URLs to local paths
                        if file_url.startswith("file://"):
                            file_path = file_url[7:]
                        else:
                            file_path = file_url

                        file_paths.append(file_path)

    return file_paths


def inspect_specific_file_physical_schema(
    file_path: str, content_type: ContentType
) -> Dict[str, Any]:
    """Inspect the physical schema of a specific file."""

    try:
        if not os.path.exists(file_path):
            return {"error": f"File not found: {file_path}"}

        if content_type == ContentType.PARQUET:
            parquet_file = pq.ParquetFile(file_path)
            arrow_schema = parquet_file.schema_arrow
            parquet_schema = parquet_file.schema
            parquet_schema_string = str(parquet_schema)
            column_info = {}
            parquet_col_index = 0

            for i in range(len(arrow_schema)):
                arrow_field = arrow_schema.field(i)
                arrow_type_str = str(arrow_field.type)

                # For collection types, we need to handle them specially
                col = parquet_schema.column(parquet_col_index)
                if col.max_definition_level > 1 or col.max_repetition_level > 1:
                    parquet_physical_type_name_suffix = "Unknown"
                    if col.max_repetition_level > 0 and "list" in parquet_schema_string:
                        parquet_physical_type_name_suffix = "List"
                    elif col.max_definition_level > 0:
                        if "map" in parquet_schema_string:
                            parquet_physical_type_name_suffix = "Map"
                        else:
                            parquet_physical_type_name_suffix = "Struct"
                    parquet_physical_type_name_prefix = (
                        f"{col.max_definition_level}-Level"
                        if col.max_definition_level > 0
                        else ""
                    )
                    parquet_physical_type_name = f"{parquet_physical_type_name_prefix} {parquet_physical_type_name_suffix}"
                    parquet_logical_type_name = (
                        "LIST"
                        if "(List)" in parquet_schema_string
                        else "MAP"
                        if "(Map)" in parquet_schema_string
                        else ""
                    )
                    # For collection types, use the Arrow type as the "physical" representation
                    # since Parquet's physical schema doesn't directly represent these structures
                    print(f"Logical Type: {parquet_logical_type_name}")
                    print(f"Physical Type: {parquet_physical_type_name}")
                    print(f"Path: {col.path}")
                    print(f"Max Definition Level: {col.max_definition_level}")
                    print(f"Max Repetition Level: {col.max_repetition_level}")
                    column_info[f"column_{i}"] = {
                        "arrow_type": arrow_type_str,
                        "parquet_physical_type": parquet_physical_type_name,
                        "parquet_logical_type": parquet_logical_type_name,
                        "parquet_converted_type": "unknown",
                        "nullable": arrow_field.nullable,
                    }
                    # Skip the nested columns that are part of this complex type
                    if "list<" in arrow_type_str.lower():
                        parquet_col_index += 1  # Lists have nested structure
                    elif "struct<" in arrow_type_str.lower():
                        # Count the number of fields in the struct
                        struct_fields = (
                            arrow_type_str.count(",") + 1
                            if "," in arrow_type_str
                            else 1
                        )
                        parquet_col_index += struct_fields
                    elif "dictionary<" in arrow_type_str.lower():
                        parquet_col_index += 1  # Dictionary has values storage
                else:
                    # For simple types, use the actual Parquet column info
                    try:
                        col = parquet_schema.column(parquet_col_index)
                        column_info[f"column_{i}"] = {
                            "arrow_type": arrow_type_str,
                            "parquet_physical_type": str(col.physical_type),
                            "parquet_logical_type": str(col.logical_type)
                            if col.logical_type
                            else None,
                            "parquet_converted_type": str(col.converted_type)
                            if col.converted_type
                            else None,
                            "nullable": arrow_field.nullable,
                        }
                        parquet_col_index += 1
                    except (IndexError, Exception):
                        # Fallback if we can't match to parquet column
                        column_info[f"column_{i}"] = {
                            "arrow_type": arrow_type_str,
                            "parquet_physical_type": "UNKNOWN",
                            "parquet_logical_type": None,
                            "parquet_converted_type": None,
                            "nullable": arrow_field.nullable,
                        }

            return {
                "format": "parquet",
                "columns": column_info,
                "file_size": os.path.getsize(file_path),
                "file_path": file_path,
            }

        elif content_type == ContentType.FEATHER:
            feather_table = feather.read_table(file_path)

            column_info = {}
            for i, field in enumerate(feather_table.schema):
                column_info[f"column_{i}"] = {
                    "arrow_type": str(field.type),
                    "feather_preserved_type": str(field.type),
                    "nullable": field.nullable,
                }

            return {
                "format": "feather",
                "columns": column_info,
                "file_size": os.path.getsize(file_path),
                "file_path": file_path,
            }

        elif content_type == ContentType.AVRO:
            # For Avro, use fastavro to read the schema
            import fastavro

            with open(file_path, "rb") as f:
                reader = fastavro.reader(f)
                avro_schema = reader.writer_schema

            column_info = {}
            if "fields" in avro_schema:
                for i, field in enumerate(avro_schema["fields"]):
                    field_type = field["type"]
                    # Handle union types (used for nullable fields)
                    if isinstance(field_type, list):
                        # Find the non-null type in union
                        non_null_types = [t for t in field_type if t != "null"]
                        if non_null_types:
                            field_type = non_null_types[0]
                        nullable = "null" in field_type
                    else:
                        nullable = False

                    column_info[f"column_{i}"] = {
                        "field_name": field["name"],
                        "avro_type": str(field_type),
                        "nullable": nullable,
                        "original_field": field,
                    }

            return {
                "format": "avro",
                "columns": column_info,
                "avro_schema": avro_schema,
                "file_size": os.path.getsize(file_path),
                "file_path": file_path,
            }

        elif content_type == ContentType.ORC:
            orc_file = orc.ORCFile(file_path)

            column_info = {}
            for i, field in enumerate(orc_file.schema):
                column_info[f"column_{i}"] = {
                    "arrow_type": str(field.type),
                    "orc_type_kind": str(field.type).split("(")[0]
                    if "(" in str(field.type)
                    else str(field.type),
                    "nullable": field.nullable,
                }

            return {
                "format": "orc",
                "columns": column_info,
                "file_size": os.path.getsize(file_path),
                "file_path": file_path,
            }

    except (PanicException, Exception) as e:
        return {
            "error": f"Physical inspection failed: {str(e)}",
            "error_type": type(e).__name__,
            "file_path": file_path,
        }


def run_single_test(
    arrow_type_name: str,
    arrow_type_code: str,
    test_data: List[Any],
    dataset_type: DatasetType,
    content_type: ContentType,
    catalog_name: str,
) -> Dict[str, Any]:
    """Run a single test with proper file-to-test mapping using dc.list."""

    try:
        # Create Arrow table
        arrow_type = eval(arrow_type_code)
        arrow_table = pa.Table.from_arrays(
            [pa.array(test_data, type=arrow_type)], names=[arrow_type_name]
        )

        # Convert to dataset type
        write_dataset = from_pyarrow(arrow_table, dataset_type)

        # Create unique table name with timestamp to avoid conflicts
        timestamp = datetime.now().strftime("%H%M%S%f")
        table_name = f"test_{arrow_type_name}_{dataset_type.value}_{content_type.value.replace('/', '_')}_{timestamp}"
        namespace = "test_namespace"

        print(f"    Writing to table: {table_name}")

        # Write to DeltaCAT
        dc.write_to_table(
            data=write_dataset,
            table=table_name,
            namespace=namespace,
            catalog=catalog_name,
            mode=TableWriteMode.CREATE,
            content_type=content_type,
        )

        # Read back and verify
        read_result = dc.read_table(
            table=table_name,
            namespace=namespace,
            catalog=catalog_name,
            read_as=DatasetType.PYARROW,
            max_parallelism=1,
        )

        # Use dc.list with recursive=True to find the objects for this specific table
        table_url = dc.DeltaCatUrl(f"dc://{catalog_name}/{namespace}/{table_name}")
        print(f"    Listing objects for: {table_url}")

        try:
            table_objects = dc.list(table_url, recursive=True)
            print(f"    Found {len(table_objects)} objects for table")

            # Extract file paths from Delta objects
            file_paths = extract_file_paths_from_deltas(table_objects)
            print(f"    Extracted {len(file_paths)} file paths")

            if file_paths:
                # Use the first file path (should be the one we just wrote)
                file_path = file_paths[0]
                print(f"    Inspecting file: {file_path}")

                # Inspect the physical schema of this specific file
                physical_schema = inspect_specific_file_physical_schema(
                    file_path, content_type
                )
            else:
                physical_schema = {"error": "No file paths found in Delta objects"}

        except Exception as e:
            physical_schema = {"error": f"Failed to list table objects: {str(e)}"}

        return {
            "arrow_type": arrow_type_name,
            "dataset_type": dataset_type.value,
            "content_type": content_type.value,
            "success": True,
            "original_arrow_type": str(arrow_type),
            "read_back_type": str(read_result.schema.field(0).type)
            if hasattr(read_result, "schema")
            else "unknown",
            "physical_schema": physical_schema,
            "type_preserved": str(arrow_type) == str(read_result.schema.field(0).type)
            if hasattr(read_result, "schema")
            else False,
            "error": None,
            "table_name": table_name,
        }

    except (PanicException, Exception) as e:
        print(f"    Test failed with error: {str(e)}")
        return {
            "arrow_type": arrow_type_name,
            "dataset_type": dataset_type.value,
            "content_type": content_type.value,
            "success": False,
            "original_arrow_type": str(eval(arrow_type_code))
            if "eval" not in str(e)
            else "unknown",
            "physical_schema": {},
            "error": str(e),
            "error_category": "unknown",
            "table_name": f"failed_{arrow_type_name}_{dataset_type.value}",
        }


def main():
    print("=" * 80)
    print("PROPER PHYSICAL SCHEMA EXTRACTION TEST")
    print("=" * 80)
    print("Using dc.list with table-specific URLs to map files to tests")

    # Setup
    temp_dir = tempfile.mkdtemp()
    catalog_name = f"test-catalog-{uuid.uuid4()}"
    catalog_props = CatalogProperties(root=temp_dir)
    dc.put_catalog(catalog_name, catalog=Catalog(config=catalog_props))

    print(f"Using catalog directory: {temp_dir}")

    arrow_types = get_comprehensive_test_types()
    dataset_types = [
        DatasetType.PYARROW,
        DatasetType.PANDAS,
        DatasetType.POLARS,
        DatasetType.DAFT,
        DatasetType.RAY_DATASET,
    ]  # All dataset types
    content_types = [
        ContentType.PARQUET,
        ContentType.FEATHER,
        ContentType.AVRO,
        ContentType.ORC,
    ]  # Test 4 content types

    print(
        f"Testing {len(arrow_types)} Arrow types × {len(dataset_types)} dataset types × {len(content_types)} content types"
    )
    print()

    all_results = []
    test_count = 0
    total_tests = len(arrow_types) * len(dataset_types) * len(content_types)

    for arrow_type_name, arrow_type_code, test_data in arrow_types:
        print(f"Testing PyArrow type: {arrow_type_name}")

        for dataset_type in dataset_types:
            for content_type in content_types:
                test_count += 1
                print(
                    f"  [{test_count:2d}/{total_tests}] {dataset_type.value} → {content_type.value}"
                )

                result = run_single_test(
                    arrow_type_name,
                    arrow_type_code,
                    test_data,
                    dataset_type,
                    content_type,
                    catalog_name,
                )

                if result["success"]:
                    if result["physical_schema"].get("error"):
                        print(
                            f"    ❌ Physical schema error: {result['physical_schema']['error']}"
                        )
                    else:
                        # Show extracted physical type
                        columns = result["physical_schema"].get("columns", {})
                        if columns:
                            first_col = next(iter(columns.values()))
                            if content_type == ContentType.PARQUET:
                                physical_type = first_col.get(
                                    "parquet_physical_type", "unknown"
                                )
                                print(f"    ✅ Physical type: {physical_type}")
                            elif content_type == ContentType.FEATHER:
                                physical_type = first_col.get(
                                    "feather_preserved_type", "unknown"
                                )
                                print(f"    ✅ Physical type: {physical_type}")
                            elif content_type == ContentType.AVRO:
                                physical_type = first_col.get("avro_type", "unknown")
                                print(f"    ✅ Physical type: {physical_type}")
                            elif content_type == ContentType.ORC:
                                physical_type = first_col.get(
                                    "orc_type_kind", "unknown"
                                )
                                print(f"    ✅ Physical type: {physical_type}")
                        else:
                            print(f"    ❌ No column info found")
                else:
                    print(f"    ❌ {result.get('error', 'unknown')}")

                all_results.append(result)
        print()

    # Save detailed results with version information
    version_info = get_version_info()
    output_data = {"metadata": version_info, "test_results": all_results}

    output_file_name = "generate_type_mappings_results.json"
    with open(output_file_name, "w") as f:
        json.dump(output_data, f, indent=2, default=str)

    print(f"Detailed results saved to: {output_file_name}")
    print(f"Catalog directory: {temp_dir}")

    # Don't cleanup for manual inspection
    print("NOTE: Catalog directory not cleaned up for manual inspection")


if __name__ == "__main__":
    main()
