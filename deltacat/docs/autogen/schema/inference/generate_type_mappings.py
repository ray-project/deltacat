import os
import json
import pickle
import tempfile
import uuid
import base64
import shutil
from datetime import datetime
from polars.exceptions import PanicException
from typing import List, Dict, Any, Tuple

import deltacat as dc
from deltacat import Catalog
from deltacat.catalog import CatalogProperties
from deltacat.types.media import ContentType, DatasetType
from deltacat.types.tables import (
    from_pyarrow,
    TableWriteMode,
    get_dataset_type,
    get_table_length,
    get_table_column_names,
    get_table_schema,
)
from deltacat.storage import Metafile, Delta
from deltacat.utils.pyarrow import get_supported_test_types
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
    return get_supported_test_types()


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


def test_dataset_read_compatibility(
    table_name: str,
    namespace: str,
    catalog_name: str,
    dataset_types: List[DatasetType],
) -> List[Dict[str, Any]]:
    """Test reading the table with different dataset types."""
    read_results = []

    for read_dataset_type in dataset_types:
        print(f"      Testing read with {read_dataset_type.value}")
        try:
            read_result = dc.read_table(
                table=table_name,
                namespace=namespace,
                catalog=catalog_name,
                read_as=read_dataset_type,
                max_parallelism=1,
            )

            # Verify the actual dataset type matches what we expected
            actual_dataset_type = get_dataset_type(read_result)

            # Extract basic information about the read result
            result_info = {
                "dataset_type": read_dataset_type.value,
                "actual_dataset_type": actual_dataset_type.value,
                "success": True,
                "error": None,
                "result_type": type(read_result).__name__,
            }

            # Use proper utility functions based on expected dataset type
            try:
                result_info["num_rows"] = get_table_length(read_result)
            except Exception as e:
                result_info["num_rows"] = f"Error getting length: {str(e)}"

            try:
                column_names = get_table_column_names(read_result)
                result_info["num_columns"] = len(column_names)
                result_info["column_names"] = column_names
            except Exception as e:
                result_info["num_columns"] = f"Error getting columns: {str(e)}"

            # Get schema information using the utility function
            try:
                schema = get_table_schema(read_result)
                result_info["schema"] = str(schema)
                if schema.metadata is not None:
                    result_info["has_metadata"] = True
            except Exception as e:
                result_info["schema"] = f"Schema error: {str(e)}"

            read_results.append(result_info)
            print(f"        ✅ Read successful")

        except (PanicException, Exception) as e:
            read_results.append(
                {
                    "dataset_type": read_dataset_type.value,
                    "success": False,
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "result_type": None,
                    "schema": None,
                    "num_columns": 0,
                    "num_rows": 0,
                }
            )
            print(f"        ❌ Read failed: {str(e)[:100]}...")

    return read_results


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

        # Write to DeltaCAT with reader compatibility validation disabled
        dc.write_to_table(
            data=write_dataset,
            table=table_name,
            namespace=namespace,
            catalog=catalog_name,
            mode=TableWriteMode.CREATE,
            content_type=content_type,
            table_properties={
                "supported_reader_types": None  # Disable reader compatibility validation
            },
        )

        # Try to read back with PyArrow for type verification
        pyarrow_read_success = True
        read_result = None
        pyarrow_read_error = None

        try:
            read_result = dc.read_table(
                table=table_name,
                namespace=namespace,
                catalog=catalog_name,
                read_as=DatasetType.PYARROW,
                max_parallelism=1,
            )
            print(f"    ✅ PyArrow read-back successful")
        except Exception as e:
            pyarrow_read_success = False
            pyarrow_read_error = str(e)
            print(f"    ⚠️ PyArrow read-back failed: {str(e)[:100]}...")

        # Test read compatibility with different dataset types
        print(f"    Testing read compatibility with other dataset types...")
        additional_dataset_types = [
            DatasetType.PANDAS,
            DatasetType.POLARS,
            DatasetType.DAFT,
            DatasetType.RAY_DATASET,
        ]

        dataset_read_results = test_dataset_read_compatibility(
            table_name, namespace, catalog_name, additional_dataset_types
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

        # Serialize the PyArrow type for reliable deserialization later
        serialized_arrow_type = base64.b64encode(pickle.dumps(arrow_type)).decode(
            "utf-8"
        )

        return {
            "arrow_type": arrow_type_name,
            "dataset_type": dataset_type.value,
            "content_type": content_type.value,
            "success": True,  # Write was successful
            "pyarrow_read_success": pyarrow_read_success,
            "pyarrow_read_error": pyarrow_read_error,
            "original_arrow_type": str(arrow_type),
            "serialized_arrow_type": serialized_arrow_type,
            "read_back_type": str(read_result.schema.field(0).type)
            if read_result and hasattr(read_result, "schema")
            else "unknown",
            "physical_schema": physical_schema,
            "type_preserved": str(arrow_type) == str(read_result.schema.field(0).type)
            if read_result and hasattr(read_result, "schema")
            else False,
            "error": None,
            "table_name": table_name,
            "dataset_read_results": dataset_read_results,
        }

    except (PanicException, Exception) as e:
        print(f"    Test failed with error: {str(e)}")

        # Try to serialize the arrow_type even on failure (if arrow_type was created)
        try:
            arrow_type = eval(arrow_type_code)
            original_arrow_type = str(arrow_type)
            serialized_arrow_type = base64.b64encode(pickle.dumps(arrow_type)).decode(
                "utf-8"
            )
        except Exception:
            # If we can't create the arrow_type, we can't serialize it
            original_arrow_type = "unknown"
            serialized_arrow_type = None

        return {
            "arrow_type": arrow_type_name,
            "dataset_type": dataset_type.value,
            "content_type": content_type.value,
            "success": False,  # Write failed
            "pyarrow_read_success": False,
            "pyarrow_read_error": None,  # Write failed, not read
            "original_arrow_type": original_arrow_type,
            "serialized_arrow_type": serialized_arrow_type,
            "read_back_type": "unknown",
            "physical_schema": {},
            "type_preserved": False,
            "error": str(e),
            "error_category": "unknown",
            "table_name": f"failed_{arrow_type_name}_{dataset_type.value}",
            "dataset_read_results": [],
        }


def run_type_mapping_tests(catalog_name: str) -> List[Dict[str, Any]]:
    """Run the actual type mapping tests and return results."""
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
                    # Write was successful, check read status
                    read_status = (
                        "✅" if result.get("pyarrow_read_success", True) else "⚠️"
                    )

                    if result["physical_schema"].get("error"):
                        print(
                            f"    {read_status} Write ✅, Physical schema error: {result['physical_schema']['error']}"
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
                                print(
                                    f"    {read_status} Write ✅, Physical type: {physical_type}"
                                )
                            elif content_type == ContentType.FEATHER:
                                physical_type = first_col.get(
                                    "feather_preserved_type", "unknown"
                                )
                                print(
                                    f"    {read_status} Write ✅, Physical type: {physical_type}"
                                )
                            elif content_type == ContentType.AVRO:
                                physical_type = first_col.get("avro_type", "unknown")
                                print(
                                    f"    {read_status} Write ✅, Physical type: {physical_type}"
                                )
                            elif content_type == ContentType.ORC:
                                physical_type = first_col.get(
                                    "orc_type_kind", "unknown"
                                )
                                print(
                                    f"    {read_status} Write ✅, Physical type: {physical_type}"
                                )
                        else:
                            print(f"    {read_status} Write ✅, No column info found")

                    # Show read error if any
                    if not result.get("pyarrow_read_success", True):
                        read_error = result.get("pyarrow_read_error", "unknown")
                        print(f"      PyArrow read failed: {read_error[:100]}...")
                else:
                    print(f"    ❌ Write failed: {result.get('error', 'unknown')}")

                all_results.append(result)
    print()

    return all_results


def main():
    print("=" * 80)
    print("PHYSICAL SCHEMA EXTRACTION TEST")
    print("=" * 80)
    print("Using dc.list with table-specific URLs to map files to tests")

    # Setup
    temp_dir = tempfile.mkdtemp()
    catalog_name = f"test-catalog-{uuid.uuid4()}"
    catalog_props = CatalogProperties(root=temp_dir)
    dc.put_catalog(catalog_name, catalog=Catalog(config=catalog_props))

    print(f"Using catalog directory: {temp_dir}")

    try:
        # Run the tests
        all_results = run_type_mapping_tests(catalog_name)

        # Save detailed results with version information
        version_info = get_version_info()
        output_data = {"metadata": version_info, "test_results": all_results}

        output_file_name = "generate_type_mappings_results.json"
        with open(output_file_name, "w") as f:
            json.dump(output_data, f, indent=2, default=str)

        print(f"Detailed results saved to: {output_file_name}")
        print(f"Catalog directory: {temp_dir}")

    finally:
        # Clean up test catalog and temporary directory
        try:
            dc.clear_catalogs()  # Clear catalog from memory
            shutil.rmtree(temp_dir)  # Remove temporary directory and all contents
            print(f"✅ Cleaned up test catalog directory: {temp_dir}")
        except Exception as cleanup_error:
            print(
                f"⚠️ Warning: Failed to clean up catalog directory {temp_dir}: {cleanup_error}"
            )
            print("NOTE: You may need to manually delete this directory")


if __name__ == "__main__":
    main()
