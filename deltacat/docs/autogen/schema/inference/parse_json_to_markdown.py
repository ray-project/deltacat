"""
Systematic approach: Parse the existing JSON test results and convert to proper markdown.
This separates the physical schema extraction from the markdown generation.
"""

import json
import sys
from typing import Dict, List, Any
from pathlib import Path


def load_test_data(json_file: str) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Load test results and metadata from JSON file."""
    with open(json_file, "r") as f:
        data = json.load(f)

    if isinstance(data, dict):
        if "test_results" in data and "metadata" in data:
            # New format with metadata
            return data["test_results"], data["metadata"]
        else:
            raise ValueError(f"Unexpected JSON structure in {json_file}")
    elif isinstance(data, list):
        # Old format - just a list of results
        return data, {}
    else:
        raise ValueError(f"Unexpected JSON structure in {json_file}")


def load_test_results(json_file: str) -> List[Dict[str, Any]]:
    """Load test results from JSON file (backward compatibility)."""
    results, _ = load_test_data(json_file)
    return results


def extract_physical_type_mapping_from_json(
    result: Dict[str, Any], content_type_key: str
) -> str:
    """Extract physical type mapping from JSON result."""
    if not result.get("success", False):
        return None

    # Even if PyArrow read failed, we can still extract physical schema if files were written
    # The physical schema inspection happens at the file level, not via PyArrow read

    physical_schema = result.get("physical_schema", {})

    if physical_schema.get("error"):
        return None

    if content_type_key == "parquet":
        columns = physical_schema.get("columns", {})
        if columns:
            first_col = next(iter(columns.values()))
            physical_type = first_col.get("parquet_physical_type", "unknown")
            logical_type = first_col.get("parquet_logical_type")
            if logical_type and logical_type != "None":
                return f"{physical_type} ({logical_type})"
            return physical_type

    elif content_type_key == "feather":
        columns = physical_schema.get("columns", {})
        if columns:
            first_col = next(iter(columns.values()))
            return first_col.get("feather_preserved_type", "unknown")

    elif content_type_key == "avro":
        columns = physical_schema.get("columns", {})
        if columns:
            first_col = next(iter(columns.values()))
            avro_type = first_col.get("avro_type")
            if avro_type:
                return str(avro_type)
        return "unknown"

    elif content_type_key == "orc":
        columns = physical_schema.get("columns", {})
        if columns:
            first_col = next(iter(columns.values()))
            return first_col.get("orc_type_kind", "unknown")

    return None


def generate_type_table_markdown(
    arrow_type: str, arrow_description: str, results: List[Dict[str, Any]]
) -> str:
    """Generate a single type table in markdown format."""

    # Filter results for this arrow type
    type_results = [r for r in results if r["arrow_type"] == arrow_type]

    if not type_results:
        return (
            f"\n#### **{arrow_description}** \nNo test results found for this type.\n"
        )

    # Organize results by dataset type and content type
    dataset_types = ["pyarrow", "pandas", "polars", "daft", "ray_dataset"]
    content_types = [
        "application/parquet",
        "application/feather",
        "application/avro",
        "application/orc",
    ]
    content_type_keys = ["parquet", "feather", "avro", "orc"]

    # Build result matrix and physical mappings per dataset type
    result_matrix = {}
    dataset_physical_mappings = {}

    for dataset_type in dataset_types:
        result_matrix[dataset_type] = {}
        dataset_physical_mappings[dataset_type] = {}

        for content_type in content_types:
            # Find the specific result
            specific_result = next(
                (
                    r
                    for r in type_results
                    if r["dataset_type"] == dataset_type
                    and r["content_type"] == content_type
                ),
                None,
            )

            if specific_result:
                write_success = specific_result["success"]

                if write_success:
                    result_matrix[dataset_type][content_type] = "✅"
                else:
                    result_matrix[dataset_type][content_type] = "❌"  # Write failed

                # Extract physical type mapping for this dataset type
                content_key = content_type.replace("application/", "")
                physical_type = extract_physical_type_mapping_from_json(
                    specific_result, content_key
                )
                if physical_type and physical_type != "unknown":
                    dataset_physical_mappings[dataset_type][content_key] = physical_type
            else:
                result_matrix[dataset_type][content_type] = "❓"

    # Generate markdown table
    markdown = f"\n#### **{arrow_description}**\n"
    markdown += "| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |\n"
    markdown += "|--------------|---------|---------|------|-----|---------------|\n"

    for dataset_type in dataset_types:
        row_results = result_matrix.get(dataset_type, {})
        parquet_result = row_results.get("application/parquet", "❓")
        feather_result = row_results.get("application/feather", "❓")
        avro_result = row_results.get("application/avro", "❓")
        orc_result = row_results.get("application/orc", "❓")

        # Build physical types string for this dataset type
        dataset_mappings = dataset_physical_mappings.get(dataset_type, {})
        physical_parts = []

        for content_key in content_type_keys:
            if content_key in dataset_mappings:
                physical_parts.append(
                    f"{content_key.title()}:`{dataset_mappings[content_key]}`"
                )

        physical_col = "; ".join(physical_parts) if physical_parts else ""

        markdown += f"| `{dataset_type}` | {parquet_result} | {feather_result} | {avro_result} | {orc_result} | {physical_col} |\n"

    return markdown


def generate_read_compatibility_matrix_markdown(
    results: List[Dict[str, Any]], arrow_type_descriptions: Dict[str, str]
) -> str:
    """Generate read compatibility matrix markdown from test results."""

    # Collect all read compatibility data
    read_compat_data = (
        {}
    )  # arrow_type -> writer_dataset -> content_type -> {reader_dataset: success}

    for result in results:
        arrow_type = result["arrow_type"]
        arrow_type_description = arrow_type_descriptions.get(arrow_type, arrow_type)
        writer_dataset = result["dataset_type"]
        content_type = result["content_type"]
        write_success = result.get("success", False)
        dataset_read_results = result.get("dataset_read_results", [])

        if arrow_type_description not in read_compat_data:
            read_compat_data[arrow_type_description] = {}
        if writer_dataset not in read_compat_data[arrow_type_description]:
            read_compat_data[arrow_type_description][writer_dataset] = {}
        if content_type not in read_compat_data[arrow_type_description][writer_dataset]:
            read_compat_data[arrow_type_description][writer_dataset][content_type] = {}

        if write_success:
            # Only process read results if the write was successful
            # Add PyArrow read result based on actual read success
            # If pyarrow_read_success field is missing, we can't assume it succeeded
            pyarrow_read_success = result.get("pyarrow_read_success")
            if pyarrow_read_success is not None:
                read_compat_data[arrow_type_description][writer_dataset][content_type][
                    "pyarrow"
                ] = pyarrow_read_success

            # Add other dataset type read results
            for read_result in dataset_read_results:
                reader_dataset = read_result["dataset_type"]
                success = read_result["success"]
                read_compat_data[arrow_type_description][writer_dataset][content_type][
                    reader_dataset
                ] = success
        else:
            # Write failed - mark all readers as incompatible (represented by "—")
            # This ensures the writer appears in the table but shows no compatibility data
            pass

    if not read_compat_data:
        return (
            "\n## Read Compatibility Tables\n\nNo read compatibility data available.\n"
        )

    # Generate markdown
    markdown = """\n## Read Compatibility Tables\n\n
The following tables show read compatibility for each Arrow type across available writer/reader combinations.\n

This information is automatically used by DeltaCAT at write time to ensure that data written in one format can be
read by all supported reader types defined in a table's `SUPPORTED_READER_TYPES` table property. If data to be
written cannot be read by one or more supported reader types, then a `TableValidationError` will be raised.
"""

    # Get all dataset types that appear as readers
    all_readers = set()
    for arrow_data in read_compat_data.values():
        for writer_data in arrow_data.values():
            for content_data in writer_data.values():
                all_readers.update(content_data.keys())
    all_readers = sorted(list(all_readers))

    # Generate table for each arrow type
    for arrow_type in sorted(read_compat_data.keys()):
        markdown += f"\n### {arrow_type}\n\n"

        # Organize by content type
        content_types = set()
        for writer_data in read_compat_data[arrow_type].values():
            content_types.update(writer_data.keys())
        content_types = sorted(list(content_types))

        for content_type in content_types:
            markdown += f"\n#### {content_type}\n\n"

            # Find all writers for this content type
            writers = []
            for writer_dataset in sorted(read_compat_data[arrow_type].keys()):
                if content_type in read_compat_data[arrow_type][writer_dataset]:
                    writers.append(writer_dataset)

            if not writers:
                continue

            # Create table header
            markdown += "| Writer \\ Reader | " + " | ".join(all_readers) + " |\n"
            markdown += "|" + "---|" * (len(all_readers) + 1) + "\n"

            # Create table rows
            for writer in writers:
                row = [f"**{writer}**"]
                reader_data = read_compat_data[arrow_type][writer][content_type]

                for reader in all_readers:
                    if reader in reader_data:
                        result = reader_data[reader]
                        row.append("✅" if result else "❌")
                    else:
                        row.append("—")

                markdown += "| " + " | ".join(row) + " |\n"

            markdown += "\n"

    return markdown


def _normalize_complex_types(arrow_type: str) -> str:
    """Normalize complex arrow types to their base type names without parameters."""
    # Only normalize specific complex types, otherwise return as-is
    if arrow_type.startswith("list<"):
        return "list"
    elif arrow_type.startswith("map<"):
        return "map"
    elif arrow_type.startswith("struct<"):
        return "struct"
    elif arrow_type.startswith("dictionary<"):
        return "dictionary"
    elif arrow_type.startswith("decimal128"):
        return "decimal128"
    elif arrow_type.startswith("decimal256"):
        return "decimal256"
    elif arrow_type.startswith("timestamp["):
        # For timestamps, check timezone info and extract precision
        if "UTC" in arrow_type.upper() or "utc" in arrow_type:
            # Extract precision from the string (e.g., "timestamp[s, tz=UTC]" -> "s")
            import re

            precision_match = re.search(r"timestamp\[([^,\]]+)", arrow_type)
            if precision_match:
                precision = precision_match.group(1)
                return f"timestamp_tz[{precision}]"
            else:
                return "timestamp_tz"
        else:
            return arrow_type
    else:
        return arrow_type


def generate_reader_compatibility_mapping(
    results: List[Dict[str, Any]],
    output_file: str = "./reader_compatibility_mapping.py",
) -> str:
    """Generate reader compatibility mapping Python file from test results."""

    # Collect compatibility data: (arrow_type, writer_dataset) -> list of compatible readers
    compatibility_mapping = {}

    for result in results:
        if not result.get("success", False):
            continue

        # Use original_arrow_type which contains the base PyArrow DataType name
        raw_arrow_type = result.get("original_arrow_type", result["arrow_type"])

        # Normalize complex types to base type names
        arrow_type = _normalize_complex_types(raw_arrow_type)
        writer_dataset = result["dataset_type"]
        content_type = result["content_type"]

        # Create key tuple
        key = (arrow_type, writer_dataset, content_type)

        compatible_readers = []

        # Check PyArrow read success
        pyarrow_read_success = result.get("pyarrow_read_success")
        if pyarrow_read_success:
            compatible_readers.append("PYARROW")

        # Check other dataset type read results
        dataset_read_results = result.get("dataset_read_results", [])
        for read_result in dataset_read_results:
            reader_dataset = read_result["dataset_type"]
            success = read_result["success"]
            if success:
                # Map to DatasetType enum values
                dataset_type_mapping = {
                    "pyarrow": "PYARROW",
                    "pandas": "PANDAS",
                    "polars": "POLARS",
                    "daft": "DAFT",
                    "ray_dataset": "RAY_DATASET",
                }
                enum_value = dataset_type_mapping.get(reader_dataset)
                if enum_value and enum_value not in compatible_readers:
                    compatible_readers.append(enum_value)

        if compatible_readers:
            # Merge with existing compatibility for same key (union of compatible readers)
            if key in compatibility_mapping:
                existing_readers = set(compatibility_mapping[key])
                new_readers = set(compatible_readers)
                compatibility_mapping[key] = list(existing_readers.union(new_readers))
            else:
                compatibility_mapping[key] = compatible_readers

    # Generate Python file content
    python_content = '''"""
Reader compatibility mapping generated from test results.

This mapping shows which DatasetType readers can successfully read data
written by each (arrow_type, writer_dataset_type, content_type) combination.

Keys: (arrow_type, writer_dataset_type, content_type)
Values: List of compatible DatasetType enum values
"""

from deltacat.types.tables import DatasetType

# Mapping of (arrow_type, writer_dataset_type, content_type) -> list of compatible readers
READER_COMPATIBILITY_MAPPING = {
'''

    # Sort keys for consistent output
    for key in sorted(compatibility_mapping.keys()):
        compatible_readers = compatibility_mapping[key]
        arrow_type, writer_dataset, content_type = key

        # Format as Python tuple and list
        readers_str = (
            "["
            + ", ".join(
                [f"DatasetType.{reader}" for reader in sorted(compatible_readers)]
            )
            + "]"
        )
        python_content += f'    ("{arrow_type}", "{writer_dataset}", "{content_type}"): {readers_str},\n'

    python_content += '''}

def get_compatible_readers(arrow_type: str, writer_dataset_type: str, content_type: str):
    """Get list of compatible reader DatasetTypes for given combination."""
    key = (arrow_type, writer_dataset_type, content_type)
    return READER_COMPATIBILITY_MAPPING.get(key, [])

def is_reader_compatible(arrow_type: str, writer_dataset_type: str, content_type: str, reader_dataset_type: DatasetType) -> bool:
    """Check if a specific reader is compatible with given combination."""
    compatible_readers = get_compatible_readers(arrow_type, writer_dataset_type, content_type)
    return reader_dataset_type in compatible_readers
'''

    # Write to file
    with open(output_file, "w") as f:
        f.write(python_content)

    print(f"✅ Generated reader compatibility mapping: {output_file}")
    return output_file


def generate_complete_markdown_from_json(
    json_file: str, output_file: str = "./docs/schema/README.md"
):
    """Generate complete markdown from JSON results."""

    print(f"Loading results from {json_file}...")
    results, metadata = load_test_data(json_file)
    print(f"Loaded {len(results)} test results")

    if metadata:
        print(f"Found metadata with test date: {metadata.get('test_date', 'unknown')}")
        print(f"PyArrow version: {metadata.get('pyarrow_version', 'unknown')}")
    else:
        raise ValueError(f"No metadata found in {json_file}")

    # Get unique arrow types from results
    arrow_types_in_results = sorted(list(set(r["arrow_type"] for r in results)))
    print(
        f"Found {len(arrow_types_in_results)} unique arrow types: {arrow_types_in_results}"
    )
    # map arrow type names to their descriptions using each results original_arrow_type field
    arrow_type_descriptions = {}
    for arrow_type in arrow_types_in_results:
        # extract original_arrow_type field from each result
        original_arrow_type = next(
            (
                r["original_arrow_type"]
                for r in results
                if r["arrow_type"] == arrow_type
            ),
            None,
        )
        if original_arrow_type:
            arrow_type_descriptions[arrow_type] = original_arrow_type

    # Generate dynamic metadata section
    test_date = metadata.get("test_date", "unknown")
    if "T" in test_date:
        # Convert ISO format to date only
        test_date = test_date.split("T")[0]

    pyarrow_version = metadata.get("pyarrow_version", "unknown")

    markdown = f"""# Schemas

DeltaCAT tables may either be schemaless or backed by a schema based on the [Arrow type system](https://arrow.apache.org/docs/python/api/datatypes.html).

## Schemaless Tables
A schemaless table is created via `dc.create_table(table_name, schema=None)`. Schemaless tables only save a
record of files written to them over time without schema inference, data validation, or data coercion.
Since it may not be possible to derive a unified schema on read, data returned via `dc.read_table(table_name)` is
always an ordered list of files written to the table and their manifest entry info (e.g., size, content type,
content encoding, etc.). For example:

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


## Standard Tables
Tables with schemas have their data validation and schema evolution behavior governed by **Schema
Consistency Types** and **Schema Evolution Modes** to ensure that the table can always be materialized
with a unified schema at read time. By default, a DeltaCAT table created via `dc.create_table(table_name)`
infers a unified Arrow schema on write.

## Schema Consistency Types
DeltaCAT table schemas can either be **inferred** to follow the shape of written data or **enforced**
to define the shape of written data. The default schema consistency type of all fields in a DeltaCAT
table schema is configured by setting the `DEFAULT_SCHEMA_CONSISTENCY_TYPE` table property to one
of the following values:

\n\n**NONE** (default): No data consistency checks are run. The schema field's type will be automatically
promoted to the most permissive Arrow data type that all values can be safely cast to using
`pyarrow.unify_schemas(schemas, promote_options="permissive")`. If safe casting is impossible,
then a `SchemaValidationError` will be raised.

\n\n**COERCE**: Coerce fields to fit the schema whenever possible, even if data truncation is required. Fields
will be coerced using either `pyarrow.compute.cast` or `daft.expression.cast` with default options. If the
field cannot be coerced to fit the given type, then a `SchemaValidationError` will be raised.

\n\n**VALIDATE**: Strict data consistency checks. An error is raised for any field that doesn't fit the schema.

A field's Schema Consistency Type can only be updated from least to most permissive (VALIDATE -> COERCE -> NONE).

## Schema Evolution Modes
Schema evolution modes control how schema changes are handled when writing to a table.
A table's schema evolution mode is configured by setting the `SCHEMA_EVOLUTION_MODE`
table property to one of the following values:

\n\n**AUTO** (default): Schema changes are automatically handled. New fields are added to
the schema with their Schema Consistency Type determined by the
`DEFAULT_SCHEMA_CONSISTENCY_TYPE` table property.

\n\n**MANUAL**: Schema changes must be made explicitly via `dc.alter_table()`. Attempts to
write data with fields not in the existing schema will raise a `SchemaValidationError`.

\n\n**DISABLED**: Schema changes are disabled. The schema that the table was first
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
**Generation Date:** {test_date}
\n**PyArrow Version:** {pyarrow_version}"""

    # Add other version information if available
    if metadata.get("deltacat_version"):
        markdown += f"\n\n**DeltaCAT Version:** {metadata['deltacat_version']}"
    if metadata.get("pandas_version"):
        markdown += f"\n\n**Pandas Version:** {metadata['pandas_version']}"
    if metadata.get("polars_version"):
        markdown += f"\n\n**Polars Version:** {metadata['polars_version']}"
    if metadata.get("daft_version") and metadata["daft_version"] != "not_available":
        markdown += f"\n\n**Daft Version:** {metadata['daft_version']}"
    if metadata.get("ray_version") and metadata["ray_version"] != "not_available":
        markdown += f"\n\n**Ray Version:** {metadata['ray_version']}"

    markdown += f"""

### Type Mapping Tables
"""

    # Generate tables for each arrow type
    for arrow_type in arrow_types_in_results:
        description = arrow_type_descriptions.get(arrow_type, arrow_type)
        type_table = generate_type_table_markdown(arrow_type, description, results)
        markdown += type_table
        print(f"Generated table for {arrow_type}")

    # Generate read compatibility matrix
    print("Generating read compatibility matrix...")
    read_compat_markdown = generate_read_compatibility_matrix_markdown(
        results, arrow_type_descriptions
    )
    markdown += read_compat_markdown
    print("Generated read compatibility matrix")

    # Write to file
    with open(output_file, "w") as f:
        f.write(markdown)

    print(f"✅ Generated markdown: {output_file}")

    # Analyze the results to identify the physical schema extraction issues
    print("\n" + "=" * 80)
    print("ANALYSIS: Physical Schema Extraction Issues")
    print("=" * 80)

    successful_extractions = 0
    failed_extractions = 0
    no_physical_data = 0

    for result in results:
        if result.get("success", False):
            physical_schema = result.get("physical_schema", {})
            if physical_schema.get("error"):
                failed_extractions += 1
                if "no written files found" in physical_schema.get("error", "").lower():
                    no_physical_data += 1
            elif physical_schema.get("columns"):
                successful_extractions += 1
            else:
                no_physical_data += 1

    print(f"Successful physical schema extractions: {successful_extractions}")
    print(f"Failed extractions: {failed_extractions}")
    print(f"No physical data: {no_physical_data}")
    print(
        f"Total successful tests: {len([r for r in results if r.get('success', False)])}"
    )

    return output_file


def main():
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print(
            "Usage: python parse_json_to_markdown.py <json_results_file> [--generate-mapping]"
        )
        sys.exit(1)

    json_file = sys.argv[1]
    generate_mapping = len(sys.argv) == 3 and sys.argv[2] == "--generate-mapping"

    if generate_mapping:
        # Generate reader compatibility mapping
        print(f"Loading results from {json_file} for compatibility mapping...")
        results, _ = load_test_data(json_file)
        print(f"Loaded {len(results)} test results")

        # Navigate to project root for output
        project_root = Path(__file__)
        while project_root.name != "deltacat":
            project_root = project_root.parent
        output_file_path = project_root / "utils" / "reader_compatibility_mapping.py"
        print(f"Writing reader compatibility mapping to {output_file_path}")
        generate_reader_compatibility_mapping(results, str(output_file_path))
    else:
        # Generate markdown documentation
        # keep navigating to parent directories until we find the docs directory
        docs_dir = Path(__file__)
        while docs_dir.name != "docs":
            docs_dir = docs_dir.parent
        output_file_path = docs_dir / "schema" / "README.md"
        print(f"Writing to {output_file_path}")
        generate_complete_markdown_from_json(json_file, output_file_path)


if __name__ == "__main__":
    main()
