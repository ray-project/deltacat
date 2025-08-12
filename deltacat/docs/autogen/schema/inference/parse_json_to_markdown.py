#!/usr/bin/env python3
"""
Systematic approach: Parse the existing JSON test results and convert to proper markdown.
This separates the physical schema extraction from the markdown generation.
"""

import json
import sys
from typing import Dict, List, Any


def load_test_data(json_file: str) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Load test results and metadata from JSON file."""
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    if isinstance(data, dict):
        if 'test_results' in data and 'metadata' in data:
            # New format with metadata
            return data['test_results'], data['metadata']
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


def extract_physical_type_mapping_from_json(result: Dict[str, Any], content_type_key: str) -> str:
    """Extract physical type mapping from JSON result."""
    if not result.get('success', False):
        return None
    
    physical_schema = result.get('physical_schema', {})
    
    if physical_schema.get('error'):
        return None
    
    if content_type_key == 'parquet':
        columns = physical_schema.get('columns', {})
        if columns:
            first_col = next(iter(columns.values()))
            physical_type = first_col.get('parquet_physical_type', 'unknown')
            logical_type = first_col.get('parquet_logical_type')
            if logical_type and logical_type != 'None':
                return f"{physical_type} ({logical_type})"
            return physical_type
    
    elif content_type_key == 'feather':
        columns = physical_schema.get('columns', {})
        if columns:
            first_col = next(iter(columns.values()))
            return first_col.get('feather_preserved_type', 'unknown')
    
    elif content_type_key == 'avro':
        columns = physical_schema.get('columns', {})
        if columns:
            first_col = next(iter(columns.values()))
            avro_type = first_col.get('avro_type')
            if avro_type:
                return str(avro_type)
        return 'unknown'
    
    elif content_type_key == 'orc':
        columns = physical_schema.get('columns', {})
        if columns:
            first_col = next(iter(columns.values()))
            return first_col.get('orc_type_kind', 'unknown')
    
    return None


def generate_type_table_markdown(arrow_type: str, arrow_description: str, results: List[Dict[str, Any]]) -> str:
    """Generate a single type table in markdown format."""
    
    # Filter results for this arrow type
    type_results = [r for r in results if r['arrow_type'] == arrow_type]
    
    if not type_results:
        return f"\n#### **{arrow_type}** (`{arrow_description}`)\nNo test results found for this type.\n"
    
    # Organize results by dataset type and content type
    dataset_types = ['pyarrow', 'pandas', 'polars', 'daft', 'ray_dataset']
    content_types = ['application/parquet', 'application/feather', 'application/avro', 'application/orc']
    content_type_keys = ['parquet', 'feather', 'avro', 'orc']
    
    # Build result matrix and physical mappings per dataset type
    result_matrix = {}
    dataset_physical_mappings = {}
    
    for dataset_type in dataset_types:
        result_matrix[dataset_type] = {}
        dataset_physical_mappings[dataset_type] = {}
        
        for content_type in content_types:
            # Find the specific result
            specific_result = next(
                (r for r in type_results if r['dataset_type'] == dataset_type and r['content_type'] == content_type),
                None
            )
            
            if specific_result:
                result_matrix[dataset_type][content_type] = '✅' if specific_result['success'] else '❌'
                
                # Extract physical type mapping for this dataset type
                content_key = content_type.replace('application/', '')
                physical_type = extract_physical_type_mapping_from_json(specific_result, content_key)
                if physical_type and physical_type != 'unknown':
                    dataset_physical_mappings[dataset_type][content_key] = physical_type
            else:
                result_matrix[dataset_type][content_type] = '❓'
    
    # Generate markdown table
    markdown = f"\n#### **{arrow_type}** (`{arrow_description}`)\n"
    markdown += "| Dataset Type | Parquet | Feather | Avro | ORC | Physical Types |\n"
    markdown += "|--------------|---------|---------|------|-----|---------------|\n"
    
    for dataset_type in dataset_types:
        row_results = result_matrix.get(dataset_type, {})
        parquet_result = row_results.get('application/parquet', '❓')
        feather_result = row_results.get('application/feather', '❓')
        avro_result = row_results.get('application/avro', '❓')
        orc_result = row_results.get('application/orc', '❓')
        
        # Build physical types string for this dataset type
        dataset_mappings = dataset_physical_mappings.get(dataset_type, {})
        physical_parts = []
        
        for content_key in content_type_keys:
            if content_key in dataset_mappings:
                physical_parts.append(f"{content_key.title()}:`{dataset_mappings[content_key]}`")
        
        physical_col = "; ".join(physical_parts) if physical_parts else ""
        
        markdown += f"| `{dataset_type}` | {parquet_result} | {feather_result} | {avro_result} | {orc_result} | {physical_col} |\n"
    
    return markdown


def generate_complete_markdown_from_json(json_file: str, output_file: str = "corrected_comprehensive_matrix.md"):
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
    arrow_types_in_results = sorted(list(set(r['arrow_type'] for r in results)))
    print(f"Found {len(arrow_types_in_results)} unique arrow types: {arrow_types_in_results}")
    
    # Map arrow types to their descriptions
    arrow_type_descriptions = {
        'int32': 'int32', 'int64': 'int64', 'float32': 'float', 'float64': 'double',
        'bool_': 'bool_', 'string': 'string', 'binary': 'binary',
        'null': 'null', 'dictionary_string': 'dictionary<values=string, indices=int32>',
        'int8': 'int8', 'int16': 'int16', 'uint8': 'uint8', 'uint16': 'uint16',
        'uint32': 'uint32', 'uint64': 'uint64', 'date32': 'date32[day]',
        'timestamp_us': 'timestamp[us]', 'timestamp_s_utc': 'timestamp[s, tz=UTC]',
        'large_string': 'large_string', 'large_binary': 'large_binary',
        'decimal128_5_2': 'decimal128(5, 2)', 'list_int32': 'list<element: int32>',
        'list_string': 'list<element: string>', 'struct_simple': 'struct<name: string, age: int32>'
    }
    
    # Generate dynamic metadata section
    test_date = metadata.get('test_date', 'unknown')
    if 'T' in test_date:
        # Convert ISO format to date only
        test_date = test_date.split('T')[0]
    
    pyarrow_version = metadata.get('pyarrow_version', 'unknown')
    
    markdown = f"""### Arrow Type Mappings by Dataset and File Type

**Generation Date:** {test_date}
**\n\nPyArrow Version:** {pyarrow_version}"""
    
    # Add other version information if available
    if metadata.get('deltacat_version'):
        markdown += f"\n\n**DeltaCAT Version:** {metadata['deltacat_version']}"
    if metadata.get('pandas_version'):
        markdown += f"\n\n**Pandas Version:** {metadata['pandas_version']}"
    if metadata.get('polars_version'):
        markdown += f"\n\n**Polars Version:** {metadata['polars_version']}"
    if metadata.get('daft_version') and metadata['daft_version'] != 'not_available':
        markdown += f"\n\n**Daft Version:** {metadata['daft_version']}"
    if metadata.get('ray_version') and metadata['ray_version'] != 'not_available':
        markdown += f"\n\n**Ray Version:** {metadata['ray_version']}"
    
    markdown += f"""
**\n\nPyArrow Data Type Reference:** https://arrow.apache.org/docs/python/api/datatypes.html

"""
    
    # Generate tables for each arrow type
    for arrow_type in arrow_types_in_results:
        description = arrow_type_descriptions.get(arrow_type, arrow_type)
        type_table = generate_type_table_markdown(arrow_type, description, results)
        markdown += type_table
        print(f"Generated table for {arrow_type}")
     
    # Write to file
    with open(output_file, 'w') as f:
        f.write(markdown)
    
    print(f"✅ Generated corrected markdown: {output_file}")
    
    # Analyze the results to identify the physical schema extraction issues
    print("\n" + "="*80)
    print("ANALYSIS: Physical Schema Extraction Issues")
    print("="*80)
    
    successful_extractions = 0
    failed_extractions = 0
    no_physical_data = 0
    
    for result in results:
        if result.get('success', False):
            physical_schema = result.get('physical_schema', {})
            if physical_schema.get('error'):
                failed_extractions += 1
                if 'no written files found' in physical_schema.get('error', '').lower():
                    no_physical_data += 1
            elif physical_schema.get('columns'):
                successful_extractions += 1
            else:
                no_physical_data += 1
    
    print(f"Successful physical schema extractions: {successful_extractions}")
    print(f"Failed extractions: {failed_extractions}")
    print(f"No physical data: {no_physical_data}")
    print(f"Total successful tests: {len([r for r in results if r.get('success', False)])}")
    
    return output_file


def main():
    if len(sys.argv) != 2:
        print("Usage: python parse_json_to_markdown.py <json_results_file>")
        sys.exit(1)
    
    json_file = sys.argv[1]
    generate_complete_markdown_from_json(json_file)


if __name__ == "__main__":
    main()
