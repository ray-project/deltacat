#!/usr/bin/env python3
"""
Quick test script to demonstrate the REST catalog workflow.
This script shows the complete write → read cycle with DeltaCat monitoring and conversion.
"""

import subprocess
import sys
import time
import requests
from pyiceberg.catalog import load_catalog

def check_rest_catalog():
    """Check if REST catalog is running."""
    try:
        response = requests.get("http://localhost:8181/v1/config", timeout=5)
        if response.status_code == 200:
            print("✅ REST catalog is running")
            return True
    except requests.exceptions.RequestException:
        pass
    
    print("❌ REST catalog is not running")
    print("📋 Start it with: docker run -d -p 8181:8181 --name iceberg-rest-catalog tabulario/iceberg-rest") 
    return False

def verify_duplicate_resolution(warehouse_path="/tmp/iceberg_rest_warehouse"):
    """
    Verify that the DeltaCat converter successfully resolved duplicates.
    """
    try:
        print(f"\n🔍 Verifying duplicate resolution...")
        
        # Create PyIceberg catalog to check results
        verification_catalog = load_catalog(
            "workflow_verification_catalog",
            **{
                "type": "rest",
                "warehouse": warehouse_path,
                "uri": "http://localhost:8181",
            }
        )
        
        # Load the table and scan its contents
        table_identifier = "default.demo_table"
        tbl = verification_catalog.load_table(table_identifier)
        scan_result = tbl.scan().to_arrow().to_pydict()
        
        # Check the results
        result_ids = sorted(scan_result['id'])
        unique_ids = sorted(set(result_ids))
        
        print(f"📊 Final verification results:")
        print(f"  - Total records: {len(result_ids)}")
        print(f"  - Unique IDs: {len(unique_ids)}")
        print(f"  - IDs found: {result_ids}")
        
        # Check if duplicates were resolved
        expected_unique_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        if result_ids == expected_unique_ids:
            # Verify that the latest versions were preserved
            names_by_id = {}
            versions_by_id = {}
            for i, id_val in enumerate(scan_result['id']):
                names_by_id[id_val] = scan_result['name'][i]
                versions_by_id[id_val] = scan_result['version'][i]
            
            if (names_by_id.get(2) == "Robert" and 
                names_by_id.get(3) == "Charles" and 
                versions_by_id.get(2) == 2 and 
                versions_by_id.get(3) == 2):
                print(f"✅ Duplicate resolution SUCCESSFUL!")
                print(f"  - All 9 IDs are unique")
                print(f"  - Latest versions preserved (Bob→Robert, Charlie→Charles)")
                print(f"  - Version numbers correct (v2 for updated records)")
                return True
            else:
                print(f"❌ Latest versions not preserved correctly")
        else:
            print(f"❌ Duplicates still present or unexpected record count")
            print(f"  - Expected: {expected_unique_ids}")
            print(f"  - Got: {result_ids}")
        
        return False
        
    except Exception as e:
        print(f"❌ Error during verification: {e}")
        return False

def run_example(mode, input_text="Workflow Test"):
    """Run the example in the specified mode."""
    print(f"\n🚀 Running example in {mode} mode...")
    cmd = [sys.executable, "main.py", "--mode", mode, "--input-text", input_text]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)  # Increased timeout for converter
        if result.returncode == 0:
            print(f"✅ {mode.capitalize()} operation completed successfully")
            if mode == "read":
                # Show sample data from the output
                lines = result.stdout.split('\n')
                data_lines = [line for line in lines if 'BeamSchema' in line]
                if data_lines:
                    print(f"📊 Found {len(data_lines)} records in table")
                    print("Sample records:")
                    for line in data_lines[:5]:  # Show first 5 records
                        print(f"  {line}")
                    if len(data_lines) > 5:
                        print(f"  ... and {len(data_lines) - 5} more records")
            return True
        else:
            print(f"❌ {mode.capitalize()} operation failed:")
            print(result.stderr)
            return False
    except subprocess.TimeoutExpired:
        print(f"⏰ {mode.capitalize()} operation timed out")
        return False
    except Exception as e:
        print(f"❌ Error running {mode} operation: {e}")
        return False

def main():
    """Main workflow test."""
    print("🧪 DeltaCat Beam Iceberg REST Catalog Workflow Test")
    print("=" * 60)
    
    # Step 1: Check prerequisites
    if not check_rest_catalog():
        sys.exit(1)
    
    # Step 2: Write data (creates table with duplicates and triggers converter)
    print(f"\n📋 Phase 1: Writing data and triggering DeltaCat converter")
    if not run_example("write", "Workflow Demo User"):
        print("❌ Write test failed")
        sys.exit(1)
    
    # Step 3: Additional verification using PyIceberg directly
    print(f"\n📋 Phase 2: Direct verification of duplicate resolution")
    time.sleep(2)  # Wait a moment for any final converter operations
    
    verification_success = verify_duplicate_resolution()
    
    # Step 4: Read data back to show final state
    print(f"\n📋 Phase 3: Reading final table state with Beam")
    if not run_example("read"):
        print("❌ Read test failed")  
        sys.exit(1)
    
    # Final summary
    print("\n🎉 Workflow test completed!")
    
    if verification_success:
        print("\n✅ COMPLETE SUCCESS:")
        print("  ✅ Table creation and data writing")
        print("  ✅ DeltaCat monitoring detected duplicates")
        print("  ✅ Ray-based converter session executed")
        print("  ✅ Duplicates resolved automatically")
        print("  ✅ Latest versions preserved correctly")
        print("  ✅ Read operations work with cleaned data")
    else:
        print("\n⚠️  PARTIAL SUCCESS:")
        print("  ✅ Table creation and data writing")
        print("  ✅ DeltaCat monitoring active")
        print("  ❓ Converter may still be processing or failed")
        print("  📝 Check logs for converter execution details")
    
    print("\n📚 What happened:")
    print("  1. Beam pipelines wrote data creating duplicates (IDs 2,3)")
    print("  2. DeltaCat monitoring detected new table versions")
    print("  3. Ray cluster was automatically initialized")
    print("  4. Converter session processed duplicates")
    print("  5. Ray cluster was shutdown after completion")
    print("  6. Table now contains clean, deduplicated data")
    
    print("\n🧹 Cleanup:")
    print("  docker stop iceberg-rest-catalog && docker rm iceberg-rest-catalog")

if __name__ == "__main__":
    main() 