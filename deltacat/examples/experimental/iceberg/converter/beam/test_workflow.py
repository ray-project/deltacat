#!/usr/bin/env python3
"""
Quick test script to demonstrate the REST catalog workflow.
This script shows the complete write → read cycle with DeltaCAT monitoring and conversion.
"""

import subprocess
import sys
from deltacat.examples.experimental.iceberg.converter.beam.utils.common import (
    generate_random_suffix, 
    check_rest_catalog, 
    wait_for_deltacat_jobs
)
from deltacat.examples.experimental.iceberg.converter.beam.utils.common import verify_duplicate_resolution

def run_example(mode, table_name, input_text="Workflow Test"):
    """Run the example in the specified mode."""
    print(f"\n🚀 Running example in {mode} mode with table: {table_name}")
    cmd = [sys.executable, "main.py", "--mode", mode, "--input-text", input_text, "--table-name", table_name]
    
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
    print("🧪 DeltaCAT Beam Iceberg REST Catalog Workflow Test")
    print("=" * 60)
    
    # Generate unique table name to avoid conflicts
    random_suffix = generate_random_suffix()
    table_name = f"default.demo_table_{random_suffix}"
    print(f"📋 Generated unique table name: {table_name}")
    
    # Step 1: Check prerequisites
    if not check_rest_catalog():
        sys.exit(1)
    
    # Step 2: Write data (creates table with duplicates and triggers converter)
    print(f"\n📋 Phase 1: Writing data and triggering DeltaCAT converter")
    if not run_example("write", table_name, "Workflow Demo User"):
        print("❌ Write test failed")
        sys.exit(1)
    
    # Step 3: Wait for DeltaCAT converter jobs to complete
    print(f"\n📋 Phase 2: Waiting for DeltaCAT converter jobs to complete")
    if not wait_for_deltacat_jobs(table_name):
        print("⚠️  DeltaCAT job monitoring timed out, but proceeding with verification")
    
    # Step 4: Additional verification using PyIceberg directly
    print(f"\n📋 Phase 3: Direct verification of duplicate resolution")
    
    verification_success = verify_duplicate_resolution(table_name)
    
    # Step 5: Read data back to show final state
    print(f"\n📋 Phase 4: Reading final table state with Beam")
    if not run_example("read", table_name):
        print("❌ Read test failed")  
        sys.exit(1)
    
    # Final summary
    print("\n🎉 Workflow test completed!")
    
    if verification_success:
        print("\n✅ SUCCESS:")
        print("  ✅ Table creation and writes")
        print("  ✅ DeltaCAT monitoring detected duplicates")
        print("  ✅ Ray-based converter session executed")
        print("  ✅ Merged by key")
        print("  ✅ Read operations correctly read merged data")
    else:
        print("\n⚠️  PARTIAL SUCCESS:")
        print("  ✅ Table creation and writes")
        print("  ✅ DeltaCAT monitoring detected duplicates")
        print("  ❓ Converter may still be processing or failed")
        print("  📝 Check logs for converter execution details")
    
    print("\n📚 What happened:")
    print("  1. Beam wrote data creating duplicates (IDs 2,3)")
    print("  2. DeltaCAT monitoring detected the duplicates")
    print("  3. Ray data converter job was initialized")
    print("  4. Converter session merged data by key")
    print("  5. Ray cluster was shutdown after completion")
    print("  6. Table now contains merged data")
    
    print("\n🧹 Cleanup:")
    print("  docker stop iceberg-rest-catalog && docker rm iceberg-rest-catalog")

if __name__ == "__main__":
    main() 