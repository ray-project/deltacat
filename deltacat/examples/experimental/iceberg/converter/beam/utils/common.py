"""
Common utility functions for the Iceberg converter example.
"""

import random
import string
import time
from pyiceberg.catalog import load_catalog
import requests
from deltacat import local_job_client
from deltacat.constants import DEFAULT_NAMESPACE
from deltacat.experimental.converter_agent.table_monitor import _generate_job_name


def generate_random_suffix(length=8):
    """Generate a random string of specified length using letters and digits."""
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


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
    print(
        "📋 Start it with: docker run -d -p 8181:8181 --name iceberg-rest-catalog tabulario/iceberg-rest:1.6.0"
    )
    return False


def wait_for_deltacat_jobs(
    table_name, warehouse_path="/tmp/iceberg_rest_warehouse", timeout=120
):
    """
    Wait for DeltaCAT converter jobs to complete by checking job status.

    Args:
        table_name: Full table name (e.g., "default.demo_table_abc123")
        warehouse_path: Warehouse path used for job tracking
        timeout: Maximum seconds to wait for job completion

    Returns:
        True if all jobs completed, False if timeout
    """
    print(f"\n⏳ Monitoring DeltaCAT converter jobs for table: {table_name}")

    # Parse table name to get namespace and table name
    if "." in table_name:
        namespace, actual_table_name = table_name.split(".", 1)
    else:
        namespace = DEFAULT_NAMESPACE
        actual_table_name = table_name

    # Create job key matching the format used in managed.py
    job_name = _generate_job_name(
        warehouse_path=warehouse_path, namespace=namespace, table_name=actual_table_name
    )

    start_time = time.time()

    try:
        # Get the job client
        client = local_job_client(ray_init_args={"local_mode": True})

        while time.time() - start_time < timeout:
            job_details_list = client.list_jobs()
            print(f"🔍 Job details list: {job_details_list}")
            job_submission_ids = [
                job_details.submission_id for job_details in job_details_list
            ]

            # Check if we have any tracked jobs for this table
            print(f"🔍 Looking for submission ID: {job_name} in {job_submission_ids}")
            if job_name in job_submission_ids:
                # Check job status with Ray
                try:
                    job_status = client.get_job_status(job_name)
                    print(f"📊 Job {job_name} status: {job_status}")
                    # Check if job is still running
                    if job_status and str(job_status) in ["PENDING", "RUNNING"]:
                        time.sleep(2)  # Short polling interval
                        continue
                    else:
                        print(f"✅ Job {job_name} completed with status: {job_status}")
                        return True

                except Exception as e:
                    print(f"⚠️  Could not check job status for {job_name}: {e}")
                    # If we can't check status, assume job is done
                    return True
            time.sleep(1)
        print(f"⏰ Timeout waiting for DeltaCAT job completion after {timeout} seconds")
        return False

    except Exception as e:
        print(f"❌ Error monitoring DeltaCAT jobs: {e}")
        # Fall back to short sleep if monitoring fails
        print(f"🔄 Falling back to {timeout}-second wait...")
        time.sleep(timeout)
        return True


def verify_duplicate_resolution(
    table_name, warehouse_path="/tmp/iceberg_rest_warehouse"
):
    """
    Verify that the DeltaCAT converter successfully resolved duplicates.
    """
    try:
        print(f"\n🔍 Verifying duplicate resolution for table: {table_name}")

        # Create PyIceberg catalog to check results
        verification_catalog = load_catalog(
            "workflow_verification_catalog",
            **{
                "type": "rest",
                "warehouse": warehouse_path,
                "uri": "http://localhost:8181",
            },
        )

        # Load the table and scan its contents
        table_identifier = table_name
        tbl = verification_catalog.load_table(table_identifier)
        scan_result = tbl.scan().to_arrow().to_pydict()

        # Check the results
        result_ids = sorted(scan_result["id"])
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
            for i, id_val in enumerate(scan_result["id"]):
                names_by_id[id_val] = scan_result["name"][i]
                versions_by_id[id_val] = scan_result["version"][i]

            if (
                names_by_id.get(2) == "Robert"
                and names_by_id.get(3) == "Charles"
                and versions_by_id.get(2) == 2
                and versions_by_id.get(3) == 2
            ):
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
