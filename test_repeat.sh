#!/bin/bash

# pytest deltacat/tests/compute/test_janitor.py -s

# 1️⃣ Set how many times to repeat each test run
n=20   # <-- change this number as needed

# 2️⃣ Counters for failures
fail_no_s=0
fail_with_s=0

echo "Running WITHOUT -s (no print output)..."
for i in $(seq 1 $n); do
    echo "Run $i/$n (WITHOUT -s)"
    pytest deltacat/tests/storage/model/test_metafile_io.py::TestMetafileIO::test_txn_conflict_concurrent_multiprocess_table_create > run_no_s.log 2>&1
    if [ $? -ne 0 ]; then
        fail_no_s=$((fail_no_s+1))
        echo "❌ Failed (WITHOUT -s)"
    else
        echo "✅ Passed"
    fi
done

echo ""
echo "Running WITH -s (print output enabled)..."
for i in $(seq 1 $n); do
    echo "Run $i/$n (WITH -s)"
    pytest -s deltacat/tests/storage/model/test_metafile_io.py::TestMetafileIO::test_txn_conflict_concurrent_multiprocess_table_create > run_with_s.log 2>&1
    if [ $? -ne 0 ]; then
        fail_with_s=$((fail_with_s+1))
        echo "❌ Failed (WITH -s)"
    else
        echo "✅ Passed"
    fi
done

# 3️⃣ Final summary
echo ""
echo "results WITHOUT print output enabled: $fail_no_s/$n runs failed."
echo "results WITH print output enabled:    $fail_with_s/$n runs failed."
