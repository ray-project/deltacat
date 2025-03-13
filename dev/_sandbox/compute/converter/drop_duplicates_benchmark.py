import resource
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import time
import hashlib
import secrets

# This benchmark script is meant for testing out performance results based on different combinations of Pyarrow.compute functions for converter.
# Main performance factors are: latency and memory consumption.


def generate_random_str_pk_column_sha1(number_of_records):
    sha1_hashes = []
    for _ in range(number_of_records):
        # Generate a random string using secrets module for security
        random_string = secrets.token_urlsafe(10)

        # Calculate SHA1 hash
        sha1_hash = hashlib.sha1(random_string.encode("utf-8")).hexdigest()
        sha1_hashes.append(sha1_hash)
    return pa.array(sha1_hashes)


# Example 1:
# 1. Invert used memory is negligible
# 2. Group_by + aggregate used memory is subjective to duplication rate, less duplication, more replicate of the group_by columns.
# 3. is_in is zero-copy, memory used also is subjective to duplication rate, worst case copy of sort column
def basic_pyarrow_compute_function_used_performance_demo():
    # Creating table
    pk_column = np.random.randint(0, 100000, size=1000000)
    data_table_1 = pa.table({"pk_hash": pk_column})
    num_rows_1 = data_table_1.num_rows
    row_numbers_1 = pa.array(np.arange(num_rows_1, dtype=np.int64))
    data_table_1 = data_table_1.append_column("record_idx", row_numbers_1)
    memory_used_after_creating_table = resource.getrusage(
        resource.RUSAGE_SELF
    ).ru_maxrss
    print(f"memory_used_after_creating_table:{memory_used_after_creating_table}")

    # Group_by + aggregate on unique sort column to drop duplicates within same pyarrow table
    data_table_1_indices = data_table_1.group_by(
        "pk_hash", use_threads=False
    ).aggregate([("record_idx", "max")])
    memory_used_after_group_by = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    memory_used_by_group_by = (
        memory_used_after_group_by - memory_used_after_creating_table
    )
    print(f"memory_used_by_group_by:{memory_used_by_group_by}")

    # is_in to get indices to keep
    mask_table_1 = pc.is_in(
        data_table_1["record_idx"],
        value_set=data_table_1_indices["record_idx_max"],
    )
    memory_used_after_is_in = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    memory_used_by_is_in = memory_used_after_is_in - memory_used_after_group_by
    print(f"memory_used_by_is_in:{memory_used_by_is_in}")

    # invert to get indices to delete
    mask_table_1_invert = pc.invert(mask_table_1)
    memory_used_after_invert = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    memory_used_by_invert = memory_used_after_invert - memory_used_after_is_in
    print(f"memory_used_by_invert :{memory_used_by_invert}")

    # filter to get the actual table
    table_to_delete_1 = data_table_1.filter(mask_table_1_invert)
    memory_used_after_filter = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    memory_used_by_filter = memory_used_after_filter - memory_used_after_invert
    print(f"memory_used_by_filter:{memory_used_by_filter}")
    res = table_to_delete_1
    return res


# Approach 1A: deterministic record to keep, extendable to support custom sort keys
# Drop duplicates within each file, then global drop duplicates.
# Pros: a.With high duplication rate table, global drop duplicates consume less memory
# b.Reocrd index column appended is a required field by position delete file, so not using "additional" memory for pos column
# Assumption: when concat table for final global drop duplicates, maintain ascending file_sequence_number: pa.concat_table([table_1,table_2, ...,table_N])
def group_by_each_file_then_global_drop_duplicates_using_max_record_position():

    pk_column = np.random.randint(0, 100, size=10000)

    data_table_1 = pa.table({"pk_hash": pk_column})
    data_table_2 = pa.table({"pk_hash": pk_column})
    num_rows_1 = data_table_1.num_rows
    row_numbers_1 = pa.array(np.arange(num_rows_1, dtype=np.int64))
    data_table_1 = data_table_1.append_column("record_idx", row_numbers_1)
    num_rows_2 = data_table_2.num_rows
    row_numbers_2 = pa.array(np.arange(num_rows_2, dtype=np.int64))
    data_table_2 = data_table_2.append_column("record_idx", row_numbers_2)

    start = time.monotonic()
    data_table_1_indices = data_table_1.group_by(
        "pk_hash", use_threads=False
    ).aggregate([("record_idx", "max")])
    data_table_2_indices = data_table_2.group_by(
        "pk_hash", use_threads=False
    ).aggregate([("record_idx", "max")])

    # Drop duplicates within same file
    mask_table_1 = pc.is_in(
        data_table_1["record_idx"],
        value_set=data_table_1_indices["record_idx_max"],
    )
    mask_table_1_invert = pc.invert(mask_table_1)
    table_to_delete_1 = data_table_1.filter(mask_table_1_invert)
    table_to_keep_1 = data_table_1.filter(mask_table_1)

    mask_table_2 = pc.is_in(
        data_table_2["record_idx"],
        value_set=data_table_2_indices["record_idx_max"],
    )
    mask_table_2_invert = pc.invert(mask_table_2)
    table_to_delete_2 = data_table_2.filter(mask_table_2_invert)
    table_to_keep_2 = data_table_2.filter(mask_table_2)

    # Global drop duplicates with
    final_data_table = pa.concat_tables([table_to_keep_1, table_to_keep_2])
    num_rows = final_data_table.num_rows
    row_numbers = pa.array(np.arange(num_rows, dtype=np.int64))
    final_data_table = final_data_table.append_column("record_idx_final", row_numbers)
    final_data_table_indices = final_data_table.group_by(
        "pk_hash", use_threads=False
    ).aggregate([("record_idx_final", "max")])
    end = time.monotonic()
    print(f"Time taken to perform two group_by:{end - start}")
    final_data_table_to_delete = final_data_table.filter(
        pc.invert(
            pc.is_in(
                final_data_table["record_idx_final"],
                value_set=final_data_table_indices["record_idx_final_max"],
            )
        )
    )
    final_data_table_to_delete = final_data_table_to_delete.drop(["record_idx_final"])
    result = pa.concat_tables(
        [final_data_table_to_delete, table_to_delete_1, table_to_delete_2]
    )
    print(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return result


# Approach 1B: deterministic record to keep, extendable to support custom sort keys
# Slight difference than 1A in using "last" record instead of "max" record in sort column, shows reduction in latency.
def group_by_each_file_then_global_drop_duplicates_using_last_record_position():
    print(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

    pk_column = np.random.randint(0, 100, size=10000)

    data_table_1 = pa.table({"pk_hash": pk_column})
    data_table_2 = pa.table({"pk_hash": pk_column})
    num_rows_1 = data_table_1.num_rows
    row_numbers_1 = pa.array(np.arange(num_rows_1, dtype=np.int64))
    data_table_1 = data_table_1.append_column("record_idx", row_numbers_1)
    num_rows_2 = data_table_2.num_rows
    row_numbers_2 = pa.array(np.arange(num_rows_2, dtype=np.int64))
    data_table_2 = data_table_2.append_column("record_idx", row_numbers_2)

    print(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

    start = time.monotonic()
    data_table_1_indices = data_table_1.group_by(
        "pk_hash", use_threads=False
    ).aggregate([("record_idx", "last")])
    data_table_2_indices = data_table_2.group_by(
        "pk_hash", use_threads=False
    ).aggregate([("record_idx", "last")])

    mask_table_1 = pc.is_in(
        data_table_1["record_idx"],
        value_set=data_table_1_indices["record_idx_last"],
    )

    mask_table_1_invert = pc.invert(mask_table_1)
    table_to_delete_1 = data_table_1.filter(mask_table_1_invert)
    table_to_keep_1 = data_table_1.filter(mask_table_1)

    mask_table_2 = pc.is_in(
        data_table_2["record_idx"],
        value_set=data_table_2_indices["record_idx_last"],
    )
    mask_table_2_invert = pc.invert(mask_table_2)
    table_to_delete_2 = data_table_2.filter(mask_table_2_invert)
    table_to_keep_2 = data_table_2.filter(mask_table_2)

    final_data_table = pa.concat_tables([table_to_keep_1, table_to_keep_2])
    num_rows = final_data_table.num_rows
    row_numbers = pa.array(np.arange(num_rows, dtype=np.int64))
    final_data_table = final_data_table.append_column("record_idx_final", row_numbers)
    final_data_table_indices = final_data_table.group_by(
        "pk_hash", use_threads=False
    ).aggregate([("record_idx_final", "last")])
    end = time.monotonic()
    print(f"Time taken to perform two group_by:{end - start}")
    final_data_table_to_delete = final_data_table.filter(
        pc.invert(
            pc.is_in(
                final_data_table["record_idx_final"],
                value_set=final_data_table_indices["record_idx_final_last"],
            )
        )
    )
    final_data_table_to_delete = final_data_table_to_delete.drop(["record_idx_final"])
    result = pa.concat_tables(
        [final_data_table_to_delete, table_to_delete_1, table_to_delete_2]
    )
    print(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return result


# Approach 2: deterministic record to keep, additional code changes may need to sort any custom sort keys
# Global group_by based on two aggregation 1. file_sequence_number as primary 2. index column
# Pros: With lower duplication rate table, compared with file-level drop duplicates then global drop duplicates in Approach 1, can save (N-1) group_by operation.
# Pros: Table concat order doesn't have to be maintained as file_sequence_number will be appended as a column and used as criteria to aggregation.
# Cons: Any custom sort keys other than record_idx may need verification of correctness before replacing the record_idx column.
# Cons: higher risk of OOM with global dropping duplicates.
def group_by_each_file_then_global_drop_duplicates_using_just_file_sequence_number():
    print(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

    pk_column = np.random.randint(0, 10, size=10000)
    file_sequence_number_column_1 = np.repeat(1, (10000))
    file_sequence_number_column_2 = np.repeat(2, (10000))
    data_table_1 = pa.table(
        {"pk_hash": pk_column, "file_sequence_number": file_sequence_number_column_1}
    )
    data_table_2 = pa.table(
        {"pk_hash": pk_column, "file_sequence_number": file_sequence_number_column_2}
    )
    num_rows_1 = data_table_1.num_rows
    row_numbers_1 = pa.array(np.arange(num_rows_1, dtype=np.int64))
    data_table_1 = data_table_1.append_column("record_idx", row_numbers_1)
    num_rows_2 = data_table_2.num_rows
    row_numbers_2 = pa.array(np.arange(num_rows_2, dtype=np.int64))
    data_table_2 = data_table_2.append_column("record_idx", row_numbers_2)

    start = time.monotonic()

    final_data_table = pa.concat_tables([data_table_1, data_table_2])
    num_rows = final_data_table.num_rows
    row_numbers = pa.array(np.arange(num_rows, dtype=np.int64))
    final_data_table = final_data_table.append_column("record_idx_final", row_numbers)
    final_data_table_indices = final_data_table.group_by(
        "pk_hash", use_threads=False
    ).aggregate([("file_sequence_number", "max"), ("record_idx_final", "max")])
    end = time.monotonic()
    print(f"Time taken to perform two group_by:{end - start}")
    final_data_table_to_delete = final_data_table.filter(
        pc.is_in(
            final_data_table["record_idx_final"],
            value_set=final_data_table_indices["record_idx_final_max"],
        )
    )
    final_data_table_to_delete = final_data_table_to_delete.drop(["record_idx_final"])
    result = final_data_table_to_delete

    print(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return result


# Approach 3: For large dataset: deterministic record to keep, extendable to support custom sort keys
# To be able to process large dataset, we can repeat the process of:
# From file sequence number N, to first file in descending order
# 1. For file N-1, Aggregate by record_idx to drop duplicates within same file, res1 = records to delete because of duplicates
# 2. use Pyarrow is_in to delete in File N-1 for records have same primary key as records in File N, res2 = records to delete because of same primary key
# 3. delete for File N-1 = res1 + res2
# 4. Append to primary keys the distinct keys appear in file N-1 and use this growing union of primary keys to delete for files N-2, N-3...1.
def multiple_is_in_for_large_dataset():
    print(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

    pk_column = np.random.randint(0, 10, size=10000)
    file_sequence_number_column_1 = np.repeat(1, (10000))
    file_sequence_number_column_2 = np.repeat(2, (10000))
    file_sequence_number_column_3 = np.repeat(3, (10000))
    data_table_1 = pa.table(
        {"pk_hash": pk_column, "file_sequence_number": file_sequence_number_column_1}
    )
    data_table_2 = pa.table(
        {"pk_hash": pk_column, "file_sequence_number": file_sequence_number_column_2}
    )
    data_table_3 = pa.table(
        {"pk_hash": pk_column, "file_sequence_number": file_sequence_number_column_3}
    )
    num_rows_1 = data_table_1.num_rows
    row_numbers_1 = pa.array(np.arange(num_rows_1, dtype=np.int64))
    data_table_1 = data_table_1.append_column("record_idx", row_numbers_1)
    num_rows_2 = data_table_2.num_rows
    row_numbers_2 = pa.array(np.arange(num_rows_2, dtype=np.int64))
    data_table_2 = data_table_2.append_column("record_idx", row_numbers_2)
    num_rows_3 = data_table_3.num_rows
    row_numbers_3 = pa.array(np.arange(num_rows_3, dtype=np.int64))
    data_table_3 = data_table_3.append_column("record_idx", row_numbers_3)

    print(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

    # Drop duplicates in table 3 (table with largest file sequence number)
    data_table_3_indices = data_table_3.group_by(
        "pk_hash", use_threads=False
    ).aggregate([("record_idx", "max")])

    mask_table_3 = pc.is_in(
        data_table_1["record_idx"],
        value_set=data_table_3_indices["record_idx_max"],
    )
    mask_table_3_invert = pc.invert(mask_table_3)
    table_to_delete_3 = data_table_3.filter(mask_table_3_invert)
    table_to_keep_3 = data_table_3.filter(mask_table_3)

    # drop duplicates in data table 2
    data_table_2_indices = data_table_2.group_by(
        "pk_hash", use_threads=False
    ).aggregate([("record_idx", "max")])
    data_table_2_to_delete_for_duplicates = data_table_2.filter(
        pc.invert(
            pc.is_in(
                data_table_2["record_idx"],
                value_set=data_table_2_indices["record_idx_max"],
            )
        )
    )
    data_table_2 = data_table_2.filter(
        pc.is_in(
            data_table_2["record_idx"], value_set=data_table_2_indices["record_idx_max"]
        )
    )
    table_to_delete_2_indices = pc.is_in(
        data_table_2["pk_hash"],
        value_set=table_to_keep_3["pk_hash"],
    )
    new_indices_in_table_2 = pc.invert(table_to_delete_2_indices)
    table_to_delete_2 = data_table_2.filter(table_to_delete_2_indices)
    new_table_to_keep = data_table_2.filter(new_indices_in_table_2)
    primary_key_indices_to_keep_after_2 = pa.concat_tables(
        [new_table_to_keep, table_to_keep_3]
    )

    result_for_file_2 = pa.concat_tables(
        [table_to_delete_2, data_table_2_to_delete_for_duplicates]
    )
    return table_to_delete_3, result_for_file_2, primary_key_indices_to_keep_after_2
