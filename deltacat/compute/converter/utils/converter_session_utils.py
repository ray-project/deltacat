from collections import defaultdict
import logging
from deltacat import logs
from deltacat.compute.converter.model.convert_input_files import ConvertInputFiles

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def check_data_files_sequence_number(data_files_list, equality_delete_files_list):
    # Sort by file sequence number
    data_files_list.sort(key=lambda file_tuple: file_tuple[0])
    equality_delete_files_list.sort(key=lambda file_tuple: file_tuple[0])

    equality_delete_files = []
    result_data_file = []

    # Pointer for list data_file
    data_file_pointer = 0

    # Loop through each value in equality_delete_file
    for equality_file_tuple in equality_delete_files_list:
        # Find all values in data_file that are smaller than val_equality
        valid_values = []

        # Move data_file_pointer to the first value in data_file that is smaller than val_equality
        while (
            data_file_pointer < len(data_files_list)
            and data_files_list[data_file_pointer][0] < equality_file_tuple[0]
        ):
            valid_values.append(data_files_list[data_file_pointer])
            data_file_pointer += 1
            equality_delete_files.append(equality_file_tuple)

        # Append the value from equality_delete_file and the corresponding valid values from data_file
        if valid_values:
            result_data_file.append(valid_values)

    result_equality_delete_file = append_larger_sequence_number_data_files(
        equality_delete_files
    )

    return result_equality_delete_file, result_data_file


def append_larger_sequence_number_data_files(data_files_list):
    result = []
    # Iterate over the input list
    for i in range(len(data_files_list)):
        sublist = data_files_list[i:]
        sublist_file_list = []
        for file in sublist:
            sublist_file_list.append(file)
        result.append(sublist_file_list)
    return result


def construct_iceberg_table_prefix(
    iceberg_warehouse_bucket_name, table_name, iceberg_namespace
):
    return f"{iceberg_warehouse_bucket_name}/{iceberg_namespace}/{table_name}/data"


def partition_value_record_to_partition_value_string(partition):
    # Get string representation of partition value out of Record[partition_value]
    partition_value_str = partition.__repr__().split("[", 1)[1].split("]")[0]
    return partition_value_str


def group_all_files_to_each_bucket(
    data_file_dict, equality_delete_dict, pos_delete_dict
):
    convert_input_files_for_all_buckets = []
    files_for_each_bucket_for_deletes = defaultdict(tuple)
    if equality_delete_dict:
        for partition_value, equality_delete_file_list in equality_delete_dict.items():
            (
                result_equality_delete_file,
                result_data_file,
            ) = check_data_files_sequence_number(
                data_files_list=data_file_dict[partition_value],
                equality_delete_files_list=equality_delete_dict[partition_value],
            )
            files_for_each_bucket_for_deletes[partition_value] = (
                result_data_file,
                result_equality_delete_file,
                [],
            )
            if partition_value not in data_file_dict:
                convert_input_file = ConvertInputFiles.of(
                    partition_value=partition_value,
                    applicable_data_files=result_data_file,
                    applicable_equalitu_delete_files=result_equality_delete_file,
                )
                convert_input_files_for_all_buckets.append(convert_input_file)

    for partition_value, all_data_files_for_each_bucket in data_file_dict.items():
        convert_input_file = ConvertInputFiles.of(
            partition_value=partition_value,
            all_data_files_for_dedupe=all_data_files_for_each_bucket,
        )
        if partition_value in files_for_each_bucket_for_deletes:
            convert_input_file.applicable_data_files = (
                files_for_each_bucket_for_deletes[partition_value][0]
            )
            convert_input_file.applicable_delete_files = (
                files_for_each_bucket_for_deletes[partition_value][1]
            )
        convert_input_files_for_all_buckets.append(convert_input_file)
    return convert_input_files_for_all_buckets
