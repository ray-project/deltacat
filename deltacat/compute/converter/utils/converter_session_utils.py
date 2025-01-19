def check_data_files_sequence_number(data_files_list, equality_delete_files_list):
    data_files_list.sort(key=lambda file_tuple: file_tuple[0])
    equality_delete_files_list.sort(key=lambda file_tuple: file_tuple[0])

    equality_delete_files = []
    result_data_file = []

    # Pointer for list data_file
    data_file_pointer = 0

    debug_equality_delete_files = []

    # Loop through each value in equality_delete_file
    for equality_file_tuple in equality_delete_files_list:
        # Find all values in data_file that are smaller than val_equality
        valid_values = []

        debug_valid_values = []
        # Move data_file_pointer to the first value in data_file that is smaller than val_equality
        while (
            data_file_pointer < len(data_files_list)
            and data_files_list[data_file_pointer][0] < equality_file_tuple[0]
        ):
            valid_values.append(data_files_list[data_file_pointer][1])
            debug_valid_values.append(data_files_list[data_file_pointer])
            data_file_pointer += 1
            equality_delete_files.append(equality_file_tuple[1])
            debug_equality_delete_files.append(equality_file_tuple)

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
