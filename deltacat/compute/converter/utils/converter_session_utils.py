from collections import defaultdict
import logging
from deltacat import logs
from deltacat.compute.converter.model.convert_input_files import (
    ConvertInputFiles,
    DataFileList,
    DataFileListGroup,
)
from typing import List, Dict, Tuple, Any
from enum import Enum
from pyiceberg.manifest import DataFile

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def check_data_files_sequence_number(
    data_files_list: DataFileList,
    equality_delete_files_list: DataFileList,
) -> Tuple[DataFileListGroup, DataFileListGroup]:
    # Sort by file sequence number
    data_files_list.sort(key=lambda file_tuple: file_tuple[0])
    equality_delete_files_list.sort(key=lambda file_tuple: file_tuple[0])
    data_file_delete_applicable = []
    result_eq_files_list = []

    # Loop through each value in equality_delete_file
    for data_file_tuple in data_files_list:

        # Find all values in equality delete file that having a larger sequence number than current data file
        valid_values_eq = []

        # Pointer for equality delete file
        eq_file_pointer = 0
        # Move data_file_pointer to the first value in data_file that is smaller than val_equality
        while (
            eq_file_pointer < len(equality_delete_files_list)
            and equality_delete_files_list[eq_file_pointer][0] > data_file_tuple[0]
        ):
            valid_values_eq.append(equality_delete_files_list[eq_file_pointer])
            eq_file_pointer += 1

        if valid_values_eq:
            # Append the value for both applicable eq files list and applicable data files list
            data_file_delete_applicable.append(data_file_tuple)
            result_eq_files_list.append(valid_values_eq)

    res_data_file_list = []
    res_equality_delete_file_list = []
    merged_file_dict = defaultdict(list)
    for data_file_sublist, eq_delete_sublist in zip(
        data_file_delete_applicable, result_eq_files_list
    ):
        merged_file_dict[tuple(eq_delete_sublist)].append(data_file_sublist)
    for eq_file_list, data_file_list in merged_file_dict.items():
        res_data_file_list.append(list(set(data_file_list)))
        res_equality_delete_file_list.append(list(set(eq_file_list)))

    assert len(res_data_file_list) == len(res_equality_delete_file_list), (
        f"length of applicable data files list: {len(res_data_file_list)} "
        f"should equal to length of equality delete files list:{len(res_equality_delete_file_list)}"
    )

    return res_equality_delete_file_list, res_data_file_list


def construct_iceberg_table_prefix(
    iceberg_warehouse_bucket_name: str, table_name: str, iceberg_namespace: str
) -> str:
    return f"{iceberg_warehouse_bucket_name}/{iceberg_namespace}/{table_name}/data"


def partition_value_record_to_partition_value_string(partition: Any) -> str:
    # Get string representation of partition value out of Record[partition_value]
    partition_value_str = partition.__repr__().split("[", 1)[1].split("]")[0]
    return partition_value_str


def group_all_files_to_each_bucket(
    data_file_dict: Dict[Any, DataFileList],
    equality_delete_dict: Dict[Any, DataFileList],
    pos_delete_dict: Dict[Any, DataFileList],
) -> List[ConvertInputFiles]:
    convert_input_files_for_all_buckets = []
    files_for_each_bucket_for_deletes = defaultdict(tuple)
    if equality_delete_dict:
        for partition_value, equality_delete_file_list in equality_delete_dict.items():
            if partition_value in data_file_dict:
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

    for partition_value, all_data_files_for_each_bucket in data_file_dict.items():
        convert_input_file = ConvertInputFiles.of(
            partition_value=partition_value,
            all_data_files_for_dedupe=all_data_files_for_each_bucket,
        )
        if partition_value in files_for_each_bucket_for_deletes:
            convert_input_file.applicable_data_files = (
                files_for_each_bucket_for_deletes[partition_value][0]
            )
            convert_input_file.applicable_equality_delete_files = (
                files_for_each_bucket_for_deletes[partition_value][1]
            )
        convert_input_files_for_all_buckets.append(convert_input_file)
    return convert_input_files_for_all_buckets


def sort_data_files_maintaining_order(data_files: DataFileList) -> DataFileList:
    """
    Sort data files deterministically based on two criterias:
    1. Sequence number: Newly added files will have a higher sequence number
    2. File path: If file sequence is the same, files are guaranteed to be returned in a deterministic order since file path is unique.
    """
    if data_files:
        data_files = sorted(data_files, key=lambda f: (f[0], f[1].file_path))
    return data_files


class SnapshotType(Enum):
    """Enumeration of possible snapshot types."""

    NONE = "none"
    APPEND = "append"
    REPLACE = "replace"
    DELETE = "delete"


def _get_snapshot_action_description(
    snapshot_type: SnapshotType,
    files_to_delete: List[List[DataFile]],
    files_to_add: List[DataFile],
) -> str:
    """Get a human-readable description of the snapshot action."""
    descriptions = {
        SnapshotType.NONE: "No changes needed",
        SnapshotType.APPEND: f"Adding {len(files_to_add)} new files",
        SnapshotType.REPLACE: f"Replacing {sum(len(files) for files in files_to_delete)} files with {len(files_to_add)} new files",
        SnapshotType.DELETE: f"Deleting {sum(len(files) for files in files_to_delete)} files",
    }
    return descriptions[snapshot_type]


def _determine_snapshot_type(
    to_be_deleted_files: List[List[DataFile]], to_be_added_files: List[DataFile]
) -> SnapshotType:
    """
    Determine the snapshot type based on file changes.

    Args:
        to_be_deleted_files: List of files to be deleted
        to_be_added_files: List of files to be added

    Returns:
        SnapshotType indicating what kind of snapshot to commit
    """
    has_files_to_delete = bool(to_be_deleted_files)
    has_files_to_add = bool(to_be_added_files)

    if not has_files_to_delete and not has_files_to_add:
        return SnapshotType.NONE
    elif not has_files_to_delete and has_files_to_add:
        return SnapshotType.APPEND
    elif has_files_to_delete and has_files_to_add:
        return SnapshotType.REPLACE
    else:  # has_files_to_delete and not has_files_to_add
        return SnapshotType.DELETE
