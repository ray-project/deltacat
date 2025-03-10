from typing import Optional, List
import uuid
from pyiceberg.table.snapshots import (
    Operation,
)
from pyiceberg.manifest import (
    DataFileContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestFile,
    write_manifest,
)
import itertools
from pyiceberg.utils.concurrent import ExecutorFactory
from pyiceberg.table import UpdateSnapshot, _SnapshotProducer


class _ReplaceFiles(_SnapshotProducer["_ReplaceFiles"]):
    """Overwrites data from the table. This will produce an OVERWRITE snapshot.

    Data and delete files were added and removed in a logical overwrite operation.
    """

    def _existing_manifests(self) -> List[ManifestFile]:
        """Determine if there are any existing manifest files."""
        existing_files = []
        snapshot = self._transaction.table_metadata.current_snapshot()
        if snapshot:
            for manifest_file in snapshot.manifests(io=self._io):
                entries = manifest_file.fetch_manifest_entry(
                    io=self._io, discard_deleted=True
                )

                found_deleted_data_files = [
                    entry.data_file
                    for entry in entries
                    if entry.data_file in self._deleted_data_files
                ]

                if len(found_deleted_data_files) == 0:
                    existing_files.append(manifest_file)
                else:
                    # We have to replace the manifest file without the deleted data files
                    if any(
                        entry.data_file not in found_deleted_data_files
                        for entry in entries
                    ):
                        with write_manifest(
                            format_version=self._transaction.table_metadata.format_version,
                            spec=self._transaction.table_metadata.specs()[
                                manifest_file.partition_spec_id
                            ],
                            schema=self._transaction.table_metadata.schema(),
                            output_file=self.new_manifest_output(),
                            snapshot_id=self._snapshot_id,
                        ) as writer:
                            [
                                writer.add_entry(
                                    ManifestEntry(
                                        status=ManifestEntryStatus.EXISTING,
                                        snapshot_id=entry.snapshot_id,
                                        sequence_number=entry.sequence_number,
                                        file_sequence_number=entry.file_sequence_number,
                                        data_file=entry.data_file,
                                    )
                                )
                                for entry in entries
                                if entry.data_file not in found_deleted_data_files
                            ]
                        existing_files.append(writer.to_manifest_file())
        return existing_files

    def _deleted_entries(self) -> List[ManifestEntry]:
        """To determine if we need to record any deleted entries.

        With a full overwrite all the entries are considered deleted.
        With partial overwrites we have to use the predicate to evaluate
        which entries are affected.
        """
        if self._parent_snapshot_id is not None:
            previous_snapshot = self._transaction.table_metadata.snapshot_by_id(
                self._parent_snapshot_id
            )
            if previous_snapshot is None:
                # This should never happen since you cannot overwrite an empty table
                raise ValueError(
                    f"Could not find the previous snapshot: {self._parent_snapshot_id}"
                )

            executor = ExecutorFactory.get_or_create()

            def _get_entries(manifest: ManifestFile) -> List[ManifestEntry]:
                return [
                    ManifestEntry(
                        status=ManifestEntryStatus.DELETED,
                        snapshot_id=entry.snapshot_id,
                        sequence_number=entry.sequence_number,
                        file_sequence_number=entry.file_sequence_number,
                        data_file=entry.data_file,
                    )
                    for entry in manifest.fetch_manifest_entry(
                        self._io, discard_deleted=True
                    )
                    if entry.data_file.content == DataFileContent.DATA
                    and entry.data_file in self._deleted_data_files
                ]

            list_of_entries = executor.map(
                _get_entries, previous_snapshot.manifests(self._io)
            )
            return list(itertools.chain(*list_of_entries))
        else:
            return []


def replace(
    self,
    commit_uuid: Optional[uuid.UUID] = None,
    using_starting_sequence: Optional[bool] = False,
) -> _ReplaceFiles:
    print(
        f"tansaction_current_snapshot:{self._transaction.table_metadata.current_snapshot()}"
    )
    return _ReplaceFiles(
        commit_uuid=commit_uuid,
        operation=Operation.REPLACE
        if self._transaction.table_metadata.current_snapshot() is not None
        else Operation.APPEND,
        transaction=self._transaction,
        io=self._io,
        snapshot_properties=self._snapshot_properties,
        using_starting_sequence=using_starting_sequence,
    )


UpdateSnapshot.replace = replace


def commit_replace_snapshot(
    iceberg_table, to_be_deleted_files_list, new_position_delete_files
):
    tx = iceberg_table.transaction()
    snapshot_properties = {}
    commit_uuid = uuid.uuid4()
    update_snapshot = tx.update_snapshot(snapshot_properties=snapshot_properties)
    replace_snapshot = replace(
        self=update_snapshot, commit_uuid=commit_uuid, using_starting_sequence=False
    )
    for to_be_deleted_file in to_be_deleted_files_list:
        replace_snapshot.append_data_file(to_be_deleted_file)
    for to_be_added_file in new_position_delete_files:
        replace_snapshot.delete_data_file(to_be_added_file)
    replace_snapshot._commit()
    tx.commit_transaction()


def commit_overwrite_snapshot(
    iceberg_table, to_be_deleted_files_list, new_position_delete_files
):
    commit_uuid = uuid.uuid4()
    with iceberg_table.transaction() as tx:
        if iceberg_table.metadata.name_mapping() is None:
            iceberg_table.set_properties(
                **{
                    "schema.name-mapping.default": iceberg_table.table_metadata.schema().name_mapping.model_dump_json()
                }
            )
        with tx.update_snapshot().overwrite(
            commit_uuid=commit_uuid
        ) as overwrite_snapshot:
            for data_file in new_position_delete_files:
                overwrite_snapshot.append_data_file(data_file)
            for original_data_file in to_be_deleted_files_list:
                overwrite_snapshot.delete_data_file(original_data_file)
