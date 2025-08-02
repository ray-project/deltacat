from typing import List, Dict
from collections import defaultdict
import uuid
from pyiceberg.table import Table
from pyiceberg.table.snapshots import (
    Operation,
)
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestFile,
    write_manifest,
)
import itertools
from pyiceberg.utils.concurrent import ExecutorFactory
from pyiceberg.table.update.snapshot import _SnapshotProducer, UpdateSnapshot


def replace_delete_files_override(
    update_snapshot: UpdateSnapshot,
) -> "_ReplaceDeleteFilesOverride":
    commit_uuid = uuid.uuid4()
    return _ReplaceDeleteFilesOverride(
        commit_uuid=commit_uuid,
        operation=Operation.OVERWRITE,
        transaction=update_snapshot._transaction,
        io=update_snapshot._io,
        snapshot_properties=update_snapshot._snapshot_properties,
    )


class _ReplaceDeleteFilesOverride(_SnapshotProducer):
    def _manifests(self) -> List[ManifestFile]:
        def _write_added_manifest() -> List[ManifestFile]:
            if self._added_data_files:
                with write_manifest(
                    format_version=self._transaction.table_metadata.format_version,
                    spec=self._transaction.table_metadata.spec(),
                    schema=self._transaction.table_metadata.schema(),
                    output_file=self.new_manifest_output(),
                    snapshot_id=self._snapshot_id,
                ) as writer:
                    for data_file in self._added_data_files:
                        writer.add(
                            ManifestEntry(
                                status=ManifestEntryStatus.ADDED,
                                snapshot_id=self._snapshot_id,
                                sequence_number=None,
                                file_sequence_number=None,
                                data_file=data_file,
                            )
                        )
                        writer.content = self.writer_content
                return [writer.to_manifest_file()]
            else:
                return []

        def _write_delete_manifest() -> List[ManifestFile]:
            # Check if we need to mark the files as deleted
            deleted_entries = self._deleted_entries()
            if len(deleted_entries) > 0:
                deleted_manifests = []
                partition_groups: Dict[int, List[ManifestEntry]] = defaultdict(list)
                for deleted_entry in deleted_entries:
                    partition_groups[deleted_entry.data_file.spec_id].append(
                        deleted_entry
                    )
                for spec_id, entries in partition_groups.items():
                    with write_manifest(
                        format_version=self._transaction.table_metadata.format_version,
                        spec=self._transaction.table_metadata.specs()[spec_id],
                        schema=self._transaction.table_metadata.schema(),
                        output_file=self.new_manifest_output(),
                        snapshot_id=self._snapshot_id,
                    ) as writer:
                        for entry in entries:
                            writer.add_entry(entry)
                    deleted_manifests.append(writer.to_manifest_file())
                return deleted_manifests
            else:
                return []

        executor = ExecutorFactory.get_or_create()

        added_manifests = executor.submit(_write_added_manifest)
        existing_manifests = executor.submit(self._existing_manifests)
        delete_manifests = executor.submit(_write_delete_manifest)
        return self._process_manifests(
            added_manifests.result()
            + existing_manifests.result()
            + delete_manifests.result()
        )

    def writer_content(self) -> ManifestContent:
        return ManifestContent.DELETES

    def _existing_manifests(self) -> List[ManifestFile]:
        """To determine if there are any existing manifest files.

        A fast append will add another ManifestFile to the ManifestList.
        All the existing manifest files are considered existing.
        """
        existing_manifests = []

        if self._parent_snapshot_id is not None:
            previous_snapshot = self._transaction.table_metadata.snapshot_by_id(
                self._parent_snapshot_id
            )

            if previous_snapshot is None:
                raise ValueError(
                    f"Snapshot could not be found: {self._parent_snapshot_id}"
                )

            for manifest in previous_snapshot.manifests(io=self._io):
                if (
                    manifest.has_added_files()
                    or manifest.has_existing_files()
                    or manifest.added_snapshot_id == self._snapshot_id
                ):
                    existing_manifests.append(manifest)

        return existing_manifests

    def _deleted_entries(self) -> List[ManifestEntry]:
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
                    if entry.data_file.content == DataFileContent.EQUALITY_DELETES
                    and entry.data_file in self._deleted_data_files
                ]

            list_of_entries = executor.map(
                _get_entries, previous_snapshot.manifests(self._io)
            )
            return list(itertools.chain(*list_of_entries))
        else:
            return []


def commit_append_snapshot(
    iceberg_table: Table, new_position_delete_files: List[DataFile]
) -> str:
    tx = iceberg_table.transaction()
    try:
        if iceberg_table.metadata.name_mapping() is None:
            tx.set_properties(
                **{
                    "schema.name-mapping.default": tx.table_metadata.schema().name_mapping.model_dump_json()
                }
            )
        with append_delete_files_override(tx.update_snapshot()) as append_snapshot:
            if new_position_delete_files:
                for data_file in new_position_delete_files:
                    append_snapshot.append_data_file(data_file)

    except Exception as e:
        raise e
    else:
        metadata = tx.commit_transaction().metadata
        return (metadata, append_snapshot._snapshot_id)


def append_delete_files_override(
    update_snapshot: UpdateSnapshot,
) -> "_AppendDeleteFilesOverride":
    commit_uuid = uuid.uuid4()
    return _AppendDeleteFilesOverride(
        commit_uuid=commit_uuid,
        operation=Operation.APPEND,
        transaction=update_snapshot._transaction,
        io=update_snapshot._io,
        snapshot_properties=update_snapshot._snapshot_properties,
    )


class _AppendDeleteFilesOverride(_SnapshotProducer):
    def _manifests(self) -> List[ManifestFile]:
        def _write_added_manifest() -> List[ManifestFile]:
            if self._added_data_files:
                with write_manifest(
                    format_version=self._transaction.table_metadata.format_version,
                    spec=self._transaction.table_metadata.spec(),
                    schema=self._transaction.table_metadata.schema(),
                    output_file=self.new_manifest_output(),
                    snapshot_id=self._snapshot_id,
                ) as writer:
                    for data_file in self._added_data_files:
                        writer.add(
                            ManifestEntry(
                                status=ManifestEntryStatus.ADDED,
                                snapshot_id=self._snapshot_id,
                                sequence_number=None,
                                file_sequence_number=None,
                                data_file=data_file,
                            )
                        )
                        writer.content = self.writer_content
                return [writer.to_manifest_file()]
            else:
                return []

        executor = ExecutorFactory.get_or_create()

        added_manifests = executor.submit(_write_added_manifest)
        existing_manifests = executor.submit(self._existing_manifests)

        return self._process_manifests(
            added_manifests.result() + existing_manifests.result()
        )

    def writer_content(self) -> ManifestContent:
        return ManifestContent.DELETES

    def _existing_manifests(self) -> List[ManifestFile]:
        """To determine if there are any existing manifest files.

        A fast append will add another ManifestFile to the ManifestList.
        All the existing manifest files are considered existing.
        """
        existing_manifests = []

        if self._parent_snapshot_id is not None:
            previous_snapshot = self._transaction.table_metadata.snapshot_by_id(
                self._parent_snapshot_id
            )

            if previous_snapshot is None:
                raise ValueError(
                    f"Snapshot could not be found: {self._parent_snapshot_id}"
                )

            for manifest in previous_snapshot.manifests(io=self._io):
                if (
                    manifest.has_added_files()
                    or manifest.has_existing_files()
                    or manifest.added_snapshot_id == self._snapshot_id
                ):
                    existing_manifests.append(manifest)

        return existing_manifests

    def _deleted_entries(self) -> List[ManifestEntry]:
        """To determine if we need to record any deleted manifest entries.

        In case of an append, nothing is deleted.
        """
        return []


def commit_replace_snapshot(
    iceberg_table: Table,
    new_position_delete_files: List[DataFile],
    to_be_deleted_files: List[DataFile],
) -> str:
    tx = iceberg_table.transaction()
    try:
        if iceberg_table.metadata.name_mapping() is None:
            tx.set_properties(
                **{
                    "schema.name-mapping.default": tx.table_metadata.schema().name_mapping.model_dump_json()
                }
            )
        with replace_delete_files_override(
            tx.update_snapshot()
        ) as replace_delete_snapshot:
            if new_position_delete_files:
                for data_file in new_position_delete_files:
                    replace_delete_snapshot.append_data_file(data_file)
            if to_be_deleted_files:
                for delete_file in to_be_deleted_files:
                    replace_delete_snapshot.delete_data_file(delete_file)
    except Exception as e:
        raise e
    else:
        metadata = tx.commit_transaction().metadata
        return (metadata, replace_delete_snapshot._snapshot_id)
