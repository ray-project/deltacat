
def replace(self, commit_uuid: Optional[uuid.UUID] = None, using_starting_sequence: Optional[bool] = False) -> _ReplaceFiles:
    print(f"tansaction_current_snapshot:{self._transaction.table_metadata.current_snapshot()}")
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

# Overrides SnapshotProducer to allow using deleted files' sequence number
def _manifests(self) -> List[ManifestFile]:
    def _write_added_manifest() -> List[ManifestFile]:
        replace_manifest_using_starting_sequence = None
        if self.using_starting_sequence and self._operation == Operation.REPLACE and self._deleted_entries:
            snapshot = self._transaction.table_metadata.current_snapshot()
            for manifest_file in snapshot.manifests(io=self._io):
                entries = manifest_file.fetch_manifest_entry(io=self._io, discard_deleted=False)
            relevant_manifests = [entry for entry in entries if entry.data_file in self._deleted_data_files]
            replace_manifest_using_starting_sequence = min(
                entry.sequence_number for entry in relevant_manifests)
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
                            sequence_number=replace_manifest_using_starting_sequence if replace_manifest_using_starting_sequence else None,
                            file_sequence_number=None,
                            data_file=data_file,
                        )
                    )
            return [writer.to_manifest_file()]
        else:
            return []


class _ReplaceFiles(_SnapshotProducer["_ReplaceFiles"]):
    """Overwrites data from the table. This will produce an OVERWRITE snapshot.

    Data and delete files were added and removed in a logical overwrite operation.
    """

    def _existing_manifests(self) -> List[ManifestFile]:
        """Determine if there are any existing manifest files."""
        existing_files = []

        if snapshot := self._transaction.table_metadata.current_snapshot():
            for manifest_file in snapshot.manifests(io=self._io):
                entries = manifest_file.fetch_manifest_entry(io=self._io, discard_deleted=True)

                found_deleted_data_files = [entry.data_file for entry in entries if entry.data_file in self._deleted_data_files]

                if len(found_deleted_data_files) == 0:
                    existing_files.append(manifest_file)
                else:
                    # We have to replace the manifest file without the deleted data files
                    if any(entry.data_file not in found_deleted_data_files for entry in entries):
                        with write_manifest(
                            format_version=self._transaction.table_metadata.format_version,
                            spec=self._transaction.table_metadata.specs()[manifest_file.partition_spec_id],
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
            previous_snapshot = self._transaction.table_metadata.snapshot_by_id(self._parent_snapshot_id)
            if previous_snapshot is None:
                # This should never happen since you cannot overwrite an empty table
                raise ValueError(f"Could not find the previous snapshot: {self._parent_snapshot_id}")

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
                    for entry in manifest.fetch_manifest_entry(self._io, discard_deleted=True)
                    if entry.data_file.content == DataFileContent.DATA and entry.data_file in self._deleted_data_files
                ]

            list_of_entries = executor.map(_get_entries, previous_snapshot.manifests(self._io))
            return list(itertools.chain(*list_of_entries))
        else:
            return []


