import pytest
import os
import pyarrow
import msgpack
import posixpath


from deltacat.storage import (
    Transaction,
    TransactionOperation,
    TransactionOperationType,
    Namespace,
    NamespaceLocator,
    Metafile,
)

from deltacat.constants import (
    TXN_DIR_NAME,
    RUNNING_TXN_DIR_NAME,
    PAUSED_TXN_DIR_NAME,
)


class TestAbsToRelative:
    @classmethod
    def setup_method(cls):
        cls.catalog_root = "/catalog/root/path"

    # Test cases for the abs_to_relative function
    def test_abs_to_relative_simple(self):
        """
        Tests the function which relativizes absolute paths (string) into relative paths (string)
        """
        catalog_root = TestAbsToRelative.catalog_root
        absolute_path = "/catalog/root/path/namespace/table/table_version/stream_id/partition_id/00000000000000000001.mpk"
        relative_path = Transaction._abs_txn_meta_path_to_relative(
            catalog_root, absolute_path
        )
        assert (
            relative_path
            == "namespace/table/table_version/stream_id/partition_id/00000000000000000001.mpk"
        )

    def test_abs_to_relative_same_paths(self):
        catalog_root = TestAbsToRelative.catalog_root
        absolute_path = TestAbsToRelative.catalog_root
        with pytest.raises(
            ValueError,
            match="Target and root are identical, but expected target to be a child of root.",
        ):
            Transaction._abs_txn_meta_path_to_relative(catalog_root, absolute_path)

    def test_abs_to_relative_root_with_trailing_slash(self):
        catalog_root = "/catalog/root/path/"
        absolute_path = "/catalog/root/path/namespace/table/table_version/stream_id/partition_id/00000000000000000001.mpk"
        relative_path = Transaction._abs_txn_meta_path_to_relative(
            catalog_root, absolute_path
        )
        assert (
            relative_path
            == "namespace/table/table_version/stream_id/partition_id/00000000000000000001.mpk"
        )

    def test_abs_to_relative_bad_root(self):
        catalog_root = TestAbsToRelative.catalog_root
        absolute_path = "/cat/rt/pth/namespace/table/table_version/stream_id/partition_id/00000000000000000001.mpk"
        with pytest.raises(ValueError, match="Expected target to be a child of root."):
            Transaction._abs_txn_meta_path_to_relative(catalog_root, absolute_path)

    def test_abs_to_relative_empty_path(self):
        with pytest.raises(ValueError, match="Expected target to be a child of root."):
            Transaction._abs_txn_meta_path_to_relative("", "/lorem/ipsum")
        with pytest.raises(ValueError, match="Expected target to be a child of root."):
            Transaction._abs_txn_meta_path_to_relative("/lorem/ipsum/", "")

    # Test cases for the relativize_operation_paths function
    def test_relativizemetafile_write_paths(self):
        catalog_root = "/catalog/root"
        absolute_paths = [
            "/catalog/root/path/to/metafile1.mpk",
            "/catalog/root/path/to/metafile2.mpk",
            "/catalog/root/another/path/lore_ipsum.mpk",
            "/catalog/root/another/path/meta/to/lorem_ipsum.mpk",
            "/catalog/root/another/path/lorem_ipsum.mpk",
            "/catalog/root/here.mpk",
        ]
        expected_relative_paths = [
            "path/to/metafile1.mpk",
            "path/to/metafile2.mpk",
            "another/path/lore_ipsum.mpk",
            "another/path/meta/to/lorem_ipsum.mpk",
            "another/path/lorem_ipsum.mpk",
            "here.mpk",
        ]
        # Create a dummy transaction operation with absolute paths
        dest_metafile = Metafile({"id": "dummy_metafile_id"})
        transaction_operation = TransactionOperation.of(
            operation_type=TransactionOperationType.CREATE,
            dest_metafile=dest_metafile,
        )
        # use replace method as setter
        transaction_operation.metafile_write_paths = absolute_paths
        # Create a transaction and relativize paths
        transaction = Transaction.of([transaction_operation])
        transaction.relativize_operation_paths(transaction_operation, catalog_root)
        # Verify the paths have been correctly relativized
        assert transaction_operation.metafile_write_paths == expected_relative_paths

    def test_relativize_locator_write_paths(self):
        catalog_root = "/catalog/root"
        absolute_paths = [
            "/catalog/root/path/to/loc1.mpk",
            "/catalog/root/path/to/loc2.mpk",
            "/catalog/root/another/path/lore_ipsum.mpk",
            "/catalog/root/another/path/meta/to/lorem_ipsum.mpk",
            "/catalog/root/another/path/lorem_ipsum.mpk",
            "/catalog/root/here.mpk",
        ]
        expected_relative_paths = [
            "path/to/loc1.mpk",
            "path/to/loc2.mpk",
            "another/path/lore_ipsum.mpk",
            "another/path/meta/to/lorem_ipsum.mpk",
            "another/path/lorem_ipsum.mpk",
            "here.mpk",
        ]
        # Create a dummy transaction operation with absolute paths
        dest_metafile = Metafile({"id": "dummy_metafile_id"})
        transaction_operation = TransactionOperation.of(
            operation_type=TransactionOperationType.CREATE,
            dest_metafile=dest_metafile,
        )
        # use replace as setter
        transaction_operation.locator_write_paths = absolute_paths
        # Create a transaction and relativize paths
        transaction = Transaction.of(txn_operations=[transaction_operation])
        transaction.relativize_operation_paths(transaction_operation, catalog_root)
        # Verify the paths have been correctly relativized
        assert transaction_operation.locator_write_paths == expected_relative_paths

    def test_relativize_metafile_and_locator_paths(self):
        catalog_root = "/meta_catalog/root_dir/a/b/c"
        meta_absolute_paths = [
            "/meta_catalog/root_dir/a/b/c/namespace/table/table_version/stream_id/partition_id/00000000000000000001.mpk",
            "/meta_catalog/root_dir/a/b/c/namespace/table/table_version/stream_id/partition_id/00000000000000000002.mpk",
            "/meta_catalog/root_dir/a/b/c/namespace/table/table_version/stream_id/partition_id/00000000000000000003.mpk",
        ]
        loc_absolute_paths = [
            "/meta_catalog/root_dir/a/b/c/d/table/table_version/stream_id/partition_id/00000000000000000001.mpk",
            "/meta_catalog/root_dir/a/b/c/e/table/table_version/stream_id/partition_id/00000000000000000002.mpk",
            "/meta_catalog/root_dir/a/b/c/f/table/table_version/stream_id/partition_id/00000000000000000003.mpk",
        ]
        meta_relative_paths = [
            "namespace/table/table_version/stream_id/partition_id/00000000000000000001.mpk",
            "namespace/table/table_version/stream_id/partition_id/00000000000000000002.mpk",
            "namespace/table/table_version/stream_id/partition_id/00000000000000000003.mpk",
        ]
        loc_relative_paths = [
            "d/table/table_version/stream_id/partition_id/00000000000000000001.mpk",
            "e/table/table_version/stream_id/partition_id/00000000000000000002.mpk",
            "f/table/table_version/stream_id/partition_id/00000000000000000003.mpk",
        ]
        # Create a dummy transaction operation with absolute paths
        dest_metafile = Metafile({"id": "dummy_metafile_id"})
        transaction_operation = TransactionOperation.of(
            operation_type=TransactionOperationType.CREATE,
            dest_metafile=dest_metafile,
        )
        # use replace as setter
        transaction_operation.metafile_write_paths = meta_absolute_paths
        transaction_operation.locator_write_paths = loc_absolute_paths
        # Create a transaction and relativize paths
        transaction = Transaction.of([transaction_operation])
        transaction.relativize_operation_paths(transaction_operation, catalog_root)
        # Verify the paths have been correctly relativized
        assert (
            transaction_operation.metafile_write_paths == meta_relative_paths
        ), f"Expected: {meta_relative_paths}, but got: {transaction_operation.metafile_write_paths}"
        assert (
            transaction_operation.locator_write_paths == loc_relative_paths
        ), f"Expected: {loc_relative_paths}, but got: {transaction_operation.locator_write_paths}"

    def test_multiple_operations_relativize_paths(self):
        catalog_root = "/catalog/root"
        meta_absolute_paths = [
            "/catalog/root/path/to/metafile1.mpk",
            "/catalog/root/path/to/metafile2.mpk",
            "/catalog/root/another/path/lore_ipsum.mpk",
            "/catalog/root/another/path/meta/to/lorem_ipsum.mpk",
            "/catalog/root/another/path/lorem_ipsum.mpk",
            "/catalog/root/here.mpk",
        ]
        loc_absolute_paths = [
            "/catalog/root/path/to/loc1.mpk",
            "/catalog/root/path/to/loc2.mpk",
            "/catalog/root/another/path/lore_ipsum.mpk",
            "/catalog/root/another/path/meta/to/lorem_ipsum.mpk",
            "/catalog/root/another/path/lorem_ipsum.mpk",
            "/catalog/root/here.mpk",
        ]
        meta_expected_relative_paths = [
            "path/to/metafile1.mpk",
            "path/to/metafile2.mpk",
            "another/path/lore_ipsum.mpk",
            "another/path/meta/to/lorem_ipsum.mpk",
            "another/path/lorem_ipsum.mpk",
            "here.mpk",
        ]
        loc_expected_relative_paths = [
            "path/to/loc1.mpk",
            "path/to/loc2.mpk",
            "another/path/lore_ipsum.mpk",
            "another/path/meta/to/lorem_ipsum.mpk",
            "another/path/lorem_ipsum.mpk",
            "here.mpk",
        ]
        # Create a dummy transaction operation with absolute paths
        dest_metafile = Metafile({"id": "dummy_metafile_id"})
        transaction_operations = []
        for i in range(11):
            transaction_operation = TransactionOperation.of(
                operation_type=TransactionOperationType.CREATE,
                dest_metafile=dest_metafile,
            )
            transaction_operation.metafile_write_paths = meta_absolute_paths
            transaction_operation.locator_write_paths = loc_absolute_paths
            transaction_operations.append(transaction_operation)
        # Create a transaction and relativize paths
        transaction = Transaction.of(transaction_operations)
        for operation in transaction_operations:
            transaction.relativize_operation_paths(operation, catalog_root)
        # Verify the paths have been correctly relativized
        for operation in transaction_operations:
            assert operation.metafile_write_paths == meta_expected_relative_paths
            assert operation.locator_write_paths == loc_expected_relative_paths

    def test_empty_metafile_and_locator_write_paths(self):
        catalog_root = "/catalog/root"
        transaction_operation = TransactionOperation.of(
            operation_type=TransactionOperationType.CREATE,
            dest_metafile=Metafile({"id": "dummy_metafile_id"}),
        )
        # Empty paths
        transaction_operation.metafile_write_paths = []
        transaction_operation.locator_write_paths = []
        transaction = Transaction.of([transaction_operation])
        transaction.relativize_operation_paths(transaction_operation, catalog_root)
        assert transaction_operation.metafile_write_paths == []
        assert transaction_operation.locator_write_paths == []

    def test_large_number_of_paths(self):
        catalog_root = "/catalog/root"
        absolute_paths = [f"/catalog/root/path/to/file{i}.mpk" for i in range(5000)]
        expected_paths = [f"path/to/file{i}.mpk" for i in range(5000)]
        transaction_operation = TransactionOperation.of(
            operation_type=TransactionOperationType.CREATE,
            dest_metafile=Metafile({"id": "dummy_metafile_id"}),
        )
        transaction_operation.metafile_write_paths = absolute_paths
        transaction = Transaction.of([transaction_operation])
        transaction.relativize_operation_paths(transaction_operation, catalog_root)
        assert transaction_operation.metafile_write_paths == expected_paths

    def test_large_number_of_paths_multi_ops(self):
        catalog_root = "/catalog/root"
        absolute_paths = [f"/catalog/root/path/to/file{i}.mpk" for i in range(1000)]
        expected_paths = [f"path/to/file{i}.mpk" for i in range(1000)]

        # Different operation types to test
        operation_types = [
            TransactionOperationType.CREATE,
            # TransactionOperationType.UPDATE,
            TransactionOperationType.DELETE,
            TransactionOperationType.READ_EXISTS,
            TransactionOperationType.READ_LATEST,
            TransactionOperationType.READ_CHILDREN,
            TransactionOperationType.READ_SIBLINGS,
        ]

        transaction_ops = []
        for op_type in operation_types:
            transaction_operation = TransactionOperation.of(
                operation_type=op_type,
                dest_metafile=Metafile({"id": "dummy_metafile_id"}),
            )
            transaction_operation.metafile_write_paths = absolute_paths
            transaction_ops.append(transaction_operation)
        transaction = Transaction.of([transaction_operation])
        transaction.relativize_operation_paths(transaction_operation, catalog_root)
        # Assert paths are relativized correctly
        assert (
            transaction_operation.metafile_write_paths == expected_paths
        ), f"Failed for operation type {op_type}"


class TestTransactionPersistence:

    # Verifies that transactions initialized with empty or None operations are marked interactive,
    # while valid operations are not
    def test_create_iterative_transaction(self):
        txn_1 = Transaction.of(txn_operations=[])
        txn_2 = Transaction.of(txn_operations=None)
        op = TransactionOperation.of(
            operation_type=TransactionOperationType.CREATE,
            dest_metafile=Metafile({"id": "dummy_metafile_id"}),
        )
        txn_3 = Transaction.of(txn_operations=[op, op])
        assert (
            txn_1.interactive
        )  # check if constructor detect empty list --> interactive transaction
        assert (
            txn_2.interactive
        )  # check if we can initialize with no list --> interactive transaction
        assert (
            not txn_3.interactive
        )  # check that valid operations_list --> not interactive transaction

    # Builds and commits a transaction step-by-step, then validates the output files and transaction success log
    def test_commit_iterative_transaction(self, temp_dir):
        # Create two simple namespaces
        namespace_locator1 = NamespaceLocator.of(namespace="test_ns_1")
        namespace_locator2 = NamespaceLocator.of(namespace="test_ns_2")
        ns1 = Namespace.of(locator=namespace_locator1)
        ns2 = Namespace.of(locator=namespace_locator2)
        # Start with an empty transaction (interactive)
        transaction = Transaction.of()
        txn = transaction.start(temp_dir)  # operate on deep-copy
        # Build operations manually and step them in
        op1 = TransactionOperation.of(
            operation_type=TransactionOperationType.CREATE,
            dest_metafile=ns1,
        )
        op2 = TransactionOperation.of(
            operation_type=TransactionOperationType.CREATE,
            dest_metafile=ns2,
        )
        # steps
        txn.step(op1)
        txn.step(op2)

        # seal() for interactive transactions
        write_paths, success_log_path = txn.seal()

        # Check output files exist and are valid
        deserialized_ns1 = Namespace.read(write_paths[0])
        deserialized_ns2 = Namespace.read(write_paths[1])

        assert ns1.equivalent_to(deserialized_ns1)
        assert ns2.equivalent_to(deserialized_ns2)
        assert success_log_path.endswith(str(txn.end_time))

    # Ensures that stepping and committing a transaction writes non-empty output files and a valid success log
    def test_commit_iterative_file_creation(self, temp_dir):
        ns = Namespace.of(locator=NamespaceLocator.of(namespace="check_writes"))
        txn = Transaction.of().start(temp_dir)
        op = TransactionOperation.of(TransactionOperationType.CREATE, dest_metafile=ns)
        txn.step(op)
        write_paths, success_log_path = txn.seal()

        # check the files were created
        for path in write_paths:
            abs_path = os.path.join(temp_dir, path)
            assert os.path.exists(abs_path)
            assert os.path.getsize(abs_path) > 0

        # check the success log exists
        assert os.path.exists(success_log_path)
        assert os.path.getsize(success_log_path) > 0

    # Confirms that a transaction can be paused, resumed, and successfully committed without data los
    def test_transaction_pause_and_resume_roundtrip(self, temp_dir):
        # Create a test namespace
        ns = Namespace.of(locator=NamespaceLocator.of(namespace="paused_resume_ns"))

        # Start interactive transaction
        txn = Transaction.of().start(temp_dir)
        op = TransactionOperation.of(TransactionOperationType.CREATE, dest_metafile=ns)

        txn.step(op)

        # Pause transaction (writes to paused/)
        txn.pause()

        # Resume transaction (reads from paused/)
        txn.resume()

        # Commit resumed transaction
        write_paths, success_log_path = txn.seal()

        # Validate outputs
        deserialized = Namespace.read(write_paths[0])
        assert ns.equivalent_to(deserialized)
        assert os.path.exists(success_log_path)
        assert success_log_path.endswith(str(txn.end_time))

    # Validates that transaction state, including ID and write paths, is correctly preserved across pause/resume cycles
    def test_resume_preserves_state_after_pause(self, temp_dir):
        ns = Namespace.of(locator=NamespaceLocator.of(namespace="resume_state_check"))

        txn = Transaction.of().start(temp_dir)
        op = TransactionOperation.of(TransactionOperationType.CREATE, dest_metafile=ns)

        txn.step(op)
        txn_id_before = txn.id

        txn.pause()
        txn.resume()

        # Ensure the ID and provider are still valid
        assert txn.id == txn_id_before
        assert txn._time_provider is not None
        assert hasattr(txn, "metafile_write_paths")
        assert len(txn.metafile_write_paths) == 1

        # Check commit still works
        _, success_log_path = txn.seal()
        assert os.path.exists(success_log_path)

    # Explicitly checks that fields are preserved
    def test_resume_preserves_state_after_pause_deep(self, temp_dir):
        ns = Namespace.of(locator=NamespaceLocator.of(namespace="resume_state_check"))

        txn = Transaction.of().start(temp_dir)
        op = TransactionOperation.of(TransactionOperationType.CREATE, dest_metafile=ns)

        txn.step(op)

        # Save values before pause
        txn_id_before = txn.id
        start_time_before = txn.start_time
        root_before = txn.catalog_root_normalized
        meta_paths_before = list(txn.metafile_write_paths)
        locator_paths_before = list(txn.locator_write_paths)

        txn.pause()
        txn.resume()

        # Field-by-field checks
        assert txn.id == txn_id_before, "Transaction ID should be preserved"
        assert txn._time_provider is not None, "Time provider should be reinitialized"
        assert txn.start_time == start_time_before, "Start time should be preserved"
        assert txn.catalog_root_normalized == root_before, "Catalog root should match"
        assert (
            txn.metafile_write_paths == meta_paths_before
        ), "Metafile paths must match"
        assert (
            txn.locator_write_paths == locator_paths_before
        ), "Locator paths must match"
        assert (
            isinstance(txn.operations, list) and len(txn.operations) == 1
        ), "Operations must be restored"
        assert txn.pause_time is not None, "Pause time should be restored"

        # Final commit still works
        write_paths, success_log_path = txn.seal()
        assert os.path.exists(success_log_path)

    # Checks that pausing a transaction moves its log from running/ to paused/ and preserves valid transaction state
    def test_pause_moves_running_to_paused(self, temp_dir):
        # Set up a transaction and a single operation
        locator = NamespaceLocator.of(namespace="pause_test")
        ns = Namespace.of(locator=locator)
        txn = Transaction.of().start(temp_dir)

        op = TransactionOperation.of(TransactionOperationType.CREATE, dest_metafile=ns)
        txn.step(op)

        fs = pyarrow.fs.LocalFileSystem()
        txn_id = txn.id
        txn_log_dir = posixpath.join(temp_dir, TXN_DIR_NAME)

        running_path = posixpath.join(txn_log_dir, RUNNING_TXN_DIR_NAME, txn_id)
        paused_path = posixpath.join(txn_log_dir, PAUSED_TXN_DIR_NAME, txn_id)

        # Sanity check: file should be in running/
        assert fs.get_file_info(running_path).type == pyarrow.fs.FileType.File

        # Pause transaction
        txn.pause()
        # Ensure the running file is deleted
        assert fs.get_file_info(running_path).type == pyarrow.fs.FileType.NotFound

        # Ensure the paused file exists and contains valid msgpack
        paused_info = fs.get_file_info(paused_path)
        assert paused_info.type == pyarrow.fs.FileType.File
        with fs.open_input_stream(paused_path) as f:
            data = f.readall()
            txn_loaded = msgpack.loads(data)
            assert "operations" in txn_loaded

    # Simulates a full multi-step transaction with multiple pause/resume cycles and verifies correctness of all outputs
    def test_transaction_pause_and_resume_roundtrip_complex(self, temp_dir):
        # Step 0: Create an empty interactive transaction
        txn = Transaction.of().start(temp_dir)

        # Step 1: Add first namespace, pause
        ns1 = Namespace.of(locator=NamespaceLocator.of(namespace="roundtrip_ns_1"))
        op1 = TransactionOperation.of(
            TransactionOperationType.CREATE, dest_metafile=ns1
        )
        txn.step(op1)
        txn.pause()

        # Step 2: Resume, add second namespace, pause
        txn.resume()
        ns2 = Namespace.of(locator=NamespaceLocator.of(namespace="roundtrip_ns_2"))
        op2 = TransactionOperation.of(
            TransactionOperationType.CREATE, dest_metafile=ns2
        )
        txn.step(op2)
        txn.pause()

        # Step 3: Resume again, add third namespace, commit
        txn.resume()
        ns3 = Namespace.of(locator=NamespaceLocator.of(namespace="roundtrip_ns_3"))
        op3 = TransactionOperation.of(
            TransactionOperationType.CREATE, dest_metafile=ns3
        )
        txn.step(op3)

        # Final commit
        write_paths, success_log_path = txn.seal()

        # Read and verify written namespaces
        for i, ns in enumerate([ns1, ns2, ns3]):
            written_path = write_paths[i]
            deserialized_ns = Namespace.read(written_path)
            assert ns.equivalent_to(
                deserialized_ns
            ), f"Mismatch in ns{i+1}: {ns} != {deserialized_ns}"
            assert os.path.exists(written_path), f"Missing file: {written_path}"
            assert os.path.getsize(written_path) > 0

        # Check success log exists and is correct
        assert os.path.exists(success_log_path)
        assert success_log_path.endswith(str(txn.end_time))

    # Repeats a complex pause/resume flow with additional assertions on namespace equality and time consistency
    def test_transaction_pause_and_resume_roundtrip_complex_2(self, temp_dir):
        # Step 0: Create an empty interactive transaction
        txn = Transaction.of().start(temp_dir)

        # Step 1: Add first namespace, pause
        ns1 = Namespace.of(locator=NamespaceLocator.of(namespace="roundtrip_ns_1"))
        op1 = TransactionOperation.of(
            TransactionOperationType.CREATE, dest_metafile=ns1
        )
        txn.step(op1)
        txn.pause()

        # Step 2: Resume, add second namespace, pause
        txn.resume()
        ns2 = Namespace.of(locator=NamespaceLocator.of(namespace="roundtrip_ns_2"))
        op2 = TransactionOperation.of(
            TransactionOperationType.CREATE, dest_metafile=ns2
        )
        txn.step(op2)

        txn.pause()

        # Step 3: Resume again, add third namespace, commit
        txn.resume()
        ns3 = Namespace.of(locator=NamespaceLocator.of(namespace="roundtrip_ns_3"))
        op3 = TransactionOperation.of(
            TransactionOperationType.CREATE, dest_metafile=ns3
        )
        txn.step(op3)

        # Final commit
        write_paths, success_log_path = txn.seal()

        assert txn.start_time < txn.end_time

        # Read and verify written namespaces
        for i, ns in enumerate([ns1, ns2, ns3]):
            written_path = write_paths[i]

            # Confirm file was created and is non-empty
            assert os.path.exists(written_path), f"Missing file: {written_path}"
            assert os.path.getsize(written_path) > 0, f"Empty file: {written_path}"

            # Deserialize and verify content
            deserialized_ns = Namespace.read(written_path)
            assert ns.equivalent_to(deserialized_ns), f"Namespace mismatch at index {i}"
            assert ns.locator.namespace == deserialized_ns.locator.namespace
            assert ns.locator_alias == deserialized_ns.locator_alias
            assert ns.properties == deserialized_ns.properties

        # Verify success log
        assert os.path.exists(success_log_path)
        assert success_log_path.endswith(str(txn.end_time))


class TestTransactionCommitMessage:
    """Test commit message preservation and retrieval for transactions."""

    def test_transaction_with_commit_message(self):
        """Test that commit messages are stored and retrievable from transactions."""
        commit_msg = "Test commit message for transaction functionality"

        # Create transaction with commit message
        txn = Transaction.of(commit_message=commit_msg)

        # Verify commit message is stored correctly
        assert txn.commit_message == commit_msg
        assert txn.get("commit_message") == commit_msg

    def test_transaction_without_commit_message(self):
        """Test that transactions work normally without commit messages."""
        # Create transaction without commit message
        txn = Transaction.of()

        # Verify no commit message is stored
        assert txn.commit_message is None
        assert txn.get("commit_message") is None

    def test_transaction_commit_message_setter(self):
        """Test that commit messages can be set after transaction creation."""
        # Create transaction without commit message
        txn = Transaction.of()
        assert txn.commit_message is None

        # Set commit message using property setter
        commit_msg = "Added commit message after creation"
        txn.commit_message = commit_msg

        # Verify commit message is stored correctly
        assert txn.commit_message == commit_msg
        assert txn.get("commit_message") == commit_msg

    def test_transaction_serialization_with_commit_message(self, temp_dir):
        """Test that commit messages persist through transaction serialization."""
        commit_msg = "Serialization test commit message"

        # Create namespace for testing
        ns = Namespace.of(locator=NamespaceLocator.of(namespace="serialization_test"))

        # Create transaction with commit message
        txn = Transaction.of(commit_message=commit_msg).start(temp_dir)
        op = TransactionOperation.of(TransactionOperationType.CREATE, dest_metafile=ns)
        txn.step(op)

        # Commit transaction (this should serialize the transaction with commit message)
        _, success_log_path = txn.seal()

        # Read the transaction log and verify commit message persisted
        txn_read = Transaction.read(success_log_path)
        assert txn_read.commit_message == commit_msg

        # Verify other transaction properties are intact
        assert txn_read.start_time == txn.start_time
        assert txn_read.end_time == txn.end_time
        assert len(txn_read.operations) == 1
