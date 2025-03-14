import pytest

from deltacat.storage import (
    Transaction,
    TransactionOperation,
    TransactionType,
    TransactionOperationType,
)
from deltacat.storage.model.metafile import (
    Metafile,
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
    def test_relativize_metafile_write_paths(self):
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
        transaction = Transaction.of(
            txn_type=TransactionType.APPEND, txn_operations=[transaction_operation]
        )
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
        transaction = Transaction.of(
            txn_type=TransactionType.APPEND, txn_operations=[transaction_operation]
        )
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
        transaction = Transaction.of(
            txn_type=TransactionType.APPEND, txn_operations=[transaction_operation]
        )
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
        transaction = Transaction.of(
            txn_type=TransactionType.APPEND, txn_operations=transaction_operations
        )
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
        transaction = Transaction.of(
            txn_type=TransactionType.APPEND, txn_operations=[transaction_operation]
        )
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
        transaction = Transaction.of(
            txn_type=TransactionType.APPEND, txn_operations=[transaction_operation]
        )
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

        # Different transaction types to test
        txn_types = [
            TransactionType.APPEND,
            TransactionType.ALTER,
            TransactionType.DELETE,
            TransactionType.OVERWRITE,
            TransactionType.READ,
            TransactionType.RESTATE,
        ]

        for txn_type in txn_types:
            transaction_ops = []
            for op_type in operation_types:
                transaction_operation = TransactionOperation.of(
                    operation_type=op_type,
                    dest_metafile=Metafile({"id": "dummy_metafile_id"}),
                )
                transaction_operation.metafile_write_paths = absolute_paths
                transaction_ops.append(transaction_operation)
            transaction = Transaction.of(
                txn_type=txn_type, txn_operations=[transaction_operation]
            )
            transaction.relativize_operation_paths(transaction_operation, catalog_root)
            # Assert paths are relativized correctly
            assert (
                transaction_operation.metafile_write_paths == expected_paths
            ), f"Failed for transaction type {txn_type} and operation type {op_type}"
