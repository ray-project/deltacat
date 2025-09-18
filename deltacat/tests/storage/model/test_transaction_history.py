"""
Comprehensive test suite for transaction history queries (dc.transactions function).
Tests all functionality to prevent regression and ensure robust behavior.
"""

import time
import inspect
import pytest

import pandas as pd
import pyarrow as pa
import polars as pl
import daft

import deltacat as dc
from deltacat.types.tables import DatasetType
from deltacat.catalog.model.catalog import Catalog
from deltacat.storage.model.types import TransactionStatus, TransactionState


class TestTransactionHistory:
    """Comprehensive test suite for transaction history queries."""

    def setup_method(self):
        """Set up fresh catalog for each test."""
        dc.clear_catalogs()  # Clear any existing catalogs

    def teardown_method(self):
        """Clean up after each test."""
        dc.clear_catalogs()

    def create_test_transactions(self):
        """Create a variety of test transactions with different characteristics."""
        transactions_created = []

        # Transaction 1: Simple single-table
        data1 = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        with dc.transaction(commit_message="Initial user data") as txn:
            dc.write(data1, "users")
            transactions_created.append(
                {
                    "id": txn.id,
                    "commit_message": "Initial user data",
                    "expected_tables": 1,
                    "start_time": txn.start_time,
                }
            )

        time.sleep(0.01)  # Ensure different timestamps

        # Transaction 2: Multi-table transaction
        products = pd.DataFrame({"product_id": [101, 102], "name": ["Laptop", "Phone"]})
        orders = pd.DataFrame(
            {"order_id": [1, 2], "user_id": [1, 2], "product_id": [101, 102]}
        )

        with dc.transaction(commit_message="Create products and orders") as txn:
            dc.write(
                products, "products", namespace="inventory", auto_create_namespace=True
            )
            dc.write(orders, "orders", namespace="sales", auto_create_namespace=True)
            transactions_created.append(
                {
                    "id": txn.id,
                    "commit_message": "Create products and orders",
                    "expected_tables": 2,
                    "start_time": txn.start_time,
                }
            )

        time.sleep(0.01)

        # Transaction 3: Update existing table
        more_users = pd.DataFrame({"id": [3, 4], "name": ["Charlie", "Diana"]})
        with dc.transaction(commit_message="Add more users") as txn:
            dc.write(more_users, "users")
            transactions_created.append(
                {
                    "id": txn.id,
                    "commit_message": "Add more users",
                    "expected_tables": 1,
                    "start_time": txn.start_time,
                }
            )

        time.sleep(0.01)

        # Transaction 4: Large transaction (for operation count testing)
        analytics = pd.DataFrame(
            {"metric": ["page_views", "clicks"], "value": [1000, 150]}
        )
        reports = pd.DataFrame({"report_id": [1], "status": ["complete"]})

        with dc.transaction(commit_message="Analytics and reporting") as txn:
            dc.write(
                analytics, "metrics", namespace="analytics", auto_create_namespace=True
            )
            dc.write(
                reports, "reports", namespace="analytics", auto_create_namespace=True
            )
            transactions_created.append(
                {
                    "id": txn.id,
                    "commit_message": "Analytics and reporting",
                    "expected_tables": 2,
                    "start_time": txn.start_time,
                }
            )

        return transactions_created

    def test_basic_transaction_history_query(self, temp_catalog_properties):
        """Test basic transaction history querying with default parameters."""
        # Initialize catalog using the fixture
        dc.init()
        dc.put_catalog("test", Catalog(temp_catalog_properties))

        created_txns = self.create_test_transactions()

        # Basic query - should return all SUCCESS transactions
        result = dc.transactions(read_as=DatasetType.PANDAS)

        assert isinstance(result, pd.DataFrame)
        assert (
            len(result) == 5
        ), f"Expected 5 transactions (4 test + 1 default namespace), got {len(result)}"

        # Verify schema
        expected_columns = [
            "transaction_id",
            "commit_message",
            "start_time",
            "end_time",
            "status",
            "operation_count",
            "operation_types",
            "namespace_count",
            "table_count",
            "table_version_count",
            "stream_count",
            "partition_count",
            "delta_count",
        ]
        assert list(result.columns) == expected_columns

        # Verify all are SUCCESS status
        assert all(result["status"] == "SUCCESS")

        # Verify sorting (most recent first)
        start_times = result["start_time"].tolist()
        assert start_times == sorted(start_times, reverse=True)

        # Verify commit messages are preserved (excluding None from default namespace creation)
        commit_messages = set(result["commit_message"])
        expected_messages = {txn["commit_message"] for txn in created_txns}
        expected_messages.add(
            None
        )  # Add None for default namespace creation transaction
        assert commit_messages == expected_messages

    def test_all_dataset_types_output(self, temp_catalog_properties):
        """Test that all supported dataset types work correctly."""
        # Initialize catalog using the fixture
        dc.init()
        dc.put_catalog("test", Catalog(temp_catalog_properties))

        self.create_test_transactions()

        # Test each dataset type
        dataset_types = [
            DatasetType.PANDAS,
            DatasetType.PYARROW,
            DatasetType.POLARS,
            DatasetType.RAY_DATASET,
            DatasetType.DAFT,
        ]

        for dataset_type in dataset_types:
            result = dc.transactions(read_as=dataset_type, limit=2)

            # Verify basic properties based on type
            if dataset_type == DatasetType.PANDAS:
                assert isinstance(result, pd.DataFrame)
                assert len(result) == 2
                assert list(result.columns)[0] == "transaction_id"

            elif dataset_type == DatasetType.PYARROW:
                assert isinstance(result, pa.Table)
                assert result.num_rows == 2
                assert result.column_names[0] == "transaction_id"

            elif dataset_type == DatasetType.POLARS:
                assert isinstance(result, pl.DataFrame)
                assert len(result) == 2
                assert result.columns[0] == "transaction_id"

            elif dataset_type == DatasetType.RAY_DATASET:
                # Ray dataset might be returned as different types
                assert result is not None
                # Convert to check count
                df = result.to_pandas()
                assert len(df) == 2

            elif dataset_type == DatasetType.DAFT:
                assert isinstance(result, daft.DataFrame)
                # Convert to check count
                df = result.to_pandas()
                assert len(df) == 2

    def test_transaction_state_filtering(self, temp_catalog_properties):
        """Test filtering by different transaction states."""
        # Initialize catalog using the fixture
        dc.init()
        dc.put_catalog("test", Catalog(temp_catalog_properties))

        self.create_test_transactions()

        # Test default (SUCCESS only)
        success_only = dc.transactions(read_as=DatasetType.PANDAS)
        assert all(success_only["status"] == "SUCCESS")
        base_count = len(success_only)

        # Test with RUNNING status (should be same as default since no running transactions)
        with_running = dc.transactions(
            read_as=DatasetType.PANDAS,
            status_in=[TransactionStatus.SUCCESS, TransactionStatus.RUNNING],
        )
        assert len(with_running) == base_count
        assert all(
            status in ["SUCCESS", "RUNNING"] for status in with_running["status"]
        )

        # Test with FAILED status
        with_failed = dc.transactions(
            read_as=DatasetType.PANDAS,
            status_in=[TransactionStatus.SUCCESS, TransactionStatus.FAILED],
        )
        assert len(with_failed) == base_count
        assert all(status in ["SUCCESS", "FAILED"] for status in with_failed["status"])

        # Test with PAUSED status
        with_paused = dc.transactions(
            read_as=DatasetType.PANDAS,
            status_in=[TransactionStatus.SUCCESS, TransactionStatus.PAUSED],
        )
        assert len(with_paused) == base_count
        assert all(status in ["SUCCESS", "PAUSED"] for status in with_paused["status"])

        # Test all states
        all_states = dc.transactions(
            read_as=DatasetType.PANDAS,
            status_in=[
                TransactionStatus.SUCCESS,
                TransactionStatus.RUNNING,
                TransactionStatus.FAILED,
                TransactionStatus.PAUSED,
            ],
        )
        assert len(all_states) == base_count

        # Test multiple states
        multi_state = dc.transactions(
            read_as=DatasetType.PANDAS,
            status_in=[
                TransactionStatus.SUCCESS,
                TransactionStatus.RUNNING,
                TransactionStatus.FAILED,
                TransactionStatus.PAUSED,
            ],
        )
        assert len(multi_state) == base_count

    def test_time_based_filtering(self, temp_catalog_properties):
        """Test filtering transactions by time ranges."""
        # Initialize catalog using the fixture
        dc.init()
        dc.put_catalog("test", Catalog(temp_catalog_properties))

        created_txns = self.create_test_transactions()

        # Get all transactions first
        all_txns = dc.transactions(read_as=DatasetType.PANDAS)

        # Basic verification - we should have 5 transactions (4 test + 1 default namespace)
        assert (
            len(all_txns) == 5
        ), f"Expected 5 transactions (4 test + 1 default namespace), got {len(all_txns)}"

        # Test simple case: start_time that should include all transactions
        earliest_time = min(txn["start_time"] for txn in created_txns)
        all_from_start = dc.transactions(
            read_as=DatasetType.PANDAS, start_time=earliest_time
        )
        assert (
            len(all_from_start) == 4
        )  # Only test transactions (default namespace created earlier)

        # Test start_time filtering after some transactions
        # Get the second-to-last transaction"s start time
        sorted_times = sorted([txn["start_time"] for txn in created_txns])
        mid_time = sorted_times[1]  # Second earliest
        recent_txns = dc.transactions(read_as=DatasetType.PANDAS, start_time=mid_time)

        # Should get at least the transactions at or after that time
        assert (
            len(recent_txns) >= 2
        ), f"Expected at least 2 transactions after mid_time, got {len(recent_txns)}"
        assert all(t >= mid_time for t in recent_txns["start_time"])

        # Test future start_time (should return empty)
        future_time = time.time_ns() + 1000000000  # 1 second in future
        future_txns = dc.transactions(
            read_as=DatasetType.PANDAS, start_time=future_time
        )
        assert len(future_txns) == 0

    def test_limit_and_pagination(self, temp_catalog_properties):
        """Test limiting results and pagination behavior."""
        # Initialize catalog using the fixture
        dc.init()
        dc.put_catalog("test", Catalog(temp_catalog_properties))

        self.create_test_transactions()

        # Test limit
        limited = dc.transactions(read_as=DatasetType.PANDAS, limit=2)
        assert len(limited) == 2

        # Test limit larger than available
        all_limited = dc.transactions(read_as=DatasetType.PANDAS, limit=10)
        assert len(all_limited) == 5  # 4 test transactions + 1 default namespace exist

        # Test limit=0 (should raise ValueError for invalid limit)
        with pytest.raises(ValueError):
            dc.transactions(read_as=DatasetType.PANDAS, limit=0)

        # Test limit=1 with different states
        single_all_states = dc.transactions(
            read_as=DatasetType.PANDAS,
            limit=1,
            status_in=[
                TransactionStatus.SUCCESS,
                TransactionStatus.RUNNING,
                TransactionStatus.FAILED,
                TransactionStatus.PAUSED,
            ],
        )
        assert len(single_all_states) == 1

        # Verify limit respects sorting (most recent first)
        all_txns = dc.transactions(read_as=DatasetType.PANDAS)
        limited_txns = dc.transactions(read_as=DatasetType.PANDAS, limit=2)

        # Limited should be the first 2 from the full list
        assert (
            limited_txns["transaction_id"].tolist()
            == all_txns["transaction_id"].head(2).tolist()
        )

    def test_commit_message_functionality(self, temp_catalog_properties):
        """Test commit message storage and retrieval."""
        # Initialize catalog using the fixture
        dc.init()
        dc.put_catalog("test", Catalog(temp_catalog_properties))

        # Create transactions with various commit message patterns
        test_messages = [
            "Simple commit message",
            "Multi-word commit with special chars: @#$%",
            "",  # Empty commit message
            "Very long commit message that contains lots of text to test handling of lengthy descriptions and ensure they are properly stored and retrieved without truncation or corruption",
            "Commit with\nmultiple\nlines",
            "Unicode test: 🚀 ñ ü é",
        ]

        created_txn_ids = []
        for msg in test_messages:
            data = pd.DataFrame({"id": [1], "value": [f"data_{len(created_txn_ids)}"]})
            with dc.transaction(commit_message=msg) as txn:
                dc.write(data, f"table_{len(created_txn_ids)}")
                created_txn_ids.append(txn.id)

        # Query and verify all commit messages
        result = dc.transactions(read_as=DatasetType.PANDAS)

        # Create mapping of transaction_id to commit_message
        result_messages = {
            row["transaction_id"]: row["commit_message"] for _, row in result.iterrows()
        }

        # Verify each commit message is preserved correctly
        for txn_id, expected_msg in zip(created_txn_ids, test_messages):
            assert txn_id in result_messages
            actual_msg = result_messages[txn_id]
            # Handle empty commit messages
            if expected_msg == "" and actual_msg is None:
                continue
            assert (
                actual_msg == expected_msg
            ), f"Commit message mismatch for {txn_id}: expected {expected_msg!r}, got {actual_msg!r}"

    def test_transaction_metadata_accuracy(self, temp_catalog_properties):
        """Test accuracy of operation counts and table counts."""
        # Initialize catalog using the fixture
        dc.init()
        dc.put_catalog("test", Catalog(temp_catalog_properties))

        # Create transactions with known characteristics
        test_cases = []

        # Single table, single operation
        data1 = pd.DataFrame({"id": [1], "name": ["test"]})
        with dc.transaction(commit_message="Single table") as txn:
            dc.write(data1, "single_table")
            test_cases.append({"id": txn.id, "expected_tables": 1})

        # Multi-table, multiple operations
        data2a = pd.DataFrame({"id": [1], "name": ["table_a"]})
        data2b = pd.DataFrame({"id": [1], "name": ["table_b"]})
        data2c = pd.DataFrame({"id": [1], "name": ["table_c"]})

        with dc.transaction(commit_message="Multi table") as txn:
            dc.write(data2a, "multi_a", namespace="test_ns", auto_create_namespace=True)
            dc.write(data2b, "multi_b", namespace="test_ns", auto_create_namespace=True)
            dc.write(
                data2c, "multi_c", namespace="another_ns", auto_create_namespace=True
            )
            test_cases.append({"id": txn.id, "expected_tables": 3})

        # Query results
        result = dc.transactions(read_as=DatasetType.PANDAS)

        # Create mapping of transaction_id to metadata
        result_metadata = {
            row["transaction_id"]: {
                "table_count": row["table_count"],
                "operation_count": row["operation_count"],
            }
            for _, row in result.iterrows()
        }

        # Verify metadata for each test case
        for test_case in test_cases:
            txn_id = test_case["id"]
            assert txn_id in result_metadata

            metadata = result_metadata[txn_id]

            # Verify table count
            expected_table_count = test_case["expected_tables"]
            actual_table_count = metadata["table_count"]
            assert (
                actual_table_count == expected_table_count
            ), f"Table count mismatch for {txn_id}: expected {expected_table_count}, got {actual_table_count}"

            # Verify operation count is reasonable (should be > table count due to internal operations)
            assert (
                metadata["operation_count"] > 0
            ), f"Operation count should be > 0 for {txn_id}"
            assert (
                metadata["operation_count"] >= metadata["table_count"]
            ), f"Operation count should be >= table count for {txn_id}"

    def test_empty_catalog_graceful_handling(self, temp_catalog_properties):
        """Test graceful handling of catalogs with no user transactions (only default namespace creation)."""
        # Initialize catalog using the fixture
        dc.init()
        dc.put_catalog("test", Catalog(temp_catalog_properties))

        # Test parameter combinations on catalog with only default namespace transaction
        test_cases = [
            # Test cases that should return the default namespace transaction
            ({}, 1),  # No filter - should return default namespace transaction
            (
                {"limit": 5},
                1,
            ),  # Limit > 1 - should return default namespace transaction
            (
                {"status_in": [TransactionStatus.SUCCESS, TransactionStatus.RUNNING]},
                1,
            ),  # Include SUCCESS
            (
                {"status_in": [TransactionStatus.SUCCESS, TransactionStatus.FAILED]},
                1,
            ),  # Include SUCCESS
            (
                {"status_in": [TransactionStatus.SUCCESS, TransactionStatus.PAUSED]},
                1,
            ),  # Include SUCCESS
            (
                {
                    "status_in": [
                        TransactionStatus.SUCCESS,
                        TransactionStatus.RUNNING,
                        TransactionStatus.FAILED,
                        TransactionStatus.PAUSED,
                    ]
                },
                1,
            ),  # Include SUCCESS
            (
                {
                    "limit": 1,
                    "status_in": [
                        TransactionStatus.SUCCESS,
                        TransactionStatus.RUNNING,
                        TransactionStatus.FAILED,
                        TransactionStatus.PAUSED,
                    ],
                },
                1,
            ),  # Include SUCCESS, limit 1
            # Test cases that should return empty results
            (
                {"start_time": time.time_ns() + 1000000000},
                0,
            ),  # Future start time (no transactions)
            (
                {"end_time": time.time_ns() - 24 * 3600000000000},
                0,
            ),  # End time 24 hours ago (before default namespace creation)
        ]

        for params, expected_count in test_cases:
            result = dc.transactions(read_as=DatasetType.PANDAS, **params)

            assert isinstance(result, pd.DataFrame), f"Failed for params {params}"
            assert (
                len(result) == expected_count
            ), f"Expected {expected_count} results for params {params}, got {len(result)}"

            # Verify schema is correct even for empty results
            expected_columns = [
                "transaction_id",
                "commit_message",
                "start_time",
                "end_time",
                "status",
                "operation_count",
                "operation_types",
                "namespace_count",
                "table_count",
                "table_version_count",
                "stream_count",
                "partition_count",
                "delta_count",
            ]
            assert (
                list(result.columns) == expected_columns
            ), f"Schema mismatch for params {params}"

    def test_error_handling_and_edge_cases(self, temp_catalog_properties):
        """Test error handling for various edge cases."""
        # Initialize catalog using the fixture
        dc.init()
        dc.put_catalog("test", Catalog(temp_catalog_properties))

        self.create_test_transactions()

        # Test invalid dataset type
        with pytest.raises((ValueError, AttributeError)):
            dc.transactions(read_as="INVALID_TYPE")

        # Test negative limit (should raise ValueError for invalid limit)
        with pytest.raises(ValueError):
            dc.transactions(read_as=DatasetType.PANDAS, limit=-1)

        # Test invalid time values
        # Very old start_time (should work, return all transactions)
        old_time_result = dc.transactions(
            read_as=DatasetType.PANDAS, start_time=1000000000  # Very old timestamp
        )
        assert len(old_time_result) == 5  # 4 test transactions + 1 default namespace

        # start_time > end_time (should return empty)
        invalid_time_result = dc.transactions(
            read_as=DatasetType.PANDAS,
            start_time=time.time_ns(),
            end_time=time.time_ns() - 1000000000,  # 1 second ago
        )
        assert len(invalid_time_result) == 0

    def test_status_in_corner_cases(self, temp_catalog_properties):
        """Test corner cases for status_in parameter."""
        # Initialize catalog using the fixture
        dc.init()
        dc.put_catalog("test", Catalog(temp_catalog_properties))

        self.create_test_transactions()

        # Get baseline count of SUCCESS transactions
        baseline_result = dc.transactions(read_as=DatasetType.PANDAS)
        baseline_count = len(baseline_result)
        assert baseline_count == 5  # 4 test transactions + 1 default namespace
        assert all(baseline_result["status"] == "SUCCESS")

        # Test status_in=None (should default to SUCCESS)
        none_result = dc.transactions(read_as=DatasetType.PANDAS, status_in=None)
        assert len(none_result) == baseline_count
        assert all(none_result["status"] == "SUCCESS")

        # Test status_in=[] (empty list - should default to SUCCESS)
        empty_result = dc.transactions(read_as=DatasetType.PANDAS, status_in=[])
        assert len(empty_result) == baseline_count
        assert all(empty_result["status"] == "SUCCESS")

        # Verify the results are identical
        assert len(baseline_result) == len(none_result) == len(empty_result)

        # Test that the transaction IDs are the same (same transactions returned)
        baseline_ids = set(baseline_result["transaction_id"])
        none_ids = set(none_result["transaction_id"])
        empty_ids = set(empty_result["transaction_id"])

        assert baseline_ids == none_ids == empty_ids

    def test_concurrent_transaction_handling(self, temp_catalog_properties):
        """Test behavior when transactions are created while querying."""
        # Initialize catalog using the fixture
        dc.init()
        dc.put_catalog("test", Catalog(temp_catalog_properties))

        # Create initial transactions
        initial_count = 2
        for i in range(initial_count):
            data = pd.DataFrame({"id": [i], "name": [f"user_{i}"]})
            with dc.transaction(commit_message=f"Transaction {i}"):
                dc.write(data, f"table_{i}")

        # Query initial state
        initial_result = dc.transactions(read_as=DatasetType.PANDAS)
        assert len(initial_result) == initial_count + 1  # +1 for default namespace

        # Create another transaction
        new_data = pd.DataFrame({"id": [999], "name": ["new_user"]})
        with dc.transaction(commit_message="New transaction"):
            dc.write(new_data, "new_table")

        # Query updated state
        updated_result = dc.transactions(read_as=DatasetType.PANDAS)
        assert (
            len(updated_result) == initial_count + 1 + 1
        )  # +1 for default namespace, +1 for new transaction

        # Verify new transaction appears first (most recent)
        assert updated_result.iloc[0]["commit_message"] == "New transaction"

    def test_namespace_isolation_in_table_counting(self, temp_catalog_properties):
        """Test that table counting correctly handles namespace isolation."""
        # Initialize catalog using the fixture
        dc.init()
        dc.put_catalog("test", Catalog(temp_catalog_properties))

        # Create transaction with tables in different namespaces
        data = pd.DataFrame({"id": [1], "value": ["test"]})

        with dc.transaction(commit_message="Multi-namespace transaction") as txn:
            # Same table name in different namespaces should count as different tables
            dc.write(
                data, "shared_name", namespace="namespace_a", auto_create_namespace=True
            )
            dc.write(
                data, "shared_name", namespace="namespace_b", auto_create_namespace=True
            )
            dc.write(
                data, "unique_name", namespace="namespace_a", auto_create_namespace=True
            )
            expected_txn_id = txn.id

        result = dc.transactions(read_as=DatasetType.PANDAS)

        # Find our transaction
        our_txn = result[result["transaction_id"] == expected_txn_id]
        assert len(our_txn) == 1

        # Should count distinct tables
        table_count = our_txn.iloc[0]["table_count"]
        assert table_count == 3, f"Expected 3 tables, got {table_count}"

        table_count = our_txn.iloc[0]["table_version_count"]
        assert table_count == 3, f"Expected 3 table versions, got {table_count}"

        stream_count = our_txn.iloc[0]["stream_count"]
        assert stream_count == 3, f"Expected 3 streams, got {stream_count}"

        partition_count = our_txn.iloc[0]["partition_count"]
        assert partition_count == 3, f"Expected 3 partitions, got {partition_count}"

        delta_count = our_txn.iloc[0]["delta_count"]
        assert delta_count == 3, f"Expected 3 deltas, got {delta_count}"

    def test_parameter_combinations(self, temp_catalog_properties):
        """Test various parameter combinations work correctly."""
        # Initialize catalog using the fixture
        dc.init()
        dc.put_catalog("test", Catalog(temp_catalog_properties))

        self.create_test_transactions()

        # Complex parameter combinations
        test_combinations = [
            # Time + limit
            {"start_time": time.time_ns() - 3600000000000, "limit": 2},
            # States + limit
            {
                "status_in": [
                    TransactionStatus.SUCCESS,
                    TransactionStatus.RUNNING,
                    TransactionStatus.FAILED,
                    TransactionStatus.PAUSED,
                ],
                "limit": 1,
            },
            # Time + states
            {
                "start_time": time.time_ns() - 3600000000000,
                "status_in": [TransactionStatus.SUCCESS, TransactionStatus.RUNNING],
            },
            # Everything combined
            {
                "start_time": time.time_ns() - 3600000000000,
                "end_time": time.time_ns(),
                "limit": 3,
                "status_in": [
                    TransactionStatus.SUCCESS,
                    TransactionStatus.RUNNING,
                    TransactionStatus.FAILED,
                    TransactionStatus.PAUSED,
                ],
            },
        ]

        for params in test_combinations:
            result = dc.transactions(read_as=DatasetType.PANDAS, **params)

            # Should not crash and should return valid DataFrame
            assert isinstance(result, pd.DataFrame)
            assert len(result) >= 0

            # If limit is specified, result should not exceed it
            if "limit" in params and params["limit"] > 0:
                assert len(result) <= params["limit"]


class TestTransactionHistoryRegression:
    """Regression tests to ensure consistent behavior over time."""

    def setup_method(self):
        """Set up fresh catalog for each test."""
        dc.clear_catalogs()

    def teardown_method(self):
        """Clean up after each test."""
        dc.clear_catalogs()

    def test_schema_consistency(self, temp_catalog_properties):
        """Ensure the output schema remains consistent."""
        # Initialize catalog using the fixture
        dc.init()
        dc.put_catalog("test", Catalog(temp_catalog_properties))

        # Create a simple transaction
        data = pd.DataFrame({"id": [1], "name": ["test"]})
        with dc.transaction(commit_message="Schema test"):
            dc.write(data, "test_table")

        result = dc.transactions(read_as=DatasetType.PANDAS)

        # Verify exact schema
        expected_columns = [
            "transaction_id",
            "commit_message",
            "start_time",
            "end_time",
            "status",
            "operation_count",
            "operation_types",
            "namespace_count",
            "table_count",
            "table_version_count",
            "stream_count",
            "partition_count",
            "delta_count",
        ]
        assert list(result.columns) == expected_columns

        # Verify data types
        assert result["transaction_id"].dtype == "object"  # String
        assert result["commit_message"].dtype == "object"  # String
        assert result["start_time"].dtype == "int64"  # Integer timestamp
        assert result["end_time"].dtype == "int64"  # Integer timestamp
        assert result["status"].dtype == "object"  # String
        assert result["operation_count"].dtype == "int64"  # Integer
        assert result["table_count"].dtype == "int64"  # Integer

    def test_sorting_consistency(self, temp_catalog_properties):
        """Ensure transactions are consistently sorted by start_time descending."""
        # Initialize catalog using the fixture
        dc.init()
        dc.put_catalog("test", Catalog(temp_catalog_properties))

        # Create transactions with deliberate timing
        transaction_times = []
        for i in range(5):
            data = pd.DataFrame({"id": [i], "name": [f"user_{i}"]})
            with dc.transaction(commit_message=f"Transaction {i}") as txn:
                dc.write(data, f"table_{i}")
                transaction_times.append(txn.start_time)
            time.sleep(0.01)  # Small delay

        result = dc.transactions(read_as=DatasetType.PANDAS)

        # Verify descending order by start_time (filter out default namespace transaction)
        result_times = result["start_time"].tolist()
        expected_times = sorted(transaction_times, reverse=True)

        # The result includes the default namespace transaction, so we need to extract just the test transaction times
        # Since our test transactions should be the most recent, they'll be at the start of the sorted list
        test_result_times = result_times[: len(transaction_times)]

        assert (
            test_result_times == expected_times
        ), "Transactions not properly sorted by start_time descending"

    def test_function_signature_stability(self, temp_catalog_properties):
        """Ensure function signature remains stable."""
        # Initialize catalog using the fixture (though not needed for signature test)
        dc.init()
        dc.put_catalog("test", Catalog(temp_catalog_properties))

        sig = inspect.signature(dc.transactions)
        expected_params = [
            "catalog_name",
            "read_as",
            "start_time",
            "end_time",
            "limit",
            "status_in",
        ]

        actual_params = list(sig.parameters.keys())
        assert (
            actual_params == expected_params
        ), f"Function signature changed: {actual_params}"

        # Verify default values
        assert sig.parameters["catalog_name"].default is None
        assert sig.parameters["read_as"].default == DatasetType.PYARROW
        assert sig.parameters["start_time"].default is None
        assert sig.parameters["end_time"].default is None
        assert sig.parameters["limit"].default == 10
        assert sig.parameters["status_in"].default == [TransactionStatus.SUCCESS]

    def test_read_transaction(self, temp_catalog_properties):
        """Test the read_transaction() function for loading transactions returned by transactions()."""

        # Initialize a clean catalog for testing using the fixture
        dc.init()
        dc.put_catalog("test", Catalog(temp_catalog_properties))

        # Create multiple transactions with data
        commit_msg_1 = "First test transaction"
        commit_msg_2 = "Second test transaction"

        # Create first transaction
        data1 = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        with dc.transaction(commit_message=commit_msg_1):
            dc.write(data1, "users")

        # Create second transaction
        data2 = pd.DataFrame({"id": [3, 4], "name": ["Charlie", "Diana"]})
        with dc.transaction(commit_message=commit_msg_2):
            dc.write(data2, "customers")

        # Test transactions() query functionality
        result = dc.transactions(read_as=DatasetType.PANDAS)

        # Verify we have the right number of transactions
        assert len(result) == 3  # 2 test transactions + 1 default namespace creation

        # Verify column structure
        expected_columns = [
            "transaction_id",
            "commit_message",
            "start_time",
            "end_time",
            "status",
            "operation_count",
            "operation_types",
            "namespace_count",
            "table_count",
            "table_version_count",
            "stream_count",
            "partition_count",
            "delta_count",
        ]
        assert list(result.columns) == expected_columns

        # Verify commit messages are preserved
        commit_messages = set(result["commit_message"])
        assert commit_msg_1 in commit_messages
        assert commit_msg_2 in commit_messages

        # Filter out default namespace transaction for metadata verification
        test_transactions = result[result["commit_message"].notna()]

        # Verify transaction metadata for test transactions only
        assert all(test_transactions["status"] == "SUCCESS")
        assert all(test_transactions["operation_count"] > 0)
        assert all(test_transactions["namespace_count"] == 1)
        assert all(test_transactions["table_count"] == 1)
        assert all(test_transactions["table_version_count"] == 1)
        assert all(test_transactions["stream_count"] == 1)
        assert all(test_transactions["partition_count"] == 1)
        assert all(test_transactions["delta_count"] == 1)

        # Read and validate the transactions
        transaction_id = result.iloc[0]["transaction_id"]
        transaction_obj = dc.read_transaction(transaction_id)
        assert transaction_obj.id == transaction_id
        assert transaction_obj.commit_message == commit_msg_2
        assert transaction_obj.start_time == result.iloc[0]["start_time"]
        assert transaction_obj.end_time == result.iloc[0]["end_time"]
        assert (
            transaction_obj.state(temp_catalog_properties.root)
            == TransactionState.SUCCESS
        )
        # Note: Expected operation count may vary based on implementation changes
        actual_ops = len(transaction_obj.operations)
        reported_ops = result.iloc[0]["operation_count"]
        assert (
            actual_ops == reported_ops
        ), f"Operation count mismatch: actual={actual_ops}, reported={reported_ops}"

        transaction_id = result.iloc[1]["transaction_id"]
        transaction_obj = dc.read_transaction(transaction_id)
        assert transaction_obj.id == transaction_id
        assert transaction_obj.commit_message == commit_msg_1
        assert transaction_obj.start_time == result.iloc[1]["start_time"]
        assert transaction_obj.end_time == result.iloc[1]["end_time"]
        assert (
            transaction_obj.state(temp_catalog_properties.root)
            == TransactionState.SUCCESS
        )
        # 1st transaction contains more operations than 2nd since only it needed to create the namespace
        assert len(transaction_obj.operations) == result.iloc[1]["operation_count"]
