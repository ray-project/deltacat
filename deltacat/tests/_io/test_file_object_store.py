import unittest
from unittest import mock


class TestFileObjectStore(unittest.TestCase):
    """
    Unit tests for FileObjectStore using mocked dependencies.

    Note: We use test-level mocking rather than class-level module patching
    to avoid flaky tests caused by import order dependencies and mock conflicts.
    """

    TEST_VALUE = "test-value"

    @mock.patch("deltacat.io.file_object_store.cloudpickle.dumps")
    @mock.patch(
        "deltacat.io.file_object_store.open",
        new_callable=mock.mock_open,
        read_data="data",
    )
    def test_put_many_sanity(self, mock_file, mock_dumps):
        from deltacat.io.file_object_store import FileObjectStore

        object_store = FileObjectStore(dir_path="")
        mock_dumps.return_value = self.TEST_VALUE
        result = object_store.put_many(["a", "b"])

        self.assertEqual(2, len(result))
        self.assertEqual(2, mock_file.call_count)

    @mock.patch("deltacat.io.file_object_store.cloudpickle.dumps")
    @mock.patch(
        "deltacat.io.file_object_store.open",
        new_callable=mock.mock_open,
        read_data="data",
    )
    def test_put_sanity(self, mock_file, mock_dumps):
        from deltacat.io.file_object_store import FileObjectStore

        object_store = FileObjectStore(dir_path="")
        mock_dumps.return_value = self.TEST_VALUE

        result = object_store.put("test")

        self.assertIsNotNone(result)
        self.assertEqual(1, mock_file.call_count)

    @mock.patch("deltacat.io.file_object_store.cloudpickle.loads")
    @mock.patch(
        "deltacat.io.file_object_store.open",
        new_callable=mock.mock_open,
        read_data="data",
    )
    def test_get_many_sanity(self, mock_file, mock_loads):
        from deltacat.io.file_object_store import FileObjectStore

        object_store = FileObjectStore(dir_path="")
        mock_loads.return_value = self.TEST_VALUE

        result = object_store.get_many(["test", "test"])

        self.assertEqual(2, len(result))
        self.assertEqual(2, mock_file.call_count)

    @mock.patch("deltacat.io.file_object_store.cloudpickle.loads")
    @mock.patch(
        "deltacat.io.file_object_store.open",
        new_callable=mock.mock_open,
        read_data="data",
    )
    def test_get_sanity(self, mock_file, mock_loads):
        from deltacat.io.file_object_store import FileObjectStore

        object_store = FileObjectStore(dir_path="")
        mock_loads.return_value = self.TEST_VALUE

        result = object_store.get("test")

        self.assertEqual(self.TEST_VALUE, result)
        self.assertEqual(1, mock_file.call_count)

    @mock.patch("deltacat.io.file_object_store.os.remove")
    def test_delete_sanity(self, mock_remove):
        from deltacat.io.file_object_store import FileObjectStore

        object_store = FileObjectStore(dir_path="")

        delete_success = object_store.delete("test")

        self.assertTrue(delete_success)
        self.assertEqual(1, mock_remove.call_count)

    @mock.patch("deltacat.io.file_object_store.os.remove")
    def test_delete_many_sanity(self, mock_remove):
        from deltacat.io.file_object_store import FileObjectStore

        object_store = FileObjectStore(dir_path="")

        delete_success = object_store.delete_many(["test", "test"])

        self.assertTrue(delete_success)
        self.assertEqual(2, mock_remove.call_count)
