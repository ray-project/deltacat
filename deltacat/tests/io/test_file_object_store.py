import unittest
from unittest import mock


class TestFileObjectStore(unittest.TestCase):

    TEST_VALUE = "test-value"

    @classmethod
    def setUpClass(cls):
        cls.ray_mock = mock.MagicMock()
        cls.os_mock = mock.MagicMock()

        cls.module_patcher = mock.patch.dict(
            "sys.modules", {"ray": cls.ray_mock, "os": cls.os_mock}
        )
        cls.module_patcher.start()

        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.module_patcher.stop()

    @mock.patch(
        "deltacat.io.file_object_store.open",
        new_callable=mock.mock_open,
        read_data="data",
    )
    def test_put_many_sanity(self, mock_file):
        from deltacat.io.file_object_store import FileObjectStore

        object_store = FileObjectStore(dir_path="")
        self.ray_mock.cloudpickle.dumps.return_value = self.TEST_VALUE
        result = object_store.put_many(["a", "b"])

        self.assertEqual(2, len(result))
        self.assertEqual(2, mock_file.call_count)

    @mock.patch(
        "deltacat.io.file_object_store.open",
        new_callable=mock.mock_open,
        read_data="data",
    )
    def test_get_many_sanity(self, mock_file):
        from deltacat.io.file_object_store import FileObjectStore

        object_store = FileObjectStore(dir_path="")
        self.ray_mock.cloudpickle.loads.return_value = self.TEST_VALUE

        result = object_store.get_many(["test", "test"])

        self.assertEqual(2, len(result))
        self.assertEqual(2, mock_file.call_count)

    @mock.patch(
        "deltacat.io.file_object_store.open",
        new_callable=mock.mock_open,
        read_data="data",
    )
    def test_get_sanity(self, mock_file):
        from deltacat.io.file_object_store import FileObjectStore

        object_store = FileObjectStore(dir_path="")
        self.ray_mock.cloudpickle.loads.return_value = self.TEST_VALUE

        result = object_store.get("test")

        self.assertEqual(self.TEST_VALUE, result)
        self.assertEqual(1, mock_file.call_count)

    @mock.patch(
        "deltacat.io.file_object_store.open",
        new_callable=mock.mock_open,
        read_data="data",
    )
    def test_put_sanity(self, mock_file):
        from deltacat.io.file_object_store import FileObjectStore

        object_store = FileObjectStore(dir_path="")
        self.ray_mock.cloudpickle.dumps.return_value = self.TEST_VALUE

        result = object_store.put("test")

        self.assertIsNotNone(result)
        self.assertEqual(1, mock_file.call_count)
