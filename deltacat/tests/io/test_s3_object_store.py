import unittest
from unittest import mock


class TestS3ObjectStore(unittest.TestCase):

    TEST_VALUE = "test-value"

    @classmethod
    def setUpClass(cls):
        cls.ray_mock = mock.MagicMock()

        cls.module_patcher = mock.patch.dict("sys.modules", {"ray": cls.ray_mock})
        cls.module_patcher.start()

        from deltacat.io.s3_object_store import S3ObjectStore

        cls.object_store = S3ObjectStore(bucket_prefix="test")

        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.module_patcher.stop()

    @mock.patch("deltacat.io.s3_object_store.s3_utils.upload")
    def test_put_many_sanity(self, mock_upload):
        self.ray_mock.cloudpickle.dumps.return_value = self.TEST_VALUE
        result = self.object_store.put_many(["a", "b"])

        self.assertEqual(2, len(result))
        self.assertEqual(2, mock_upload.call_count)

    @mock.patch("deltacat.io.s3_object_store.s3_utils.download")
    def test_get_many_sanity(self, mock_download):
        self.ray_mock.cloudpickle.loads.return_value = self.TEST_VALUE

        result = self.object_store.get_many(["test", "test"])

        self.assertEqual(2, len(result))
        self.assertEqual(2, mock_download.call_count)

    @mock.patch("deltacat.io.s3_object_store.s3_utils.download")
    def test_get_sanity(self, mock_download):
        self.ray_mock.cloudpickle.loads.return_value = self.TEST_VALUE

        result = self.object_store.get("test")

        self.assertEqual(self.TEST_VALUE, result)
        self.assertEqual(1, mock_download.call_count)

    @mock.patch("deltacat.io.s3_object_store.s3_utils.upload")
    def test_put_sanity(self, mock_upload):
        self.ray_mock.cloudpickle.dumps.return_value = self.TEST_VALUE

        result = self.object_store.put("test")

        self.assertIsNotNone(result)
        self.assertEqual(1, mock_upload.call_count)
