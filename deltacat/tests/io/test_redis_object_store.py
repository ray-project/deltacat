import unittest
from unittest import mock


class TestRedisObjectStore(unittest.TestCase):

    TEST_VALUE = "test-value"

    def setUp(self):
        self.cloudpickle_patcher = mock.patch(
            "deltacat.io.redis_object_store.cloudpickle"
        )
        self.cloudpickle_mock = self.cloudpickle_patcher.start()
        self.socket_patcher = mock.patch("deltacat.io.redis_object_store.socket")
        self.socket_mock = self.socket_patcher.start()

        self.cloudpickle_mock.dumps.return_value = self.TEST_VALUE
        self.cloudpickle_mock.loads.return_value = self.TEST_VALUE
        self.socket_mock.gethostbyname.return_value = "0.0.0.0"
        self.socket_mock.gethostname.return_value = "test-host"

        from deltacat.io.redis_object_store import RedisObjectStore

        self.object_store = RedisObjectStore()

        super().setUpClass()

    def tearDown(self) -> None:
        self.cloudpickle_patcher.stop()
        self.socket_patcher.stop()

    @mock.patch("deltacat.io.redis_object_store.redis")
    def test_put_many_sanity(self, mock_client):
        mock_client.Redis.return_value.mset.return_value = ["a", "b"]

        result = self.object_store.put_many(["a", "b"])

        self.assertEqual(2, len(result))
        self.assertRegex(result[0], ".*_.*")
        self.assertEqual(1, mock_client.Redis.return_value.mset.call_count)

    @mock.patch("deltacat.io.redis_object_store.redis")
    def test_put_many_when_cache_fails(self, mock_client):
        mock_client.Redis.return_value.mset.return_value = []

        with self.assertRaises(RuntimeError):
            self.object_store.put_many(["a", "b"])

        self.assertEqual(1, mock_client.Redis.return_value.mset.call_count)

    @mock.patch("deltacat.io.redis_object_store.redis")
    def test_get_many_sanity(self, mock_client):
        mock_client.Redis.return_value.mget.return_value = ["a", "b"]

        result = self.object_store.get_many(["test_ip", "test_ip"])

        self.assertEqual(2, len(result))
        self.assertEqual(1, mock_client.Redis.return_value.mget.call_count)

    @mock.patch("deltacat.io.redis_object_store.redis")
    def test_get_many_when_cache_expired(self, mock_client):
        mock_client.Redis.return_value.mget.return_value = ["value1"]

        with self.assertRaises(AssertionError):
            self.object_store.get_many(["test_ip", "test_ip"])

        self.assertEqual(1, mock_client.Redis.return_value.mget.call_count)

    @mock.patch("deltacat.io.redis_object_store.redis")
    def test_get_sanity(self, mock_client):
        mock_client.Redis.return_value.get.return_value = self.TEST_VALUE

        result = self.object_store.get("test_ip")

        self.assertEqual(self.TEST_VALUE, result)
        self.assertEqual(1, mock_client.Redis.return_value.get.call_count)

    @mock.patch("deltacat.io.redis_object_store.redis")
    def test_get_when_cache_fails(self, mock_client):
        mock_client.Redis.return_value.get.side_effect = RuntimeError()

        with self.assertRaises(RuntimeError):
            self.object_store.get("test_ip")

        self.assertEqual(1, mock_client.Redis.return_value.get.call_count)

    @mock.patch("deltacat.io.redis_object_store.redis")
    def test_put_sanity(self, mock_client):
        mock_client.Redis.return_value.set.return_value = True

        result = self.object_store.put("test")

        self.assertIsNotNone(result)
        self.assertEqual(1, mock_client.Redis.return_value.set.call_count)

    @mock.patch("deltacat.io.redis_object_store.redis")
    def test_put_when_cache_fails(self, mock_client):
        mock_client.Redis.return_value.set.return_value = False

        with self.assertRaises(RuntimeError):
            self.object_store.put("test_ip")

        self.assertEqual(1, mock_client.Redis.return_value.set.call_count)
