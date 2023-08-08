import unittest
from unittest import mock


@mock.patch("deltacat.io.memcached_object_store.cloudpickle")
@mock.patch("deltacat.io.memcached_object_store.socket")
class TestMemcachedObjectStore(unittest.TestCase):

    TEST_VALUE = "test-value"

    def setUp(self):
        from deltacat.io.memcached_object_store import MemcachedObjectStore

        self.object_store = MemcachedObjectStore()

    @mock.patch("deltacat.io.memcached_object_store.Client")
    @mock.patch("deltacat.io.memcached_object_store.RetryingClient")
    def test_put_many_sanity(
        self,
        mock_retrying_client,
        mock_client,
        mock_socket,
        mock_cloudpickle,
    ):
        mock_cloudpickle.dumps.return_value = self.TEST_VALUE
        mock_cloudpickle.loads.return_value = self.TEST_VALUE
        mock_socket.gethostbyname.return_value = "0.0.0.0"
        mock_socket.gethostname.return_value = "test-host"
        mock_retrying_client.return_value = mock_client.return_value
        mock_client.return_value.set_many.return_value = []

        result = self.object_store.put_many(["a", "b"])

        self.assertEqual(2, len(result))
        self.assertRegex(result[0], ".*_.*")
        self.assertEqual(1, mock_client.return_value.set_many.call_count)

    @mock.patch("deltacat.io.memcached_object_store.Client")
    @mock.patch("deltacat.io.memcached_object_store.RetryingClient")
    def test_put_many_when_cache_fails(
        self, mock_retrying_client, mock_client, mock_socket, mock_cloudpickle
    ):
        mock_cloudpickle.dumps.return_value = self.TEST_VALUE
        mock_cloudpickle.loads.return_value = self.TEST_VALUE
        mock_socket.gethostbyname.return_value = "0.0.0.0"
        mock_socket.gethostname.return_value = "test-host"
        mock_retrying_client.return_value = mock_client.return_value
        mock_client.return_value.set_many.return_value = ["abcd"]

        with self.assertRaises(RuntimeError):
            self.object_store.put_many(["a", "b"])

        self.assertEqual(1, mock_client.return_value.set_many.call_count)

    @mock.patch("deltacat.io.memcached_object_store.Client")
    @mock.patch("deltacat.io.memcached_object_store.RetryingClient")
    def test_get_many_sanity(
        self, mock_retrying_client, mock_client, mock_socket, mock_cloudpickle
    ):
        mock_cloudpickle.dumps.return_value = self.TEST_VALUE
        mock_cloudpickle.loads.return_value = self.TEST_VALUE
        mock_socket.gethostbyname.return_value = "0.0.0.0"
        mock_socket.gethostname.return_value = "test-host"
        mock_client.return_value.get_many.return_value = {
            "key1": "value1",
            "key2": "value2",
        }
        mock_retrying_client.return_value = mock_client.return_value

        result = self.object_store.get_many(["test_ip", "test_ip"])

        self.assertEqual(2, len(result))
        self.assertEqual(1, mock_client.return_value.get_many.call_count)

    @mock.patch("deltacat.io.memcached_object_store.Client")
    @mock.patch("deltacat.io.memcached_object_store.RetryingClient")
    def test_get_many_when_cache_expired(
        self, mock_retrying_client, mock_client, mock_socket, mock_cloudpickle
    ):
        mock_cloudpickle.dumps.return_value = self.TEST_VALUE
        mock_cloudpickle.loads.return_value = self.TEST_VALUE
        mock_socket.gethostbyname.return_value = "0.0.0.0"
        mock_socket.gethostname.return_value = "test-host"
        mock_client.return_value.get_many.return_value = {"key1": "value1"}
        mock_retrying_client.return_value = mock_client.return_value

        with self.assertRaises(AssertionError):
            self.object_store.get_many(["test_ip", "test_ip"])

        self.assertEqual(1, mock_client.return_value.get_many.call_count)

    @mock.patch("deltacat.io.memcached_object_store.Client")
    @mock.patch("deltacat.io.memcached_object_store.RetryingClient")
    def test_get_sanity(
        self, mock_retrying_client, mock_client, mock_socket, mock_cloudpickle
    ):
        mock_cloudpickle.dumps.return_value = self.TEST_VALUE
        mock_cloudpickle.loads.return_value = self.TEST_VALUE
        mock_socket.gethostbyname.return_value = "0.0.0.0"
        mock_socket.gethostname.return_value = "test-host"
        mock_client.return_value.get.return_value = self.TEST_VALUE
        mock_retrying_client.return_value = mock_client.return_value

        result = self.object_store.get("test_ip")

        self.assertEqual(self.TEST_VALUE, result)
        self.assertEqual(1, mock_client.return_value.get.call_count)

    @mock.patch("deltacat.io.memcached_object_store.Client")
    @mock.patch("deltacat.io.memcached_object_store.RetryingClient")
    def test_get_when_cache_fails(
        self, mock_retrying_client, mock_client, mock_socket, mock_cloudpickle
    ):
        mock_cloudpickle.dumps.return_value = self.TEST_VALUE
        mock_cloudpickle.loads.return_value = self.TEST_VALUE
        mock_socket.gethostbyname.return_value = "0.0.0.0"
        mock_socket.gethostname.return_value = "test-host"
        mock_client.return_value.get.side_effect = RuntimeError()
        mock_retrying_client.return_value = mock_client.return_value

        with self.assertRaises(RuntimeError):
            self.object_store.get("test_ip")

        self.assertEqual(1, mock_client.return_value.get.call_count)

    @mock.patch("deltacat.io.memcached_object_store.Client")
    @mock.patch("deltacat.io.memcached_object_store.RetryingClient")
    def test_put_sanity(
        self, mock_retrying_client, mock_client, mock_socket, mock_cloudpickle
    ):
        mock_cloudpickle.dumps.return_value = self.TEST_VALUE
        mock_cloudpickle.loads.return_value = self.TEST_VALUE
        mock_socket.gethostbyname.return_value = "0.0.0.0"
        mock_socket.gethostname.return_value = "test-host"
        mock_retrying_client.return_value = mock_client.return_value
        mock_client.return_value.set.return_value = True

        result = self.object_store.put("test")

        self.assertIsNotNone(result)
        self.assertEqual(1, mock_client.return_value.set.call_count)

    @mock.patch("deltacat.io.memcached_object_store.Client")
    @mock.patch("deltacat.io.memcached_object_store.RetryingClient")
    def test_put_when_cache_fails(
        self, mock_retrying_client, mock_client, mock_socket, mock_cloudpickle
    ):
        mock_cloudpickle.dumps.return_value = self.TEST_VALUE
        mock_cloudpickle.loads.return_value = self.TEST_VALUE
        mock_socket.gethostbyname.return_value = "0.0.0.0"
        mock_socket.gethostname.return_value = "test-host"
        mock_retrying_client.return_value = mock_client.return_value
        mock_client.return_value.set.return_value = False

        with self.assertRaises(RuntimeError):
            self.object_store.put("test_ip")

        self.assertEqual(1, mock_client.return_value.set.call_count)
