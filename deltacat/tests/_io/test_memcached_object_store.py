import unittest
import numpy as np
from unittest import mock
from deltacat.exceptions import (
    PymemcachedPutObjectError,
)


class MockPyMemcacheClient:
    def __init__(self) -> None:
        self.store = {}

    def set_many(self, value_dict, *args, **kwargs):
        self.store.update(value_dict)
        return []

    def get_many(self, keys, *args, **kwargs):
        result = {}
        for key in keys:
            result[key] = self.store[key]

        return result

    def set(self, key, value, *args, **kwargs):
        self.store[key] = value
        return True

    def get(self, key, *args, **kwargs):
        return self.store.get(key)

    def delete(self, key, *args, **kwargs):
        self.store.pop(key, None)
        return True

    def delete_many(self, keys, *args, **kwargs):
        for key in keys:
            self.store.pop(key, None)
        return True

    def flush_all(self, *args, **kwargs):
        for key, value in self.store.items():
            self.store[key] = None


class TestMemcachedObjectStore(unittest.TestCase):

    TEST_VALUE_LARGE = "test-value-greater-than-10-bytes"

    def setUp(self):
        from deltacat.io.memcached_object_store import MemcachedObjectStore

        self.object_store = MemcachedObjectStore(
            storage_node_ips=["172.1.1.1", "172.2.2.2", "172.3.3.3"],
            max_item_size_bytes=10,
        )

    @mock.patch("deltacat.io.memcached_object_store.Client")
    @mock.patch("deltacat.io.memcached_object_store.RetryingClient")
    def test_put_many_sanity(
        self,
        mock_retrying_client,
        mock_client,
    ):
        mock_client.return_value = MockPyMemcacheClient()
        mock_retrying_client.return_value = mock_client.return_value

        result = self.object_store.put_many(["a", "b", "c"])

        # assert
        self.assertEqual(3, len(result))
        self.assertRegex(result[0], ".*_.*_.*")
        expected = self.object_store.get_many(result)
        self.assertEqual(expected, ["a", "b", "c"])

    @mock.patch("deltacat.io.memcached_object_store.Client")
    @mock.patch("deltacat.io.memcached_object_store.RetryingClient")
    def test_put_many_greater_than_max_item_size(
        self,
        mock_retrying_client,
        mock_client,
    ):
        mock_client.return_value = MockPyMemcacheClient()
        mock_retrying_client.return_value = mock_client.return_value

        put_request = [np.arange(99), np.zeros(1000), np.zeros(100)]
        result = self.object_store.put_many(put_request)

        # assert
        self.assertEqual(3, len(result))
        self.assertRegex(result[0], ".*_.*_.*")
        expected = self.object_store.get_many(result)
        for index, val in enumerate(expected):
            self.assertTrue(np.array_equal(val, put_request[index]))

    @mock.patch("deltacat.io.memcached_object_store.Client")
    @mock.patch("deltacat.io.memcached_object_store.RetryingClient")
    def test_put_many_when_cache_fails(self, mock_retrying_client, mock_client):
        mock_retrying_client.return_value = mock_client.return_value
        mock_client.return_value.set_many.return_value = ["abcd"]

        with self.assertRaises(PymemcachedPutObjectError):
            self.object_store.put_many(["a", "b"])

        self.assertEqual(1, mock_client.return_value.set_many.call_count)

    @mock.patch("deltacat.io.memcached_object_store.Client")
    @mock.patch("deltacat.io.memcached_object_store.RetryingClient")
    def test_get_many_sanity(self, mock_retrying_client, mock_client):
        mock_client.return_value = MockPyMemcacheClient()
        mock_retrying_client.return_value = mock_client.return_value

        # setup
        ref1 = self.object_store.put("a")
        ref2 = self.object_store.put(np.arange(100))

        # action
        result = self.object_store.get_many([ref2, ref1])

        # assert
        self.assertEqual(2, len(result))
        self.assertEqual(result[1], "a")
        self.assertTrue(
            np.array_equal(result[0], np.arange(100)), f"Arrays not equal: {result[0]}"
        )

    @mock.patch("deltacat.io.memcached_object_store.Client")
    @mock.patch("deltacat.io.memcached_object_store.RetryingClient")
    def test_get_many_when_cache_expired(self, mock_retrying_client, mock_client):
        mock_client.return_value.get_many.return_value = {"uuid_ip_1": "value1"}
        mock_retrying_client.return_value = mock_client.return_value

        with self.assertRaises(AssertionError):
            self.object_store.get_many(["uuid_ip_1", "uuid2_ip_1"])

        self.assertEqual(1, mock_client.return_value.get_many.call_count)

    @mock.patch("deltacat.io.memcached_object_store.Client")
    @mock.patch("deltacat.io.memcached_object_store.RetryingClient")
    def test_get_and_put_sanity(self, mock_retrying_client, mock_client):
        # setup
        mock_client.return_value = MockPyMemcacheClient()
        mock_retrying_client.return_value = mock_client.return_value

        # action
        ref = self.object_store.put(self.TEST_VALUE_LARGE)
        result = self.object_store.get(ref)

        # assert
        self.assertEqual(self.TEST_VALUE_LARGE, result)
        _, _, count = ref.split(self.object_store.SEPARATOR)
        self.assertTrue(int(count) > 1, f"The chunks={count} is not > 1")

    @mock.patch("deltacat.io.memcached_object_store.Client")
    @mock.patch("deltacat.io.memcached_object_store.RetryingClient")
    def test_get_when_cache_fails(self, mock_retrying_client, mock_client):
        mock_client.return_value.get.side_effect = RuntimeError()
        mock_retrying_client.return_value = mock_client.return_value

        with self.assertRaises(RuntimeError):
            self.object_store.get("test_ip_1")

        self.assertEqual(1, mock_client.return_value.get.call_count)

    @mock.patch("deltacat.io.memcached_object_store.Client")
    @mock.patch("deltacat.io.memcached_object_store.RetryingClient")
    def test_put_single_chunk(self, mock_retrying_client, mock_client):
        from deltacat.io.memcached_object_store import MemcachedObjectStore

        object_store = MemcachedObjectStore(storage_node_ips=["1.2.2.2"])
        mock_retrying_client.return_value = mock_client.return_value
        mock_client.return_value.set.return_value = True

        result = object_store.put("test")

        self.assertIsNotNone(result)
        self.assertEqual(1, mock_client.return_value.set.call_count)

    @mock.patch("deltacat.io.memcached_object_store.Client")
    @mock.patch("deltacat.io.memcached_object_store.RetryingClient")
    def test_put_when_cache_fails(self, mock_retrying_client, mock_client):
        mock_client.return_value.set.return_value = False
        mock_retrying_client.return_value = mock_client.return_value

        with self.assertRaises(PymemcachedPutObjectError):
            self.object_store.put("test_ip")

        self.assertEqual(1, mock_client.return_value.set.call_count)

    @mock.patch("deltacat.io.memcached_object_store.Client")
    @mock.patch("deltacat.io.memcached_object_store.RetryingClient")
    @mock.patch("deltacat.io.memcached_object_store.socket")
    def test_put_when_storage_nodes_none(
        self, mock_socket, mock_retrying_client, mock_client
    ):
        from deltacat.io.memcached_object_store import MemcachedObjectStore

        object_store = MemcachedObjectStore()

        # setup
        mock_socket.gethostbyname.return_value = "0.0.0.0"
        mock_socket.gethostname.return_value = "localhost"
        mock_client.return_value = MockPyMemcacheClient()
        mock_retrying_client.return_value = mock_client.return_value

        # action
        ref = object_store.put(self.TEST_VALUE_LARGE)

        # assert
        result = self.object_store.get(ref)
        self.assertEqual(result, self.TEST_VALUE_LARGE)

    @mock.patch("deltacat.io.memcached_object_store.Client")
    @mock.patch("deltacat.io.memcached_object_store.RetryingClient")
    def test_delete_sanity(self, mock_retrying_client, mock_client):
        mock_client.return_value = MockPyMemcacheClient()
        mock_retrying_client.return_value = mock_client.return_value

        # setup
        ref = self.object_store.put(np.arange(100))

        # action
        delete_success = self.object_store.delete(ref)

        # assert
        self.assertTrue(delete_success)

    @mock.patch("deltacat.io.memcached_object_store.Client")
    @mock.patch("deltacat.io.memcached_object_store.RetryingClient")
    def test_delete_many_sanity(self, mock_retrying_client, mock_client):
        mock_client.return_value = MockPyMemcacheClient()
        mock_retrying_client.return_value = mock_client.return_value

        # setup
        ref1 = self.object_store.put("a")
        ref2 = self.object_store.put(np.arange(100))

        # action
        delete_success = self.object_store.delete_many([ref2, ref1])

        # assert
        self.assertTrue(delete_success)

    @mock.patch("deltacat.io.memcached_object_store.Client")
    @mock.patch("deltacat.io.memcached_object_store.RetryingClient")
    def test_clear_sanity(self, mock_retrying_client, mock_client):
        # setup
        mock_client.return_value = MockPyMemcacheClient()
        mock_retrying_client.return_value = mock_client.return_value

        # action
        ref = self.object_store.put(self.TEST_VALUE_LARGE)
        self.object_store.clear()

        # assert
        with self.assertRaises(ValueError):
            self.object_store.get(ref)
