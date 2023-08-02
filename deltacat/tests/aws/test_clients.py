from unittest.mock import MagicMock, patch, call
import unittest
import json
from http import HTTPStatus

HAPPY_RESPONSE = {
    "AccessKeyId": "ASIA123456789",
    "Code": "Success",
    "Expiration": "2023-08-02T13:50:33Z",
    "LastUpdated": "2023-08-02T07:23:35Z",
    "SecretAccessKey": "bar",
    "Token": "foo",
    "Type": "AWS-HMAC",
}


class MockResponse:
    """
    A mock object denoting the response of requests method.
    """

    def __init__(self, status_code: int, text: str, reason: str = "") -> None:
        self.status_code = status_code
        self.text = text
        self.reason = reason


class TestBlockUntilInstanceMetadataServiceReturnsSuccess(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        pass

    @patch("deltacat.aws.clients.Session")
    def test_sanity(self, session_mock):
        from deltacat.aws.clients import (
            block_until_instance_metadata_service_returns_success,
            INSTANCE_METADATA_SERVICE_IPV4_URI,
        )

        mocked_session = MagicMock()
        mocked_session.__enter__.return_value.get.return_value = MockResponse(
            200, "foo"
        )
        session_mock.return_value = mocked_session
        self.assertEqual(
            block_until_instance_metadata_service_returns_success().status_code, 200
        )
        expect_call_to_INSTANCE_METADATA_SERVICE_IPV4_URI = [
            call.__enter__().get(INSTANCE_METADATA_SERVICE_IPV4_URI)
        ]
        session_mock.return_value.assert_has_calls(
            expect_call_to_INSTANCE_METADATA_SERVICE_IPV4_URI,
            any_order=True,
        )

    @patch("deltacat.aws.clients.Session")
    def test_retrying_on_statuses_in_status_force_list(self, session_mock):
        from deltacat.aws.clients import (
            block_until_instance_metadata_service_returns_success,
            INSTANCE_METADATA_SERVICE_IPV4_URI,
        )

        mocked_session = MagicMock()
        mocked_session.__enter__.return_value.get.side_effect = [
            MockResponse(HTTPStatus.OK, json.dumps(HAPPY_RESPONSE)),
            MockResponse(HTTPStatus.TOO_MANY_REQUESTS, "foo"),
            MockResponse(HTTPStatus.INTERNAL_SERVER_ERROR, "foo"),
        ]
        session_mock.return_value = mocked_session
        self.assertEqual(
            block_until_instance_metadata_service_returns_success().status_code, 200
        )
        expect_call_to_INSTANCE_METADATA_SERVICE_IPV4_URI = [
            call.__enter__().get(INSTANCE_METADATA_SERVICE_IPV4_URI)
        ]
        session_mock.return_value.assert_has_calls(
            expect_call_to_INSTANCE_METADATA_SERVICE_IPV4_URI,
            any_order=True,
        )
