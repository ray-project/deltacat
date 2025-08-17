from unittest.mock import patch, Mock
import unittest
from http import HTTPStatus
import requests
from requests import HTTPError

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
        self.status_code: requests.Response.status_code = status_code
        self.text = text
        self.reason = reason

    def raise_for_status(*args, **kwargs):
        pass


class TestBlockUntilInstanceMetadataServiceReturnsSuccess(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        pass

    @patch("deltacat.aws.clients.requests")
    def test_sanity(self, requests_mock):
        from deltacat.aws.clients import (
            block_until_instance_metadata_service_returns_success,
        )

        requests_mock.get.return_value = MockResponse(200, "foo")
        self.assertEqual(
            block_until_instance_metadata_service_returns_success().status_code, 200
        )

    @patch("deltacat.aws.clients.requests")
    def test_retrying_on_statuses_in_status_force_list(self, requests_mock):
        from deltacat.aws.clients import (
            block_until_instance_metadata_service_returns_success,
        )

        requests_mock.get.side_effect = [
            MockResponse(HTTPStatus.OK, "foo"),
            MockResponse(HTTPStatus.TOO_MANY_REQUESTS, "foo"),
            MockResponse(HTTPStatus.INTERNAL_SERVER_ERROR, "foo"),
            MockResponse(HTTPStatus.NOT_IMPLEMENTED, "bar"),
            MockResponse(HTTPStatus.SERVICE_UNAVAILABLE, "bar"),
            MockResponse(HTTPStatus.GATEWAY_TIMEOUT, "bar"),
        ]
        self.assertEqual(
            block_until_instance_metadata_service_returns_success().status_code, 200
        )

    @patch("deltacat.aws.clients.requests.get")
    def test_retrying_on_initial_failures(self, requests_mock_get):
        from deltacat.aws.clients import (
            block_until_instance_metadata_service_returns_success,
        )

        mock_success_response = Mock()
        mock_success_response.raise_for_status.return_value = None
        mock_success_response.status_code = 200

        mock_service_unavailable_response = Mock()
        mock_service_unavailable_response.status_code = 503

        mock_errors = []
        for err in range(3):
            mock_error_response = Mock()
            mock_error_response.raise_for_status.side_effect = HTTPError(
                response=mock_service_unavailable_response
            )
            mock_errors.append(mock_error_response)

        mock_connection_error_response = requests.exceptions.ConnectionError()
        mock_errors.append(mock_connection_error_response)

        requests_mock_get.side_effect = [*mock_errors, mock_success_response]
        self.assertEqual(
            block_until_instance_metadata_service_returns_success().status_code, 200
        )
        self.assertEqual(requests_mock_get.call_count, 5)

    @patch("deltacat.aws.clients.requests")
    def test_retrying_status_on_shortlist_returns_early(self, requests_mock):
        from deltacat.aws.clients import (
            block_until_instance_metadata_service_returns_success,
        )

        requests_mock.get.side_effect = [
            MockResponse(HTTPStatus.FORBIDDEN, "foo"),
        ]
        self.assertEqual(
            block_until_instance_metadata_service_returns_success().status_code, 403
        )
