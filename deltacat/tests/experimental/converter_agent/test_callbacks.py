import json
import time
from unittest.mock import Mock, patch

from deltacat.experimental.converter_agent.callbacks import (
    logging_callback,
    sns_callback,
    metrics_callback,
    slack_notification_callback,
)
from deltacat.experimental.converter_agent.table_monitor import (
    CallbackContext,
    CallbackResult,
    CallbackStage,
)


class TestLoggingCallback:
    """Test class for logging_callback function."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.test_context = CallbackContext.of(
            {
                "last_write_time": time.time_ns(),
                "last_snapshot_id": "snap-001",
                "snapshot_id": "snap-002",
                "catalog_uri": "http://localhost:8181",
                "namespace": "test_ns",
                "table_name": "test_table",
                "merge_keys": ["id"],
                "max_converter_parallelism": 2,
                "stage": CallbackStage.PRE.value,
            }
        )

    @patch("deltacat.experimental.converter_agent.callbacks.logger")
    def test_logging_callback_logs_json(self, mock_logger):
        """Test that logging_callback logs context as JSON."""
        logging_callback(self.test_context)

        # Verify logger.info was called once
        assert mock_logger.info.call_count == 1

        # Get the logged message
        logged_message = mock_logger.info.call_args[0][0]

        # Verify it's valid JSON
        parsed = json.loads(logged_message)
        assert parsed["namespace"] == "test_ns"
        assert parsed["table_name"] == "test_table"
        assert parsed["stage"] == CallbackStage.PRE.value

    def test_logging_callback_returns_none(self):
        """Test that logging_callback returns None."""
        result = logging_callback(self.test_context)
        assert result is None


class TestSnsCallback:
    """Test class for sns_callback function."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.test_context = CallbackContext.of(
            {
                "last_write_time": time.time_ns(),
                "last_snapshot_id": "snap-001",
                "snapshot_id": "snap-002",
                "catalog_uri": "http://localhost:8181",
                "namespace": "test_ns",
                "table_name": "test_table",
                "merge_keys": ["id"],
                "max_converter_parallelism": 2,
                "stage": CallbackStage.POST.value,
                "conversion_start_time": time.time_ns() - int(10 * 1e9),
                "conversion_end_time": time.time_ns(),
            }
        )

    @patch.dict("os.environ", {}, clear=True)
    def test_sns_callback_no_topic_arn(self):
        """Test SNS callback when topic ARN is not configured."""
        result = sns_callback(self.test_context)
        assert isinstance(result, CallbackResult)
        assert result.status == "skipped"
        assert "SNS topic ARN not configured" in result.reason

    @patch.dict(
        "os.environ",
        {
            "DELTACAT_MONITOR_SNS_TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:test-topic"
        },
    )
    @patch("deltacat.aws.clients.client_cache")
    def test_sns_callback_success(self, mock_client_cache):
        """Test successful SNS callback execution."""
        # Mock SNS client
        mock_sns = Mock()
        mock_sns.publish.return_value = {
            "MessageId": "test-message-id",
            "SequenceNumber": "12345",
        }
        mock_client_cache.return_value = mock_sns

        result = sns_callback(self.test_context)

        # Verify result
        assert isinstance(result, CallbackResult)
        assert result.status == "success"
        assert result["messageId"] == "test-message-id"
        assert result["sequenceNumber"] == "12345"

        # Verify SNS publish was called
        assert mock_sns.publish.call_count == 1
        call_kwargs = mock_sns.publish.call_args[1]

        # Verify message is JSON serialized context
        message_content = json.loads(call_kwargs["Message"])
        assert message_content["namespace"] == "test_ns"
        assert message_content["table_name"] == "test_table"
        assert message_content["stage"] == CallbackStage.POST.value

        # Verify client_cache was called with None (boto3 default)
        mock_client_cache.assert_called_once_with("sns", None)

    @patch.dict(
        "os.environ",
        {
            "DELTACAT_MONITOR_SNS_TOPIC_ARN": "arn:aws:sns:us-west-2:123456789012:test-topic",
            "DELTACAT_MONITOR_SNS_REGION": "us-west-2",
        },
    )
    @patch("deltacat.aws.clients.client_cache")
    def test_sns_callback_respects_region_env_var(self, mock_client_cache):
        """Test that SNS callback respects region from environment variable."""
        mock_sns = Mock()
        mock_sns.publish.return_value = {"MessageId": "test-id"}
        mock_client_cache.return_value = mock_sns

        sns_callback(self.test_context)

        # Verify client_cache was called with us-west-2
        mock_client_cache.assert_called_once_with("sns", "us-west-2")

    @patch.dict(
        "os.environ",
        {
            "DELTACAT_MONITOR_SNS_TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:test-topic"
        },
    )
    @patch("deltacat.aws.clients.client_cache")
    def test_sns_callback_respects_region_parameter(self, mock_client_cache):
        """Test that SNS callback respects region parameter over environment."""
        mock_sns = Mock()
        mock_sns.publish.return_value = {"MessageId": "test-id"}
        mock_client_cache.return_value = mock_sns

        sns_callback(self.test_context, region="eu-west-1")

        # Verify client_cache was called with eu-west-1
        mock_client_cache.assert_called_once_with("sns", "eu-west-1")


class TestMetricsCallback:
    """Test class for metrics_callback function."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.base_time = time.time_ns()
        self.context_pre = CallbackContext.of(
            {
                "last_write_time": self.base_time,
                "last_snapshot_id": "snap-001",
                "snapshot_id": "snap-002",
                "catalog_uri": "http://localhost:8181",
                "namespace": "test_ns",
                "table_name": "test_table",
                "merge_keys": ["id"],
                "max_converter_parallelism": 2,
                "stage": CallbackStage.PRE.value,
            }
        )

        self.context_post = CallbackContext.of(
            {
                "last_write_time": self.base_time,
                "last_snapshot_id": "snap-001",
                "snapshot_id": "snap-002",
                "catalog_uri": "http://localhost:8181",
                "namespace": "test_ns",
                "table_name": "test_table",
                "merge_keys": ["id"],
                "max_converter_parallelism": 2,
                "stage": CallbackStage.POST.value,
                "conversion_start_time": self.base_time,
                "conversion_end_time": self.base_time + int(10.5 * 1e9),
            }
        )

    @patch("deltacat.aws.clients.client_cache")
    def test_metrics_callback_pre_conversion(self, mock_client_cache):
        """Test metrics callback for pre-conversion stage."""
        mock_cloudwatch = Mock()
        mock_client_cache.return_value = mock_cloudwatch

        result = metrics_callback(self.context_pre)

        # Verify result
        assert isinstance(result, CallbackResult)
        assert result.status == "success"
        assert result["metrics_published"] == 1

        # Verify CloudWatch put_metric_data was called
        assert mock_cloudwatch.put_metric_data.call_count == 1
        call_kwargs = mock_cloudwatch.put_metric_data.call_args[1]

        # Verify metric data
        metric_data = call_kwargs["MetricData"]
        assert len(metric_data) == 1
        assert metric_data[0]["MetricName"] == "ConversionStarted"
        assert metric_data[0]["Value"] == 1
        assert metric_data[0]["Unit"] == "Count"

        # Verify dimensions
        dimensions = metric_data[0]["Dimensions"]
        dim_dict = {d["Name"]: d["Value"] for d in dimensions}
        assert dim_dict["Namespace"] == "test_ns"
        assert dim_dict["Table"] == "test_table"
        assert dim_dict["Stage"] == "pre"
        assert dim_dict["CatalogURI"] == "http://localhost:8181"

        # Verify client_cache was called with None (boto3 default)
        mock_client_cache.assert_called_once_with("cloudwatch", None)

    @patch("deltacat.aws.clients.client_cache")
    def test_metrics_callback_post_conversion(self, mock_client_cache):
        """Test metrics callback for post-conversion stage."""
        mock_cloudwatch = Mock()
        mock_client_cache.return_value = mock_cloudwatch

        result = metrics_callback(self.context_post)

        # Verify result
        assert isinstance(result, CallbackResult)
        assert result.status == "success"
        assert result["metrics_published"] == 2

        # Verify CloudWatch put_metric_data was called
        assert mock_cloudwatch.put_metric_data.call_count == 1
        call_kwargs = mock_cloudwatch.put_metric_data.call_args[1]

        # Verify metric data
        metric_data = call_kwargs["MetricData"]
        assert len(metric_data) == 2

        # Check ConversionCompleted metric
        completed_metric = next(
            m for m in metric_data if m["MetricName"] == "ConversionCompleted"
        )
        assert completed_metric["Value"] == 1
        assert completed_metric["Unit"] == "Count"

        # Check ConversionDuration metric
        duration_metric = next(
            m for m in metric_data if m["MetricName"] == "ConversionDuration"
        )
        assert abs(duration_metric["Value"] - 10.5) < 0.001
        assert duration_metric["Unit"] == "Seconds"

        # Verify client_cache was called with None (boto3 default)
        mock_client_cache.assert_called_once_with("cloudwatch", None)

    @patch.dict("os.environ", {"DELTACAT_MONITOR_CLOUDWATCH_REGION": "ap-southeast-1"})
    @patch("deltacat.aws.clients.client_cache")
    def test_metrics_callback_respects_region_env_var(self, mock_client_cache):
        """Test that metrics callback respects region from environment variable."""
        mock_cloudwatch = Mock()
        mock_client_cache.return_value = mock_cloudwatch

        metrics_callback(self.context_pre)

        # Verify client_cache was called with ap-southeast-1
        mock_client_cache.assert_called_once_with("cloudwatch", "ap-southeast-1")

    @patch("deltacat.aws.clients.client_cache")
    def test_metrics_callback_respects_region_parameter(self, mock_client_cache):
        """Test that metrics callback respects region parameter over environment."""
        mock_cloudwatch = Mock()
        mock_client_cache.return_value = mock_cloudwatch

        metrics_callback(self.context_pre, region="ca-central-1")

        # Verify client_cache was called with ca-central-1
        mock_client_cache.assert_called_once_with("cloudwatch", "ca-central-1")


class TestSlackNotificationCallback:
    """Test class for slack_notification_callback function."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.base_time = time.time_ns()
        self.context_pre = CallbackContext.of(
            {
                "last_write_time": self.base_time,
                "last_snapshot_id": "snap-001",
                "snapshot_id": "snap-002",
                "catalog_uri": "http://localhost:8181",
                "namespace": "test_ns",
                "table_name": "test_table",
                "merge_keys": ["id", "ts"],
                "max_converter_parallelism": 4,
                "stage": CallbackStage.PRE.value,
            }
        )

        self.context_post = CallbackContext.of(
            {
                "last_write_time": self.base_time,
                "last_snapshot_id": "snap-001",
                "snapshot_id": "snap-003",
                "catalog_uri": "http://localhost:8181",
                "namespace": "test_ns",
                "table_name": "test_table",
                "merge_keys": ["id", "ts"],
                "max_converter_parallelism": 4,
                "stage": CallbackStage.POST.value,
                "conversion_start_time": self.base_time,
                "conversion_end_time": self.base_time + int(25.7 * 1e9),
            }
        )

    @patch.dict("os.environ", {}, clear=True)
    def test_slack_callback_no_webhook_url(self):
        """Test Slack callback when webhook URL is not configured."""
        result = slack_notification_callback(self.context_pre)
        assert isinstance(result, CallbackResult)
        assert result.status == "skipped"
        assert "Slack webhook URL not configured" in result.reason

    @patch.dict(
        "os.environ", {"SLACK_WEBHOOK_URL": "https://hooks.slack.com/services/test"}
    )
    @patch("requests.post")
    def test_slack_callback_pre_conversion(self, mock_post):
        """Test Slack callback for pre-conversion stage."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        result = slack_notification_callback(self.context_pre)

        # Verify result
        assert isinstance(result, CallbackResult)
        assert result.status == "success"
        assert result["response_code"] == 200

        # Verify requests.post was called
        assert mock_post.call_count == 1
        call_kwargs = mock_post.call_args[1]

        # Verify message structure
        message = call_kwargs["json"]
        assert "🚀" in message["blocks"][0]["text"]["text"]
        assert "test_ns.test_table" in message["text"]

        # Verify fields include pre-conversion info
        fields = message["blocks"][1]["fields"]
        field_texts = [f["text"] for f in fields]
        assert any("Parallelism" in text for text in field_texts)
        assert any("Merge Keys" in text for text in field_texts)

    @patch.dict(
        "os.environ", {"SLACK_WEBHOOK_URL": "https://hooks.slack.com/services/test"}
    )
    @patch("requests.post")
    def test_slack_callback_post_conversion(self, mock_post):
        """Test Slack callback for post-conversion stage."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        result = slack_notification_callback(self.context_post)

        # Verify result
        assert isinstance(result, CallbackResult)
        assert result.status == "success"

        # Verify message structure
        message = mock_post.call_args[1]["json"]
        assert "✅" in message["blocks"][0]["text"]["text"]

        # Verify fields include duration
        fields = message["blocks"][1]["fields"]
        field_texts = [f["text"] for f in fields]
        assert any("Duration" in text and "25.7" in text for text in field_texts)

    @patch.dict(
        "os.environ", {"SLACK_WEBHOOK_URL": "https://hooks.slack.com/services/test"}
    )
    @patch("requests.post")
    def test_slack_callback_handles_request_error(self, mock_post):
        """Test that Slack callback handles request errors gracefully."""
        mock_post.side_effect = Exception("Network error")

        result = slack_notification_callback(self.context_pre)

        # Should return error status
        assert isinstance(result, CallbackResult)
        assert result.status == "error"
        assert "Network error" in result.reason
