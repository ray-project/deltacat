"""
Callback functions for DeltaCAT Table Monitor.

Can be used directly or as a starting point for custom callbacks.
Each callback handles different integration points (logging, SNS notifications,
CloudWatch metrics, Slack notifications).

Usage Examples:
    # Programmatic usage with direct callable:
    from deltacat.experimental.converter_agent.callbacks import sns_callback
    monitor_table(..., post_conversion_callback=sns_callback)

    # CLI usage with string spec:
    python table_monitor.py \
        --post-conversion-callback "deltacat.experimental.converter_agent.callbacks:sns_callback" \
        ...
"""

import json
import logging
from typing import Optional
from deltacat.constants import NANOS_PER_SEC
from deltacat.experimental.converter_agent.table_monitor import (
    CallbackContext,
    CallbackResult,
    CallbackStage,
)

logger = logging.getLogger(__name__)


def logging_callback(context: CallbackContext) -> None:
    """
    Logging callback that emits the full callback context as JSON.

    This callback logs all conversion metadata in a structured format suitable for
    log aggregation and monitoring systems.

    Args:
        context: CallbackContext containing conversion parameters and metadata
    """
    logger.info(json.dumps(dict(context), default=str))


def sns_callback(
    context: CallbackContext, region: Optional[str] = None
) -> CallbackResult:
    """
    SNS notification callback that publishes conversion events.

    Publishes all conversion metadata to AWS SNS for downstream processing.
    The SNS message contains the complete CallbackContext serialized as JSON,
    including stage, timing, snapshot IDs, and table metadata.

    This callback can be used for both pre-conversion and post-conversion stages.
    The 'stage' field in the context distinguishes between them.

    Args:
        context: CallbackContext containing all conversion parameters and metadata.
        region: AWS region for SNS client. If not specified, uses boto3 default region resolution.

    Returns:
        CallbackResult containing SNS response metadata (messageId, sequenceNumber, status)

    Environment Variables:
        DELTACAT_MONITOR_SNS_TOPIC_ARN: SNS topic ARN to publish to (required)
        DELTACAT_MONITOR_SNS_REGION: AWS region (optional, overridden by region param)

    Example SNS message body (all CallbackContext fields):
        {
            "stage": "post",
            "last_write_time": 1759965570958012000,
            "conversion_start_time": 1759965570958012000,
            "conversion_end_time": 1759965616458012000,
            "last_snapshot_id": "snapshot-001",
            "snapshot_id": "snapshot-002",
            "catalog_uri": "http://localhost:8181",
            "namespace": "default",
            "table_name": "my_table",
            "merge_keys": ["id"],
            "max_converter_parallelism": 4
        }
    """
    try:
        import os
        import deltacat.aws.clients as aws_utils

        # Get SNS topic ARN from environment variable
        topic_arn = os.environ.get("DELTACAT_MONITOR_SNS_TOPIC_ARN")
        if not topic_arn:
            logger.warning(
                "DELTACAT_MONITOR_SNS_TOPIC_ARN not set, skipping SNS notification"
            )
            return CallbackResult.of(
                {"status": "skipped", "reason": "SNS topic ARN not configured"}
            )

        # Determine region (priority: param > env var > None for boto3 default)
        aws_region = region or os.environ.get("DELTACAT_MONITOR_SNS_REGION")

        # Create SNS client
        sns = aws_utils.client_cache("sns", aws_region)

        # Serialize entire context as message (no extra fields)
        message = json.dumps(dict(context), default=str)

        # Publish to SNS with descriptive subject
        response = sns.publish(
            TopicArn=topic_arn,
            Subject=f"DeltaCAT Conversion {context.stage.upper()}: {context.namespace}.{context.table_name}",
            Message=message,
        )

        return CallbackResult.of(
            {
                "messageId": response["MessageId"],
                "sequenceNumber": response.get("SequenceNumber"),
                "status": "success",
            }
        )

    except ImportError:
        logger.error("boto3 not installed, cannot publish to SNS")
        return CallbackResult.of({"status": "error", "reason": "boto3 not installed"})
    except Exception as e:
        logger.error(f"Failed to publish SNS notification: {e}", exc_info=True)
        return CallbackResult.of({"status": "error", "reason": str(e)})


def metrics_callback(
    context: CallbackContext, region: Optional[str] = None
) -> CallbackResult:
    """
    CloudWatch metrics callback for monitoring conversion operations.

    Publishes comprehensive metrics to CloudWatch with namespace, table, stage,
    and catalog URI dimensions for detailed operational monitoring.

    For pre-conversion (stage='pre'):
        - ConversionStarted: Count of conversion initiations

    For post-conversion (stage='post'):
        - ConversionCompleted: Count of successful conversions
        - ConversionDuration: Time taken for conversion (seconds)

    All metrics include Namespace, Table, Stage, and CatalogURI dimensions for
    precise filtering and aggregation in monitoring dashboards.

    Args:
        context: CallbackContext containing conversion parameters and metadata
        region: AWS region for CloudWatch client. If not specified, uses boto3 default region resolution.

    Returns:
        CallbackResult containing metric publishing status and count

    Environment Variables:
        DELTACAT_MONITOR_CLOUDWATCH_REGION: AWS region (optional, overridden by region param)
        AWS credentials must be configured for CloudWatch access
    """
    try:
        import os
        import deltacat.aws.clients as aws_utils

        # Determine region (priority: param > env var > None for boto3 default)
        aws_region = region or os.environ.get("DELTACAT_MONITOR_CLOUDWATCH_REGION")

        # Create CloudWatch client
        cloudwatch = aws_utils.client_cache("cloudwatch", aws_region)

        namespace = context.namespace
        table_name = context.table_name
        stage = context.stage
        catalog_uri = context.catalog_uri or "unknown"

        # Base dimensions shared by all metrics
        base_dimensions = [
            {"Name": "Namespace", "Value": namespace},
            {"Name": "Table", "Value": table_name},
            {"Name": "Stage", "Value": stage},
            {"Name": "CatalogURI", "Value": catalog_uri},
        ]

        metric_data = []

        if stage == CallbackStage.PRE.value:
            # Pre-conversion: record that conversion started
            metric_data.append(
                {
                    "MetricName": "ConversionStarted",
                    "Value": 1,
                    "Unit": "Count",
                    "Dimensions": base_dimensions,
                }
            )
        elif stage == CallbackStage.POST.value:
            # Post-conversion: record completion and duration
            start_time = context.conversion_start_time
            end_time = context.conversion_end_time
            duration = (
                (end_time - start_time) / NANOS_PER_SEC
                if end_time and start_time
                else 0
            )

            metric_data.extend(
                [
                    {
                        "MetricName": "ConversionCompleted",
                        "Value": 1,
                        "Unit": "Count",
                        "Dimensions": base_dimensions,
                    },
                    {
                        "MetricName": "ConversionDuration",
                        "Value": duration,
                        "Unit": "Seconds",
                        "Dimensions": base_dimensions,
                    },
                ]
            )

        # Publish metrics
        cloudwatch.put_metric_data(
            Namespace="DeltaCAT/TableMonitor", MetricData=metric_data
        )

        return CallbackResult.of(
            {"status": "success", "metrics_published": len(metric_data)}
        )

    except ImportError:
        logger.error("boto3 not installed, cannot publish metrics")
        return CallbackResult.of({"status": "error", "reason": "boto3 not installed"})
    except Exception as e:
        logger.error(f"Failed to publish metrics: {e}", exc_info=True)
        return CallbackResult.of({"status": "error", "reason": str(e)})


def slack_notification_callback(context: CallbackContext) -> CallbackResult:
    """
    Slack notification callback for conversion events.

    Sends rich Slack notifications with comprehensive conversion metadata.
    Notifications are customized based on stage (pre/post conversion).

    Pre-conversion notifications include:
        - Table identification (namespace.table_name)
        - Current snapshot ID
        - Catalog URI
        - Merge keys configuration
        - Parallelism settings

    Post-conversion notifications include all pre-conversion fields plus:
        - Conversion duration
        - Previous snapshot ID (for tracking transitions)
        - Timestamp information

    Args:
        context: CallbackContext containing conversion parameters and metadata

    Returns:
        CallbackResult containing notification status and response code

    Environment Variables:
        SLACK_WEBHOOK_URL: Slack webhook URL to post to (required)
    """
    try:
        import os
        import requests

        webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
        if not webhook_url:
            logger.warning("SLACK_WEBHOOK_URL not set, skipping Slack notification")
            return CallbackResult.of(
                {"status": "skipped", "reason": "Slack webhook URL not configured"}
            )

        namespace = context.namespace
        table_name = context.table_name
        snapshot_id = context.snapshot_id
        catalog_uri = context.catalog_uri or "N/A"
        stage = context.stage
        merge_keys = ", ".join(context.merge_keys) if context.merge_keys else "None"
        parallelism = context.max_converter_parallelism

        # Build message based on stage
        if stage == CallbackStage.PRE.value:
            header_text = "🚀 Table Conversion Starting"
            fields = [
                {"type": "mrkdwn", "text": f"*Table:*\n`{namespace}.{table_name}`"},
                {"type": "mrkdwn", "text": f"*Snapshot ID:*\n`{snapshot_id}`"},
                {"type": "mrkdwn", "text": f"*Catalog:*\n`{catalog_uri}`"},
                {"type": "mrkdwn", "text": f"*Merge Keys:*\n`{merge_keys}`"},
                {"type": "mrkdwn", "text": f"*Parallelism:*\n`{parallelism}`"},
                {
                    "type": "mrkdwn",
                    "text": f"*Last Snapshot:*\n`{context.last_snapshot_id or 'initial'}`",
                },
            ]
        else:  # POST
            # Calculate duration from nanosecond timestamps
            start_time = context.conversion_start_time
            end_time = context.conversion_end_time
            duration = (
                (end_time - start_time) / NANOS_PER_SEC
                if end_time and start_time
                else 0
            )

            header_text = "✅ Table Conversion Complete"
            fields = [
                {"type": "mrkdwn", "text": f"*Table:*\n`{namespace}.{table_name}`"},
                {"type": "mrkdwn", "text": f"*Duration:*\n`{duration:.2f}s`"},
                {"type": "mrkdwn", "text": f"*Snapshot ID:*\n`{snapshot_id}`"},
                {
                    "type": "mrkdwn",
                    "text": f"*Last Snapshot:*\n`{context.last_snapshot_id or 'initial'}`",
                },
                {"type": "mrkdwn", "text": f"*Catalog:*\n`{catalog_uri}`"},
                {"type": "mrkdwn", "text": f"*Merge Keys:*\n`{merge_keys}`"},
            ]

        message = {
            "text": f"DeltaCAT Conversion {stage.upper()}: {namespace}.{table_name}",
            "blocks": [
                {"type": "header", "text": {"type": "plain_text", "text": header_text}},
                {"type": "section", "fields": fields},
            ],
        }

        # Send to Slack
        response = requests.post(webhook_url, json=message, timeout=10)
        response.raise_for_status()

        return CallbackResult.of(
            {"status": "success", "response_code": response.status_code}
        )

    except ImportError:
        logger.error("requests library not installed")
        return CallbackResult.of(
            {"status": "error", "reason": "requests not installed"}
        )
    except Exception as e:
        logger.error(f"Failed to send Slack notification: {e}", exc_info=True)
        return CallbackResult.of({"status": "error", "reason": str(e)})
