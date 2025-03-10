import unittest
from unittest.mock import MagicMock, ANY, call
from deltacat.utils.metrics import (
    metrics,
    success_metric,
    failure_metric,
    latency_metric,
    MetricsConfigCache,
    MetricsConfig,
    MetricsTarget,
    METRICS_TARGET_TO_EMITTER_DICT,
)
from deltacat.utils.performance import timed_invocation


def method_not_annotated(mock_func):
    mock_func("called")


@metrics
def metrics_annotated_method(mock_func):
    mock_func("called")


@metrics
def metrics_annotated_method_error(mock_func):
    raise ValueError()


@metrics(prefix="test_prefix")
def metrics_with_prefix_annotated_method(mock_func):
    mock_func("called")


@metrics(prefix="test_prefix")
def metrics_with_prefix_annotated_method_error(mock_func):
    raise ValueError()


class TestMetricsAnnotation(unittest.TestCase):
    def test_metrics_annotation_sanity(self):
        mock, mock_target = MagicMock(), MagicMock()
        METRICS_TARGET_TO_EMITTER_DICT[MetricsTarget.NOOP] = mock_target
        config = MetricsConfig("us-east-1", MetricsTarget.NOOP)
        MetricsConfigCache.metrics_config = config

        # action
        metrics_annotated_method(mock)

        mock.assert_called_once()
        self.assertEqual(2, mock_target.call_count)
        mock_target.assert_has_calls(
            [
                call(
                    metrics_name="metrics_annotated_method_time",
                    metrics_config=ANY,
                    value=ANY,
                ),
                call(
                    metrics_name="metrics_annotated_method_success_count",
                    metrics_config=ANY,
                    value=ANY,
                ),
            ],
            any_order=True,
        )

    def test_metrics_annotation_when_error(self):
        mock, mock_target = MagicMock(), MagicMock()
        METRICS_TARGET_TO_EMITTER_DICT[MetricsTarget.NOOP] = mock_target
        config = MetricsConfig("us-east-1", MetricsTarget.NOOP)
        MetricsConfigCache.metrics_config = config

        # action
        self.assertRaises(ValueError, lambda: metrics_annotated_method_error(mock))

        mock.assert_not_called()
        self.assertEqual(3, mock_target.call_count)
        mock_target.assert_has_calls(
            [
                call(
                    metrics_name="metrics_annotated_method_error_time",
                    metrics_config=ANY,
                    value=ANY,
                ),
                call(
                    metrics_name="metrics_annotated_method_error_failure_count",
                    metrics_config=ANY,
                    value=ANY,
                ),
                call(
                    metrics_name="metrics_annotated_method_error_failure_count.ValueError",
                    metrics_config=ANY,
                    value=ANY,
                ),
            ],
            any_order=True,
        )

    def test_metrics_with_prefix_annotation_sanity(self):
        mock, mock_target = MagicMock(), MagicMock()
        METRICS_TARGET_TO_EMITTER_DICT[MetricsTarget.NOOP] = mock_target
        config = MetricsConfig("us-east-1", MetricsTarget.NOOP)
        MetricsConfigCache.metrics_config = config

        # action
        metrics_with_prefix_annotated_method(mock)

        mock.assert_called_once()
        self.assertEqual(2, mock_target.call_count)
        mock_target.assert_has_calls(
            [
                call(metrics_name="test_prefix_time", metrics_config=ANY, value=ANY),
                call(
                    metrics_name="test_prefix_success_count",
                    metrics_config=ANY,
                    value=ANY,
                ),
            ],
            any_order=True,
        )

    def test_metrics_annotation_performance(self):
        mock, mock_target = MagicMock(), MagicMock()
        METRICS_TARGET_TO_EMITTER_DICT[MetricsTarget.NOOP] = mock_target
        config = MetricsConfig("us-east-1", MetricsTarget.NOOP)
        MetricsConfigCache.metrics_config = config

        # action with annotation
        _, actual_latency = timed_invocation(metrics_annotated_method, mock)
        _, second_call_latency = timed_invocation(metrics_annotated_method, mock)

        mock.assert_called()
        self.assertEqual(4, mock_target.call_count)
        self.assertLess(
            second_call_latency,
            actual_latency,
            "Second call to actor must be much faster",
        )

    def test_metrics_with_prefix_annotation_when_error(self):
        mock, mock_target = MagicMock(), MagicMock()
        METRICS_TARGET_TO_EMITTER_DICT[MetricsTarget.NOOP] = mock_target
        config = MetricsConfig("us-east-1", MetricsTarget.NOOP)
        MetricsConfigCache.metrics_config = config

        # action
        self.assertRaises(
            ValueError, lambda: metrics_with_prefix_annotated_method_error(mock)
        )

        mock.assert_not_called()
        self.assertEqual(3, mock_target.call_count)
        mock_target.assert_has_calls(
            [
                call(metrics_name="test_prefix_time", metrics_config=ANY, value=ANY),
                call(
                    metrics_name="test_prefix_failure_count",
                    metrics_config=ANY,
                    value=ANY,
                ),
                call(
                    metrics_name="test_prefix_failure_count.ValueError",
                    metrics_config=ANY,
                    value=ANY,
                ),
            ],
            any_order=True,
        )

    def test_metrics_with_prefix_annotation_without_config(self):
        mock, mock_target = MagicMock(), MagicMock()
        METRICS_TARGET_TO_EMITTER_DICT[MetricsTarget.NOOP] = mock_target
        MetricsConfigCache.metrics_config = None

        # action
        self.assertRaises(
            ValueError, lambda: metrics_with_prefix_annotated_method_error(mock)
        )

        mock.assert_not_called()
        mock_target.assert_not_called()


@latency_metric
def latency_metric_annotated_method(mock_func):
    mock_func("called")


@latency_metric
def latency_metric_annotated_method_error(mock_func):
    raise ValueError()


@latency_metric(name="test")
def latency_metric_with_name_annotated_method(mock_func):
    mock_func("called")


@latency_metric(name="test")
def latency_metric_with_name_annotated_method_error(mock_func):
    raise ValueError()


class TestLatencyMetricAnnotation(unittest.TestCase):
    def test_annotation_sanity(self):
        mock, mock_target = MagicMock(), MagicMock()
        METRICS_TARGET_TO_EMITTER_DICT[MetricsTarget.NOOP] = mock_target
        config = MetricsConfig("us-east-1", MetricsTarget.NOOP)
        MetricsConfigCache.metrics_config = config

        # action
        latency_metric_annotated_method(mock)

        mock.assert_called_once()
        self.assertEqual(1, mock_target.call_count)
        mock_target.assert_has_calls(
            [
                call(
                    metrics_name="latency_metric_annotated_method_time",
                    metrics_config=ANY,
                    value=ANY,
                )
            ],
            any_order=True,
        )

    def test_annotation_when_error(self):
        mock, mock_target = MagicMock(), MagicMock()
        METRICS_TARGET_TO_EMITTER_DICT[MetricsTarget.NOOP] = mock_target
        config = MetricsConfig("us-east-1", MetricsTarget.NOOP)
        MetricsConfigCache.metrics_config = config

        # action
        self.assertRaises(
            ValueError, lambda: latency_metric_annotated_method_error(mock)
        )

        mock.assert_not_called()
        self.assertEqual(1, mock_target.call_count)
        mock_target.assert_has_calls(
            [
                call(
                    metrics_name="latency_metric_annotated_method_error_time",
                    metrics_config=ANY,
                    value=ANY,
                )
            ],
            any_order=True,
        )

    def test_annotation_with_args_sanity(self):
        mock, mock_target = MagicMock(), MagicMock()
        METRICS_TARGET_TO_EMITTER_DICT[MetricsTarget.NOOP] = mock_target
        config = MetricsConfig("us-east-1", MetricsTarget.NOOP)
        MetricsConfigCache.metrics_config = config

        # action
        latency_metric_with_name_annotated_method(mock)

        mock.assert_called_once()
        self.assertEqual(1, mock_target.call_count)
        mock_target.assert_has_calls(
            [call(metrics_name="test", metrics_config=ANY, value=ANY)], any_order=True
        )

    def test_annotation_with_args_when_error(self):
        mock, mock_target = MagicMock(), MagicMock()
        METRICS_TARGET_TO_EMITTER_DICT[MetricsTarget.NOOP] = mock_target
        config = MetricsConfig("us-east-1", MetricsTarget.NOOP)
        MetricsConfigCache.metrics_config = config

        # action
        self.assertRaises(
            ValueError, lambda: latency_metric_with_name_annotated_method_error(mock)
        )

        mock.assert_not_called()
        self.assertEqual(1, mock_target.call_count)
        mock_target.assert_has_calls(
            [call(metrics_name="test", metrics_config=ANY, value=ANY)], any_order=True
        )

    def test_annotation_without_config(self):
        mock, mock_target = MagicMock(), MagicMock()
        METRICS_TARGET_TO_EMITTER_DICT[MetricsTarget.NOOP] = mock_target
        MetricsConfigCache.metrics_config = None

        # action
        self.assertRaises(
            ValueError, lambda: latency_metric_with_name_annotated_method_error(mock)
        )

        mock.assert_not_called()
        mock_target.assert_not_called()


@success_metric
def success_metric_annotated_method(mock_func):
    mock_func("called")


@success_metric
def success_metric_annotated_method_error(mock_func):
    raise ValueError()


@success_metric(name="test")
def success_metric_with_name_annotated_method(mock_func):
    mock_func("called")


@success_metric(name="test")
def success_metric_with_name_annotated_method_error(mock_func):
    raise ValueError()


class TestSuccessMetricAnnotation(unittest.TestCase):
    def test_annotation_sanity(self):
        mock, mock_target = MagicMock(), MagicMock()
        METRICS_TARGET_TO_EMITTER_DICT[MetricsTarget.NOOP] = mock_target
        config = MetricsConfig("us-east-1", MetricsTarget.NOOP)
        MetricsConfigCache.metrics_config = config

        # action
        success_metric_annotated_method(mock)

        mock.assert_called_once()
        self.assertEqual(1, mock_target.call_count)
        mock_target.assert_has_calls(
            [
                call(
                    metrics_name="success_metric_annotated_method_success_count",
                    metrics_config=ANY,
                    value=ANY,
                )
            ],
            any_order=True,
        )

    def test_annotation_when_error(self):
        mock, mock_target = MagicMock(), MagicMock()
        METRICS_TARGET_TO_EMITTER_DICT[MetricsTarget.NOOP] = mock_target
        config = MetricsConfig("us-east-1", MetricsTarget.NOOP)
        MetricsConfigCache.metrics_config = config

        # action
        self.assertRaises(
            ValueError, lambda: success_metric_annotated_method_error(mock)
        )

        mock.assert_not_called()
        mock_target.assert_not_called()

    def test_annotation_with_args_sanity(self):
        mock, mock_target = MagicMock(), MagicMock()
        METRICS_TARGET_TO_EMITTER_DICT[MetricsTarget.NOOP] = mock_target
        config = MetricsConfig("us-east-1", MetricsTarget.NOOP)
        MetricsConfigCache.metrics_config = config

        # action
        success_metric_with_name_annotated_method(mock)

        mock.assert_called_once()
        self.assertEqual(1, mock_target.call_count)
        mock_target.assert_has_calls(
            [call(metrics_name="test", metrics_config=ANY, value=ANY)], any_order=True
        )

    def test_annotation_with_args_when_error(self):
        mock, mock_target = MagicMock(), MagicMock()
        METRICS_TARGET_TO_EMITTER_DICT[MetricsTarget.NOOP] = mock_target
        config = MetricsConfig("us-east-1", MetricsTarget.NOOP)
        MetricsConfigCache.metrics_config = config

        # action
        self.assertRaises(
            ValueError, lambda: success_metric_with_name_annotated_method_error(mock)
        )

        mock.assert_not_called()
        mock_target.assert_not_called()

    def test_annotation_without_config(self):
        mock, mock_target = MagicMock(), MagicMock()
        METRICS_TARGET_TO_EMITTER_DICT[MetricsTarget.NOOP] = mock_target
        MetricsConfigCache.metrics_config = None

        # action
        success_metric_annotated_method(mock)

        mock.assert_called_once()
        mock_target.assert_not_called()


@failure_metric
def failure_metric_annotated_method(mock_func):
    mock_func("called")


@failure_metric
def failure_metric_annotated_method_error(mock_func):
    raise ValueError()


@failure_metric(name="test")
def failure_metric_with_name_annotated_method(mock_func):
    mock_func("called")


@failure_metric(name="test")
def failure_metric_with_name_annotated_method_error(mock_func):
    raise ValueError()


class TestFailureMetricAnnotation(unittest.TestCase):
    def test_annotation_sanity(self):
        mock, mock_target = MagicMock(), MagicMock()
        METRICS_TARGET_TO_EMITTER_DICT[MetricsTarget.NOOP] = mock_target
        config = MetricsConfig("us-east-1", MetricsTarget.NOOP)
        MetricsConfigCache.metrics_config = config

        # action
        failure_metric_annotated_method(mock)

        mock.assert_called_once()
        mock_target.assert_not_called()

    def test_annotation_when_error(self):
        mock, mock_target = MagicMock(), MagicMock()
        METRICS_TARGET_TO_EMITTER_DICT[MetricsTarget.NOOP] = mock_target
        config = MetricsConfig("us-east-1", MetricsTarget.NOOP)
        MetricsConfigCache.metrics_config = config

        # action
        self.assertRaises(
            ValueError, lambda: failure_metric_annotated_method_error(mock)
        )

        mock.assert_not_called()
        self.assertEqual(2, mock_target.call_count)
        mock_target.assert_has_calls(
            [
                call(
                    metrics_name="failure_metric_annotated_method_error_failure_count.ValueError",
                    metrics_config=ANY,
                    value=ANY,
                ),
                call(
                    metrics_name="failure_metric_annotated_method_error_failure_count",
                    metrics_config=ANY,
                    value=ANY,
                ),
            ],
            any_order=True,
        )

    def test_annotation_with_args_sanity(self):
        mock, mock_target = MagicMock(), MagicMock()
        METRICS_TARGET_TO_EMITTER_DICT[MetricsTarget.NOOP] = mock_target
        config = MetricsConfig("us-east-1", MetricsTarget.NOOP)
        MetricsConfigCache.metrics_config = config

        # action
        failure_metric_with_name_annotated_method(mock)

        mock.assert_called_once()
        mock_target.assert_not_called()

    def test_annotation_with_args_when_error(self):
        mock, mock_target = MagicMock(), MagicMock()
        METRICS_TARGET_TO_EMITTER_DICT[MetricsTarget.NOOP] = mock_target
        config = MetricsConfig("us-east-1", MetricsTarget.NOOP)
        MetricsConfigCache.metrics_config = config

        # action
        self.assertRaises(
            ValueError, lambda: failure_metric_with_name_annotated_method_error(mock)
        )

        mock.assert_not_called()
        self.assertEqual(2, mock_target.call_count)
        mock_target.assert_has_calls(
            [
                call(metrics_name="test.ValueError", metrics_config=ANY, value=ANY),
                call(metrics_name="test", metrics_config=ANY, value=ANY),
            ],
            any_order=True,
        )

    def test_annotation_without_config(self):
        mock, mock_target = MagicMock(), MagicMock()
        METRICS_TARGET_TO_EMITTER_DICT[MetricsTarget.NOOP] = mock_target
        MetricsConfigCache.metrics_config = None

        # action
        self.assertRaises(
            ValueError, lambda: failure_metric_with_name_annotated_method_error(mock)
        )

        mock.assert_not_called()
        mock_target.assert_not_called()
