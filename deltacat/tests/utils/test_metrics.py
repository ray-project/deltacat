from unittest import TestCase
from deltacat.utils.metrics import (
    MetricsConfig,
    MetricsConfigSingleton,
    MetricsTarget,
)


class TestS3PartialParquetFileToTable(TestCase):
    def test_singleton_behavior(self):
        # Disallow direct instantiation
        with self.assertRaises(Exception):
            MetricsConfigSingleton()

        # Raise exception is instance() called for the first time without metrics_config
        with self.assertRaises(Exception):
            MetricsConfigSingleton.instance()

        metrics_config = MetricsConfig("test-region", MetricsTarget.CLOUDWATCH)
        instance = MetricsConfigSingleton.instance(metrics_config)

        # instance should return the same instance every time
        self.assertEqual(id(instance), id(MetricsConfigSingleton.instance()))

    def test_singleton_ignore_subsequent_initializations(self):
        region_orig = "test-region"
        metrics_target_orig = MetricsTarget.CLOUDWATCH
        MetricsConfigSingleton.instance(MetricsConfig(region_orig, metrics_target_orig))

        # "Second" initialization should be ignored
        metrics_config_test = MetricsConfig(
            "different-region", MetricsTarget.CLOUDWATCH_EMF
        )
        instance_test = MetricsConfigSingleton.instance(metrics_config_test)

        # Assert values are unchanged
        self.assertEqual(instance_test.metrics_config.region, region_orig)
        self.assertEqual(
            instance_test.metrics_config.metrics_target, metrics_target_orig
        )

    def test_singleton_immutability(self):
        metrics_config = MetricsConfig("test-region", MetricsTarget.CLOUDWATCH)
        instance = MetricsConfigSingleton.instance(metrics_config)
        with self.assertRaises(Exception):
            instance.metrics_config = MetricsConfig(
                "us-east-1", MetricsTarget.CLOUDWATCH_EMF
            )

    def test_metrics_config_immutability(self):
        metrics_config = MetricsConfig(
            "us-east-1", MetricsTarget.CLOUDWATCH_EMF, metrics_dimensions=[{}]
        )
        with self.assertRaises(Exception):
            metrics_config.region = "test-region"
        with self.assertRaises(Exception):
            metrics_config.metrics_target = MetricsTarget.CLOUDWATCH
        with self.assertRaises(Exception):
            metrics_config.metrics_namespace = "test_namespace"
        with self.assertRaises(Exception):
            metrics_config.metrics_dimensions = []
        with self.assertRaises(Exception):
            metrics_config.metrics_dimensions[0] = {"Name": "Value"}
        with self.assertRaises(Exception):
            metrics_config.metrics_kwargs["test"] = "test"
