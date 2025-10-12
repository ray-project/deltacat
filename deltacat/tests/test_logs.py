import unittest
import json
import ray
from unittest import mock
from logging import LogRecord
from deltacat.logs import JsonFormatter


class TestJsonFormatter(unittest.TestCase):
    def test_usesTime_sanity(self):

        formatter = JsonFormatter()

        self.assertFalse(formatter.usesTime())

    def test_usesTime_success_case(self):

        formatter = JsonFormatter(fmt_dict={"asctime": "asctime"})

        self.assertTrue(formatter.usesTime())

    def test_formatMessage_sanity(self):

        formatter = JsonFormatter({"message": "msg"})

        record = LogRecord(
            level="INFO",
            name="test",
            pathname="test",
            lineno=0,
            message="test_message",
            msg="test_message",
            args=None,
            exc_info=None,
        )

        result = formatter.formatMessage(record)

        self.assertEqual({"message": "test_message"}, result)

    def test_format_sanity(self):
        ray.shutdown()
        formatter = JsonFormatter({"message": "msg"})

        record = LogRecord(
            level="INFO",
            name="test",
            pathname="test",
            lineno=0,
            message="test_message",
            msg="test_message",
            args=None,
            exc_info=None,
        )

        result = formatter.format(record)

        self.assertEqual({"message": "test_message"}, json.loads(result))
        self.assertFalse(ray.is_initialized())
        self.assertNotIn("ray_runtime_context", json.loads(result))

    def test_format_when_ray_initialized(self):
        ray.init(local_mode=True, ignore_reinit_error=True)

        formatter = JsonFormatter({"message": "msg"})

        record = LogRecord(
            level="INFO",
            name="test",
            pathname="test",
            lineno=0,
            message="test_message",
            msg="test_message",
            args=None,
            exc_info=None,
        )

        result = formatter.format(record)
        result = json.loads(result)

        self.assertEqual("test_message", result["message"])
        self.assertTrue(ray.is_initialized())
        self.assertIn("ray_runtime_context", result)
        self.assertIn("job_id", result["ray_runtime_context"])
        self.assertIn("node_id", result["ray_runtime_context"])
        self.assertIn("worker_id", result["ray_runtime_context"])
        self.assertNotIn(
            "task_id",
            result["ray_runtime_context"],
            "We expect task ID not be present outside a remote task",
        )
        ray.shutdown()

    def test_format_when_ray_initialized_in_task(self):
        # worker mode is only true when local_mode is False
        ray.init(local_mode=False, ignore_reinit_error=True)

        @ray.remote
        def ray_remote_task():
            formatter = JsonFormatter({"message": "msg"})

            record = LogRecord(
                level="INFO",
                name="test",
                pathname="test",
                lineno=0,
                message="test_message",
                msg="test_message",
                args=None,
                exc_info=None,
            )

            result = formatter.format(record)
            result = json.loads(result)
            return result

        result = ray.get(ray_remote_task.remote())
        self.assertEqual("test_message", result["message"])
        self.assertTrue(ray.is_initialized())
        self.assertIn("ray_runtime_context", result)
        self.assertIn("job_id", result["ray_runtime_context"])
        self.assertIn("node_id", result["ray_runtime_context"])
        self.assertIn("worker_id", result["ray_runtime_context"])
        self.assertIn(
            "task_id",
            result["ray_runtime_context"],
            "We expect task ID to be present inside a remote task",
        )
        ray.shutdown()

    def test_format_when_ray_initialized_with_context_kwargs(self):
        ray.init(local_mode=True, ignore_reinit_error=True)

        formatter = JsonFormatter(
            {"message": "msg"}, context_kwargs={"custom_key": "custom_val"}
        )

        record = LogRecord(
            level="INFO",
            name="test",
            pathname="test",
            lineno=0,
            message="test_message",
            msg="test_message",
            args=None,
            exc_info=None,
        )

        result = formatter.format(record)
        result = json.loads(result)

        self.assertEqual("test_message", result["message"])
        self.assertTrue(ray.is_initialized())
        self.assertIn("ray_runtime_context", result)
        self.assertIn("job_id", result["ray_runtime_context"])
        self.assertIn("node_id", result["ray_runtime_context"])
        self.assertIn("worker_id", result["ray_runtime_context"])
        self.assertNotIn(
            "task_id",
            result["ray_runtime_context"],
            "We expect task ID not be present outside a remote task",
        )
        self.assertEqual("custom_val", result["additional_context"]["custom_key"])
        ray.shutdown()

    def test_format_with_context_kwargs(self):
        ray.shutdown()
        formatter = JsonFormatter(
            {"message": "msg"}, context_kwargs={"custom_key": "custom_val"}
        )

        record = LogRecord(
            level="INFO",
            name="test",
            pathname="test",
            lineno=0,
            message="test_message",
            msg="test_message",
            args=None,
            exc_info=None,
        )

        result = formatter.format(record)

        self.assertEqual(
            {
                "message": "test_message",
                "additional_context": {"custom_key": "custom_val"},
            },
            json.loads(result),
        )
        self.assertFalse(ray.is_initialized())
        self.assertNotIn("ray_runtime_context", json.loads(result))

    @mock.patch("deltacat.logs.DELTACAT_LOGGER_CONTEXT", '{"DATABASE_URL": "mytemp"}')
    def test_format_with_env_context_kwargs(self):
        ray.shutdown()
        formatter = JsonFormatter(
            {"message": "msg"}, context_kwargs={"custom_key": "custom_val"}
        )

        record = LogRecord(
            level="INFO",
            name="test",
            pathname="test",
            lineno=0,
            message="test_message",
            msg="test_message",
            args=None,
            exc_info=None,
        )

        result = formatter.format(record)

        self.assertEqual(
            {
                "message": "test_message",
                "additional_context": {
                    "custom_key": "custom_val",
                    "DATABASE_URL": "mytemp",
                },
            },
            json.loads(result),
        )
        self.assertFalse(ray.is_initialized())
        self.assertNotIn("ray_runtime_context", json.loads(result))


class TestCompressingRotatingFileHandler(unittest.TestCase):
    """Tests for the custom rolling, compressing log handler."""

    def setUp(self):
        import tempfile
        import pathlib

        self.tmpdir = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self):
        import shutil

        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _logger_with_handler(self, **kwargs):
        import logging
        from deltacat.logs import CompressingRotatingFileHandler

        handler = CompressingRotatingFileHandler(self.tmpdir / "test.log", **kwargs)
        logger = logging.getLogger(f"test_{id(handler)}")
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
        return logger, handler

    def test_rollover_and_compression(self):
        import gzip

        logger, handler = self._logger_with_handler(maxBytes=200, backupCount=1)

        for _ in range(50):
            logger.info("a" * 50)
        handler.close()

        gz_files = list(self.tmpdir.glob("*.gz"))
        self.assertTrue(gz_files, "Expected at least one compressed file")

        for path in gz_files:
            with gzip.open(path, "rt") as f:
                text = f.read()
            self.assertIn("a", text)

    def test_backup_count_limit(self):
        logger, handler = self._logger_with_handler(maxBytes=200, backupCount=2)
        for _ in range(200):
            logger.info("b" * 50)
        handler.close()

        gz_files = list(self.tmpdir.glob("*.gz"))
        self.assertLessEqual(len(gz_files), 2)

    def test_configured_logger_uses_custom_handler(self):
        import logging
        from deltacat import logs

        logger = logging.getLogger(f"configured_test_{id(self)}")
        adapter = logs.configure_deltacat_logger(logger)
        handler_types = {type(h).__name__ for h in adapter.logger.handlers}
        self.assertIn("CompressingRotatingFileHandler", handler_types)
