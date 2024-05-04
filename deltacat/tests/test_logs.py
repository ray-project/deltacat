import unittest
import json
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

        print({"message": "test_message"}, json.loads(result))
