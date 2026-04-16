"""
test_trigger_handler.py  —  tests/unit/
Unit tests for Lambda trigger handler.
Mocks AWS clients with moto and unittest.mock.
"""

import json
from unittest.mock import MagicMock, patch

import pytest


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def s3_event():
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "etl-raw-dev-123456789"},
                    "object": {"key": "orders/2024/01/15/orders_20240115.json", "size": 5120},
                }
            }
        ]
    }


@pytest.fixture
def s3_event_unknown_source():
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "etl-raw-dev-123456789"},
                    "object": {"key": "unknown/2024/01/15/file.json", "size": 1024},
                }
            }
        ]
    }


@pytest.fixture
def s3_event_empty_file():
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "etl-raw-dev-123456789"},
                    "object": {"key": "orders/2024/01/15/orders.json", "size": 0},
                }
            }
        ]
    }


# ── Tests ──────────────────────────────────────────────────────────────────────

class TestClassifySource:
    def test_known_source(self):
        from trigger.handler import classify_source
        assert classify_source("orders/2024/01/15/file.json") == "orders"
        assert classify_source("products/snapshot.parquet") == "products"
        assert classify_source("customers/2024/file.csv") == "customers"

    def test_unknown_source(self):
        from trigger.handler import classify_source
        assert classify_source("unknown/2024/file.json") is None

    def test_empty_key(self):
        from trigger.handler import classify_source
        assert classify_source("") is None


class TestValidateFile:
    @patch("trigger.handler.s3")
    def test_valid_file(self, mock_s3):
        mock_s3.head_object.return_value = {}
        from trigger.handler import validate_file
        ok, msg = validate_file("bucket", "orders/file.json", 1024)
        assert ok is True
        assert msg == "OK"

    def test_empty_file_rejected(self):
        from trigger.handler import validate_file
        ok, msg = validate_file("bucket", "orders/file.json", 0)
        assert ok is False
        assert "empty" in msg.lower()

    def test_unsupported_format(self):
        from trigger.handler import validate_file
        ok, msg = validate_file("bucket", "orders/file.xml", 1024)
        assert ok is False
        assert "unsupported" in msg.lower()


class TestLambdaHandler:
    @patch("trigger.handler.trigger_glue_job", return_value="jr_abc123")
    @patch("trigger.handler.s3")
    @patch("trigger.handler.glue")
    @patch("trigger.handler.sns")
    def test_successful_trigger(self, mock_sns, mock_glue, mock_s3, mock_trigger, s3_event):
        mock_s3.head_object.return_value = {}
        from trigger.handler import lambda_handler
        response = lambda_handler(s3_event, {})
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body[0]["status"] == "glue_triggered"
        assert body[0]["run_id"] == "jr_abc123"

    @patch("trigger.handler.s3")
    @patch("trigger.handler.sns")
    def test_empty_file_sends_alert(self, mock_sns, mock_s3, s3_event_empty_file):
        from trigger.handler import lambda_handler
        response = lambda_handler(s3_event_empty_file, {})
        mock_sns.publish.assert_called_once()
        body = json.loads(response["body"])
        assert body[0]["status"] == "validation_failed"

    @patch("trigger.handler.s3")
    @patch("trigger.handler.sns")
    def test_unknown_source_skipped(self, mock_sns, mock_s3, s3_event_unknown_source):
        mock_s3.head_object.return_value = {}
        from trigger.handler import lambda_handler
        response = lambda_handler(s3_event_unknown_source, {})
        body = json.loads(response["body"])
        assert body[0]["status"] == "unknown_source"
        mock_sns.publish.assert_not_called()
