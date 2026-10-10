"""
tests/test_raw_logger.py
Unit tests for RawInputLogger and RawInputReplayer.
Verifies Parquet schema, batching, disk writes, and replay consistency.
"""

import os
import sys
import shutil
import tempfile
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from forward_tester.raw_logger import RawInputLogger, RawInputReplayer, SCHEMA


def test_raw_logger_write_and_replay():
    test_dir = tempfile.mkdtemp()
    try:
        logger = RawInputLogger(
            log_dir=test_dir,
            session_date="2026-10-10",
            batch_size=2,
            auto_flush_seconds=10.0
        )

        msg1 = logger.log(
            redis_key="md:candle:NSE_INDEX|Nifty 50:1m:1728550500",
            event_type="BAR",
            payload={"open": 25000.0, "high": 25020.0, "low": 24990.0, "close": 25010.0}
        )

        msg2 = logger.log(
            redis_key="chain:NIFTY:2026-10-15",
            event_type="CHAIN",
            payload={"25000:CE": "NSE_FO|68401", "25000:PE": "NSE_FO|68402"}
        )

        # Buffer size is 2, so it should flush automatically
        assert msg1 == 1
        assert msg2 == 2

        msg3 = logger.log(
            redis_key="md:quote:NSE_FO|68401",
            event_type="QUOTE",
            payload={"ltp": "125.50", "bid": "125.00", "ask": "126.00"}
        )
        assert msg3 == 3

        logger.close()

        # Verify file exists
        expected_file = os.path.join(test_dir, "raw_inputs_2026-10-10.parquet")
        assert os.path.exists(expected_file)

        # Verify replay
        events = RawInputReplayer.load_events(expected_file)
        assert len(events) == 3
        assert events[0]["message_id"] == 1
        assert events[0]["event_type"] == "BAR"
        assert events[1]["message_id"] == 2
        assert events[1]["event_type"] == "CHAIN"
        assert events[2]["message_id"] == 3
        assert events[2]["event_type"] == "QUOTE"
        assert "125.50" in events[2]["payload_json"]

        # Verify generator replay
        iter_events = list(RawInputReplayer.iter_events(expected_file))
        assert len(iter_events) == 3
        assert iter_events[1]["redis_key"] == "chain:NIFTY:2026-10-15"

    finally:
        shutil.rmtree(test_dir, ignore_errors=True)
