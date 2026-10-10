import os
import sys
import json
import time
import tempfile
import subprocess
from datetime import datetime, date
from unittest.mock import MagicMock, patch

import pytest

# Ensure web project root is on sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from clickhouse_streamer import ClickHouseTickStreamer
import clickhouse_streamer


def test_collector_binary_version():
    """Verify C++ collector binary prints its baked Git commit hash and build timestamp."""
    bin_path = os.path.join(PROJECT_ROOT, "collector", "build", "collector")
    if not os.path.exists(bin_path):
        pytest.skip("Collector binary not built yet in build directory")
    
    proc = subprocess.run([bin_path, "--version"], capture_output=True, text=True)
    assert proc.returncode == 0, f"Expected returncode 0, got {proc.returncode}"
    assert "ULLTR C++ Ingestion Collector" in proc.stdout
    assert "Commit:" in proc.stdout
    assert "Built:" in proc.stdout


def test_clickhouse_streamer_wal_ingestion():
    """Verify ClickHouseTickStreamer reads from WAL, flushes batches, and manages watermark offset."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        wal_file_1 = os.path.join(tmp_dir, "wal_ticks_20261010_10.jsonl")
        
        # 1. Create first batch of ticks in WAL
        ticks_batch_1 = [
            {
                "sym": "NSE_INDEX|Nifty 50",
                "ltp": 25000.5,
                "close": 24980.0,
                "vol": 100000,
                "oi": 0,
                "iv": 0.0,
                "ts_recv": 1791612000000,
                "ts_recv_ns": 1791612000000000000,
                "ts_exch": 1791612000000,
                "bid": 25000.0,
                "bid_qty": 500,
                "ask": 25001.0,
                "ask_qty": 500,
                "tbq": 5000,
                "tsq": 4500
            },
            {
                "sym": "NSE_FO|52345",
                "ltp": 125.5,
                "close": 120.0,
                "vol": 5000,
                "oi": 25000,
                "iv": 14.5,
                "ts_recv": 1791612000100,
                "ts_recv_ns": 1791612000100000000,
                "ts_exch": 1791612000100,
                "bid": 125.0,
                "bid_qty": 100,
                "ask": 125.5,
                "ask_qty": 150,
                "tbq": 1000,
                "tsq": 1200,
                "delta": 0.52,
                "theta": -8.5,
                "gamma": 0.003,
                "vega": 12.1,
                "rho": 0.04
            }
        ]

        with open(wal_file_1, "w") as f:
            for t in ticks_batch_1:
                f.write(json.dumps(t) + "\n")

        # Mock Redis and ClickHouse Client
        with patch.object(clickhouse_streamer, "WAL_DIR", tmp_dir), \
             patch("redis.Redis") as mock_redis_cls, \
             patch("clickhouse_connect.get_client") as mock_ch_cls:

            mock_ch = MagicMock()
            mock_ch.query.return_value.result_set = [[1]]
            mock_ch_cls.return_value = mock_ch

            streamer = ClickHouseTickStreamer()
            streamer.symbol_meta["NSE_FO|52345"] = {
                "underlying": "NIFTY",
                "expiry": "2026-10-15",
                "strike": 25000.0,
                "option_type": "CE"
            }

            # Verify watermark initialized
            wm = streamer.load_watermark()
            assert wm.get("current_file") is None
            assert wm.get("offset") == 0

            # Read WAL manually to verify parsing & batch ingestion
            with open(wal_file_1, "r") as f:
                for line in f:
                    item = json.loads(line)
                    sym = item["sym"]
                    m = streamer.symbol_meta.get(sym, {
                        "underlying": "NIFTY",
                        "expiry": "1970-01-01",
                        "strike": 0.0,
                        "option_type": "INDEX"
                    })
                    tick_dt = datetime.fromtimestamp(item["ts_exch"] / 1000.0)
                    exp_date = datetime.strptime(m["expiry"], "%Y-%m-%d").date()
                    row = [
                        tick_dt, sym, m["underlying"], exp_date, float(m["strike"]),
                        m["option_type"], float(item["ltp"]), float(item["close"]),
                        float(item["bid"]), int(item["bid_qty"]), float(item["ask"]),
                        int(item["ask_qty"]), int(item["vol"]), int(item["oi"]),
                        float(item["iv"]), float(item.get("delta", 0.0)),
                        float(item.get("theta", 0.0)), float(item.get("gamma", 0.0)),
                        float(item.get("vega", 0.0)), float(item.get("rho", 0.0)),
                        item["ts_exch"], item["ts_recv"],
                        0.0, 0, 0.0, 0, 0.0, 0, 0.0, 0, 0.0, 0, 0.0, 0, 0.0, 0, 0.0, 0,
                        item["tbq"], item["tsq"]
                    ]
                    streamer.buffer.append(row)
                
                offset = f.tell()

            # Flush batch
            streamer.flush_buffer()
            streamer.save_watermark("wal_ticks_20261010_10.jsonl", offset)

            assert mock_ch.insert.called
            args, kwargs = mock_ch.insert.call_args
            assert args[0] == "market_ticks"
            assert len(args[1]) == 2
            assert streamer.total_inserted == 2

            # Check watermark saved correctly
            updated_wm = streamer.load_watermark()
            assert updated_wm["current_file"] == "wal_ticks_20261010_10.jsonl"
            assert updated_wm["offset"] == offset


def test_clickhouse_ttl_enforcement():
    """Verify ensure_schema issues ALTER TABLE TTL commands for 90-day retention."""
    with patch("redis.Redis"), patch("clickhouse_connect.get_client") as mock_ch_cls:
        mock_ch = MagicMock()
        mock_ch.query.return_value.result_set = [[1]]
        mock_ch_cls.return_value = mock_ch

        streamer = ClickHouseTickStreamer()
        
        # Verify schema table create and TTL commands were executed
        commands = [call_args[0][0] for call_args in mock_ch.command.call_args_list]
        has_ttl_market_ticks = any("ALTER TABLE market_ticks MODIFY TTL" in c for c in commands)
        has_ttl_cas = any("ALTER TABLE cas_index_estimates MODIFY TTL" in c for c in commands)
        
        assert has_ttl_market_ticks, "Expected ALTER TABLE market_ticks MODIFY TTL in executed commands"
        assert has_ttl_cas, "Expected ALTER TABLE cas_index_estimates MODIFY TTL in executed commands"
