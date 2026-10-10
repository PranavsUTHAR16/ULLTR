"""Regression and verification tests for the zero-slippage execution pipeline.

Verifies:
1. HighPrecisionClock timer accuracy.
2. Strategy 6 Ahead-Of-Time (AOT) pre-computation and pre-staging hooks.
3. Upstox V3 HFT broker gateway initialization and microsecond latency telemetry.
4. Model 07 pipelined option quote retrieval.
"""

import os
import sys
import time
import pytest
from unittest.mock import MagicMock

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from forward_tester.streaming_engine import HighPrecisionClock, RedisEventPushStream
from forward_tester.broker_gateway import UpstoxBrokerGateway
from forward_tester.config import MultiModelConfig
from forward_tester.models.strategy_6 import Strategy6Model


def test_high_precision_clock_drift():
    """Verifies HighPrecisionClock accuracy over short intervals."""
    now = time.time()
    # Trigger target 200ms ahead
    target_dt = time.localtime(now + 0.2)
    drift_us = HighPrecisionClock.wait_until_sharp_time(
        target_dt.tm_hour, target_dt.tm_min, target_dt.tm_sec
    )
    # Timer drift must be sub-millisecond (< 1000 µs)
    assert abs(drift_us) < 1000.0, f"Timer drift {drift_us} µs exceeds 1000 µs threshold"


def test_strategy_6_aot_precomputation_hooks():
    """Verifies that Strategy 6 caches precomputed values to eliminate trigger lag."""
    mock_client = MagicMock()
    config = MultiModelConfig()

    s6 = Strategy6Model(data_client=mock_client, config=config)

    # Initial state
    assert s6.cached_regime is None
    assert s6.cached_morning_jump is None
    assert s6.cached_df_opts is None

    # Inject mock regime and jump
    s6.cached_regime = ("High", "Rising")
    s6.cached_morning_jump = (True, 0.0015)
    s6.cached_df_opts_ts = time.time()
    s6.underlying = "NIFTY"
    s6.expiry = "2026-10-15"

    mock_client.get_spot_price.return_value = 25000.0

    # Mock select_closer_expiry should NOT be called if cached_regime is present
    s6._select_closer_expiry = MagicMock()
    s6._compute_micro_regime = MagicMock()
    s6._detect_morning_jump = MagicMock()

    # Pre-warm dummy options dataframe
    import pandas as pd
    dummy_df = pd.DataFrame([{
        "symbol": "NSE_FO|12345",
        "strike": 25100.0,
        "option_type": "CE",
        "entry_price": 50.0,
        "delta": 0.25
    }, {
        "symbol": "NSE_FO|12346",
        "strike": 24900.0,
        "option_type": "PE",
        "entry_price": 45.0,
        "delta": 0.25
    }])
    s6.cached_df_opts = dummy_df

    # Execute entry
    positions = s6.execute_0918_entry()

    # Verify that heavy functions were bypassed thanks to cache
    s6._select_closer_expiry.assert_not_called()
    s6._compute_micro_regime.assert_not_called()
    s6._detect_morning_jump.assert_not_called()
    assert s6.regime == ("High", "Rising")
    assert s6.morning_active is True
    assert s6.entry_executed is True


def test_upstox_v3_gateway_structure():
    """Verifies UpstoxBrokerGateway initializes with V3 HFT and HTTP/2 client."""
    gw = UpstoxBrokerGateway()
    assert gw.hft_v3_enabled is True
    assert hasattr(gw, "httpx_client")
    assert hasattr(gw, "place_order_v3")


def test_pipelined_quote_retrieval_logic():
    """Verifies pipelined quote mapping logic."""
    mock_redis = MagicMock()
    stream = RedisEventPushStream()
    stream.r = mock_redis

    # Mock pipeline return
    mock_pipe = MagicMock()
    mock_redis.pipeline.return_value = mock_pipe
    # Return [ltp, last_price, bid, ask] for 2 symbols
    mock_pipe.execute.return_value = [
        ["120.5", "120.5", "120.0", "121.0"],
        ["0", "0", "45.0", "46.0"]  # Mid-price should resolve to 45.5
    ]

    symbols_map = {
        (25100.0, "CE"): "NSE_FO|101",
        (24900.0, "PE"): "NSE_FO|102",
    }

    quotes = stream.fetch_option_chain_quotes_pipelined(symbols_map)
    assert quotes[(25100.0, "CE")] == 120.5
    assert quotes[(24900.0, "PE")] == 45.5
