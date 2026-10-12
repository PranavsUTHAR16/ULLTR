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
from forward_tester.models.model_07 import Model07Strategy, Model07Tranche


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


def test_model_07_continuous_pnl_tracking():
    """Verifies that Model 07 computes continuous realized and unrealized PnL."""
    from forward_tester.models.model_07 import Model07Strategy
    from forward_tester.run_model_07 import render_model07_dashboard, publish_model07_state_to_redis
    from forward_tester.engine import MultiModelEngine

    strategy = Model07Strategy(lots=1, friction_pt=1.25)
    strategy.init_trading_day("2026-10-15", expiry_date="2026-10-15")

    # Initial state
    r, u, tot = strategy.get_realized_and_unrealized_pnl()
    assert r == 0.0 and u == 0.0 and tot == 0.0

    # 09:30: Enter Tranche 1
    # Spot = 25000 -> ATM 25000 -> CE 25100 @ 50.0, PE 24900 @ 40.0
    quotes_entry = {
        (25100.0, "CE"): 50.0,
        (24900.0, "PE"): 40.0
    }
    events = strategy.on_minute_bar(
        bar_time_str="09:30",
        open_px=25000.0,
        high_px=25005.0,
        low_px=24995.0,
        close_px=25000.0,
        quotes_lookup=quotes_entry
    )
    assert strategy.active_tranche is not None
    # On entry: realized = 0, unrealized = -1.25 * 65 = -81.25
    r, u, tot = strategy.get_realized_and_unrealized_pnl()
    assert r == 0.0
    assert u == -81.25
    assert tot == -81.25
    assert len(strategy.active_positions) == 2

    # 09:45: Holding and decaying (CE drops to 30.0, PE drops to 25.0)
    # Gross points = (50 - 30) + (40 - 25) = 20 + 15 = 35 pts
    # Net points = 35 - 1.25 = 33.75 pts
    # Unrealized PnL = 33.75 * 65 = 2193.75
    quotes_decay = {
        (25100.0, "CE"): 30.0,
        (24900.0, "PE"): 25.0
    }
    strategy.on_minute_bar(
        bar_time_str="09:45",
        open_px=25010.0,
        high_px=25020.0,
        low_px=25005.0,
        close_px=25015.0,
        quotes_lookup=quotes_decay
    )
    r, u, tot = strategy.get_realized_and_unrealized_pnl()
    assert r == 0.0
    assert u == 2193.75
    assert tot == 2193.75

    # Test MultiModelEngine integration
    from unittest.mock import patch
    from forward_tester.models.strategy_6 import Strategy6Model

    with patch.object(
        Strategy6Model, "_compute_micro_regime", return_value=("LOW", "EXPANDING")
    ):
        engine = MultiModelEngine(dry_run=True)
        engine.model07 = strategy
        eng_r, eng_u, eng_tot = engine.calculate_model_pnl("MODEL_07")
        assert eng_r == 0.0
        assert eng_u == 2193.75
        assert eng_tot == 2193.75

    # Test dashboard render without errors
    render_model07_dashboard(strategy, current_spot=25015.0)

    # Test Redis state publisher with mock
    mock_redis = MagicMock()
    publish_model07_state_to_redis(mock_redis, strategy)
    mock_redis.set.assert_called_once()
    mock_redis.hset.assert_called_once()
    mock_redis.publish.assert_called_once()

    # 10:30: Strikes unchanged (ATM=25000) -> Tranche seamlessly rolls forward into T2
    quotes_roll = {
        (25100.0, "CE"): 20.0,
        (24900.0, "PE"): 15.0
    }
    strategy.on_minute_bar(
        bar_time_str="10:30",
        open_px=25015.0,
        high_px=25025.0,
        low_px=25010.0,
        close_px=25020.0,
        quotes_lookup=quotes_roll
    )
    # Rolled into T2: still active, holding forward without friction
    assert strategy.active_tranche.exit_time == "11:30"
    assert strategy.active_tranche.tranches_covered == [1, 2]
    r, u, tot = strategy.get_realized_and_unrealized_pnl()
    assert r == 0.0
    assert u == 3493.75
    assert tot == 3493.75

    # 11:30: Spot shifts to 25050 -> ATM shifts to 25050 -> Strikes change -> Tranche exits normally
    quotes_exit = {
        (25100.0, "CE"): 15.0,
        (24900.0, "PE"): 10.0
    }
    strategy.on_minute_bar(
        bar_time_str="11:30",
        open_px=25030.0,
        high_px=25060.0,
        low_px=25020.0,
        close_px=25050.0,
        quotes_lookup=quotes_exit
    )
    # Gross = (50 - 15) + (40 - 10) = 35 + 30 = 65 pts
    # Net = 65 - 1.25 = 63.75 pts
    # Realized = 63.75 * 65 = 4143.75
    r, u, tot = strategy.get_realized_and_unrealized_pnl()
    assert r == 4143.75
    assert u == 0.0
    assert tot == 4143.75


def test_def_025_model07_catchup_no_halt():
    """DEF-025: Replaying historical bars without quotes must NOT halt strategy."""
    strategy = Model07Strategy(lots=1, allow_theoretical_fallback=False)
    strategy.init_trading_day("2026-10-12", "2026-10-13")

    # Replay 09:30 and 10:30 bars during catchup (quotes_lookup=None, is_catchup=True)
    events_0930 = strategy.on_minute_bar(
        bar_time_str="09:30",
        open_px=22400.0,
        high_px=22410.0,
        low_px=22390.0,
        close_px=22405.0,
        quotes_lookup=None,
        is_catchup=True
    )
    assert strategy.state == "ACTIVE"
    assert 1 in strategy.executed_tranche_indices
    assert any(e.get("event") == "TRANCHE_SKIPPED_CATCHUP" for e in events_0930)

    events_1030 = strategy.on_minute_bar(
        bar_time_str="10:30",
        open_px=22420.0,
        high_px=22430.0,
        low_px=22415.0,
        close_px=22425.0,
        quotes_lookup=None,
        is_catchup=True
    )
    assert strategy.state == "ACTIVE"
    assert 2 in strategy.executed_tranche_indices
    assert any(e.get("event") == "TRANCHE_SKIPPED_CATCHUP" for e in events_1030)

    # Now at 11:30 live bar arrives with quotes -> must successfully enter Tranche 3!
    quotes_1130 = {
        (22500.0, "CE"): 35.0,
        (22300.0, "PE"): 40.0
    }
    events_1130 = strategy.on_minute_bar(
        bar_time_str="11:30",
        open_px=22420.0,
        high_px=22430.0,
        low_px=22415.0,
        close_px=22425.0,
        quotes_lookup=quotes_1130,
        is_catchup=False
    )
    assert strategy.state == "ACTIVE"
    assert strategy.active_tranche is not None
    assert strategy.active_tranche.tranche_id == 3
    assert any(e.get("event") == "TRANCHE_ENTERED" for e in events_1130)


def test_model07_tranche_from_dict_rehydration():
    """Verify Model07Tranche rehydrates losslessly from dictionary."""
    orig = Model07Tranche(
        tranche_id=3,
        date="2026-10-12",
        entry_time="11:30",
        exit_time="12:30",
        spot_entry=22450.0,
        atm_strike=22450.0,
        ce_strike=22550.0,
        pe_strike=22350.0,
        ce_entry_px=45.0,
        pe_entry_px=50.0,
        total_entry_prem=95.0,
        running_high=22460.0,
        running_low=22440.0,
        tranches_covered=[3]
    )
    d = orig.to_dict()
    rehydrated = Model07Tranche.from_dict(d)
    assert rehydrated.tranche_id == 3
    assert rehydrated.ce_strike == 22550.0
    assert rehydrated.pe_strike == 22350.0
    assert rehydrated.total_entry_prem == 95.0


