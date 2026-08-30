import unittest
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
import numpy as np
import pandas as pd
from datetime import datetime, date

from forward_tester.config import MultiModelConfig, Model0216Config, Strategy6Config, DynamicDTEConfig, UltraTSMOMConfig
from forward_tester.position import ForwardTestPosition
from forward_tester.engine import MultiModelEngine

class TestTriModelEngine(unittest.TestCase):
    def test_config_initialization(self):
        """Test MultiModelConfig initializes all 4 model subconfigs properly."""
        cfg = MultiModelConfig()
        self.assertIsInstance(cfg.strategy6, Strategy6Config)
        self.assertIsInstance(cfg.model_0216, Model0216Config)
        self.assertIsInstance(cfg.dynamic_dte, DynamicDTEConfig)
        self.assertIsInstance(cfg.ultra_tsmom, UltraTSMOMConfig)
        self.assertEqual(cfg.strategy6.total_lots, 10)
        self.assertEqual(cfg.model_0216.lots_nifty, 10)
        self.assertEqual(cfg.model_0216.lots_sensex, 20)
        self.assertEqual(cfg.dynamic_dte.total_lots, 20)
        self.assertEqual(cfg.dynamic_dte.sl_cap_nifty, 60.0)

    def test_position_spot_sl_tp_evaluation(self):
        """Test ForwardTestPosition spot-based stop loss and profit target logic."""
        # 1. Bullish Short PE position
        pos_bull = ForwardTestPosition(
            model_id="0216_MODEL",
            underlying="NIFTY",
            expiry_date="2026-08-28",
            symbol="NSE_FO|61684",
            strike=24150.0,
            option_type="PE",
            leg_type="DIRECTIONAL_TREND",
            target_delta=0.50,
            entry_price=66.35,
            current_price=66.35,
            lots=10,
            lot_size=65,
            sl_mult=0.0,
            sl_price=24303.3,
            direction="BUY",
            spot_entry_price=24327.3,
            spot_sl_price=24303.3,
            spot_tp_price=24450.0
        )
        
        # Test no breach
        triggered, reason = pos_bull.update_spot_and_option_price(spot_high=24340, spot_low=24310, spot_close=24325, opt_price=60.0)
        self.assertFalse(triggered)
        self.assertEqual(pos_bull.status, "OPEN")
        self.assertAlmostEqual(pos_bull.pnl, (66.35 - 60.0) * 650, places=2)

        # Test Stop Loss breach (low <= 24303.3)
        triggered_sl, reason_sl = pos_bull.update_spot_and_option_price(spot_high=24320, spot_low=24300, spot_close=24302, opt_price=75.0)
        self.assertTrue(triggered_sl)
        self.assertEqual(reason_sl, "SL_HIT")
        self.assertEqual(pos_bull.status, "SL_HIT")
        self.assertAlmostEqual(pos_bull.pnl, (66.35 - 75.0) * 650, places=2)

    def test_multi_model_pnl_aggregation(self):
        """Test engine PnL calculation across Model 1, Model 2, and Model 3."""
        engine = MultiModelEngine()
        engine.strategy6.active_positions.clear()
        engine.strategy6.closed_positions.clear()
        engine.model_0216.active_positions.clear()
        engine.model_0216.closed_positions.clear()
        engine.dynamic_dte.active_positions.clear()
        engine.dynamic_dte.closed_positions.clear()

        # Add mock positions
        p1 = ForwardTestPosition("STRATEGY_6", "NIFTY", "2026-08-28", "S1", 24400, "CE", "PRIMARY", 0.25, 30.0, 20.0, 7, 65, 2.0, 60.0)
        p1.pnl = (30.0 - 20.0) * (7 * 65)  # +4,550
        
        p2 = ForwardTestPosition("0216_MODEL", "NIFTY", "2026-08-28", "S2", 24150, "PE", "DIRECTIONAL_TREND", 0.50, 66.35, 50.0, 10, 65, 0.0, 24303.3)
        p2.pnl = (66.35 - 50.0) * (10 * 65)  # +10,627.5

        p3 = ForwardTestPosition("DYNAMIC_DTE", "NIFTY", "2026-08-28", "S3", 24300, "PE", "DYNAMIC_DTE", 0.50, 45.0, 25.0, 20, 65, 0.0, 24240.0)
        p3.pnl = (45.0 - 25.0) * (20 * 65)  # +26,000

        engine.strategy6.active_positions.append(p1)
        engine.model_0216.active_positions.append(p2)
        engine.dynamic_dte.active_positions.append(p3)

        _, _, pnl_s6 = engine.calculate_model_pnl("STRATEGY_6")
        _, _, pnl_0216 = engine.calculate_model_pnl("0216_MODEL")
        _, _, pnl_dd = engine.calculate_model_pnl("DYNAMIC_DTE")

        self.assertAlmostEqual(pnl_s6, 4550.0, places=2)
        self.assertAlmostEqual(pnl_0216, 10627.5, places=2)
        self.assertAlmostEqual(pnl_dd, 26000.0, places=2)
        self.assertAlmostEqual(pnl_s6 + pnl_0216 + pnl_dd, 41177.5, places=2)

if __name__ == "__main__":
    unittest.main()
