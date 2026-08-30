# forward_tester/config.py
from dataclasses import dataclass, field
from typing import Dict, Any, Tuple, Set

@dataclass
class Strategy6Config:
    """
    Configuration parameters for Model 1: Strategy 6 (Volatility-Adaptive Regime Engine).
    Scaled to 10 Lots (650 Qty Nifty / 200 Qty Sensex) to fit within 20-Lot Combined Portfolio Cap.
    """
    entry_time: tuple = (9, 18, 1)   # 09:18:01 AM IST (sharp OPEN of 09:18 candle matching backtest 100%)
    exit_time: tuple = (15, 0, 0)
    capital_limit: float = 2500000.0 # ₹25 Lakhs allocation
    total_lots: int = 10             # Scaled down to 10 lots
    nifty_lot_size: int = 65
    sensex_lot_size: int = 20
    
    rv5_window: int = 5
    lr_window: int = 5
    
    alloc_lr: Dict[Tuple[str, str], Tuple[int, int, float]] = field(default_factory=lambda: {
        ('Low',    'Falling'): (7, 3, 1.75),
        ('Low',    'Rising'):  (3, 7, 1.75),
        ('Medium', 'Falling'): (7, 3, 2.00),
        ('Medium', 'Rising'):  (5, 5, 2.00),
        ('High',   'Rising'):  (6, 4, 2.00),
        ('High',   'Falling'): (3, 7, 2.00),
    })
    
    morning_active_thresh: float = 0.00099
    morning_amplify_keys: Set[Tuple[str, str]] = field(default_factory=lambda: {
        ('High', 'Rising'), ('Medium', 'Falling'), ('Low', 'Falling')
    })
    morning_defend_keys: Set[Tuple[str, str]] = field(default_factory=lambda: {
        ('High', 'Falling'), ('Low', 'Rising')
    })
    min_price: float = 1.0


@dataclass
class Model0216Config:
    """
    Configuration parameters for Model 2: 0216 Master Derivatives Engine.
    Scaled to 10 Lots (650 Qty Nifty / 200 Qty Sensex) to honor 20-Lot Combined Portfolio Cap.
    """
    entry_start_time: tuple = (10, 5, 0)   # 10:05 AM IST (after 09:15-10:00 morning ΔOI accumulation)
    entry_end_time: tuple = (14, 45, 0)     # 14:45 PM IST (strictly no new entries at or after 14:45)
    exit_time: tuple = (15, 0, 0)           # 15:00 PM IST (EOD hard square-off)
    capital_limit: float = 2500000.0        # ₹25 Lakhs allocation
    lots_nifty: int = 10                    # 10 Lots = 650 Qty
    lots_sensex: int = 20                   # 20 Lots = 200 Qty (10/lot)
    
    # Premium & Friction Filters
    min_opt_premium_nifty: float = 25.0
    min_opt_premium_sensex: float = 50.0
    friction_pts: float = 1.5
    
    # Risk Management & Stop Loss Caps
    sl_cap_nifty: float = 45.0
    sl_cap_sensex: float = 150.0
    fractal_swing_window: int = 5
    fractal_buffer_pts: float = 5.0
    
    # Macro Filters & Trailing Engine
    pcr_bull_threshold: float = 1.30
    pcr_bear_threshold: float = 0.70
    decay_target_pct: float = 0.35          # 35% Option Decay Tier 1 Partial Scaling
    early_be_decay_pct: float = 0.20        # 20% Option Decay Break-Even Lock


@dataclass
class DynamicDTEConfig:
    """Configuration parameters for Model 3: Dynamic DTE Arbitrage Trading Model."""
    entry_start_time: tuple = (9, 25, 0)   # 09:25 AM IST
    entry_end_time: tuple = (14, 55, 0)     # 14:55 PM IST (strictly no entries at or after 15:00)
    exit_time: tuple = (15, 15, 0)          # 15:15 PM IST (EOD square-off)
    capital_limit: float = 5000000.0        # ₹50 Lakhs
    total_lots: int = 20
    
    # Premium & Friction Filters
    min_opt_premium_nifty: float = 25.0
    min_opt_premium_sensex: float = 50.0
    friction_pts: float = 1.5
    
    # Risk Management & Stop Loss Caps
    sl_cap_nifty: float = 60.0
    sl_cap_sensex: float = 180.0
    fractal_swing_window: int = 5
    fractal_buffer_pts: float = 5.0
    
    # Adaptive Volatility Target Parameters
    tp_mult_normal: float = 0.75
    tp_mult_runaway: float = 1.75
    
    # Microstructure Rejection Quality Filters
    rejection_loc_buy: float = 0.30   # close_loc >= 0.30
    rejection_loc_sell: float = 0.70  # close_loc <= 0.70


@dataclass
class UltraTSMOMConfig:
    """Configuration parameters for Model 4: Ultra-TSMOM Production Engine."""
    entry_time: tuple = (9, 18, 1)
    exit_time: tuple = (15, 0, 0)
    capital_limit: float = 2500000.0
    total_lots: int = 10
    non_zero_dte_agg_lots: int = 5
    non_zero_dte_def_lots: int = 5
    non_zero_dte_sl_mult: float = 1.75
    tsmom_window_min: int = 15
    tsmom_z_threshold: float = 3.5
    min_price: float = 1.0


@dataclass
class MultiModelConfig:
    """Master configuration holding all modular strategy configs."""
    strategy6: Strategy6Config = field(default_factory=Strategy6Config)
    model_0216: Model0216Config = field(default_factory=Model0216Config)
    dynamic_dte: DynamicDTEConfig = field(default_factory=DynamicDTEConfig)
    ultra_tsmom: UltraTSMOMConfig = field(default_factory=UltraTSMOMConfig)
    
    index_specs: Dict[str, Any] = field(default_factory=lambda: {
        "NIFTY": {"lot_size": 65, "strike_step": 50},
        "SENSEX": {"lot_size": 20, "strike_step": 100}
    })

# Backward compatibility aliases
DualModelConfig = MultiModelConfig
ShadowConfig = MultiModelConfig
