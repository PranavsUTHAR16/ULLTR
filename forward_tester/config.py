# forward_tester/config.py
from dataclasses import dataclass, field
from typing import Dict, Any, Tuple, Set, List

@dataclass
class Strategy6Config:
    """
    Configuration parameters for Model 1: Strategy 6 (Volatility-Adaptive Regime Engine).
    Restored to Full 20 Lots (1,300 Qty Nifty / 400 Qty Sensex).
    """
    entry_time: tuple = (9, 18, 1)   # 09:18:01 AM IST (sharp OPEN of 09:18 candle matching backtest 100%)
    exit_time: tuple = (15, 0, 0)
    capital_limit: float = 5000000.0 # ₹50 Lakhs allocation
    total_lots: int = 20             # Full 20 lots allocation
    nifty_lot_size: int = 65
    sensex_lot_size: int = 20
    
    rv5_window: int = 5
    lr_window: int = 5
    
    alloc_lr: Dict[Tuple[str, str], Tuple[int, int, float]] = field(default_factory=lambda: {
        ('Low',    'Falling'): (15,  5, 1.75),
        ('Low',    'Rising'):  (5,  15, 1.75),
        ('Medium', 'Falling'): (15,  5, 2.00),
        ('Medium', 'Rising'):  (10, 10, 2.00),
        ('High',   'Rising'):  (13,  7, 2.00),
        ('High',   'Falling'): (5,  15, 2.00),
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
    [DISCONTINUED] Replaced by the new Unified Portfolio.
    """
    enabled: bool = False                   # Discontinued
    entry_start_time: tuple = (10, 5, 0)   # 10:05 AM IST
    entry_end_time: tuple = (14, 45, 0)     # 14:45 PM IST
    exit_time: tuple = (15, 0, 0)           # 15:00 PM IST
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
    """
    Configuration parameters for Model 3: Dynamic DTE Arbitrage Trading Model.
    [DISCONTINUED] Replaced by the new Unified Portfolio.
    """
    enabled: bool = False                   # Discontinued
    entry_start_time: tuple = (9, 25, 0)   # 09:25 AM IST
    entry_end_time: tuple = (14, 55, 0)     # 14:55 PM IST
    exit_time: tuple = (15, 15, 0)          # 15:15 PM IST
    capital_limit: float = 2500000.0        # ₹25 Lakhs
    total_lots: int = 10
    opt_sl_mult: float = 1.50               # 1.5x premium hard stop cap
    
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
    """
    Configuration parameters for Model 4: Ultra-TSMOM Production Engine.
    [DISCONTINUED] Discontinued and disabled per portfolio requirements.
    """
    enabled: bool = False
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
class CASModelConfig:
    """
    Configuration parameters for Model 5: CAS Closing Auction Arbitrage Model.
    [DISCONTINUED] Discontinued and disabled per portfolio requirements.
    """
    enabled: bool = False                   # Discontinued / Disabled


@dataclass
class Model07Config:
    """
    Configuration parameters for Model 07: Pure Nifty OTM Volatility Risk Premium (VRP) Harvesting Engine.
    Executes 5 rolling 60-minute tranches of 100pt OTM short strangles with dynamic breach stop and wing harvesting.
    """
    enabled: bool = True
    lots: int = 1                               # 1 Lot = 65 units base pilot (5 lots = 325 units, 10 lots = 650 units)
    lot_size: int = 65                          # NIFTY lot size
    margin_per_lot: float = 250000.0            # ₹2,50,000 (₹250k) margin anchor per lot
    otm_offset: float = 100.0                   # 100pt OTM strike offset
    friction_pts: float = 1.25                  # 1.25 points per strangle (0.625 pt per leg)
    rate: float = 0.065                         # Risk-free interest rate for BS fallback
    min_option_px: float = 0.20                 # Floor option price
    wing_harvest_mult: float = 0.30             # Opposite leg profit-take threshold (30% of entry)
    tranches: List[Tuple[str, str]] = field(default_factory=lambda: [
        ("09:30", "10:30"),
        ("10:30", "11:30"),
        ("11:30", "12:30"),
        ("12:30", "13:30"),
        ("13:30", "14:30")
    ])
    macro_exclusions: Set[str] = field(default_factory=lambda: {
        "2024-02-01", "2024-06-03", "2024-06-04", "2024-06-05", "2024-07-23", "2024-08-05",
        "2025-02-01", "2025-02-07", "2025-04-09",
        "2026-02-01"
    })


@dataclass
class MultiModelConfig:
    """Master configuration holding all modular strategy configs."""
    strategy6: Strategy6Config = field(default_factory=Strategy6Config)
    model_0216: Model0216Config = field(default_factory=Model0216Config)
    dynamic_dte: DynamicDTEConfig = field(default_factory=DynamicDTEConfig)
    ultra_tsmom: UltraTSMOMConfig = field(default_factory=UltraTSMOMConfig)
    cas: CASModelConfig = field(default_factory=CASModelConfig)
    model_07: Model07Config = field(default_factory=Model07Config)
    
    index_specs: Dict[str, Any] = field(default_factory=lambda: {
        "NIFTY": {"lot_size": 65, "strike_step": 50},
        "SENSEX": {"lot_size": 20, "strike_step": 100}
    })

# Backward compatibility aliases
DualModelConfig = MultiModelConfig
ShadowConfig = MultiModelConfig

