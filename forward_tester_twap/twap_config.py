# forward_tester_twap/twap_config.py
import os

# Sizing & Capital
LIMIT_MARGIN = 5000000.0  # ₹50 Lakhs (5.0 Million)
BASE_LOTS = 16            # Max base lots allocation for TWAP options trading
LOT_SIZE = 75             # Nifty options lot size

# Strategy Configurations
STD_MULTIPLIER = 2.0      # Multiplier for the standard deviation bands
TIMEFRAME_MINS = 3        # 3-minute bars for resampling Nifty spot
ENTRY_TIME = (9, 20, 0)   # 9:20:00 AM (Start taking entries)
LAST_ENTRY_TIME = (15, 0, 0) # 3:00:00 PM (Stop taking new entries)
EXIT_TIME = (15, 25, 0)   # 3:25:00 PM (Force square-off active positions)

# Risk Parameters
OPTION_SL_MULT = 2.0      # 2.0x premium entry stop loss (e.g. sold at 100, SL at 200)

# Paths
TWAP_DIR = "/Users/prana/Desktop/black_box/twap"
MODEL_PATH = os.path.join(TWAP_DIR, "meta_label/models/saved/gbm/model_Fold_3.txt")

# ClickHouse settings for historical cache
CH_HISTORICAL_DAYS = 60   # Number of days of history to load at morning startup
