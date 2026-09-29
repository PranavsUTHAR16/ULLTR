import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import numpy as np

# Data
dates = ['2026-08-03', '2026-08-04', '2026-08-12', '2026-08-13', '2026-08-14', '2026-08-17']
date_labels = ['Aug 03', 'Aug 04', 'Aug 12', 'Aug 13', 'Aug 14', 'Aug 17']

s6_daily = [12577.50, -6370.00, 18975.00, 45120.00, 10952.50, 2161.25]
ut_daily = [11388.00, 8531.25, -26185.00, 30396.00, 11472.50, -544.38]

s6_cum = np.cumsum(s6_daily)
ut_cum = np.cumsum(ut_daily)
combined_cum = s6_cum + ut_cum

# Set dark stylish theme
plt.style.use('dark_background')

# Chart 1: Model 1 (STRATEGY_6) Equity Curve
fig, ax = plt.subplots(figsize=(10, 5), dpi=300)
ax.plot(date_labels, s6_cum, marker='o', linewidth=2.5, color='#00E676', label='Model 1: Strategy 6 Cum PnL')
ax.bar(date_labels, s6_daily, color=['#00E676' if x >= 0 else '#FF5252' for x in s6_daily], alpha=0.4, width=0.4, label='Daily PnL')

ax.set_title('MODEL 1: STRATEGY 6 — DAY-TO-DAY EQUITY CURVE (FORWARD TEST)', fontsize=14, fontweight='bold', pad=15, color='#FFFFFF')
ax.set_ylabel('Cumulative PnL (₹)', fontsize=12, color='#CCCCCC')
ax.grid(True, linestyle='--', alpha=0.2)
ax.axhline(0, color='#888888', linestyle=':', linewidth=1)

for i, (txt_cum, txt_d) in enumerate(zip(s6_cum, s6_daily)):
    ax.annotate(f"₹{txt_cum:+,.0f}", (date_labels[i], txt_cum), textcoords="offset points", xytext=(0, 10), ha='center', fontsize=9, fontweight='bold', color='#00E676')

ax.legend(loc='upper left', frameon=True, facecolor='#1E1E1E', edgecolor='#333333')
plt.tight_layout()
plt.savefig('/Users/prana/.gemini/antigravity/brain/7a368707-4915-46dc-b01d-51619b43a757/strategy6_equity_curve.png')
plt.close()

# Chart 2: Model 2 (ULTRA_TSMOM) Equity Curve
fig, ax = plt.subplots(figsize=(10, 5), dpi=300)
ax.plot(date_labels, ut_cum, marker='s', linewidth=2.5, color='#29B6F6', label='Model 2: Ultra-TSMOM 2f444a3 Cum PnL')
ax.bar(date_labels, ut_daily, color=['#29B6F6' if x >= 0 else '#FF5252' for x in ut_daily], alpha=0.4, width=0.4, label='Daily PnL')

ax.set_title('MODEL 2: ULTRA-TSMOM (2f444a3) — DAY-TO-DAY EQUITY CURVE (FORWARD TEST)', fontsize=14, fontweight='bold', pad=15, color='#FFFFFF')
ax.set_ylabel('Cumulative PnL (₹)', fontsize=12, color='#CCCCCC')
ax.grid(True, linestyle='--', alpha=0.2)
ax.axhline(0, color='#888888', linestyle=':', linewidth=1)

for i, (txt_cum, txt_d) in enumerate(zip(ut_cum, ut_daily)):
    ax.annotate(f"₹{txt_cum:+,.0f}", (date_labels[i], txt_cum), textcoords="offset points", xytext=(0, 10), ha='center', fontsize=9, fontweight='bold', color='#29B6F6')

ax.legend(loc='upper left', frameon=True, facecolor='#1E1E1E', edgecolor='#333333')
plt.tight_layout()
plt.savefig('/Users/prana/.gemini/antigravity/brain/7a368707-4915-46dc-b01d-51619b43a757/ultratsmom_equity_curve.png')
plt.close()

# Chart 3: Side-by-Side Dual-Model Comparison Chart
fig, ax = plt.subplots(figsize=(11, 6), dpi=300)
ax.plot(date_labels, s6_cum, marker='o', linewidth=2.5, color='#00E676', label='Model 1: Strategy 6 (+₹83,416)')
ax.plot(date_labels, ut_cum, marker='s', linewidth=2.5, color='#29B6F6', label='Model 2: Ultra-TSMOM (+₹35,058)')
ax.plot(date_labels, combined_cum, marker='D', linewidth=3.0, color='#FFD700', linestyle='--', label='Combined Dual Model (+₹1,18,475)')

ax.set_title('DUAL-MODEL FORWARD TESTING — SIDE-BY-SIDE EQUITY COMPARISON', fontsize=14, fontweight='bold', pad=15, color='#FFFFFF')
ax.set_ylabel('Cumulative PnL (₹)', fontsize=12, color='#CCCCCC')
ax.grid(True, linestyle='--', alpha=0.2)
ax.axhline(0, color='#888888', linestyle=':', linewidth=1)

for i, txt in enumerate(combined_cum):
    ax.annotate(f"₹{txt:+,.0f}", (date_labels[i], txt), textcoords="offset points", xytext=(0, 12), ha='center', fontsize=9.5, fontweight='bold', color='#FFD700')

ax.legend(loc='upper left', frameon=True, facecolor='#1E1E1E', edgecolor='#333333')
plt.tight_layout()
plt.savefig('/Users/prana/.gemini/antigravity/brain/7a368707-4915-46dc-b01d-51619b43a757/dual_model_comparison_equity.png')
plt.close()

print("Charts successfully generated.")
