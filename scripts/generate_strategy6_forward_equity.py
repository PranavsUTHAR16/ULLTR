import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# All Live Forward Testing Trading Days for Strategy 6 (August 2026 MTD)
data = [
    {"date": "2026-08-03", "pnl": 12577.50, "regime": "Med-Fall", "status": "WIN"},
    {"date": "2026-08-04", "pnl": -6370.00, "regime": "Low-Rise",  "status": "LOSS"},
    {"date": "2026-08-12", "pnl": 18975.00, "regime": "High-Rise", "status": "WIN"},
    {"date": "2026-08-13", "pnl": 45120.00, "regime": "Med-Fall", "status": "WIN"},
    {"date": "2026-08-14", "pnl": 10952.50, "regime": "Low-Fall",  "status": "WIN"},
    {"date": "2026-08-17", "pnl": 2161.25,  "regime": "Med-Rise", "status": "WIN"},
    {"date": "2026-08-18", "pnl": 7412.50,  "regime": "Low-Fall",  "status": "WIN"},
    {"date": "2026-08-19", "pnl": 14560.00, "regime": "Med-Rise", "status": "WIN"},
    {"date": "2026-08-20", "pnl": 12480.00, "regime": "High-Rise", "status": "WIN"},
    {"date": "2026-08-21", "pnl": -4980.00, "regime": "High-Fall", "status": "LOSS"},
    {"date": "2026-08-24", "pnl": -6613.75, "regime": "Med-Fall", "status": "LOSS"},
]

df = pd.DataFrame(data)
df["cum_pnl"] = df["pnl"].cumsum()

# Performance Metrics
total_sessions = len(df)
wins = (df["pnl"] > 0).sum()
losses = (df["pnl"] < 0).sum()
win_rate = (wins / total_sessions) * 100.0
total_pnl = df["pnl"].sum()
gross_profit = df[df["pnl"] > 0]["pnl"].sum()
gross_loss = abs(df[df["pnl"] < 0]["pnl"].sum())
profit_factor = gross_profit / gross_loss if gross_loss > 0 else np.nan
peak = df["cum_pnl"].cummax()
drawdown = df["cum_pnl"] - peak
max_dd = drawdown.min()
daily_sharpe = (df["pnl"].mean() / df["pnl"].std()) * np.sqrt(252) if df["pnl"].std() > 0 else 0.0

print(f"Total Forward Test Sessions : {total_sessions}")
print(f"Win Rate                     : {win_rate:.1f}% ({wins}W / {losses}L)")
print(f"Cumulative Forward Test PnL  : ₹{total_pnl:+,.2f} ({total_pnl/1e5:.2f} Lakhs)")
print(f"Profit Factor                : {profit_factor:.2f}")
print(f"Annualized Sharpe Ratio      : {daily_sharpe:.2f}")
print(f"Max Drawdown                 : ₹{max_dd:,.2f}")
print(f"Avg Daily PnL                : ₹{df['pnl'].mean():+,.2f} / session")

# Generate Chart
plt.style.use('dark_background')
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(11, 8), dpi=300, gridspec_kw={'height_ratios': [2.2, 1]})

date_labels = [d.replace("2026-", "") for d in df["date"]]

# Top: Cumulative Equity Curve
ax1.plot(date_labels, df["cum_pnl"] / 1e3, marker='o', linewidth=2.8, color='#00E676', label=f'Strategy 6 Cumulative Equity (+₹{total_pnl/1e3:,.1f}k)')
ax1.fill_between(date_labels, 0, df["cum_pnl"] / 1e3, color='#00E676', alpha=0.15)
ax1.set_title('MODEL 1: STRATEGY 6 — LIVE FORWARD TEST EQUITY CURVE (AUGUST 2026 MTD)', fontsize=13, fontweight='bold', pad=15, color='#FFFFFF')
ax1.set_ylabel('Cumulative PnL (₹ in Thousands)', fontsize=11, color='#CCCCCC')
ax1.grid(True, linestyle='--', alpha=0.25)
ax1.axhline(0, color='#888888', linestyle=':', linewidth=1)

for i, txt in enumerate(df["cum_pnl"]):
    ax1.annotate(f"₹{txt/1e3:+,.1f}k", (date_labels[i], txt/1e3), textcoords="offset points", xytext=(0, 10), ha='center', fontsize=8.5, fontweight='bold', color='#00E676' if txt >= 0 else '#FF5252')

ax1.legend(loc='upper left', frameon=True, facecolor='#1E1E1E', edgecolor='#333333')

# Bottom: Daily PnL Bars
colors = ['#00E676' if x >= 0 else '#FF5252' for x in df["pnl"]]
ax2.bar(date_labels, df["pnl"] / 1e3, color=colors, alpha=0.85, width=0.55)
ax2.set_ylabel('Daily PnL (₹k)', fontsize=11, color='#CCCCCC')
ax2.set_xlabel('Trading Session', fontsize=11, color='#CCCCCC')
ax2.grid(True, linestyle='--', alpha=0.25)
ax2.axhline(0, color='#888888', linestyle='-', linewidth=0.8)

for i, txt in enumerate(df["pnl"]):
    ax2.annotate(f"₹{txt/1e3:+,.1f}k", (date_labels[i], txt/1e3), textcoords="offset points", xytext=(0, 5 if txt >= 0 else -12), ha='center', fontsize=8, fontweight='bold', color=colors[i])

plt.tight_layout()
save_path = '/Users/prana/.gemini/antigravity/brain/7a368707-4915-46dc-b01d-51619b43a757/strategy6_forward_test_equity.png'
plt.savefig(save_path)
plt.close()
print(f"Saved equity curve chart to {save_path}")
