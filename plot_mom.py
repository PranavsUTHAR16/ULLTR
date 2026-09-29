import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# Data
months = [
    '2024-01', '2024-02', '2024-03', '2024-04', '2024-05', '2024-06', '2024-07', '2024-08', '2024-09', '2024-10', '2024-11', '2024-12',
    '2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06', '2025-07', '2025-08', '2025-09', '2025-10', '2025-11', '2025-12',
    '2026-01', '2026-02', '2026-03', '2026-04', '2026-05', '2026-06', '2026-07', '2026-08'
]

monthly_pnl = [
    -56696.25, 261083.75, 275177.50, 294661.25, 214098.75, 326025.00, 121742.50, 156058.75, 114442.50, 116980.00, 161273.50, -44401.25,
    745992.00, 516811.00, 199815.00, 208353.25, 385623.75, 358981.25, 136567.50, 246391.25, 125635.00, 23232.50, 148090.00, 183281.50,
    -46605.00, 300973.75, 69795.00, 131390.00, 54426.25, 116821.25, 270113.75, 137783.75
]

cum_pnl = np.cumsum(monthly_pnl)

plt.style.use('dark_background')

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 9), dpi=300, sharex=True)

# Plot 1: Monthly Net PnL Bars
colors = ['#00E676' if x >= 0 else '#FF5252' for x in monthly_pnl]
bars = ax1.bar(months, [x/1000 for x in monthly_pnl], color=colors, alpha=0.85, width=0.65)
ax1.set_title('STRATEGY 6: MONTH-BY-MONTH NET PnL (in ₹ Thousands)', fontsize=14, fontweight='bold', pad=12, color='#FFFFFF')
ax1.set_ylabel('Monthly PnL (₹k)', fontsize=11, color='#CCCCCC')
ax1.grid(True, linestyle='--', alpha=0.2)
ax1.axhline(0, color='#888888', linestyle=':', linewidth=1)

for bar in bars:
    yval = bar.get_height()
    va = 'bottom' if yval >= 0 else 'top'
    color = '#00E676' if yval >= 0 else '#FF5252'
    ax1.annotate(f"{yval:+.0f}k",
                 (bar.get_x() + bar.get_width() / 2, yval),
                 xytext=(0, 3 if yval >= 0 else -10),
                 textcoords="offset points",
                 ha='center', va=va, fontsize=7.5, fontweight='bold', color=color)

# Plot 2: Cumulative PnL Curve
ax2.plot(months, [x/100000 for x in cum_pnl], marker='o', linewidth=2.5, color='#00E676', label='Cumulative Growth (₹ Lakhs)')
ax2.fill_between(months, 0, [x/100000 for x in cum_pnl], color='#00E676', alpha=0.15)
ax2.set_title('STRATEGY 6: CUMULATIVE PnL GROWTH (in ₹ Lakhs)', fontsize=14, fontweight='bold', pad=12, color='#FFFFFF')
ax2.set_ylabel('Cumulative PnL (₹ Lakhs)', fontsize=11, color='#CCCCCC')
ax2.grid(True, linestyle='--', alpha=0.2)
ax2.axhline(0, color='#888888', linestyle=':', linewidth=1)
plt.xticks(rotation=45, ha='right', fontsize=9)

for i, txt in enumerate(cum_pnl):
    if i % 3 == 0 or i == len(cum_pnl) - 1:
        ax2.annotate(f"₹{txt/100000:.1f}L", (months[i], txt/100000), textcoords="offset points", xytext=(0, 8), ha='center', fontsize=8, fontweight='bold', color='#00E676')

plt.tight_layout()
plt.savefig('/Users/prana/.gemini/antigravity/brain/7a368707-4915-46dc-b01d-51619b43a757/strategy6_mom_performance.png')
plt.close()

print("MoM chart successfully generated.")
