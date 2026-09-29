import os
import pandas as pd

base = '/Users/prana/Desktop/open_source/web/forward_tester'
cur_file = os.path.join(base, 'dual_model_trades.csv')
df_today = pd.read_csv(cur_file)

daily_logs_dir = os.path.join(base, 'daily_logs')
frames = []
if os.path.exists(daily_logs_dir):
    for f in sorted(os.listdir(daily_logs_dir)):
        if f.startswith('trades_') and f.endswith('.csv') and '2026-09-28' not in f:
            frames.append(pd.read_csv(os.path.join(daily_logs_dir, f)))

if frames:
    df_hist = pd.concat(frames, ignore_index=True)
    df_all = pd.concat([df_hist, df_today], ignore_index=True)
    df_all = df_all.drop_duplicates(subset=['date', 'leg_type', 'strike', 'option_type'], keep='last')
    df_all.to_csv(cur_file, index=False)
    print(f"Preserved master dual_model_trades.csv: {len(df_all)} records across {df_all['date'].nunique()} days.")

os.makedirs(daily_logs_dir, exist_ok=True)
df_today.to_csv(os.path.join(daily_logs_dir, 'trades_2026-09-28.csv'), index=False)
print("Saved daily_logs/trades_2026-09-28.csv")
