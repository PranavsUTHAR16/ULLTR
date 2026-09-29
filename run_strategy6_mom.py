import sys, time, re
sys.path.insert(0, '/Users/prana/Desktop/black_box/shadow/production')
sys.path.insert(0, '/Users/prana/Desktop/black_box/shadow')

import pandas as pd
import numpy as np
from ch_loader import get_client
from vol_adaptive_backtest import (
    load_all_data, compute_morning_signals, compute_micro_regimes, simulate_strangle,
    NIFTY_LOT_SIZE, SENSEX_LOT_SIZE, ALLOC_LR, MORNING_AMPLIFY_KEYS, MORNING_DEFEND_KEYS
)

def run():
    client = get_client()
    (df_daily, near_nifty, near_sensex, df_opts_918_n, df_opts_918_s,
     df_max_n, df_max_s, morning_rows_n, dict_spot_n, dict_spot_s) = load_all_data(client)

    df_opts_918_n['abs_delta'] = df_opts_918_n['delta'].abs()
    df_opts_918_s['abs_delta'] = df_opts_918_s['delta'].abs()

    morning_signals = compute_morning_signals(morning_rows_n)
    regime_map = compute_micro_regimes(df_daily)
    trading_dates = sorted(list(regime_map.keys()))

    records = []
    for td in trading_dates:
        exp_n = near_nifty.get(td)
        exp_s = near_sensex.get(td)
        if exp_n is None:
            continue

        if exp_s is None:
            exp, lot, spot = exp_n, NIFTY_LOT_SIZE, dict_spot_n.get(td)
            df_918 = df_opts_918_n[(df_opts_918_n['td'] == td) & (df_opts_918_n['exp'] == exp)]
            df_max = df_max_n
        else:
            dte_n, dte_s = (exp_n - td).days, (exp_s - td).days
            if dte_n <= dte_s:
                exp, lot, spot = exp_n, NIFTY_LOT_SIZE, dict_spot_n.get(td)
                df_918 = df_opts_918_n[(df_opts_918_n['td'] == td) & (df_opts_918_n['exp'] == exp)]
                df_max = df_max_n
            else:
                exp, lot, spot = exp_s, SENSEX_LOT_SIZE, dict_spot_s.get(td)
                df_918 = df_opts_918_s[(df_opts_918_s['td'] == td) & (df_opts_918_s['exp'] == exp)]
                df_max = df_max_s

        if spot is None or len(df_918) == 0:
            continue

        reg_info = regime_map.get(td, ('Medium', 'Falling'))
        m_jump   = morning_signals.get(td, False)

        p1_lots, p2_lots, sl_mult = ALLOC_LR.get(reg_info, (15, 5, 2.0))
        if reg_info in MORNING_AMPLIFY_KEYS and m_jump:
            p1_lots, p2_lots, sl_mult = 15, 5, 2.0
        elif reg_info in MORNING_DEFEND_KEYS and m_jump:
            p1_lots, p2_lots, sl_mult = 5, 15, 1.75

        pnl_primary = simulate_strangle(td, exp, lot, df_918, df_max, target_delta=0.25, qty_lots=p1_lots, sl_mult=sl_mult)
        pnl_sec     = simulate_strangle(td, exp, lot, df_918, df_max, target_delta=0.10, qty_lots=p2_lots, sl_mult=sl_mult)
        tot_pnl = pnl_primary + pnl_sec

        records.append({
            'td': pd.to_datetime(td),
            'pnl': tot_pnl,
            'is_win': tot_pnl > 0
        })

    df = pd.DataFrame(records)
    df['year_month'] = df['td'].dt.strftime('%Y-%m')

    mom = df.groupby('year_month').agg(
        days=('pnl', 'count'),
        wins=('is_win', 'sum'),
        losses=('is_win', lambda x: (x == False).sum()),
        monthly_pnl=('pnl', 'sum'),
        avg_daily=('pnl', 'mean')
    ).reset_index()

    mom['win_rate'] = (mom['wins'] / mom['days']) * 100
    mom['cum_pnl'] = mom['monthly_pnl'].cumsum()

    print("\n" + "=" * 95)
    print("📊 STRATEGY 6: MONTH-ON-MONTH (MoM) HISTORICAL PERFORMANCE SUMMARY")
    print("=" * 95)
    print(f"{'Month':<10} | {'Days':<6} | {'Win Rate':<10} | {'W / L':<8} | {'Monthly PnL (₹)':<20} | {'Cumulative PnL (₹)':<22}")
    print("-" * 95)

    for _, r in mom.iterrows():
        print(f"{r['year_month']:<10} | {r['days']:<6d} | {r['win_rate']:5.1f}%     | {r['wins']:2d}W / {r['losses']:2d}L | ₹{r['monthly_pnl']:+15,.2f}   | ₹{r['cum_pnl']:+16,.2f}")

    print("-" * 95)
    print(f"🏆 TOTAL CUMULATIVE PnL  : ₹{df['pnl'].sum():+16,.2f}")
    print(f"🏆 OVERALL WIN RATE       : {(df['is_win'].sum() / len(df))*100:.2f}% ({df['is_win'].sum()} Wins / {len(df)-df['is_win'].sum()} Losses across {len(df)} days)")
    print("=" * 95)

if __name__ == '__main__':
    run()
