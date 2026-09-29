import sys
sys.path.insert(0, '/Users/prana/Desktop/black_box/shadow/production')
sys.path.insert(0, '/Users/prana/Desktop/black_box/shadow')

import pandas as pd
import numpy as np
from ch_loader import get_client
import vol_adaptive_backtest as vab

def get_mom_report():
    client = get_client()
    res_load = vab.load_all_data(client)
    df_daily, df_918, df_max, morning_rows = res_load[0], res_load[1], res_load[2], res_load[3]
    df_morning = vab.compute_morning_signals(morning_rows)
    df_daily = vab.compute_micro_regimes(df_daily)
    df_daily = df_daily.merge(df_morning, on='trading_date', how='left')
    df_daily['morning_jump'] = df_daily['max_abs_ret'] > vab.MORNING_ACTIVE_THRESH

    results = []
    for idx, row in df_daily.iterrows():
        td = row['trading_date']
        und = row['underlying']
        exp = row['expiry']
        reg = row['regime']
        slope = row['lr_slope']
        m_jump = row['morning_jump']
        unit_lot_size = vab.SENSEX_LOT_SIZE if und == 'SENSEX' else vab.NIFTY_LOT_SIZE
        reg_key = (reg, slope)

        p1_lots, p2_lots, sl_mult = vab.ALLOC_LR.get(reg_key, (15, 5, 2.0))
        if reg_key in vab.MORNING_AMPLIFY_KEYS and m_jump:
            p1_lots, p2_lots, sl_mult = 15, 5, 2.0
        elif reg_key in vab.MORNING_DEFEND_KEYS and m_jump:
            p1_lots, p2_lots, sl_mult = 5, 15, 1.75

        df_918_day = df_918[df_918['trading_date'] == td]
        df_max_day = df_max[df_max['trading_date'] == td].set_index(['trading_date', 'expiry_date', 'strike', 'option_type'])

        pnl_primary = vab.simulate_strangle(td, exp, unit_lot_size, df_918_day, df_max_day, target_delta=0.25, qty_lots=p1_lots, sl_mult=sl_mult)
        pnl_sec = vab.simulate_strangle(td, exp, unit_lot_size, df_918_day, df_max_day, target_delta=0.10, qty_lots=p2_lots, sl_mult=sl_mult)

        tot_pnl = pnl_primary + pnl_sec
        results.append({
            'date': pd.to_datetime(td),
            'pnl': tot_pnl,
            'is_win': tot_pnl > 0
        })

    df = pd.DataFrame(results)
    df['year_month'] = df['date'].dt.strftime('%Y-%m')

    mom = df.groupby('year_month').agg(
        days=('pnl', 'count'),
        wins=('is_win', 'sum'),
        losses=('is_win', lambda x: (x == False).sum()),
        monthly_pnl=('pnl', 'sum'),
        avg_daily=('pnl', 'mean')
    ).reset_index()

    mom['win_rate'] = (mom['wins'] / mom['days']) * 100
    mom['cum_pnl'] = mom['monthly_pnl'].cumsum()

    print("=== STRATEGY 6 MONTH-ON-MONTH HISTORICAL PERFORMANCE REPORT ===")
    for _, r in mom.iterrows():
        print(f"{r['year_month']} | Days: {r['days']:2d} | WinRate: {r['win_rate']:5.1f}% ({r['wins']:2d}W/{r['losses']:2d}L) | Net PnL: ₹{r['monthly_pnl']:+11,_.2f} | Cum PnL: ₹{r['cum_pnl']:+12,_.2f}")

    print("-" * 95)
    print(f"GRAND TOTAL CUMULATIVE PnL: ₹{df['pnl'].sum():+12,_.2f}")
    print(f"OVERALL WIN RATE          : {(df['is_win'].sum() / len(df))*100:.2f}% ({df['is_win'].sum()}W / {len(df)-df['is_win'].sum()}L)")

if __name__ == '__main__':
    get_mom_report()
