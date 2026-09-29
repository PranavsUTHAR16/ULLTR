"""
Real-Time Model M.E.N.D. Maker Delta-Neutral Center (S*) Solver Daemon.
======================================================================
100% Parity with Python Research Backtest (model_mend.py).

Microstructure Architecture:
1. Pure ClickHouse Direct Solver:
   - Queries live ClickHouse Cloud tick repository for 15-minute spot & option taker flow.
   - Computes both sleeves in parallel:
     a) Model M.E.N.D. (Series): Cumulative series cycle (Wednesday to today).
     b) Model M.E.N.D. (Intraday): Fresh intraday order flow (today 09:15 to current bar).
2. Closed-Form Taylor Greek Resolution:
   - Expanding visited ATM envelope (min_visited_atm - 500 to max_visited_atm + 500).
   - Net Maker Flow = -(Taker Buy - Taker Sell).
   - Delta_maker = Sum Pos(K, otype) * Delta_table.
   - Gamma_maker = Sum Pos(K, otype) * Gamma_table.
   - S* = Spot - (Delta_maker / Gamma_maker).
3. Real-Time Redis Publishing:
   - Updates 'mend:series:s_star', 'mend:series:maker_delta',
     'mend:intra:s_star', 'mend:intra:maker_delta',
     'mend:s_star', 'mend:maker_delta', 'mend:updated_at_ms'.
"""

import datetime
import logging
import os
import sys
import time
from typing import Dict, Optional, Tuple

import clickhouse_connect
import pandas as pd
import redis

# Logging configuration
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(SCRIPT_DIR, "mend_solver.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [MEND-Solver] %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE),
    ],
)
logger = logging.getLogger("MEND_Solver")

UNIX_SOCKET_PATHS = [
    "/home/ubuntu/Desktop/open_source/web/redis.sock",
    "/Users/prana/Desktop/open_source/web/redis.sock",
    "/var/run/redis/redis-server.sock",
    "/var/run/redis/redis.sock",
]
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379

CH_HOST = os.environ.get("CLICKHOUSE_HOST", "ra5fptcofl.ap-south-1.aws.clickhouse.cloud")
CH_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8443"))
CH_USER = os.environ.get("CLICKHOUSE_USER", "default")
CH_PASS = os.environ.get("CLICKHOUSE_PASSWORD", "BhhYrZvtF3lA~")
CH_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "default")


def get_redis_client() -> redis.Redis:
    """Connect to local Redis via UNIX domain socket or TCP fallback."""
    for sock in UNIX_SOCKET_PATHS:
        if os.path.exists(sock):
            try:
                r = redis.Redis(unix_socket_path=sock, decode_responses=True)
                r.ping()
                return r
            except Exception:
                pass
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def get_clickhouse_client():
    """Connect to ClickHouse Cloud with timeout."""
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASS,
        database=CH_DATABASE,
        secure=True,
        connect_timeout=10,
    )


class MendDirectSolver:
    """Direct ClickHouse Solver providing 100% parity with Python research backtests."""

    def __init__(self):
        self.r = get_redis_client()
        self.client = None

    def _ensure_client(self):
        if self.client is None:
            self.client = get_clickhouse_client()
        return self.client

    def resolve_dates(self, today_str: str) -> Tuple[str, str]:
        """Resolve active weekly expiry and series start date."""
        client = self._ensure_client()
        exp_res = client.query(f"""
            SELECT distinct expiry 
            FROM market_ticks 
            WHERE underlying = 'NIFTY' 
              AND toDate(timestamp) = '{today_str}' 
              AND expiry >= '{today_str}'
            ORDER BY expiry ASC 
            LIMIT 1
        """).result_rows

        if not exp_res:
            target_expiry = today_str
        else:
            target_expiry = str(exp_res[0][0])

        exp_dt = pd.to_datetime(target_expiry)
        series_start_date = (exp_dt - pd.Timedelta(days=6)).strftime("%Y-%m-%d")
        return target_expiry, series_start_date

    def compute_s_star(self, today_str: str, target_expiry: str, series_start: str) -> Optional[Tuple[float, float, float]]:
        """
        Compute latest Spot, S*, and Net Maker Delta matching model_mend.py exactly.
        Returns: (spot, s_star, maker_delta) or None.
        """
        client = self._ensure_client()

        # 1. 15-minute spot bars for today
        q_spot = f"""
        SELECT 
            toDateTime(toStartOfInterval(timestamp, INTERVAL 15 MINUTE)) AS bar_time,
            toDate(timestamp) AS trade_date,
            argMin(toFloat64(ltp), timestamp) AS spot_open,
            max(toFloat64(ltp)) AS spot_high,
            min(toFloat64(ltp)) AS spot_low,
            argMax(toFloat64(ltp), timestamp) AS spot_close,
            round(argMax(toFloat64(ltp), timestamp) / 50.0) * 50.0 AS atm_strike
        FROM market_ticks
        WHERE symbol = 'NSE_INDEX|Nifty 50'
          AND toDate(timestamp) = '{today_str}'
          AND toHour(timestamp)*60 + toMinute(timestamp) >= 9*60 + 15
          AND toHour(timestamp)*60 + toMinute(timestamp) <= 15*60 + 30
          AND ltp > 1000
        GROUP BY bar_time, trade_date
        ORDER BY bar_time ASC
        """
        df_spot = client.query_df(q_spot)
        if df_spot.empty:
            return None

        # 2. 15-minute options taker order flow with table Greeks
        q_opts = f"""
        WITH raw AS (
            SELECT 
                toDateTime(toStartOfInterval(timestamp, INTERVAL 15 MINUTE)) AS bar_time,
                timestamp,
                strike,
                option_type,
                toFloat64(ltp) AS ltp,
                toFloat64(bid) AS bid,
                toFloat64(ask) AS ask,
                toFloat64(delta) AS delta,
                toFloat64(gamma) AS gamma,
                toFloat64(iv) AS iv,
                toFloat64(greatest(0, volume - lagInFrame(volume, 1, volume) OVER (PARTITION BY symbol ORDER BY timestamp))) AS ltq,
                (bid + ask) / 2.0 AS midpoint,
                if(ltp > midpoint, 1, if(ltp < midpoint, -1, 0)) AS lr_sign
            FROM market_ticks
            WHERE underlying = 'NIFTY'
              AND option_type IN ('CE', 'PE')
              AND expiry = '{target_expiry}'
              AND toDate(timestamp) >= '{series_start}'
              AND toDate(timestamp) <= '{today_str}'
              AND toHour(timestamp)*60 + toMinute(timestamp) >= 9*60 + 15
              AND toHour(timestamp)*60 + toMinute(timestamp) <= 15*60 + 30
        ),
        classified AS (
            SELECT 
                bar_time,
                timestamp,
                strike,
                option_type,
                ltp,
                bid,
                ask,
                delta,
                gamma,
                iv,
                ltq,
                if(lr_sign != 0, lr_sign, 0) AS lr_sign
            FROM raw
        )
        SELECT 
            bar_time,
            strike,
            option_type,
            sum(ltq) AS vol,
            sum(if(lr_sign > 0, ltq, if(lr_sign == 0, ltq * 0.5, 0.0))) AS taker_buy,
            sum(if(lr_sign < 0, ltq, if(lr_sign == 0, ltq * 0.5, 0.0))) AS taker_sell,
            argMax(delta, timestamp) AS delta,
            argMax(gamma, timestamp) AS gamma,
            argMax(iv, timestamp) AS iv,
            argMax(ltp, timestamp) AS close_ltp
        FROM classified
        GROUP BY bar_time, strike, option_type
        ORDER BY bar_time ASC, strike ASC
        """
        df_opts = client.query_df(q_opts)
        if df_opts.empty:
            last_spot = float(df_spot["spot_close"].iloc[-1])
            return last_spot, last_spot, 0.0

        df_opts["bar_time"] = pd.to_datetime(df_opts["bar_time"])
        opts_by_bar = {t: grp for t, grp in df_opts.groupby("bar_time")}
        all_bar_times = sorted(opts_by_bar.keys())

        df_s = df_spot.copy()
        df_s["min_visited_atm"] = df_s["atm_strike"].cummin()
        df_s["max_visited_atm"] = df_s["atm_strike"].cummax()
        atm_by_bar = dict(zip(df_s["bar_time"], df_s["atm_strike"]))
        min_atm_map = dict(zip(df_s["bar_time"], df_s["min_visited_atm"]))
        max_atm_map = dict(zip(df_s["bar_time"], df_s["max_visited_atm"]))
        spot_by_bar = dict(zip(df_s["bar_time"], df_s["spot_close"]))
        today_bar_times = set(df_s["bar_time"])

        series_inv: Dict[Tuple[int, str], Dict[str, float]] = {}
        s_star_map: Dict[Any, float] = {}
        maker_delta_map: Dict[Any, float] = {}

        for b_time in all_bar_times:
            bar_opts = opts_by_bar[b_time]
            bar_atm = atm_by_bar.get(b_time)
            min_atm = min_atm_map.get(b_time)
            max_atm = max_atm_map.get(b_time)

            for _, opt_row in bar_opts.iterrows():
                k = int(opt_row["strike"])
                otype = opt_row["option_type"]

                # FILTER: EXPANDING ATM +- 10 STRIKES (<= 500 pts from visited ATM)
                if min_atm is not None and max_atm is not None:
                    if k < (min_atm - 500) or k > (max_atm + 500):
                        continue
                elif bar_atm is not None and abs(k - bar_atm) > 500:
                    continue

                net_taker = float(opt_row["taker_buy"]) - float(opt_row["taker_sell"])
                maker_flow = -net_taker
                d = float(opt_row["delta"]) if pd.notna(opt_row["delta"]) else 0.0
                g = float(opt_row["gamma"]) if pd.notna(opt_row["gamma"]) else 0.0

                if (k, otype) not in series_inv:
                    series_inv[(k, otype)] = {"pos": 0.0, "delta": d, "gamma": g}
                series_inv[(k, otype)]["pos"] += maker_flow
                series_inv[(k, otype)]["delta"] = d
                series_inv[(k, otype)]["gamma"] = g

            if b_time in today_bar_times:
                sp = spot_by_bar.get(b_time, 23200.0)
                tot_delta = sum(v["pos"] * v["delta"] for v in series_inv.values())
                tot_gamma = sum(v["pos"] * v["gamma"] for v in series_inv.values())

                if abs(tot_gamma) > 1e-4:
                    s_star = sp - (tot_delta / tot_gamma)
                else:
                    s_star = sp

                s_star_map[b_time] = s_star
                maker_delta_map[b_time] = tot_delta

        latest_bar = sorted(today_bar_times)[-1]
        latest_spot = float(spot_by_bar[latest_bar])
        latest_s_star = float(s_star_map.get(latest_bar, latest_spot))
        latest_delta = float(maker_delta_map.get(latest_bar, 0.0))

        return latest_spot, latest_s_star, latest_delta

    def run_forever(self):
        """Continuous execution loop running every 5 seconds."""
        logger.info("Starting Direct ClickHouse Model M.E.N.D. S* Solver Daemon (100% Backtest Parity)...")
        last_date_checked = ""
        target_expiry = ""
        series_start_date = ""

        while True:
            t0 = time.time()
            try:
                today_str = datetime.datetime.now().strftime("%Y-%m-%d")
                if today_str != last_date_checked or not target_expiry:
                    target_expiry, series_start_date = self.resolve_dates(today_str)
                    last_date_checked = today_str
                    logger.info(f"Target Expiry: {target_expiry} | Series Start: {series_start_date}")

                # 1. Compute Series equilibrium
                series_res = self.compute_s_star(today_str, target_expiry, series_start_date)

                # 2. Compute Intraday equilibrium
                intra_res = self.compute_s_star(today_str, target_expiry, today_str)

                if series_res and intra_res:
                    spot_s, s_star_s, delta_s = series_res
                    spot_i, s_star_i, delta_i = intra_res
                    spot = spot_s

                    # Publish atomically to Redis
                    now_ms = int(time.time() * 1000)
                    pipe = self.r.pipeline()
                    pipe.set("mend:series:s_star", f"{s_star_s:.2f}")
                    pipe.set("mend:series:maker_delta", f"{delta_s:.2f}")
                    pipe.set("mend:intra:s_star", f"{s_star_i:.2f}")
                    pipe.set("mend:intra:maker_delta", f"{delta_i:.2f}")

                    # Compatibility aliases (default to Series)
                    pipe.set("mend:s_star", f"{s_star_s:.2f}")
                    pipe.set("mend:maker_delta", f"{delta_s:.2f}")
                    pipe.set("mend:updated_at_ms", str(now_ms))
                    pipe.execute()

                    dist_s = spot - s_star_s
                    dist_i = spot - s_star_i
                    elapsed = time.time() - t0
                    logger.info(
                        f"Exp: {target_expiry} | Spot: {spot:.2f} | "
                        f"[SERIES] S*: {s_star_s:.2f} (Dist: {dist_s:+.2f}, Δ: {delta_s:+,.0f}) | "
                        f"[INTRA] S*: {s_star_i:.2f} (Dist: {dist_i:+.2f}, Δ: {delta_i:+,.0f}) | "
                        f"Solved in {elapsed:.2f}s"
                    )

            except Exception as e:
                logger.error(f"Solver iteration error: {e}", exc_info=True)
                self.client = None

            # Sleep 5 seconds between evaluations
            sleep_time = max(1.0, 5.0 - (time.time() - t0))
            time.sleep(sleep_time)


def main():
    solver = MendDirectSolver()
    solver.run_forever()


if __name__ == "__main__":
    main()
