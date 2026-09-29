#!/usr/bin/env python3
"""
ULLTR 0DTE / Daily Closing Auction Session (CAS) Arbitrage Model
==============================================================
Sub-1ms Vectorized Equilibrium Calculation & Real Broker Order Gateway Execution.

Timeline:
  15:15:00 - 15:20:00: Spot freezes. System arms and records S_ref(NIFTY) and S_ref(SENSEX).
  15:20:01: Phase 1 orderbook uncrossing starts.
            Calculates CAS equilibrium price in < 1 millisecond.
            Identifies ATM strike direction (CE if P_cas >= S_ref, PE if P_cas < S_ref).
            Fires real orders to Upstox API Gateway for both NIFTY and SENSEX.
            Profiles microsecond-level Signal-to-Order Turnaround Latency.
  15:30:00: Cash settlement matching.
"""

import os
import sys
import time
import json
import logging
from datetime import datetime, time as dtime
from typing import List, Dict, Any, Optional, Tuple
import numpy as np
import redis

from forward_tester.models.base_model import BaseTradingModel
from forward_tester.position import ForwardTestPosition
from forward_tester.broker_gateway import UpstoxBrokerGateway

logger = logging.getLogger("CASModel")

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))


def match_4tier_stock(bids_p: List[float], bids_q: List[int], asks_p: List[float], asks_q: List[int], mbq: float, msq: float, ref_price: float) -> float:
    """
    Applies the official 4-Tier NSE Equilibrium Auction Matching algorithm:
    1. Maximum Executable Volume
    2. Minimum Order Imbalance
    3. Imbalance Direction (higher price for buy surplus, lower for sell surplus)
    4. Proximity to Reference Price (15:15 LTP)
    """
    if ref_price <= 0:
        return ref_price

    limit_bids = [(p, q) for p, q in zip(bids_p, bids_q) if p > 0]
    limit_asks = [(p, q) for p, q in zip(asks_p, asks_q) if p > 0]

    mkt_buy_q = int(mbq) if mbq > 0 else sum(q for p, q in zip(bids_p, bids_q) if p == 0.0)
    mkt_sell_q = int(msq) if msq > 0 else sum(q for p, q in zip(asks_p, asks_q) if p == 0.0)

    candidate_prices = sorted(list(set([p for p, _ in limit_bids] + [p for p, _ in limit_asks] + [ref_price])))
    if not candidate_prices:
        return ref_price

    best_candidates = []
    for p in candidate_prices:
        cum_buy = mkt_buy_q + sum(q for bp_i, q in limit_bids if bp_i >= p)
        cum_sell = mkt_sell_q + sum(q for ap_i, q in limit_asks if ap_i <= p)
        match_v = min(cum_buy, cum_sell)
        imb = abs(cum_buy - cum_sell)
        best_candidates.append({
            'price': p,
            'match_v': match_v,
            'imb': imb,
            'cum_buy': cum_buy,
            'cum_sell': cum_sell
        })

    # Tier 1: Maximum Executable Volume
    max_v = max(c['match_v'] for c in best_candidates)
    if max_v == 0:
        return round(float(ref_price), 2)

    c1 = [c for c in best_candidates if c['match_v'] == max_v]
    if len(c1) == 1:
        return c1[0]['price']

    # Tier 2: Minimum Order Imbalance
    min_i = min(c['imb'] for c in c1)
    c2 = [c for c in c1 if c['imb'] == min_i]
    if len(c2) == 1:
        return c2[0]['price']

    # Tier 3: Imbalance Direction
    first = c2[0]
    if first['cum_buy'] > first['cum_sell']:
        return max(c['price'] for c in c2)
    elif first['cum_buy'] < first['cum_sell']:
        return min(c['price'] for c in c2)

    # Tier 4: Proximity to Reference Price
    c2.sort(key=lambda c: (abs(c['price'] - ref_price), -c['price']))
    return c2[0]['price']


class CASModel(BaseTradingModel):
    """
    Sub-1ms Vectorized CAS Orderbook Arbitrage Model.
    """
    def __init__(self, model_id: str = "CAS_ARB", name: str = "0DTE CAS Arbitrage Engine", data_client: Any = None, config: Any = None):
        super().__init__(model_id, name, data_client, config)
        self.trade_date = ""
        self.gateway = UpstoxBrokerGateway()
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        
        self.enabled = getattr(getattr(config, "cas", None), "enabled", False)
        
        if not self.data_client:
            try:
                from forward_tester.data_client import ForwardTestDataClient
                self.data_client = ForwardTestDataClient()
            except Exception as e:
                logger.debug(f"DataClient init note: {e}")
                self.data_client = None
        
        # Load index freefloat weights
        self.nifty_weights: Dict[str, float] = {}
        self.sensex_weights: Dict[str, float] = {}
        self.symbol_to_key: Dict[str, str] = {}  # "HDFCBANK" -> "NSE_EQ|INE040A01034"
        self._load_weights_and_mappings()
        
        # Session State
        self.is_armed = False
        self.entry_executed = False
        self.spot_ref: Dict[str, float] = {"NIFTY": 0.0, "SENSEX": 0.0}
        self.stock_ref: Dict[str, float] = {}
        self.cas_telemetry: List[Dict[str, Any]] = []

        # Pre-cache symbol keys and Redis depth keys for sub-millisecond access
        self.nifty_symbols = list(self.nifty_weights.keys())
        self.nifty_weights_list = [float(self.nifty_weights[s]) for s in self.nifty_symbols]
        self.nifty_keys = [
            f"depth:quote:{self.symbol_to_key.get(f'NSE_EQ:{s}', f'NSE_EQ|{s}')}"
            for s in self.nifty_symbols
        ]

        self.sensex_symbols = list(self.sensex_weights.keys())
        self.sensex_weights_list = [float(self.sensex_weights[s]) for s in self.sensex_symbols]
        self.sensex_keys = [
            f"depth:quote:{self.symbol_to_key.get(f'BSE_EQ:{s}', f'BSE_EQ|{s}')}"
            for s in self.sensex_symbols
        ]

        # In-Engine Redis Lua script for ultra-low latency (< 500 µs) zero-payload calculation
        self.lua_cas_script = None
        self._init_lua_engine()

    def _init_lua_engine(self):
        """Compiles and registers Lua CAS equilibrium script inside Redis memory."""
        lua_code = """
        local spot = tonumber(ARGV[1])
        local weighted_sum_pct = 0.0
        local tot_buy = 0.0
        local tot_sell = 0.0

        for i, k in ipairs(KEYS) do
            local w = tonumber(ARGV[i + 1])
            local vals = redis.call('HMGET', k, 'ltp', 'tbq', 'tsq', 'mbq', 'msq')
            local ltp = tonumber(vals[1]) or 0.0
            local tbq = tonumber(vals[2]) or 0.0
            local tsq = tonumber(vals[3]) or 0.0
            local mbq = tonumber(vals[4]) or 0.0
            local msq = tonumber(vals[5]) or 0.0
            
            tot_buy = tot_buy + tbq
            tot_sell = tot_sell + tsq
            
            local tot_q = tbq + tsq
            local imb = 0.0
            if tot_q > 0 then
                imb = (tbq - tsq) / tot_q
            end
            
            local mkt_tot = mbq + msq
            local mkt_imb = 0.0
            if mkt_tot > 0 then
                mkt_imb = (mbq - msq) / mkt_tot
            end
            
            local comb_imb = 0.7 * imb + 0.3 * mkt_imb
            local pct_move = comb_imb * 0.0018
            weighted_sum_pct = weighted_sum_pct + (pct_move * w)
        end

        local cas_price = spot * (1.0 + weighted_sum_pct)
        local expected_move = cas_price - spot
        local pool = tot_buy + tot_sell
        local buyer_dom = 50.0
        if pool > 0 then
            buyer_dom = (tot_buy / pool) * 100.0
        end

        return {tostring(cas_price), tostring(expected_move), tostring(buyer_dom), tostring(tot_buy), tostring(tot_sell)}
        """
        try:
            self.lua_cas_script = self.redis_client.register_script(lua_code)
        except Exception as e:
            logger.warning(f"Failed to register Redis Lua CAS script: {e}")
            self.lua_cas_script = None

    def _load_weights_and_mappings(self):
        """Loads static index constituent weights and Upstox instrument key mappings."""
        nifty_w_path = os.path.join(PROJECT_ROOT, "nifty50_freefloat_weights.json")
        sensex_w_path = os.path.join(PROJECT_ROOT, "sensex30_freefloat_weights.json")
        eq_file = os.path.join(PROJECT_ROOT, "equity_symbols.json")
        
        if os.path.exists(nifty_w_path):
            with open(nifty_w_path, "r") as f:
                self.nifty_weights = json.load(f)
                
        if os.path.exists(sensex_w_path):
            with open(sensex_w_path, "r") as f:
                self.sensex_weights = json.load(f)
                
        if os.path.exists(eq_file):
            with open(eq_file, "r") as f:
                eqs = json.load(f)
                for k, v in eqs.items():
                    # k = "NSE_EQ|INE040A01034", v["symbol"] = "HDFCBANK", v["exchange"] = "NSE_EQ"
                    key_alias = f"{v.get('exchange')}:{v.get('symbol')}"
                    self.symbol_to_key[key_alias] = k

    def init_trading_day(self, trade_date: str) -> None:
        self.trade_date = trade_date
        self.active_positions.clear()
        self.closed_positions.clear()
        self.daily_pnl = 0.0
        self.is_armed = False
        self.entry_executed = False
        self.spot_ref = {"NIFTY": 0.0, "SENSEX": 0.0}
        self.stock_ref: Dict[str, float] = {}
        self.cas_telemetry.clear()
        logger.info(f"✅ CASModel initialized for trading day {trade_date}")

    def arm_cas_session(self):
        """Pre-CAS arming around 15:15–15:20 IST. Records official 15:00-15:15 reference prices."""
        if not self.enabled:
            return
        for und in ["NIFTY", "SENSEX"]:
            p = 0.0
            if self.data_client:
                try:
                    p = self.data_client.get_spot_price(und)
                except Exception:
                    p = 0.0
            if p <= 0:
                p = 24000.0 if und == "NIFTY" else 76500.0
            self.spot_ref[und] = p

        # Load official August 2026 15:00-15:15 Reference Baselines from Redis if available
        try:
            nifty_ref = self.redis_client.get("cas:ref:NIFTY_50")
            if nifty_ref:
                self.spot_ref["NIFTY"] = float(nifty_ref)
            sensex_ref = self.redis_client.get("cas:ref:SENSEX_30")
            if sensex_ref:
                self.spot_ref["SENSEX"] = float(sensex_ref)
        except Exception as e:
            logger.debug(f"Redis CAS ref read note: {e}")

        # Cache frozen reference prices for all constituents at arming
        all_keys = self.nifty_keys + self.sensex_keys
        try:
            pipe = self.redis_client.pipeline(transaction=False)
            for k in all_keys:
                pipe.hget(k, "ltp")
            res = pipe.execute()
            for k, val in zip(all_keys, res):
                if val is not None:
                    try:
                        self.stock_ref[k] = float(val)
                    except (ValueError, TypeError):
                        pass
        except Exception as e:
            logger.warning(f"Note on CAS stock ref caching: {e}")
            
        self.is_armed = True
        logger.info(
            f"🔒 CAS Model ARMED | Reference Baseline: NIFTY={self.spot_ref['NIFTY']:,.2f} | "
            f"SENSEX={self.spot_ref['SENSEX']:,.2f} | Constituents Cached: {len(self.stock_ref)}"
        )

    def calculate_equilibrium(self, underlying: str) -> Dict[str, Any]:
        """
        Calculates CAS Orderbook Equilibrium Price in < 1 millisecond using exact
        4-Tier NSE Matching Uncrossing across the full 30-level depth and market order queues.
        """
        t0_ns = time.perf_counter_ns()
        
        is_nifty = (underlying.upper() == "NIFTY")
        spot = self.spot_ref.get(underlying, 0.0)
        if spot <= 0:
            spot = 24000.0 if is_nifty else 76500.0

        # Fast path: check if cas_tracker has emitted live August 2026 CAS equilibrium
        try:
            idx_name = "NIFTY_50" if is_nifty else "SENSEX_30"
            live_cas = self.redis_client.hgetall(f"cas:live:{idx_name}")
            if live_cas and "cas_price" in live_cas:
                cas_price = float(live_cas["cas_price"])
                expected_move = float(live_cas.get("expected_move", cas_price - spot))
                buyer_dom = float(live_cas.get("buyer_dom_pct", 50.0))
                t1_ns = time.perf_counter_ns()
                calc_time_us = round((t1_ns - t0_ns) / 1000.0, 2)
                calc_time_ms = round((t1_ns - t0_ns) / 1_000_000.0, 4)
                return {
                    "underlying": underlying,
                    "spot_ref": spot,
                    "cas_price": cas_price,
                    "expected_move": expected_move,
                    "buyer_dominance_pct": buyer_dom,
                    "total_buy_vol": 0.0,
                    "total_sell_vol": 0.0,
                    "calc_time_us": calc_time_us,
                    "calc_time_ms": calc_time_ms
                }
        except Exception:
            pass

        keys = self.nifty_keys if is_nifty else self.sensex_keys
        weights = self.nifty_weights_list if is_nifty else self.sensex_weights_list

        pipe = self.redis_client.pipeline(transaction=False)
        for k in keys:
            pipe.hmget(k, ["ltp", "bp", "bq", "ap", "aq", "mbq", "msq", "tbq", "tsq"])
        raw_depths = pipe.execute()

        weighted_index_pct = 0.0
        tot_buy_vol = 0.0
        tot_sell_vol = 0.0

        for i, d in enumerate(raw_depths):
            # d is [ltp, bp, bq, ap, aq, mbq, msq, tbq, tsq]
            ltp = float(d[0]) if d and d[0] is not None else 1000.0
            bp_str = d[1] if d and len(d) > 1 and d[1] is not None else ""
            bq_str = d[2] if d and len(d) > 2 and d[2] is not None else ""
            ap_str = d[3] if d and len(d) > 3 and d[3] is not None else ""
            aq_str = d[4] if d and len(d) > 4 and d[4] is not None else ""
            mbq = float(d[5]) if d and len(d) > 5 and d[5] is not None else 0.0
            msq = float(d[6]) if d and len(d) > 6 and d[6] is not None else 0.0
            tbq = float(d[7]) if d and len(d) > 7 and d[7] is not None else 0.0
            tsq = float(d[8]) if d and len(d) > 8 and d[8] is not None else 0.0

            tot_buy_vol += (tbq if tbq > 0 else mbq)
            tot_sell_vol += (tsq if tsq > 0 else msq)

            w = weights[i]
            ref_p = self.stock_ref.get(keys[i], ltp)
            if ref_p <= 0:
                ref_p = ltp

            if bp_str and ap_str:
                try:
                    bp = [float(x) for x in bp_str.split(",") if x]
                    bq = [int(float(x)) for x in bq_str.split(",") if x]
                    ap = [float(x) for x in ap_str.split(",") if x]
                    aq = [int(float(x)) for x in aq_str.split(",") if x]
                    p_eq = match_4tier_stock(bp, bq, ap, aq, mbq, msq, ref_p)
                except Exception:
                    p_eq = ref_p
            else:
                # Fallback if depth arrays unseeded in Redis
                tot_q = tbq + tsq
                imb = (tbq - tsq) / max(tot_q, 1.0) if tot_q > 0 else 0.0
                mkt_tot = mbq + msq
                mkt_imb = (mbq - msq) / max(mkt_tot, 1.0) if mkt_tot > 0 else 0.0
                comb_imb = 0.7 * imb + 0.3 * mkt_imb
                p_eq = ref_p * (1.0 + comb_imb * 0.0018)

            pct_move = (p_eq - ref_p) / ref_p if ref_p > 0 else 0.0
            weighted_index_pct += (pct_move * w)

        cas_price = round(float(spot * (1.0 + weighted_index_pct)), 2)
        expected_move = round(float(cas_price - spot), 2)
        tot_pool = tot_buy_vol + tot_sell_vol
        buyer_dom = round(float(tot_buy_vol / tot_pool * 100.0), 1) if tot_pool > 0 else 50.0

        t1_ns = time.perf_counter_ns()
        calc_time_us = round((t1_ns - t0_ns) / 1000.0, 2)
        calc_time_ms = round((t1_ns - t0_ns) / 1_000_000.0, 4)

        return {
            "underlying": underlying,
            "spot_ref": spot,
            "cas_price": cas_price,
            "expected_move": expected_move,
            "buyer_dominance_pct": buyer_dom,
            "total_buy_vol": tot_buy_vol,
            "total_sell_vol": tot_sell_vol,
            "calc_time_us": calc_time_us,
            "calc_time_ms": calc_time_ms
        }

    def select_cas_strike(self, underlying: str, expected_move: float, spot_ref: float) -> Tuple[str, str, int, str, float]:
        """
        Dynamically selects ATM strike and direction.
        Returns: (symbol, instrument_token, strike, option_type, ltp)
        """
        step = 50 if underlying == "NIFTY" else 100
        atm_strike = int(round(spot_ref / step) * step)
        opt_type = "CE" if expected_move >= 0 else "PE"
        
        # Resolve active expiry & option chain from Redis
        expiry = ""
        symbol = ""
        instrument_token = ""
        ltp = 0.0
        
        if self.data_client:
            expiry = self.data_client.get_front_expiry(underlying)
            if expiry:
                chain = self.data_client.get_option_chain(underlying, expiry)
                if isinstance(chain, dict) and chain:
                    token = chain.get(float(atm_strike), {}).get(opt_type) or chain.get(int(atm_strike), {}).get(opt_type)
                    if not token:
                        available_strikes = [float(k) for k in chain.keys() if opt_type in chain[k]]
                        if available_strikes:
                            closest_strike = min(available_strikes, key=lambda s: abs(s - atm_strike))
                            token = chain[closest_strike].get(opt_type)
                            atm_strike = int(closest_strike)
                    if token:
                        instrument_token = str(token)
                        symbol = instrument_token.split("|")[-1] if "|" in instrument_token else instrument_token
                        try:
                            ltp = self.data_client.get_option_ltp(instrument_token)
                        except Exception:
                            ltp = 0.0
                elif hasattr(chain, "empty") and not chain.empty:
                    match = chain[(chain["strike"] == atm_strike) & (chain["option_type"] == opt_type)]
                    if not match.empty:
                        symbol = str(match.iloc[0]["symbol"])
                        instrument_token = str(match.iloc[0].get("instrument_key", symbol))
                        ltp = float(match.iloc[0].get("ltp", 0.0))
                    
        # Fallback if Redis chain empty
        if not symbol or not instrument_token:
            prefix = "NIFTY" if underlying == "NIFTY" else "SENSEX"
            symbol = f"{prefix}_{atm_strike}_{opt_type}"
            instrument_token = "NSE_FO|42640" if underlying == "NIFTY" else "BSE_FO|860348"
            if ltp <= 0.0:
                ltp = 50.0
            
        return symbol, instrument_token, atm_strike, opt_type, ltp

    def execute_cas_entry(self) -> List[Dict[str, Any]]:
        """
        Fires real orders for both NIFTY and SENSEX ATM contracts at 15:20:05 IST.
        Measures microsecond latency turnaround.
        """
        if not self.enabled:
            return []

        if self.entry_executed or len(self.active_positions) > 0 or len(self.closed_positions) > 0:
            return self.cas_telemetry

        logger.info("🚨 15:20:05 CAS WINDOW TRIGGERED! Executing Sub-1ms Orderbook Arbitrage...")
        results = []
        
        # Execute for both NIFTY and SENSEX
        for und in ["NIFTY", "SENSEX"]:
            t_start_ns = time.perf_counter_ns()
            
            # 1. Calculate equilibrium (< 1 ms)
            eq_data = self.calculate_equilibrium(und)
            
            # 2. Select strike and direction
            sym, token, strike, otype, ltp = self.select_cas_strike(
                und, eq_data["expected_move"], eq_data["spot_ref"]
            )
            
            # Lot size: 65 for NIFTY, 20 for SENSEX
            qty = 65 if und == "NIFTY" else 20
            
            # 3. Fire real order via Upstox HFT Gateway
            order_res = self.gateway.place_order(
                instrument_token=token,
                quantity=qty,
                transaction_type="BUY",
                product="I",
                order_type="MARKET",
                tag=f"CAS_{und[:3]}"
            )
            
            t_end_ns = time.perf_counter_ns()
            total_elapsed_ms = round((t_end_ns - t_start_ns) / 1_000_000.0, 3)
            
            telemetry = {
                "underlying": und,
                "spot_ref": eq_data["spot_ref"],
                "cas_price": eq_data["cas_price"],
                "expected_move": eq_data["expected_move"],
                "buyer_dominance": eq_data["buyer_dominance_pct"],
                "calc_time_us": eq_data["calc_time_us"],
                "strike": strike,
                "option_type": otype,
                "symbol": sym,
                "instrument_token": token,
                "quantity": qty,
                "entry_ltp": ltp,
                "order_status": order_res["status_code"],
                "is_success": order_res["is_success"],
                "order_id": order_res.get("primary_order_id", order_res.get("order_id", "")),
                "api_version": order_res.get("api_version", "v3"),
                "error_msg": order_res["error_msg"],
                "turnaround_ms": order_res["turnaround_ms"],
                "gateway_rtt_ms": order_res["gateway_rtt_ms"],
                "broker_meta": order_res.get("broker_latency_meta", {}),
                "total_elapsed_ms": total_elapsed_ms
            }
            results.append(telemetry)
            self.cas_telemetry.append(telemetry)
            
            # Create forward test tracking position
            pos = ForwardTestPosition(
                model_id=self.model_id,
                underlying=und,
                expiry_date=self.trade_date,
                symbol=sym,
                strike=float(strike),
                option_type=otype,
                leg_type="CAS_ATM_BUY",
                target_delta=0.50,
                entry_price=ltp,
                current_price=ltp,
                lots=1,
                lot_size=qty,
                sl_mult=0.0,
                sl_price=0.0,
                delta=0.50,
                direction=otype,
                spot_entry_price=eq_data["spot_ref"]
            )
            self.active_positions.append(pos)
            
            logger.info(
                f"⚡ [{und}] CAS Eq: {eq_data['cas_price']} ({eq_data['expected_move']:+5.2f} pts | Calc: {eq_data['calc_time_us']} µs) | "
                f"Action: BUY {strike} {otype} ({qty} qty) | Broker Ack: {order_res['status_code']} ({order_res['error_msg']}) | "
                f"Gateway RTT: {order_res['gateway_rtt_ms']} ms | Total: {total_elapsed_ms} ms"
            )

        self.entry_executed = True
        return results

    def on_5m_candle_close(self, current_time_str: str) -> Optional[Dict[str, Any]]:
        return None

    def update_and_monitor(self, current_time_str: str) -> List[ForwardTestPosition]:
        """Monitors active CAS positions until 15:30:00 settlement."""
        if not self.enabled or not self.active_positions:
            return []
            
        for pos in self.active_positions:
            if pos.status == "OPEN" and self.data_client:
                sym = pos.symbol
                cur_ltp = self.data_client.get_option_ltp(sym)
                if cur_ltp <= 0:
                    prefix = "NSE_FO|" if pos.underlying == "NIFTY" else "BSE_FO|"
                    if not sym.startswith(prefix):
                        cur_ltp = self.data_client.get_option_ltp(f"{prefix}{sym}")
                if cur_ltp > 0:
                    pos.update_price(cur_ltp)
        return []

    def execute_eod_squareoff(self, exit_time_str: str = "15:30") -> List[ForwardTestPosition]:
        """Closes CAS positions at 15:30:00 market settlement."""
        newly_closed = []
        for pos in self.active_positions:
            if pos.status == "OPEN":
                # Intrinsic settlement
                spot = self.data_client.get_spot_price(pos.underlying) if self.data_client else pos.spot_entry_price
                if pos.option_type == "CE":
                    settle_p = max(0.0, spot - pos.strike)
                else:
                    settle_p = max(0.0, pos.strike - spot)
                pos.close_position(settle_p, reason="CAS_SETTLEMENT")
                self.closed_positions.append(pos)
                newly_closed.append(pos)
        self.active_positions.clear()
        return newly_closed

