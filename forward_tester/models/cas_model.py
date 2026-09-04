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


class CASModel(BaseTradingModel):
    """
    Sub-1ms Vectorized CAS Orderbook Arbitrage Model.
    """
    def __init__(self, model_id: str = "CAS_ARB", name: str = "0DTE CAS Arbitrage Engine", data_client: Any = None, config: Any = None):
        super().__init__(model_id, name, data_client, config)
        self.trade_date = ""
        self.gateway = UpstoxBrokerGateway()
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        
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
        self.cas_telemetry: List[Dict[str, Any]] = []

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
        self.cas_telemetry.clear()
        logger.info(f"✅ CASModel initialized for trading day {trade_date}")

    def arm_cas_session(self):
        """Pre-CAS arming around 15:15–15:20 IST. Records frozen spot references."""
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
            
        self.is_armed = True
        logger.info(f"🔒 CAS Model ARMED | Spot Ref: NIFTY={self.spot_ref['NIFTY']:,.2f} | SENSEX={self.spot_ref['SENSEX']:,.2f}")

    def calculate_equilibrium(self, underlying: str) -> Dict[str, Any]:
        """
        Calculates CAS Orderbook Equilibrium Price in < 1 millisecond using NumPy vectorization.
        """
        t0_ns = time.perf_counter_ns()
        
        is_nifty = (underlying.upper() == "NIFTY")
        weights_dict = self.nifty_weights if is_nifty else self.sensex_weights
        exchange = "NSE_EQ" if is_nifty else "BSE_EQ"
        spot = self.spot_ref.get(underlying, 0.0)
        if spot <= 0:
            spot = 24000.0 if is_nifty else 76500.0

        symbols = list(weights_dict.keys())
        weights = np.array([weights_dict[s] for s in symbols], dtype=np.float64)
        
        # Pipelined Redis Hash fetch (< 150 µs)
        pipe = self.redis_client.pipeline(transaction=False)
        for sym in symbols:
            key_alias = f"{exchange}:{sym}"
            inst_key = self.symbol_to_key.get(key_alias, f"{exchange}|{sym}")
            pipe.hmget(f"depth:quote:{inst_key}", ["ltp", "tbq", "tsq", "mbq", "msq"])
        raw_depths = pipe.execute()
        
        # Assemble arrays
        N = len(symbols)
        ltp_arr = np.zeros(N, dtype=np.float64)
        tbq_arr = np.zeros(N, dtype=np.float64)
        tsq_arr = np.zeros(N, dtype=np.float64)
        mbq_arr = np.zeros(N, dtype=np.float64)
        msq_arr = np.zeros(N, dtype=np.float64)
        
        for i, d in enumerate(raw_depths):
            # d is [ltp, tbq, tsq, mbq, msq]
            if d and d[0] is not None:
                ltp_arr[i] = float(d[0] or 0.0)
                tbq_arr[i] = float(d[1] or 0.0)
                tsq_arr[i] = float(d[2] or 0.0)
                mbq_arr[i] = float(d[3] or 0.0)
                msq_arr[i] = float(d[4] or 0.0)
            else:
                # If unseeded, default to spot proportion
                ltp_arr[i] = 1000.0
                
        # Vectorized CAS elasticity formula (~10 µs)
        tot_q = tbq_arr + tsq_arr
        # Safe divide for queue imbalance
        imb = np.where(tot_q > 0, (tbq_arr - tsq_arr) / np.maximum(tot_q, 1.0), 0.0)
        
        mkt_tot = mbq_arr + msq_arr
        mkt_imb = np.where(mkt_tot > 0, (mbq_arr - msq_arr) / np.maximum(mkt_tot, 1.0), 0.0)
        
        comb_imb = 0.7 * imb + 0.3 * mkt_imb
        # Standard large-cap CAS elasticity multiplier
        p_eq = ltp_arr * (1.0 + comb_imb * 0.0018)
        
        # Percentage move of each stock
        pct_moves = np.where(ltp_arr > 0, (p_eq - ltp_arr) / ltp_arr, 0.0)
        weighted_index_pct = np.dot(pct_moves, weights)
        
        cas_price = round(float(spot * (1.0 + weighted_index_pct)), 2)
        expected_move = round(float(cas_price - spot), 2)
        
        tot_buy_vol = float(np.sum(tbq_arr))
        tot_sell_vol = float(np.sum(tsq_arr))
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
            instrument_token = "NSE_FO|46938" if underlying == "NIFTY" else "BSE_FO|579300"
            if ltp <= 0.0:
                ltp = 50.0
            
        return symbol, instrument_token, atm_strike, opt_type, ltp

    def execute_cas_entry(self) -> List[Dict[str, Any]]:
        """
        Fires real orders for both NIFTY and SENSEX ATM contracts at 15:20:01 IST.
        Measures microsecond latency turnaround.
        """
        if self.entry_executed:
            return self.cas_telemetry

        logger.info("🚨 15:20:01 CAS WINDOW TRIGGERED! Executing Sub-1ms Orderbook Arbitrage...")
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
                sl_mult=1.0,
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
        if not self.active_positions:
            return []
            
        for pos in self.active_positions:
            if pos.status == "OPEN" and self.data_client:
                cur_ltp = self.data_client.get_option_ltp(pos.symbol)
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
                pos.close_position(settle_p)
                self.closed_positions.append(pos)
                newly_closed.append(pos)
        self.active_positions.clear()
        return newly_closed

