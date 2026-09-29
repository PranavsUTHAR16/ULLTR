#!/usr/bin/env python3
"""
Tri-Model Options Portfolio Python Forward Tester Engine.
=========================================================
Implements exact 1-to-1 operational and mathematical parity with the C++ StrategyEngine:
1. Model POC V2 (09:20 - 10:30 IST): Two-Tier execution (L1 Target Lock +30pt FUT / L2 VWAP Runner)
2. Model Spatial Box with AVWAP Arm Gate (09:20 - 15:00 IST): 50-pt Compression Box with AVWAP gate & overshoot invalidation
3. Causal Dalton Value Area Traverse (10:15 - 13:30 IST): 80% Rule re-entry with 15m CVD & PCR confirmation

Consumes real-time / recorded Redis data (candles, microstructure metrics, option chains, and quotes).
"""

from collections import defaultdict
from dataclasses import dataclass, field
import datetime
import json
import math
import os
import sys
import time
from typing import Dict, List, Optional, Set, Tuple

import redis


@dataclass
class PythonPosition:
    position_id: str
    model_name: str
    symbol: str
    strike: int
    option_type: str  # "CE" or "PE"
    lots: int
    quantity: int
    entry_time: str
    entry_option_price: float
    entry_futures_price: float
    current_option_price: float
    current_futures_price: float
    margin_locked: float
    sl_futures_price: float
    tp_futures_price: float
    t1_hit: bool = False
    lot1_exit_time: str = ""
    lot1_exit_price: float = 0.0
    lot1_pnl: float = 0.0
    lot2_sl_futures_price: float = 0.0
    exit_time: str = ""
    exit_option_price: float = 0.0
    exit_futures_price: float = 0.0
    realized_pnl: float = 0.0
    exit_reason: str = ""
    target_opt_price: float = 0.0
    sl_opt_price: float = 0.0
    active_box_avwap: float = 0.0
    bars_held: int = 0
    dalton_ib_vah: float = 0.0
    dalton_ib_val: float = 0.0
    dalton_ib_poc: float = 0.0
    dalton_pcr: float = 1.0
    tpo_target_futures: float = 0.0
    is_active: bool = True
    remaining_lots: int = 1

    def get_points(self) -> float:
        if self.entry_option_price <= 0.0:
            return 0.0
        eff_exit = self.exit_option_price if self.exit_option_price > 0.0 else self.current_option_price
        return eff_exit - self.entry_option_price - 1.0  # 1.0 pt friction


class PythonFIFOPool:
    """Exact replica of C++ FIFOPool capital management and SEBI T+1 settlement."""
    def __init__(self, starting_capital: float = 25000.0, lot_size: int = 65):
        self.starting_capital = starting_capital
        self.current_equity = starting_capital
        self.lot_size = lot_size
        self.lot_cost_divisor = 10000.0
        self.max_strike_lots = 3
        self.enforce_conflict_filter = True

        self.realized_losses_today = 0.0
        self.realized_profits_today = 0.0
        self.t1_locked_profits = 0.0

        self.active_positions: List[PythonPosition] = []
        self.closed_positions: List[PythonPosition] = []
        self.trade_counter = 0

    def get_locked_margin(self) -> float:
        return sum(p.margin_locked for p in self.active_positions)

    def get_free_cash(self) -> float:
        # SEBI T+1: losses deducted immediately, profits locked until T+1
        free_c = self.starting_capital - self.get_locked_margin() - self.realized_losses_today
        return max(0.0, free_c)

    def get_realized_pnl_today(self) -> float:
        return self.realized_profits_today - self.realized_losses_today

    def get_total_portfolio_value(self) -> float:
        unrealized = sum(p.get_points() * p.remaining_lots * self.lot_size for p in self.active_positions)
        return self.starting_capital + self.get_realized_pnl_today() + unrealized

    def has_active_position_for_model(self, model_name: str) -> bool:
        return any(p.model_name == model_name for p in self.active_positions)

    def evaluate_and_allocate(
        self,
        model_name: str,
        symbol: str,
        strike: int,
        option_type: str,
        option_ask: float,
        fut_price: float,
        sl_fut: float,
        tp_fut: float,
        time_str: str,
    ) -> Tuple[bool, Optional[PythonPosition], str]:
        # 1. Directional Conflict Filter & Model Reversal Flip
        if self.enforce_conflict_filter:
            opp_type = "PE" if option_type == "CE" else "CE"
            opp_positions = [p for p in self.active_positions if p.option_type == opp_type]
            has_other_model_opp = any(p.model_name != model_name for p in opp_positions)

            if opp_positions:
                if not has_other_model_opp:
                    # Same model trend reversal: close opposing position immediately
                    for opp_pos in opp_positions:
                        self.close_position(
                            opp_pos.position_id,
                            opp_pos.current_option_price,
                            fut_price,
                            f"Model {model_name} Trend Reversal Flip -> {option_type}",
                            time_str
                        )
                else:
                    return False, None, f"BLOCKED: Directional conflict (Long {opp_type} active)"

        # 2. Strike-Level Concentration Cap
        current_strike_lots = sum(p.lots for p in self.active_positions if p.strike == strike and p.option_type == option_type)
        if current_strike_lots >= self.max_strike_lots:
            return False, None, f"REJECTED: Max strike concentration reached ({current_strike_lots}/{self.max_strike_lots})"

        # 3. FIFO Margin Sizing Calculation
        avail_cash = self.get_free_cash()
        cost_per_lot = option_ask * self.lot_size

        if avail_cash < cost_per_lot or avail_cash < 1000.0:
            return False, None, f"REJECTED: Insufficient margin (Free Cash: Rs {avail_cash:.2f} < 1 Lot Cost: Rs {cost_per_lot:.2f})"

        lots_by_capital = int(math.floor(avail_cash / self.lot_cost_divisor))
        lots_by_cost = int(math.floor(avail_cash / cost_per_lot))
        lots = min(lots_by_capital, lots_by_cost)
        lots = min(lots, self.max_strike_lots - current_strike_lots)

        if lots <= 0:
            if avail_cash >= cost_per_lot:
                lots = 1
            else:
                return False, None, f"REJECTED: Margin sizing yielded 0 lots (Free Cash: Rs {avail_cash:.2f})"

        margin_required = lots * cost_per_lot
        self.trade_counter += 1
        pid = f"UP_{self.trade_counter}"

        pos = PythonPosition(
            position_id=pid,
            model_name=model_name,
            symbol=symbol,
            strike=strike,
            option_type=option_type,
            lots=lots,
            quantity=lots * self.lot_size,
            entry_time=time_str,
            entry_option_price=option_ask,
            entry_futures_price=fut_price,
            current_option_price=option_ask,
            current_futures_price=fut_price,
            margin_locked=margin_required,
            sl_futures_price=sl_fut,
            tp_futures_price=tp_fut,
            remaining_lots=lots,
            is_active=True,
        )
        self.active_positions.append(pos)
        return True, pos, "ALLOCATED"

    def bank_lot1(self, position_id: str, exit_opt_price: float, exit_fut_price: float, exit_time: str) -> bool:
        for p in self.active_positions:
            if p.position_id == position_id and not p.t1_hit:
                p.t1_hit = True
                p.lot1_exit_time = exit_time
                p.lot1_exit_price = exit_opt_price
                lot1_pts = exit_opt_price - p.entry_option_price - 1.0
                p.lot1_pnl = lot1_pts * self.lot_size
                p.margin_locked -= p.entry_option_price * self.lot_size
                p.remaining_lots = max(0, p.remaining_lots - 1)

                if p.lot1_pnl >= 0:
                    self.realized_profits_today += p.lot1_pnl
                    self.t1_locked_profits += p.lot1_pnl
                else:
                    self.realized_losses_today += abs(p.lot1_pnl)
                return True
        return False

    def close_position(
        self,
        position_id: str,
        exit_opt_price: float,
        exit_fut_price: float,
        reason: str,
        exit_time: str,
    ) -> bool:
        for idx, p in enumerate(self.active_positions):
            if p.position_id == position_id:
                p.exit_time = exit_time
                p.exit_option_price = exit_opt_price
                p.exit_futures_price = exit_fut_price
                p.exit_reason = reason
                p.is_active = False

                pts = exit_opt_price - p.entry_option_price - 1.0
                rem_pnl = pts * (p.remaining_lots * self.lot_size)
                tot_pnl = rem_pnl + (p.lot1_pnl if p.t1_hit else 0.0)
                p.realized_pnl = tot_pnl

                if rem_pnl >= 0:
                    self.realized_profits_today += rem_pnl
                else:
                    self.realized_losses_today += abs(rem_pnl)

                p.margin_locked = 0.0
                self.closed_positions.append(p)
                self.active_positions.pop(idx)
                return True
        return False


class TriModelPythonEngine:
    """
    Complete streaming Python Tri-Model Options Portfolio Engine.
    Executes on 1-minute bars using real-time Redis data with 100% C++ StrategyEngine parity.
    """
    def __init__(self, redis_client: redis.Redis, starting_capital: float = 25000.0):
        self.r = redis_client
        self.pool = PythonFIFOPool(starting_capital=starting_capital)

        # Model POC V2 State
        self.m_prev_poc: float = 0.0
        self.m_poc_initialized: bool = False
        self.m_last_evaluated_minute: int = 0
        self.m_pending_poc_signal = {"has_signal": False, "chosen_type": "CE", "regime": ""}

        # Model Spatial Box State
        self.m_box_high: float = 0.0
        self.m_box_low: float = 0.0
        self.m_box_anchor_cvd: float = 0.0
        self.m_box_anchor_pv: float = 0.0
        self.m_box_anchor_vol: float = 0.0
        self.m_cum_pv: float = 0.0
        self.m_cum_vol: float = 0.0
        self.m_box_armed: bool = False
        self.m_box_armed_dir: int = 0  # +1 for UP (CE), -1 for DOWN (PE)
        self.m_box_initialized: bool = False
        self.m_box_last_exit_minute: int = -20

        # Causal Dalton VA State
        self.m_tpo_bracket_masks: Dict[int, int] = defaultdict(int)  # bin_idx -> bitmask
        self.m_ib_locked: bool = False
        self.m_ib_high: float = 0.0
        self.m_ib_low: float = 1e9
        self.m_ib_poc: float = 0.0
        self.m_ib_vah: float = 0.0
        self.m_ib_val: float = 0.0
        self.m_was_below_val: bool = False
        self.m_was_above_vah: bool = False
        self.m_cached_pcr: float = 1.0
        self.m_cached_pcr_time: float = 0.0

        # Front expiry cache
        self.m_cached_front_expiry: str = ""
        self.init_front_expiry()

        # Strategy toggles
        self.enable_model_poc_v2 = True
        self.enable_model_spatial_box = True
        self.enable_model_dalton_va = True

    def init_front_expiry(self) -> str:
        """Finds front weekly option chain in Redis (chain:NIFTY:YYYY-MM-DD)."""
        today_date = datetime.date.today().strftime("%Y-%m-%d")
        min_chain_key = f"chain:NIFTY:{today_date}"
        try:
            keys = self.r.keys("chain:NIFTY:*")
            if keys:
                chains = [k for k in keys if k >= min_chain_key and not k.endswith(":meta")]
                if not chains:
                    chains = [k for k in keys if not k.endswith(":meta")]
                chains.sort()
                if chains:
                    self.m_cached_front_expiry = chains[0]
                    return self.m_cached_front_expiry
        except Exception as e:
            print(f"[PythonEngine] Error querying Redis keys: {e}")
        return ""

    def get_current_dte(self) -> int:
        if not self.m_cached_front_expiry:
            self.init_front_expiry()
        if not self.m_cached_front_expiry or ":" not in self.m_cached_front_expiry:
            return 2
        exp_date_str = self.m_cached_front_expiry.split(":")[-1]
        try:
            exp_d = datetime.datetime.strptime(exp_date_str, "%Y-%m-%d").date()
            today_d = datetime.date.today()
            return max(0, (exp_d - today_d).days)
        except Exception:
            return 2

    def resolve_option_instrument_key(self, strike: int, option_type: str) -> str:
        if not self.m_cached_front_expiry:
            self.init_front_expiry()
        if not self.m_cached_front_expiry:
            return ""
        field = f"{strike}:{option_type}"
        tok = self.r.hget(self.m_cached_front_expiry, field)
        return tok if tok else ""

    def resolve_option_price(self, strike: int, option_type: str, is_ask: bool) -> float:
        tok = self.resolve_option_instrument_key(strike, option_type)
        if tok:
            field = "ask" if is_ask else "bid"
            val = self.r.hget(f"md:quote:{tok}", field)
            if val is not None and float(val) > 0.0:
                return float(val)
            ltp_val = self.r.hget(f"md:quote:{tok}", "ltp")
            if ltp_val is not None and float(ltp_val) > 0.0:
                return float(ltp_val)
        return 0.0

    def resolve_target_strike(self, fut_price: float, option_type: str, target_premium: float = 155.0) -> Tuple[int, str, float, float]:
        """Scans candidate strikes in Redis with ask >= 15.0 INR, picking closest <= 155.0 INR."""
        if not self.m_cached_front_expiry:
            self.init_front_expiry()
        atm = int(round(fut_price / 50.0)) * 50

        if not self.m_cached_front_expiry:
            ask = self.resolve_option_price(atm, option_type, True)
            bid = self.resolve_option_price(atm, option_type, False)
            return atm, "", bid, ask

        type_suffix = f":{option_type}"
        chain_map = self.r.hgetall(self.m_cached_front_expiry)
        candidates = []

        for field, token in chain_map.items():
            if field.endswith(type_suffix):
                try:
                    strike_str = field[:-len(type_suffix)]
                    strike = int(strike_str)
                    q = self.r.hmget(f"md:quote:{token}", ["ask", "bid", "ltp"])
                    ask = float(q[0]) if q[0] else 0.0
                    bid = float(q[1]) if q[1] else 0.0
                    ltp = float(q[2]) if q[2] else 0.0
                    if ask <= 0.0 and ltp > 0.0:
                        ask = ltp
                    if bid <= 0.0:
                        bid = ask

                    # Aligned filter: ask >= 15.0 INR
                    if ask >= 15.0:
                        candidates.append((strike, token, bid, ask))
                except Exception:
                    continue

        if not candidates:
            ask = self.resolve_option_price(atm, option_type, True)
            bid = self.resolve_option_price(atm, option_type, False)
            return atm, "", bid, ask

        under_prem = [c for c in candidates if c[3] <= target_premium]
        if under_prem:
            chosen = max(under_prem, key=lambda c: c[3])
        else:
            chosen = min(candidates, key=lambda c: c[3])

        return chosen[0], chosen[1], chosen[2], chosen[3]

    def get_box_avwap(self, current_tp: float) -> float:
        vol_delta = self.m_cum_vol - self.m_box_anchor_vol
        pv_delta = self.m_cum_pv - self.m_box_anchor_pv
        if vol_delta > 0.0:
            return pv_delta / vol_delta
        return current_tp

    def get_tpo_bracket_index(self, time_str: str) -> int:
        if len(time_str) < 5:
            return 0
        try:
            h, m = int(time_str[:2]), int(time_str[3:5])
            mins = (h - 9) * 60 + m - 15
            if mins < 0:
                return 0
            return min(mins // 30, 12)
        except Exception:
            return 0

    def update_tpo_profile(self, high: float, low: float, bracket_idx: int):
        if bracket_idx < 0 or bracket_idx > 12 or high <= 0 or low <= 0:
            return
        TPO_BASE_PRICE = 20000.0
        TPO_BIN_SIZE = 20.0
        min_bin = max(0, min(349, int(math.floor((low - TPO_BASE_PRICE) / TPO_BIN_SIZE))))
        max_bin = max(0, min(349, int(math.floor((high - TPO_BASE_PRICE) / TPO_BIN_SIZE))))
        bit = 1 << bracket_idx

        for b in range(min_bin, max_bin + 1):
            self.m_tpo_bracket_masks[b] |= bit

    def lock_initial_balance_value_area(self):
        self.m_ib_locked = True
        TPO_BASE_PRICE = 20000.0
        TPO_BIN_SIZE = 20.0

        tot_ib_tpos = 0
        max_ib_tpos = 0
        ib_poc_bin = -1
        active_bins = []

        for b in range(350):
            # Periods A (bit 0) and B (bit 1)
            cnt = bin(self.m_tpo_bracket_masks[b] & 0x03).count("1")
            if cnt > 0:
                tot_ib_tpos += cnt
                active_bins.append(b)
                if cnt > max_ib_tpos:
                    max_ib_tpos = cnt
                    ib_poc_bin = b

        if tot_ib_tpos > 0 and ib_poc_bin >= 0:
            self.m_ib_poc = TPO_BASE_PRICE + (ib_poc_bin + 0.5) * TPO_BIN_SIZE
            target_tpos = tot_ib_tpos * 0.70
            cur_tpos = bin(self.m_tpo_bracket_masks[ib_poc_bin] & 0x03).count("1")
            min_va_bin = ib_poc_bin
            max_va_bin = ib_poc_bin

            poc_idx = active_bins.index(ib_poc_bin)
            u = poc_idx + 1
            d = poc_idx - 1

            while cur_tpos < target_tpos and (u < len(active_bins) or d >= 0):
                u_val = bin(self.m_tpo_bracket_masks[active_bins[u]] & 0x03).count("1") if u < len(active_bins) else 0
                d_val = bin(self.m_tpo_bracket_masks[active_bins[d]] & 0x03).count("1") if d >= 0 else 0

                if u_val >= d_val and u_val > 0:
                    cur_tpos += u_val
                    max_va_bin = max(max_va_bin, active_bins[u])
                    u += 1
                elif d_val > 0:
                    cur_tpos += d_val
                    min_va_bin = min(min_va_bin, active_bins[d])
                    d -= 1
                else:
                    break

            self.m_ib_vah = TPO_BASE_PRICE + (max_va_bin + 1.0) * TPO_BIN_SIZE
            self.m_ib_val = TPO_BASE_PRICE + min_va_bin * TPO_BIN_SIZE
        else:
            self.m_ib_poc = (self.m_ib_high + self.m_ib_low) / 2.0 if self.m_ib_high > 0 else 0.0
            self.m_ib_vah = self.m_ib_high
            self.m_ib_val = self.m_ib_low

        print(f"🔒 [Python Dalton VA] IB Locked at 10:15 -> High: {self.m_ib_high:.1f}, Low: {self.m_ib_low:.1f}, "
              f"POC: {self.m_ib_poc:.1f}, VAH: {self.m_ib_vah:.1f}, VAL: {self.m_ib_val:.1f}")

    def calculate_chain_pcr(self) -> float:
        now_ts = time.time()
        if self.m_cached_pcr_time > 0 and (now_ts - self.m_cached_pcr_time) < 15.0:
            return self.m_cached_pcr

        if not self.m_cached_front_expiry:
            self.init_front_expiry()
        if not self.m_cached_front_expiry:
            return 1.0

        chain_map = self.r.hgetall(self.m_cached_front_expiry)
        tot_pe_oi = 0
        tot_ce_oi = 0

        pipe = self.r.pipeline(transaction=False)
        tokens_type = []
        for field, tok in chain_map.items():
            if field.endswith(":PE") or field.endswith(":CE"):
                is_pe = field.endswith(":PE")
                tokens_type.append((tok, is_pe))
                pipe.hget(f"md:quote:{tok}", "oi")

        res = pipe.execute()
        for i, oi_str in enumerate(res):
            if oi_str:
                try:
                    oi_val = int(oi_str)
                    if oi_val > 0:
                        if tokens_type[i][1]:
                            tot_pe_oi += oi_val
                        else:
                            tot_ce_oi += oi_val
                except Exception:
                    pass

        pcr = (float(tot_pe_oi) / float(tot_ce_oi)) if tot_ce_oi > 0 else 1.0
        self.m_cached_pcr = pcr
        self.m_cached_pcr_time = now_ts
        return pcr

    def check_active_exits(
        self,
        fut_p: float,
        high: float,
        low: float,
        session_vwap: float,
        cvd_15m: float,
        time_str: str,
        is_bar_close: bool = True
    ):
        if not self.pool.active_positions:
            return

        for p in self.pool.active_positions:
            opt_bid = self.resolve_option_price(p.strike, p.option_type, False)
            if opt_bid > 0.0:
                p.current_option_price = opt_bid
                p.current_futures_price = fut_p
            if is_bar_close:
                p.bars_held += 1

        active_copy = list(self.pool.active_positions)
        dte = self.get_current_dte()
        delta_est = 0.75 if dte <= 1 else 0.50

        for pos in active_copy:
            exit_triggered = False
            exit_reason = ""
            exit_fut_price = fut_p
            exit_opt_price = pos.current_option_price if pos.current_option_price > 0.0 else pos.entry_option_price

            # 1. Hard EOD Square-off at 15:20 IST
            if time_str >= "15:20":
                exit_triggered = True
                exit_reason = "EOD Square-Off (15:20 IST)"
                exit_fut_price = fut_p
                exit_opt_price = pos.current_option_price

            # 2. Model POC V2
            elif pos.model_name in ["Model POC V2", "ModelPOCV2", "POC V2"]:
                just_banked_t1 = False
                # A. Check Lot 1 Target (+30pt FUT)
                if not pos.t1_hit:
                    mfe = (high - pos.entry_futures_price) if pos.option_type == "CE" else (pos.entry_futures_price - low)
                    if mfe >= 30.0:
                        exit_fut = (pos.entry_futures_price + 30.0) if pos.option_type == "CE" else (pos.entry_futures_price - 30.0)
                        opt_bid = self.resolve_option_price(pos.strike, pos.option_type, False)
                        if opt_bid <= 0.0:
                            opt_bid = max(0.5, pos.entry_option_price + (30.0 * delta_est))
                        self.pool.bank_lot1(pos.position_id, opt_bid, exit_fut, time_str)
                        pos.t1_hit = True
                        just_banked_t1 = True
                        pos.lot2_sl_futures_price = (pos.entry_futures_price + 2.0) if pos.option_type == "CE" else (pos.entry_futures_price - 2.0)
                        print(f"🎯 [Python POC V2] Banked Lot 1 @ +30pt FUT | Lot 2 SL locked at Breakeven ({pos.lot2_sl_futures_price:.1f})")

                # B. If T1 NOT hit: Check Initial SL (-20pt FUT)
                if not pos.t1_hit:
                    hit_sl = (pos.option_type == "CE" and low <= (pos.entry_futures_price - 20.0)) or \
                             (pos.option_type == "PE" and high >= (pos.entry_futures_price + 20.0))
                    if hit_sl:
                        exit_triggered = True
                        exit_reason = "Initial SL (-20pt FUT)"
                        exit_fut_price = (pos.entry_futures_price - 20.0) if pos.option_type == "CE" else (pos.entry_futures_price + 20.0)
                        fb_bid = max(0.5, pos.entry_option_price - (20.0 * delta_est))
                        opt_bid = self.resolve_option_price(pos.strike, pos.option_type, False)
                        exit_opt_price = min(opt_bid, fb_bid) if opt_bid > 0.0 else fb_bid
                    elif time_str >= "15:20":
                        exit_triggered = True
                        exit_reason = "EOD Squareoff"
                        exit_fut_price = fut_p
                        exit_opt_price = pos.current_option_price

                # C. If T1 WAS hit: Manage Lot 2 Trailing Session VWAP
                elif pos.t1_hit and not just_banked_t1:
                    if pos.option_type == "CE":
                        active_sl = max(pos.lot2_sl_futures_price, (session_vwap - 5.0) if session_vwap > 0 else pos.lot2_sl_futures_price)
                        if low <= active_sl:
                            exit_triggered = True
                            exit_reason = "VWAP Trail Exit"
                            exit_fut_price = active_sl
                            fut_pts = exit_fut_price - pos.entry_futures_price
                            fb_bid = max(0.5, pos.entry_option_price + (fut_pts * delta_est))
                            opt_bid = self.resolve_option_price(pos.strike, pos.option_type, False)
                            exit_opt_price = opt_bid if opt_bid > 0.0 else fb_bid
                    else:
                        active_sl = min(pos.lot2_sl_futures_price, (session_vwap + 5.0) if session_vwap > 0 else pos.lot2_sl_futures_price)
                        if high >= active_sl:
                            exit_triggered = True
                            exit_reason = "VWAP Trail Exit"
                            exit_fut_price = active_sl
                            fut_pts = pos.entry_futures_price - exit_fut_price
                            fb_bid = max(0.5, pos.entry_option_price + (fut_pts * delta_est))
                            opt_bid = self.resolve_option_price(pos.strike, pos.option_type, False)
                            exit_opt_price = opt_bid if opt_bid > 0.0 else fb_bid

            # 3. Model Spatial Box
            elif pos.model_name == "Model Spatial Box":
                opt_bid = self.resolve_option_price(pos.strike, pos.option_type, False)
                if opt_bid <= 0.0:
                    opt_bid = pos.current_option_price

                if opt_bid >= pos.target_opt_price:
                    exit_triggered = True
                    exit_reason = "Target Reached (+45pt)"
                    exit_opt_price = pos.target_opt_price
                    exit_fut_price = fut_p
                elif opt_bid <= pos.sl_opt_price:
                    exit_triggered = True
                    exit_reason = "Stop Loss Hit (-15pt)"
                    exit_opt_price = pos.sl_opt_price
                    exit_fut_price = fut_p
                elif pos.bars_held >= 45:
                    exit_triggered = True
                    exit_reason = "Time Exit (45m)"
                    exit_opt_price = opt_bid
                    exit_fut_price = fut_p

            # 4. Model Dalton VA
            elif pos.model_name == "Causal Dalton VA":
                if pos.option_type == "CE":
                    if fut_p >= pos.tpo_target_futures:
                        exit_triggered = True
                        exit_reason = "Target Reached (VAH)"
                        exit_fut_price = pos.tpo_target_futures
                        exit_opt_price = self.resolve_option_price(pos.strike, pos.option_type, False)
                    elif fut_p <= pos.sl_futures_price:
                        exit_triggered = True
                        exit_reason = "Stop Loss Hit (VAL - 15pt)"
                        exit_fut_price = pos.sl_futures_price
                        exit_opt_price = self.resolve_option_price(pos.strike, pos.option_type, False)
                    elif time_str >= "13:30":
                        exit_triggered = True
                        exit_reason = "Window Close (13:30 IST)"
                        exit_fut_price = fut_p
                        exit_opt_price = self.resolve_option_price(pos.strike, pos.option_type, False)
                else:  # PE
                    if fut_p <= pos.tpo_target_futures:
                        exit_triggered = True
                        exit_reason = "Target Reached (VAL)"
                        exit_fut_price = pos.tpo_target_futures
                        exit_opt_price = self.resolve_option_price(pos.strike, pos.option_type, False)
                    elif fut_p >= pos.sl_futures_price:
                        exit_triggered = True
                        exit_reason = "Stop Loss Hit (VAH + 15pt)"
                        exit_fut_price = pos.sl_futures_price
                        exit_opt_price = self.resolve_option_price(pos.strike, pos.option_type, False)
                    elif time_str >= "13:30":
                        exit_triggered = True
                        exit_reason = "Window Close (13:30 IST)"
                        exit_fut_price = fut_p
                        exit_opt_price = self.resolve_option_price(pos.strike, pos.option_type, False)

            if exit_triggered:
                exit_opt = exit_opt_price
                if exit_opt <= 0.0:
                    exit_opt = self.resolve_option_price(pos.strike, pos.option_type, False)
                if exit_opt <= 0.0:
                    fut_pts = (exit_fut_price - pos.entry_futures_price) if pos.option_type == "CE" else (pos.entry_futures_price - exit_fut_price)
                    exit_opt = max(0.5, pos.entry_option_price + (fut_pts * delta_est))

                self.pool.close_position(pos.position_id, exit_opt, exit_fut_price, exit_reason, time_str)
                if pos.model_name == "Model Spatial Box":
                    h, m = int(time_str[:2]), int(time_str[3:5])
                    self.m_box_last_exit_minute = h * 60 + m
                print(f"🏁 [Python Forward Tester] CLOSED {pos.position_id} ({pos.model_name}) | "
                      f"Strike: {pos.strike} {pos.option_type} | Exit Bid: Rs {exit_opt:.2f} | Reason: {exit_reason} @ {time_str}")

    def evaluate_model_poc_v2(self, bar_dict: dict, metrics_dict: dict, time_str: str, minute_ts: int):
        curr_poc = float(metrics_dict.get("dpoc", 0.0))
        if curr_poc <= 0.0:
            return

        if not self.m_poc_initialized:
            self.m_prev_poc = curr_poc
            self.m_poc_initialized = True
            return

        if curr_poc == self.m_prev_poc:
            return

        poc_shift = curr_poc - self.m_prev_poc
        old_poc = self.m_prev_poc
        self.m_prev_poc = curr_poc  # Continuous tracking

        delta_p = float(metrics_dict.get("delta_price_15m", 0.0))
        delta_oi = float(metrics_dict.get("delta_oi_15m", 0.0))
        cvd_15m = float(metrics_dict.get("cvd_15m", 0.0))

        if delta_p >= 0.0 and delta_oi >= 0.0:
            oi_regime = "Long Buildup"
        elif delta_p >= 0.0 and delta_oi < 0.0:
            oi_regime = "Short Covering"
        elif delta_p < 0.0 and delta_oi >= 0.0:
            oi_regime = "Short Buildup"
        else:
            oi_regime = "Long Unwinding"

        chosen_type = "CE"
        has_signal = False
        regime = ""

        if poc_shift > 0.0:
            if cvd_15m < 0.0 or oi_regime in ["Short Buildup", "Long Unwinding"]:
                chosen_type = "PE"
                has_signal = True
                regime = f"Bull Trap Fade (UP Jump + CVD Neg / {oi_regime})"
            elif cvd_15m > 0.0 and oi_regime == "Long Buildup":
                chosen_type = "CE"
                has_signal = True
                regime = "True Bull Breakout (UP Jump + CVD Pos + Long Buildup)"
        elif poc_shift < 0.0:
            if cvd_15m > 0.0 or oi_regime == "Long Buildup":
                chosen_type = "CE"
                has_signal = True
                regime = f"Absorption Bottom (DOWN Jump + CVD Pos / {oi_regime})"
            elif cvd_15m < 0.0 and oi_regime == "Short Buildup":
                chosen_type = "PE"
                has_signal = True
                regime = "True Bear Breakdown (DOWN Jump + CVD Neg + Short Buildup)"

        print(f"🔍 [Python POC V2] POC Shift @ {time_str} | Shift: {poc_shift:+.1f} pts (New: {curr_poc:.1f}, Old: {old_poc:.1f}) | "
              f"CVD 15m: {cvd_15m:.0f} | Delta P: {delta_p:.1f} | Delta OI: {delta_oi:.0f} | Regime: {oi_regime} | "
              f"Signal: {regime if has_signal else 'None'}")

        if not has_signal:
            return

        # Queue signal for execution at the open of next bar (bar.minute_ts + 60)
        self.m_pending_poc_signal = {
            "has_signal": True,
            "chosen_type": chosen_type,
            "regime": regime
        }

    def execute_pending_poc_signal(self, bar_dict: dict, time_str: str):
        if not self.m_pending_poc_signal.get("has_signal", False):
            return

        entry_fut_price = float(bar_dict["open"])
        chosen_type = self.m_pending_poc_signal["chosen_type"]
        regime = self.m_pending_poc_signal["regime"]

        # Check reversal exit for opposite position
        for pos in list(self.pool.active_positions):
            if pos.model_name == "Model POC V2" and pos.is_active and pos.option_type != chosen_type:
                fut_pts = (entry_fut_price - pos.entry_futures_price) if pos.option_type == "CE" else (pos.entry_futures_price - entry_fut_price)
                dte = self.get_current_dte()
                delta_est = 0.75 if dte <= 1 else 0.50
                fb_bid = max(0.5, pos.entry_option_price + (fut_pts * delta_est))
                opt_bid = self.resolve_option_price(pos.strike, pos.option_type, False)
                exit_opt = opt_bid if opt_bid > 0.0 else fb_bid
                self.pool.close_position(pos.position_id, exit_opt, entry_fut_price, "Reversal Exit", time_str)
                print(f"🔄 [Python POC V2] Reversal Exit {pos.position_id} @ {time_str} | Exit Bid: Rs {exit_opt:.2f}")

        strike, tok, bid, ask = self.resolve_target_strike(entry_fut_price, chosen_type, 155.0)
        entry_ask = ask if ask > 0.0 else 155.0

        sl_fut = (entry_fut_price - 20.0) if chosen_type == "CE" else (entry_fut_price + 20.0)
        tp_fut = (entry_fut_price + 30.0) if chosen_type == "CE" else (entry_fut_price - 30.0)

        allocated, out_pos, reason = self.pool.evaluate_and_allocate(
            model_name="Model POC V2",
            symbol=f"NIFTY_{strike}_{chosen_type}",
            strike=strike,
            option_type=chosen_type,
            option_ask=entry_ask,
            fut_price=entry_fut_price,
            sl_fut=sl_fut,
            tp_fut=tp_fut,
            time_str=time_str
        )

        if allocated and out_pos:
            print(f"🚀 [Python POC V2] ALLOCATED {out_pos.position_id} ({out_pos.lots} lots) | "
                  f"Strike: {strike} {chosen_type} @ Rs {entry_ask:.2f} | Margin: Rs {out_pos.margin_locked:.2f} | "
                  f"{regime} @ {time_str}")
        else:
            print(f"⚠️ [Python POC V2] Allocation rejected @ {time_str} | Reason: {reason}")

        self.m_pending_poc_signal["has_signal"] = False

    def evaluate_model_spatial_box(self, bar_dict: dict, metrics_dict: dict, time_str: str, minute_ts: int):
        high_p = float(bar_dict["high"])
        low_p = float(bar_dict["low"])
        close_p = float(bar_dict["close"])
        vol = float(bar_dict["volume"])
        tp = (high_p + low_p + close_p) / 3.0

        self.m_cum_pv += tp * vol
        self.m_cum_vol += vol

        cum_cvd = float(metrics_dict.get("cum_cvd", 0.0))

        if not self.m_box_initialized:
            self.m_box_initialized = True
            self.m_box_high = high_p
            self.m_box_low = low_p
            self.m_box_anchor_cvd = cum_cvd
            self.m_box_anchor_pv = self.m_cum_pv
            self.m_box_anchor_vol = self.m_cum_vol
            self.m_box_armed = False
            self.m_box_armed_dir = 0
            return

        if self.pool.has_active_position_for_model("Model Spatial Box"):
            return

        self.m_box_high = max(self.m_box_high, high_p)
        self.m_box_low = min(self.m_box_low, low_p)
        box_range = self.m_box_high - self.m_box_low
        box_avwap = self.get_box_avwap(tp)

        # 50-pt Compression & AVWAP Overshoot Invalidation Checks
        should_reset = (box_range > 50.0)
        if not should_reset and self.m_box_armed:
            if self.m_box_armed_dir == 1 and (close_p - box_avwap) > 15.0:
                should_reset = True
            elif self.m_box_armed_dir == -1 and (box_avwap - close_p) > 15.0:
                should_reset = True

        if should_reset:
            self.m_box_high = high_p
            self.m_box_low = low_p
            self.m_box_anchor_cvd = cum_cvd
            self.m_box_anchor_pv = self.m_cum_pv
            self.m_box_anchor_vol = self.m_cum_vol
            self.m_box_armed = False
            self.m_box_armed_dir = 0
        else:
            delta_cvd = cum_cvd - self.m_box_anchor_cvd
            if delta_cvd >= 55000.0:
                if close_p <= box_avwap + 15.0:
                    self.m_box_armed = True
                    self.m_box_armed_dir = 1
            elif delta_cvd <= -55000.0:
                if close_p >= box_avwap - 15.0:
                    self.m_box_armed = True
                    self.m_box_armed_dir = -1

        h, m = int(time_str[:2]), int(time_str[3:5])
        cur_min = h * 60 + m

        if self.m_box_armed and (cur_min - self.m_box_last_exit_minute >= 15):
            chosen_type = "CE"
            triggered = False
            regime = ""

            if self.m_box_armed_dir == 1 and close_p >= self.m_box_high:
                chosen_type = "CE"
                triggered = True
                regime = "50-pt Box Breakout + AVWAP Arm Gate (CE)"
            elif self.m_box_armed_dir == -1 and close_p <= self.m_box_low:
                chosen_type = "PE"
                triggered = True
                regime = "50-pt Box Breakdown + AVWAP Arm Gate (PE)"

            if triggered:
                strike, tok, bid, ask = self.resolve_target_strike(close_p, chosen_type, 155.0)
                entry_ask = ask if ask > 0.0 else 155.0

                next_m = m + 1
                next_h = h
                if next_m >= 60:
                    next_m = 0
                    next_h += 1
                exec_time_str = f"{next_h:02d}:{next_m:02d}"

                allocated, out_pos, reason = self.pool.evaluate_and_allocate(
                    model_name="Model Spatial Box",
                    symbol=f"NIFTY_{strike}_{chosen_type}",
                    strike=strike,
                    option_type=chosen_type,
                    option_ask=entry_ask,
                    fut_price=close_p,
                    sl_fut=(close_p - 20.0) if chosen_type == "CE" else (close_p + 20.0),
                    tp_fut=(close_p + 45.0) if chosen_type == "CE" else (close_p - 45.0),
                    time_str=exec_time_str
                )

                if allocated and out_pos:
                    out_pos.target_opt_price = entry_ask + 45.0
                    out_pos.sl_opt_price = entry_ask - 15.0
                    out_pos.active_box_avwap = box_avwap

                    self.m_box_armed = False
                    self.m_box_armed_dir = 0
                    self.m_box_high = high_p
                    self.m_box_low = low_p
                    self.m_box_anchor_cvd = cum_cvd
                    self.m_box_anchor_pv = self.m_cum_pv
                    self.m_box_anchor_vol = self.m_cum_vol

                    print(f"🚀 [Python Spatial Box] ALLOCATED {out_pos.position_id} ({out_pos.lots} lots) | "
                          f"Strike: {strike} {chosen_type} @ Rs {entry_ask:.2f} | Margin: Rs {out_pos.margin_locked:.2f} | "
                          f"{regime} @ {exec_time_str}")
                else:
                    print(f"⚠️ [Python Spatial Box] Allocation rejected @ {exec_time_str} | Reason: {reason}")

    def evaluate_model_dalton_va(self, bar_dict: dict, metrics_dict: dict, time_str: str, minute_ts: int):
        if not self.m_ib_locked:
            if time_str >= "10:15":
                self.lock_initial_balance_value_area()
            else:
                return

        if time_str < "10:15" or time_str > "13:30":
            return

        if self.pool.has_active_position_for_model("Causal Dalton VA"):
            return

        high_p = float(bar_dict["high"])
        low_p = float(bar_dict["low"])
        close_p = float(bar_dict["close"])
        cvd_15m = float(metrics_dict.get("cvd_15m", 0.0))

        if low_p <= self.m_ib_val - 5.0:
            self.m_was_below_val = True
        if high_p >= self.m_ib_vah + 5.0:
            self.m_was_above_vah = True

        chosen_type = "CE"
        triggered = False
        regime = ""
        target_fut = 0.0
        sl_fut = 0.0

        if self.m_was_below_val and close_p >= self.m_ib_val and cvd_15m > 0.0:
            pcr = self.calculate_chain_pcr()
            if pcr < 0.70:
                print(f"⚠️ [Python Dalton VA] REJECTED LONG CE: PCR {pcr:.2f} < 0.70 @ {time_str}")
                return
            chosen_type = "CE"
            triggered = True
            target_fut = self.m_ib_vah
            sl_fut = self.m_ib_val - 15.0
            regime = "Dalton 80% Bullish VA Traverse (Buy CE)"

        elif self.m_was_above_vah and close_p <= self.m_ib_vah and cvd_15m < 0.0:
            pcr = self.calculate_chain_pcr()
            if pcr > 1.35:
                print(f"⚠️ [Python Dalton VA] REJECTED SHORT PE: PCR {pcr:.2f} > 1.35 @ {time_str}")
                return
            chosen_type = "PE"
            triggered = True
            target_fut = self.m_ib_val
            sl_fut = self.m_ib_vah + 15.0
            regime = "Dalton 80% Bearish VA Traverse (Buy PE)"

        if triggered:
            strike, tok, bid, ask = self.resolve_target_strike(close_p, chosen_type, 155.0)
            entry_ask = ask if ask > 0.0 else 155.0

            h, m = int(time_str[:2]), int(time_str[3:5])
            next_m = m + 1
            next_h = h
            if next_m >= 60:
                next_m = 0
                next_h += 1
            exec_time_str = f"{next_h:02d}:{next_m:02d}"

            allocated, out_pos, reason = self.pool.evaluate_and_allocate(
                model_name="Causal Dalton VA",
                symbol=f"NIFTY_{strike}_{chosen_type}",
                strike=strike,
                option_type=chosen_type,
                option_ask=entry_ask,
                fut_price=close_p,
                sl_fut=sl_fut,
                tp_fut=target_fut,
                time_str=exec_time_str
            )

            if allocated and out_pos:
                out_pos.dalton_ib_vah = self.m_ib_vah
                out_pos.dalton_ib_val = self.m_ib_val
                out_pos.dalton_ib_poc = self.m_ib_poc
                out_pos.dalton_pcr = self.m_cached_pcr
                out_pos.tpo_target_futures = target_fut

                if chosen_type == "CE":
                    self.m_was_below_val = False
                else:
                    self.m_was_above_vah = False

                print(f"🚀 [Python Dalton VA] ALLOCATED {out_pos.position_id} ({out_pos.lots} lots) | "
                      f"Strike: {strike} {chosen_type} @ Rs {entry_ask:.2f} | Margin: Rs {out_pos.margin_locked:.2f} | "
                      f"{regime} @ {exec_time_str} | PCR: {self.m_cached_pcr:.2f}")
            else:
                print(f"⚠️ [Python Dalton VA] Allocation rejected @ {exec_time_str} | Reason: {reason}")

    def on_1m_bar(self, bar_dict: dict, metrics_dict: dict, minute_ts: int):
        ist = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
        time_str = datetime.datetime.fromtimestamp(minute_ts, tz=ist).strftime("%H:%M")

        bracket_idx = self.get_tpo_bracket_index(time_str)
        high_p = float(bar_dict["high"])
        low_p = float(bar_dict["low"])
        self.update_tpo_profile(high_p, low_p, bracket_idx)

        # Track Initial Balance High & Low
        if not self.m_ib_locked:
            if time_str < "10:15":
                if high_p > self.m_ib_high:
                    self.m_ib_high = high_p
                if low_p < self.m_ib_low:
                    self.m_ib_low = low_p
            else:
                self.lock_initial_balance_value_area()

        # 0. Execute pending POC V2 signal on bar open
        if self.m_pending_poc_signal.get("has_signal", False):
            self.execute_pending_poc_signal(bar_dict, time_str)

        # 1. Evaluate exits across all active positions
        session_vwap = float(metrics_dict.get("session_vwap", 0.0))
        cvd_15m = float(metrics_dict.get("cvd_15m", 0.0))
        close_p = float(bar_dict["close"])
        self.check_active_exits(close_p, high_p, low_p, session_vwap, cvd_15m, time_str, is_bar_close=True)

        if time_str >= "15:00":
            return

        # 2. Model POC V2
        if self.enable_model_poc_v2 and "09:20" <= time_str <= "10:30":
            self.evaluate_model_poc_v2(bar_dict, metrics_dict, time_str, minute_ts)

        # 3. Model Spatial Box
        if self.enable_model_spatial_box and "09:20" <= time_str <= "15:00":
            self.evaluate_model_spatial_box(bar_dict, metrics_dict, time_str, minute_ts)

        # 4. Model Dalton VA
        if self.enable_model_dalton_va and "10:15" <= time_str <= "13:30":
            self.evaluate_model_dalton_va(bar_dict, metrics_dict, time_str, minute_ts)
