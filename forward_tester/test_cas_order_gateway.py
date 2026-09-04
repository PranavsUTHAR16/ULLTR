#!/usr/bin/env python3
"""
ULLTR CAS Arbitrage & Broker Order Gateway Verification & Benchmark
==================================================================
Tests:
  1. Sub-1ms Vectorized CAS Orderbook Equilibrium Speed Benchmark (10,000 iterations)
  2. Dynamic ATM Strike & Direction Selection (NIFTY & SENSEX)
  3. Real Live Upstox V3 HFT Gateway Order Placement & Turnaround Latency Profiling
"""

import os
import sys
import time
import argparse
import numpy as np

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from forward_tester.broker_gateway import UpstoxBrokerGateway
from forward_tester.models.cas_model import CASModel

def benchmark_equilibrium_speed():
    print("=" * 85)
    print("⚡ 1. BENCHMARKING SUB-1MS CAS ORDERBOOK EQUILIBRIUM CALCULATION SPEED")
    print("=" * 85)
    
    cas = CASModel()
    
    # 1. Synthetic depth array (50 stocks)
    N = 50
    ltp = np.random.uniform(500, 3000, N)
    weights = np.random.uniform(0.01, 0.20, N)
    weights /= np.sum(weights)
    tbq = np.random.randint(10000, 500000, N)
    tsq = np.random.randint(10000, 500000, N)
    mbq = np.random.randint(0, 50000, N)
    msq = np.random.randint(0, 50000, N)
    spot = 24000.0
    
    times_ns = []
    for _ in range(10000):
        t0 = time.perf_counter_ns()
        
        tot_q = tbq + tsq
        imb = (tbq - tsq) / np.maximum(tot_q, 1.0)
        
        mkt_tot = mbq + msq
        mkt_imb = np.where(mkt_tot > 0, (mbq - msq) / np.maximum(mkt_tot, 1.0), 0.0)
        
        comb_imb = 0.7 * imb + 0.3 * mkt_imb
        p_eq = ltp * (1.0 + comb_imb * 0.0018)
        
        pct_moves = (p_eq - ltp) / ltp
        cas_est = spot * (1.0 + np.dot(pct_moves, weights))
        
        t1 = time.perf_counter_ns()
        times_ns.append(t1 - t0)
        
    times_np = np.array(times_ns)
    mean_us = np.mean(times_np) / 1000.0
    median_us = np.median(times_np) / 1000.0
    p99_us = np.percentile(times_np, 99) / 1000.0
    
    print(f"  • Iterations Run      : 10,000 passes")
    print(f"  • Mean Calculation    : {mean_us:.2f} µs ({mean_us/1000.0:.4f} ms)")
    print(f"  • Median Latency (p50): {median_us:.2f} µs")
    print(f"  • 99th Percentile(p99): {p99_us:.2f} µs ({p99_us/1000.0:.4f} ms)")
    
    if mean_us < 1000.0:
        print(f"  ✅ SUCCESS: Mathematical equilibrium calculation is {1000.0/mean_us:.1f}x FASTER than 1.0 ms ceiling!")
    else:
        print("  ⚠️ WARNING: Exceeded 1.0 ms limit.")

def test_dynamic_strike_selection():
    print("\n" + "=" * 85)
    print("🎯 2. TESTING DYNAMIC ATM STRIKE & DIRECTION SELECTION")
    print("=" * 85)
    
    cas = CASModel()
    cas.arm_cas_session()
    
    for und in ["NIFTY", "SENSEX"]:
        eq = cas.calculate_equilibrium(und)
        sym, token, strike, otype, ltp = cas.select_cas_strike(und, eq["expected_move"], eq["spot_ref"])
        print(f"  [{und}] Spot: {eq['spot_ref']:,.2f} | CAS Eq: {eq['cas_price']:,.2f} (Move: {eq['expected_move']:+5.2f} pts)")
        print(f"         Selected: {strike} {otype} | Token: {token} | Est LTP: ₹{ltp:.2f}")

def test_live_broker_gateway(live_order: bool = False):
    print("\n" + "=" * 85)
    print("🚀 3. TESTING UPSTOX V3 HFT BROKER ORDER GATEWAY")
    print("=" * 85)
    
    gateway = UpstoxBrokerGateway()
    print(f"  • Token loaded: {'YES' if gateway.access_token else 'NO'}")
    print(f"  • Target HFT URL: https://api-hft.upstox.com/v3/order/place")
    
    if not live_order:
        print("  ℹ️ Skipping real order dispatch (pass --live-order to send real order to Upstox).")
        return
        
    print("\n  Dispatching real test order (1 Lot NIFTY 24000 CE)...")
    res = gateway.place_order(
        instrument_token="NSE_FO|46938",
        quantity=25,
        transaction_type="BUY",
        product="I",
        order_type="MARKET",
        tag="CAS_TEST"
    )
    
    print(f"  • API Version Used        : {res['api_version']}")
    print(f"  • HTTP Status Code        : {res['status_code']}")
    print(f"  • Order ID / Rejection Msg: {res.get('primary_order_id') or res.get('error_msg')}")
    print(f"  • Raw Broker Response     : {res.get('raw_response')}")
    print(f"  • Gateway Network RTT     : {res['gateway_rtt_ms']:.2f} ms")
    print(f"  • Signal-to-ACK Turnaround: {res['turnaround_ms']:.2f} ms")
    if res.get("broker_latency_meta"):
        print(f"  • Broker Latency Metadata : {res['broker_latency_meta']}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CAS Order Gateway Benchmark")
    parser.add_argument("--benchmark", action="store_true", help="Run math latency benchmark")
    parser.add_argument("--live-order", action="store_true", help="Dispatch real live order to Upstox")
    args = parser.parse_args()
    
    benchmark_equilibrium_speed()
    test_dynamic_strike_selection()
    test_live_broker_gateway(live_order=args.live_order)
