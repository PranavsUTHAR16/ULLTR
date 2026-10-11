#!/usr/bin/env python3
"""Comprehensive Upstox API Sandbox Speed & Latency Benchmark on AWS EC2 (ap-south-1).

Measures real-world order execution performance:
1. Low-level Network Latencies: DNS, TCP (TCP_NODELAY), TLS handshake to Sandbox, HFT, and Live endpoints.
2. Cold vs Hot V3 Order Placement (`POST /v3/order/place`).
3. Persistent HTTP/2 connection pooling vs cold requests.
4. Order lifecycle: Place, Modify, Cancel, Retrieve orderbook.
5. V3 vs V2 placement comparison.
6. Client serialization vs network transport vs Upstox server-side matching latency (`metadata.latency`).
"""

import json
import os
import socket
import ssl
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx

SANDBOX_BASE_URL = "https://api-sandbox.upstox.com"
DEFAULT_TOKEN = (
    "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ."
    "eyJzdWIiOiI1TEJKVzMiLCJqdGkiOiI2YWNiODFmMTdkMDBjYzQ0OTY0M2JlMTQiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzkxNzIxOTY5LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3OTQyNjE2MDB9."
    "2Pg3NrUu8f79U_A8vuGr2xyXG0blYCAMzFEy2qYSMzI"
)


def measure_network_handshakes(host: str, port: int = 443, iterations: int = 5) -> Dict[str, Any]:
    """Measures DNS resolution, raw TCP connect, and TLS handshake latencies."""
    dns_times: List[float] = []
    tcp_times: List[float] = []
    tls_times: List[float] = []
    ip = None

    for _ in range(iterations):
        # 1. DNS resolution
        t0 = time.perf_counter()
        ip = socket.gethostbyname(host)
        dns_times.append((time.perf_counter() - t0) * 1000.0)

        # 2. Raw TCP connect with TCP_NODELAY
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.settimeout(3.0)
        t1 = time.perf_counter()
        sock.connect((ip, port))
        tcp_times.append((time.perf_counter() - t1) * 1000.0)

        # 3. TLS handshake
        ctx = ssl.create_default_context()
        t2 = time.perf_counter()
        tls_sock = ctx.wrap_socket(sock, server_hostname=host)
        tls_times.append((time.perf_counter() - t2) * 1000.0)
        sock.close()

    return {
        "host": host,
        "resolved_ip": ip,
        "dns_min_ms": min(dns_times),
        "dns_avg_ms": sum(dns_times) / len(dns_times),
        "tcp_min_ms": min(tcp_times),
        "tcp_avg_ms": sum(tcp_times) / len(tcp_times),
        "tls_min_ms": min(tls_times),
        "tls_avg_ms": sum(tls_times) / len(tls_times),
    }


def run_benchmark(token: str, iterations: int = 10):
    print("=" * 88)
    print("🚀 UPSTOX API SANDBOX REAL-TIME ORDER SPEED & LATENCY BENCHMARK (AWS AP-SOUTH-1)")
    print("=" * 88)

    # ---------------------------------------------------------
    # PART 1: Network Layer Infrastructure Comparison
    # ---------------------------------------------------------
    print("\n[PART 1/5] Network Layer Infrastructure Analysis (AWS Mumbai -> Broker Endpoints)")
    print("-" * 88)
    targets = [
        ("api-sandbox.upstox.com", "Sandbox Gateway"),
        ("api-hft.upstox.com", "Live HFT Gateway (AWS Intra-Region)"),
        ("api.upstox.com", "Standard Production Gateway"),
    ]

    for host, label in targets:
        res = measure_network_handshakes(host)
        print(f"• {label:32} | Host: {host:24} (IP: {res['resolved_ip']})")
        print(f"  - DNS Resolution  : Min {res['dns_min_ms']:5.2f} ms | Avg {res['dns_avg_ms']:5.2f} ms")
        print(f"  - Raw TCP Connect : Min {res['tcp_min_ms']:5.2f} ms | Avg {res['tcp_avg_ms']:5.2f} ms")
        print(f"  - TLS Handshake   : Min {res['tls_min_ms']:5.2f} ms | Avg {res['tls_avg_ms']:5.2f} ms")

    # ---------------------------------------------------------
    # PART 2: Cold vs Hot Persistent HTTP/2 Connection Setup
    # ---------------------------------------------------------
    print("\n[PART 2/5] Connection Setup: Cold Connect vs Persistent HTTP/2 Session")
    print("-" * 88)

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
        "User-Agent": "ULLTR-HFT-Benchmark/1.0",
    }

    # Cold HTTP/2 request
    t_cold_start = time.perf_counter()
    with httpx.Client(http2=True, timeout=5.0) as cold_client:
        r_cold = cold_client.get(f"{SANDBOX_BASE_URL}/v2/order/retrieve-all", headers=headers)
        cold_total_ms = (time.perf_counter() - t_cold_start) * 1000.0
    print(f"• Cold Connection Latency (TCP + TLS + HTTP/2 Handshake + RTT): {cold_total_ms:.2f} ms (Status: {r_cold.status_code})")

    # Hot Persistent HTTP/2 Client
    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20, keepalive_expiry=120.0)
    hot_client = httpx.Client(http2=True, limits=limits, timeout=5.0)

    # Pre-warm socket
    t_warm_start = time.perf_counter()
    r_warm = hot_client.get(f"{SANDBOX_BASE_URL}/v2/order/retrieve-all", headers=headers)
    warm_total_ms = (time.perf_counter() - t_warm_start) * 1000.0
    print(f"• Pre-warm Socket Probe: {warm_total_ms:.2f} ms (HTTP Version: {r_warm.http_version})")

    # ---------------------------------------------------------
    # PART 3: V3 Place Order Benchmark (Hot Persistent Socket)
    # ---------------------------------------------------------
    print(f"\n[PART 3/5] Benchmark V3 Place Order (`POST /v3/order/place`) | {iterations} Iterations")
    print("-" * 88)
    print(f"{'Run':<4} | {'Order ID':<16} | {'Serial (µs)':<11} | {'Total (ms)':<10} | {'Server Match (ms)':<17} | {'Net Egress (ms)':<15} | Status")
    print("-" * 88)

    v3_totals: List[float] = []
    v3_serials: List[float] = []
    v3_server_latencies: List[float] = []
    v3_net_egress: List[float] = []
    placed_order_ids: List[str] = []

    for i in range(1, iterations + 1):
        payload = {
            "quantity": 1,
            "product": "D",
            "validity": "DAY",
            "price": 0.0,
            "tag": f"sbx_v3_run_{i}",
            "instrument_token": "NSE_EQ|INE848E01016",
            "order_type": "MARKET",
            "transaction_type": "BUY",
            "disclosed_quantity": 0,
            "trigger_price": 0.0,
            "is_amo": False,
            "slice": False,
        }

        # Measure serialization
        t_start = time.perf_counter()
        body_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        t_serialized = time.perf_counter()
        ser_us = (t_serialized - t_start) * 1_000_000.0

        # Submit over persistent connection
        res = hot_client.post(f"{SANDBOX_BASE_URL}/v3/order/place", headers=headers, content=body_bytes)
        t_done = time.perf_counter()

        total_ms = (t_done - t_start) * 1000.0
        egress_ms = (t_done - t_serialized) * 1000.0

        server_latency_ms = 0.0
        order_id = "N/A"
        if res.status_code == 200:
            data = res.json()
            order_ids = data.get("data", {}).get("order_ids", [])
            order_id = order_ids[0] if order_ids else "N/A"
            placed_order_ids.append(order_id)
            server_latency_ms = float(data.get("metadata", {}).get("latency", 0.0))

        v3_totals.append(total_ms)
        v3_serials.append(ser_us)
        v3_server_latencies.append(server_latency_ms)
        v3_net_egress.append(egress_ms - server_latency_ms if egress_ms >= server_latency_ms else egress_ms)

        print(f"#{i:<3} | {order_id:<16} | {ser_us:9.2f} µs | {total_ms:8.2f} ms | {server_latency_ms:15.1f} ms | {egress_ms:13.2f} ms | HTTP {res.status_code}")
        time.sleep(0.05)  # slight pause between tests

    # ---------------------------------------------------------
    # PART 4: V2 vs V3 Order Placement Comparison
    # ---------------------------------------------------------
    print(f"\n[PART 4/5] V2 vs V3 Order Placement Comparison (5 Runs Each)")
    print("-" * 88)

    v2_totals: List[float] = []
    for i in range(1, 6):
        payload_v2 = {
            "quantity": 1,
            "product": "D",
            "validity": "DAY",
            "price": 0.0,
            "tag": f"sbx_v2_run_{i}",
            "instrument_token": "NSE_EQ|INE848E01016",
            "order_type": "MARKET",
            "transaction_type": "BUY",
            "disclosed_quantity": 0,
            "trigger_price": 0.0,
            "is_amo": False,
        }
        t0 = time.perf_counter()
        body = json.dumps(payload_v2, separators=(",", ":")).encode("utf-8")
        r_v2 = hot_client.post(f"{SANDBOX_BASE_URL}/v2/order/place", headers=headers, content=body)
        dur = (time.perf_counter() - t0) * 1000.0
        v2_totals.append(dur)
        v2_oid = r_v2.json().get("data", {}).get("order_id", "N/A") if r_v2.status_code == 200 else "FAIL"
        print(f"• V2 Run #{i}: {dur:6.2f} ms | Order ID: {v2_oid} | Status: {r_v2.status_code}")
        time.sleep(0.05)

    # ---------------------------------------------------------
    # PART 5: Order Modification, Cancellation & Orderbook Fetch
    # ---------------------------------------------------------
    print(f"\n[PART 5/5] Full Order Lifecycle: Place Limit -> Modify -> Cancel -> Orderbook Fetch")
    print("-" * 88)

    # 1. Place a LIMIT order
    limit_payload = {
        "quantity": 1,
        "product": "D",
        "validity": "DAY",
        "price": 500.0,
        "tag": "sbx_lifecycle",
        "instrument_token": "NSE_EQ|INE848E01016",
        "order_type": "LIMIT",
        "transaction_type": "BUY",
        "disclosed_quantity": 0,
        "trigger_price": 0.0,
        "is_amo": False,
        "slice": False,
    }
    t_l0 = time.perf_counter()
    r_limit = hot_client.post(f"{SANDBOX_BASE_URL}/v3/order/place", headers=headers, json=limit_payload)
    limit_place_ms = (time.perf_counter() - t_l0) * 1000.0
    limit_order_id = r_limit.json().get("data", {}).get("order_ids", [""])[0] if r_limit.status_code == 200 else ""
    print(f"• Step 1: Place Limit Order (Price=500.0) -> Time: {limit_place_ms:.2f} ms | ID: {limit_order_id} | Status: {r_limit.status_code}")

    # 2. Modify Order
    if limit_order_id:
        mod_payload = {
            "order_id": limit_order_id,
            "quantity": 2,
            "price": 505.0,
            "order_type": "LIMIT",
            "validity": "DAY",
            "disclosed_quantity": 0,
            "trigger_price": 0.0,
        }
        t_m0 = time.perf_counter()
        r_mod = hot_client.put(f"{SANDBOX_BASE_URL}/v3/order/modify", headers=headers, json=mod_payload)
        mod_ms = (time.perf_counter() - t_m0) * 1000.0
        print(f"• Step 2: Modify Order (Qty=2, Price=505.0) -> Time: {mod_ms:.2f} ms | Status: {r_mod.status_code} | Msg: {r_mod.text[:60]}")

        # 3. Cancel Order
        t_c0 = time.perf_counter()
        r_can = hot_client.delete(f"{SANDBOX_BASE_URL}/v3/order/cancel", headers=headers, params={"order_id": limit_order_id})
        can_ms = (time.perf_counter() - t_c0) * 1000.0
        print(f"• Step 3: Cancel Order (`/v3/order/cancel`)  -> Time: {can_ms:.2f} ms | Status: {r_can.status_code} | Msg: {r_can.text[:60]}")

    # 4. Fetch Orderbook
    t_ob0 = time.perf_counter()
    r_ob = hot_client.get(f"{SANDBOX_BASE_URL}/v2/order/retrieve-all", headers=headers)
    ob_ms = (time.perf_counter() - t_ob0) * 1000.0
    orders_count = len(r_ob.json().get("data", [])) if r_ob.status_code == 200 else 0
    print(f"• Step 4: Fetch Full Orderbook (`retrieve-all`) -> Time: {ob_ms:.2f} ms | Status: {r_ob.status_code} | Total Orders: {orders_count}")

    hot_client.close()

    # ---------------------------------------------------------
    # STATISTICAL SUMMARY SCORECARD
    # ---------------------------------------------------------
    v3_sorted = sorted(v3_totals)
    v3_min = v3_sorted[0]
    v3_p50 = v3_sorted[len(v3_sorted) // 2]
    v3_p90 = v3_sorted[int(len(v3_sorted) * 0.9)]
    v3_max = v3_sorted[-1]
    v3_avg = sum(v3_totals) / len(v3_totals)

    v3_ser_avg = sum(v3_serials) / len(v3_serials)
    v3_srv_avg = sum(v3_server_latencies) / len(v3_server_latencies)

    print("\n" + "=" * 88)
    print("🏆 FINAL BENCHMARK PERFORMANCE SCORECARD (AWS AP-SOUTH-1 VM)")
    print("=" * 88)
    print(f"1. In-Memory JSON Serialization      : Avg {v3_ser_avg:6.2f} µs ({v3_ser_avg / 1000.0:.4f} ms)")
    print(f"2. Upstox Internal Matching Latency  : Avg {v3_srv_avg:6.2f} ms (Reported via `metadata.latency`)")
    print(f"3. V3 Order Placement Latency (Hot)  :")
    print(f"   • Min (Fastest Roundtrip)         : {v3_min:6.2f} ms")
    print(f"   • P50 (Median)                    : {v3_p50:6.2f} ms")
    print(f"   • P90 (90th Percentile)           : {v3_p90:6.2f} ms")
    print(f"   • Max                             : {v3_max:6.2f} ms")
    print(f"   • Mean (Average)                  : {v3_avg:6.2f} ms")
    print(f"4. V2 vs V3 Average Comparison       : V3 Avg {v3_avg:.2f} ms vs V2 Avg {sum(v2_totals) / len(v2_totals):.2f} ms")
    print(f"5. Order Lifecycle Turnarounds       :")
    print(f"   • Place Order Turnaround          : {limit_place_ms:.2f} ms")
    if limit_order_id:
        print(f"   • Modify Order Turnaround         : {mod_ms:.2f} ms")
        print(f"   • Cancel Order Turnaround         : {can_ms:.2f} ms")
    print(f"   • Orderbook Retrieve Turnaround   : {ob_ms:.2f} ms")
    print("=" * 88 + "\n")


if __name__ == "__main__":
    token = (
        sys.argv[1]
        if len(sys.argv) > 1
        else os.environ.get("UPSTOX_SANDBOX_TOKEN", DEFAULT_TOKEN)
    )
    iterations = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    run_benchmark(token, iterations)
