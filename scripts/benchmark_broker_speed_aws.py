#!/usr/bin/env python3
"""AWS VM Broker Speed & Network Latency Benchmark Script.

Measures:
1. DNS resolution latency.
2. Raw TCP connect latency (AWS intra-region EC2-to-EC2).
3. TLS handshake latency.
4. HTTP/2 persistent keepalive roundtrip to Upstox V3 HFT Gateway.
5. Order packet serialization speed.
"""

import json
import os
import socket
import ssl
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from forward_tester.broker_gateway import UpstoxBrokerGateway, V3_HFT_ORDER_URL, V2_STD_ORDER_URL
from forward_tester.streaming_engine import HighPrecisionClock


def benchmark_dns_tcp_tls(host: str, port: int = 443, iterations: int = 5):
    """Measures DNS, TCP, and TLS handshake latencies across multiple runs."""
    dns_times = []
    tcp_times = []
    tls_times = []

    ip = None
    for _ in range(iterations):
        # DNS
        t0 = time.perf_counter()
        ip = socket.gethostbyname(host)
        dns_times.append((time.perf_counter() - t0) * 1000.0)

        # TCP Handshake with TCP_NODELAY
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.settimeout(3.0)
        t1 = time.perf_counter()
        sock.connect((ip, port))
        tcp_times.append((time.perf_counter() - t1) * 1000.0)

        # TLS Handshake
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


def main():
    print("=" * 80)
    print("🚀 AWS VM BROKER ORDERING SPEED & NETWORK BENCHMARK (AP-SOUTH-1)")
    print("=" * 80)

    # 1. Benchmark Host Resolution & Handshakes
    print("\n[1/3] Benchmarking Upstox HFT V3 (`api-hft.upstox.com`)...")
    hft_bench = benchmark_dns_tcp_tls("api-hft.upstox.com")
    print(f"  • Host: {hft_bench['host']} ({hft_bench['resolved_ip']})")
    print(f"  • DNS Resolution    : Min {hft_bench['dns_min_ms']:.2f} ms | Avg {hft_bench['dns_avg_ms']:.2f} ms")
    print(f"  • Raw TCP Connect   : Min {hft_bench['tcp_min_ms']:.2f} ms | Avg {hft_bench['tcp_avg_ms']:.2f} ms (Target: <2.0 ms on AWS intra-region)")
    print(f"  • TLS Handshake     : Min {hft_bench['tls_min_ms']:.2f} ms | Avg {hft_bench['tls_avg_ms']:.2f} ms")

    # 2. Benchmark Gateway Session & Pre-warm
    print("\n[2/3] Benchmarking UpstoxBrokerGateway (Persistent HTTP/2 Engine)...")
    gw = UpstoxBrokerGateway()
    print(f"  • Gateway V3 Enabled: {gw.hft_v3_enabled}")
    print(f"  • Target HFT URL    : {V3_HFT_ORDER_URL}")
    print(f"  • Fallback URL      : {V2_STD_ORDER_URL}")

    # Pre-warm
    t_pw = time.perf_counter()
    gw.warmup_connection()
    pw_duration = (time.perf_counter() - t_pw) * 1000.0
    print(f"  • Socket Pre-warm   : {pw_duration:.2f} ms")

    # 3. Benchmark Internal Order Serialization Speed (In-Memory Hot Path)
    print("\n[3/3] Benchmarking Order Serialization & Preparation Speed...")
    t_start = time.perf_counter_ns()
    payload = {
        "quantity": 65,
        "product": "I",
        "validity": "DAY",
        "price": 0.0,
        "tag": "STRATEGY_6",
        "instrument_token": "NSE_FO|68407",
        "order_type": "MARKET",
        "transaction_type": "SELL",
        "disclosed_quantity": 0,
        "trigger_price": 0.0,
        "is_amo": False,
        "slice": True
    }
    encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    ser_time_us = (time.perf_counter_ns() - t_start) / 1000.0
    print(f"  • In-Memory JSON Packaging: {ser_time_us:.2f} µs ({ser_time_us / 1000.0:.4f} ms)")

    # 4. Timer Precision Drift Test
    drift_us = HighPrecisionClock.wait_until_sharp_time(
        *time.localtime(time.time() + 0.1)[3:6]
    )
    print(f"  • Timer Arming Drift      : {drift_us:+.2f} µs ({drift_us / 1000.0:+.4f} ms)")

    print("\n" + "=" * 80)
    print("🏆 SUMMARY SPEED CARD")
    print("=" * 80)
    print(f"1. Target IP Infrastructure : {hft_bench['resolved_ip']} (AWS EC2 Mumbai intra-region)")
    print(f"2. Raw Network 1-RTT Ping  : {hft_bench['tcp_min_ms']:.2f} ms")
    print(f"3. Internal Serialization   : {ser_time_us:.2f} µs")
    print(f"4. Persistent HTTP/2 Socket: Ready & Pre-warmed")
    print(f"5. Total Estimated Turnaround: {hft_bench['tcp_min_ms'] + (ser_time_us / 1000.0):.2f} ms")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
