"""Ultra-Low Latency Upstox Order Placement Gateway.

Implements high-speed order execution for Upstox V2 API using persistent HTTP/2
connection pooling, pre-warmed sockets, TCP_NODELAY, and microsecond telemetry.
"""

import json
import logging
import os
import socket
import ssl
import time
from typing import Any, Dict, Optional, Tuple

import httpx

logger = logging.getLogger("UpstoxGateway")

UPSTOX_BASE_URL = "https://api.upstox.com"
UPSTOX_ORDER_ENDPOINT = "/v2/order/place"
UPSTOX_HOST = "api.upstox.com"
UPSTOX_PORT = 443


class UpstoxOrderGateway:
    """Institutional-grade Upstox Order Execution Gateway."""

    def __init__(
        self,
        auth_token: Optional[str] = None,
        base_url: str = UPSTOX_BASE_URL,
        timeout: float = 3.0,
    ):
        """Initializes persistent HTTP/2 connection pool."""
        self.auth_token = auth_token or os.environ.get("UPSTOX_ACCESS_TOKEN", "")
        self.base_url = base_url
        self.timeout = timeout
        self.is_warmed: bool = False

        # Persistent HTTP/2 client with keepalive connection pool
        limits = httpx.Limits(
            max_keepalive_connections=20,
            max_connections=50,
            keepalive_expiry=120.0,
        )
        self.client = httpx.Client(
            http2=True,
            limits=limits,
            timeout=self.timeout,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "ULLTR-Quantitative-Gateway/1.0",
            },
        )

    def prewarm_connection(self) -> Dict[str, float]:
        """Pre-warms DNS, TCP, TLS, and HTTP/2 session before market open.

        Should be executed at 09:14:50 IST to ensure zero handshake latency
        on first trade execution.

        Returns:
            Dict containing dns_ms, tcp_ms, tls_ms, and http_roundtrip_ms.
        """
        metrics = {}

        # 1. DNS Resolution timing
        t0 = time.perf_counter()
        ip = socket.gethostbyname(UPSTOX_HOST)
        metrics["dns_ms"] = (time.perf_counter() - t0) * 1000.0

        # 2. Raw TCP Handshake timing
        t1 = time.perf_counter()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.settimeout(3.0)
        sock.connect((ip, UPSTOX_PORT))
        metrics["tcp_ms"] = (time.perf_counter() - t1) * 1000.0

        # 3. TLS Handshake timing
        ctx = ssl.create_default_context()
        t2 = time.perf_counter()
        tls_sock = ctx.wrap_socket(sock, server_hostname=UPSTOX_HOST)
        metrics["tls_ms"] = (time.perf_counter() - t2) * 1000.0
        sock.close()

        # 4. HTTP/2 Session Pre-warm via lightweight API probe
        t3 = time.perf_counter()
        try:
            headers = {}
            if self.auth_token:
                headers["Authorization"] = f"Bearer {self.auth_token}"
            res = self.client.get(f"{self.base_url}/v2/user/profile", headers=headers)
            metrics["http_roundtrip_ms"] = (time.perf_counter() - t3) * 1000.0
            metrics["http_version"] = 2.0 if res.http_version == "HTTP/2" else 1.1
            self.is_warmed = True
        except Exception as exc:
            metrics["http_roundtrip_ms"] = (time.perf_counter() - t3) * 1000.0
            metrics["error"] = str(exc)

        logger.info(
            "Upstox Pre-warm Complete | DNS: %.2fms | TCP: %.2fms | TLS: %.2fms | HTTP RTT: %.2fms",
            metrics.get("dns_ms", 0),
            metrics.get("tcp_ms", 0),
            metrics.get("tls_ms", 0),
            metrics.get("http_roundtrip_ms", 0),
        )
        return metrics

    def place_order_fast(
        self,
        instrument_token: str,
        transaction_type: str,
        quantity: int,
        order_type: str = "MARKET",
        product: str = "I",
        price: float = 0.0,
        trigger_price: float = 0.0,
        tag: str = "STRATEGY_6",
    ) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        """Submits order with sub-millisecond serialization and socket reuse.

        Returns:
            Tuple of (success: bool, order_id: Optional[str], telemetry: Dict).
        """
        t_start = time.perf_counter()

        # Pre-templated payload dictionary (zero extraneous overhead)
        payload = {
            "quantity": int(quantity),
            "product": str(product),
            "validity": "DAY",
            "price": float(price),
            "tag": str(tag),
            "instrument_token": str(instrument_token),
            "order_type": str(order_type).upper(),
            "transaction_type": str(transaction_type).upper(),
            "disclosed_quantity": 0,
            "trigger_price": float(trigger_price),
            "is_amo": False,
        }

        # Fast string serialization
        payload_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        t_serialized = time.perf_counter()

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"

        url = f"{self.base_url}{UPSTOX_ORDER_ENDPOINT}"

        # Submit over hot persistent connection
        try:
            response = self.client.post(url, headers=headers, content=payload_bytes)
            t_response = time.perf_counter()

            serialization_time_us = (t_serialized - t_start) * 1_000_000.0
            egress_time_ms = (t_response - t_serialized) * 1000.0
            total_time_ms = (t_response - t_start) * 1000.0

            telemetry = {
                "serialization_us": round(serialization_time_us, 2),
                "egress_ms": round(egress_time_ms, 3),
                "total_ms": round(total_time_ms, 3),
                "status_code": response.status_code,
                "http_version": response.http_version,
            }

            if response.status_code in (200, 201):
                res_data = response.json()
                order_id = res_data.get("data", {}).get("order_id")
                return True, order_id, telemetry
            else:
                telemetry["error_body"] = response.text[:200]
                return False, None, telemetry

        except Exception as exc:
            t_err = time.perf_counter()
            telemetry = {
                "serialization_us": round((t_serialized - t_start) * 1_000_000.0, 2),
                "egress_ms": round((t_err - t_serialized) * 1000.0, 3),
                "total_ms": round((t_err - t_start) * 1000.0, 3),
                "error": str(exc),
            }
            return False, None, telemetry

    def close(self):
        """Closes client connection pool."""
        try:
            self.client.close()
        except Exception:
            pass
