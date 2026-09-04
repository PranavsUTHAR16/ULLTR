#!/usr/bin/env python3
"""
ULLTR High-Frequency Broker Order Gateway (Upstox HFT V3 & V2 API)
=================================================================
Ultra-low latency HTTP client for executing real orders via Upstox HFT API.

Key Advantages of V3 over V2:
  1. Auto-Slicing: `"slice": True` automatically splits large orders exceeding freeze limits.
  2. Latency Metadata: Returns internal OMS/matching engine processing latency in `meta`.
  3. Optimized for HFT: Native support on `api-hft.upstox.com`.
  4. Returns `order_ids` list for multi-slice orders.
"""

import os
import sys
import time
import json
import logging
from typing import Dict, Any, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger("BrokerGateway")

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)

# Upstox Endpoints
V3_HFT_ORDER_URL = "https://api-hft.upstox.com/v3/order/place"
V2_STD_ORDER_URL = "https://api.upstox.com/v2/order/place"


class UpstoxBrokerGateway:
    """High-performance order gateway for Upstox V3 HFT & V2 fallback."""
    
    def __init__(self, token: Optional[str] = None):
        self.access_token = token
        if not self.access_token:
            self.load_token()
            
        self.session = requests.Session()
        # Configure connection pool with keep-alive
        adapter = HTTPAdapter(
            pool_connections=5,
            pool_maxsize=10,
            max_retries=Retry(total=1, backoff_factor=0.05, status_forcelist=[502, 503, 504])
        )
        self.session.mount("https://", adapter)
        
        self._update_headers()
        self.hft_v3_enabled = True
        self.warmup_connection()
        
    def load_token(self) -> bool:
        token_paths = [
            os.path.join(PROJECT_ROOT, "login", "access_token.json"),
            os.path.join(PROJECT_ROOT, "access_token.json"),
        ]
        for p in token_paths:
            if os.path.exists(p):
                try:
                    with open(p, "r") as f:
                        data = json.load(f)
                        self.access_token = data.get("access_token")
                        if self.access_token:
                            return True
                except Exception as e:
                    logger.error(f"Error reading access token from {p}: {e}")
        return False

    def _update_headers(self):
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "ULLTR-HFT-Gateway-V3/1.0"
        }
        self.session.headers.update(self.headers)

    def warmup_connection(self):
        """Pre-warms the SSL/TLS socket to eliminate handshake latency during execution."""
        try:
            self.session.get("https://api.upstox.com/v2/user/profile", timeout=3)
        except Exception as e:
            logger.debug(f"Connection pre-warm note: {e}")

    def place_order_v3(
        self,
        instrument_token: str,
        quantity: int,
        transaction_type: str,  # "BUY" or "SELL"
        product: str = "I",     # "I" for Intraday, "D" for Delivery
        order_type: str = "MARKET",
        price: float = 0.0,
        trigger_price: float = 0.0,
        validity: str = "DAY",
        slice_order: bool = True,
        tag: str = "CAS_ARB"
    ) -> Dict[str, Any]:
        """
        Dispatches order via Upstox V3 HFT API with Auto-Slicing and Latency Metadata.
        """
        if not self.access_token:
            self.load_token()
            self._update_headers()
            
        payload = {
            "quantity": int(quantity),
            "product": product,
            "validity": validity,
            "price": float(price),
            "tag": tag,
            "instrument_token": instrument_token,
            "order_type": order_type,
            "transaction_type": transaction_type.upper(),
            "disclosed_quantity": 0,
            "trigger_price": float(trigger_price),
            "is_amo": False,
            "slice": slice_order
        }

        t_signal_ns = time.perf_counter_ns()
        
        if not self.hft_v3_enabled:
            target_url = V2_STD_ORDER_URL
            api_version = "v2"
            payload_to_send = payload.copy()
            payload_to_send.pop("slice", None)
        else:
            target_url = V3_HFT_ORDER_URL
            api_version = "v3"
            payload_to_send = payload

        response_json = {}
        status_code = 0
        error_msg = ""
        order_ids = []
        broker_latency_meta = {}
        
        t_sent_ns = time.perf_counter_ns()
        try:
            resp = self.session.post(target_url, json=payload_to_send, timeout=3.0)
            t_ack_ns = time.perf_counter_ns()
            status_code = resp.status_code
            try:
                response_json = resp.json()
            except Exception:
                response_json = {"raw_text": resp.text}
        except Exception as e:
            logger.warning(f"Gateway request exception ({e})")
            status_code = 500
            error_msg = str(e)
            t_ack_ns = time.perf_counter_ns()

        # Check if V3 is blocked due to static IP restrictions (UDAPI1154) or 403
        if api_version == "v3" and (status_code in [401, 403, 404] or "UDAPI1154" in str(response_json)):
            self.hft_v3_enabled = False
            logger.warning("Upstox V3 HFT requires static IP whitelist (UDAPI1154). Automatic failover to V2 standard gateway...")
            target_url = V2_STD_ORDER_URL
            api_version = "v2"
            payload_v2 = payload.copy()
            payload_v2.pop("slice", None)
            try:
                t_sent_ns = time.perf_counter_ns()
                resp = self.session.post(target_url, json=payload_v2, timeout=3.0)
                t_ack_ns = time.perf_counter_ns()
                status_code = resp.status_code
                try:
                    response_json = resp.json()
                except Exception:
                    response_json = {"raw_text": resp.text}
            except Exception as e2:
                t_ack_ns = time.perf_counter_ns()
                status_code = 500
                error_msg = str(e2)

        turnaround_ms = (t_ack_ns - t_signal_ns) / 1_000_000.0
        gateway_rtt_ms = (t_ack_ns - t_sent_ns) / 1_000_000.0

        is_success = False
        if status_code == 200 and response_json.get("status") == "success":
            is_success = True
            data = response_json.get("data", {})
            if "order_ids" in data:
                order_ids = data["order_ids"]
            elif "order_id" in data:
                order_ids = [data["order_id"]]
            broker_latency_meta = response_json.get("meta", {})
        else:
            errors = response_json.get("errors", [])
            if errors and isinstance(errors, list):
                error_msg = errors[0].get("message", "") or errors[0].get("errorCode", "Unknown Error")
            elif not error_msg:
                error_msg = response_json.get("message", f"HTTP Status {status_code}")

        result = {
            "api_version": api_version,
            "is_success": is_success,
            "status_code": status_code,
            "order_ids": order_ids,
            "primary_order_id": order_ids[0] if order_ids else "",
            "error_msg": error_msg,
            "instrument_token": instrument_token,
            "quantity": quantity,
            "transaction_type": transaction_type,
            "target_url": target_url,
            "t_signal_ns": t_signal_ns,
            "t_sent_ns": t_sent_ns,
            "t_ack_ns": t_ack_ns,
            "turnaround_ms": round(turnaround_ms, 3),
            "gateway_rtt_ms": round(gateway_rtt_ms, 3),
            "broker_latency_meta": broker_latency_meta,
            "raw_response": response_json
        }
        
        return result

    def place_order(self, *args, **kwargs):
        """Default alias pointing to V3 HFT implementation."""
        return self.place_order_v3(*args, **kwargs)

