import os
import asyncio
import json
import logging
from typing import Dict, List, Optional
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as aioredis

# Set up clean logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("market-data-api")

app = FastAPI(
    title="Internal Market Data API",
    description="Low-latency internal market-data gateway cache and Pub/Sub fan-out service.",
    version="1.0.0"
)

# Enable CORS for internal dashboard access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Redis Connection Pool (Async)
redis_pool: Optional[aioredis.ConnectionPool] = None

@app.on_event("startup")
async def startup_event():
    global redis_pool
    redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    logger.info(f"Connecting to Redis at {redis_host}:{redis_port}...")
    redis_pool = aioredis.ConnectionPool(
        host=redis_host,
        port=redis_port,
        decode_responses=True,
        max_connections=50
    )
    logger.info("Market Data API started. Connected to Redis connection pool.")

@app.on_event("shutdown")
async def shutdown_event():
    global redis_pool
    if redis_pool:
        await redis_pool.disconnect()
        logger.info("Disconnected from Redis connection pool.")

async def get_redis_client() -> aioredis.Redis:
    if not redis_pool:
        raise HTTPException(status_code=500, detail="Redis connection pool is not initialized")
    return aioredis.Redis(connection_pool=redis_pool)

@app.get("/health", response_model=Dict[str, str])
async def health_check():
    """Verify system health and Redis connectivity."""
    try:
        r = await get_redis_client()
        pong = await r.ping()
        if pong:
            return {"status": "ok", "redis": "connected"}
    except Exception as e:
        logger.error(f"Healthcheck failed: {e}")
        raise HTTPException(status_code=503, detail="Service unavailable: Redis connection failed")
    
    raise HTTPException(status_code=503, detail="Service unavailable")

@app.get("/symbols", response_model=List[str])
async def list_symbols():
    """List all currently subscribed/cached market data symbols."""
    try:
        r = await get_redis_client()
        keys = await r.keys("md:quote:*")
        # Strip the "md:quote:" prefix to return clean symbol names
        symbols = [key.replace("md:quote:", "") for key in keys]
        return sorted(symbols)
    except Exception as e:
        logger.error(f"Failed to list symbols: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/quote/{symbol}", response_model=Dict)
async def get_quote(symbol: str):
    """Retrieve the latest cached quote snapshot for a specific symbol."""
    try:
        r = await get_redis_client()
        key = f"md:quote:{symbol}"
        key_type = await r.type(key)
        
        if key_type == "hash":
            raw_hash = await r.hgetall(key)
            if not raw_hash:
                raise HTTPException(status_code=404, detail=f"Quote not found for symbol: {symbol}")
            
            quote = {}
            greeks = {}
            for k, v in raw_hash.items():
                if k in ["ltp", "close", "volume", "oi", "iv", "bid", "bid_qty", "ask", "ask_qty", "ts_exchange", "ts_recv", "atp", "tbq", "tsq"]:
                    try:
                        quote[k] = float(v) if "." in v else int(v)
                    except ValueError:
                        quote[k] = v
                elif k in ["delta", "theta", "gamma", "vega", "rho"]:
                    try:
                        greeks[k] = float(v) if "." in v else int(v)
                    except ValueError:
                        greeks[k] = v
                else:
                    quote[k] = v
            if greeks:
                quote["option_greeks"] = greeks
            return quote
        else:
            raw_quote = await r.get(key)
            if not raw_quote:
                raise HTTPException(status_code=404, detail=f"Quote not found for symbol: {symbol}")
            return json.loads(raw_quote)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to retrieve quote for {symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/quotes", response_model=List[Dict])
async def get_quotes_batch(symbols: str):
    """
    Retrieve snapshots for multiple symbols in a single, high-performance batch call.
    Accepts a comma-separated list of symbols (e.g., `?symbols=NSE_EQ|INE020B01018,NSE_EQ|INE467B01029`).
    """
    if not symbols:
        raise HTTPException(status_code=400, detail="Symbols query parameter is required")
    
    symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]
    if not symbol_list:
        return []
        
    try:
        r = await get_redis_client()
        
        # Pipelined lookup of key types
        pipe = r.pipeline()
        for symbol in symbol_list:
            pipe.type(f"md:quote:{symbol}")
        types = await pipe.execute()
        
        # Pipelined fetch of values based on type
        pipe = r.pipeline()
        for symbol, key_type in zip(symbol_list, types):
            if key_type == "hash":
                pipe.hgetall(f"md:quote:{symbol}")
            else:
                pipe.get(f"md:quote:{symbol}")
        raw_results = await pipe.execute()
        
        quotes = []
        for symbol, key_type, raw_val in zip(symbol_list, types, raw_results):
            if not raw_val:
                quotes.append({"symbol": symbol, "status": "offline", "error": "No data in cache"})
                continue
                
            if key_type == "hash":
                quote = {}
                greeks = {}
                for k, v in raw_val.items():
                    if k in ["ltp", "close", "volume", "oi", "iv", "bid", "bid_qty", "ask", "ask_qty", "ts_exchange", "ts_recv", "atp", "tbq", "tsq"]:
                        try:
                            quote[k] = float(v) if "." in v else int(v)
                        except ValueError:
                            quote[k] = v
                    elif k in ["delta", "theta", "gamma", "vega", "rho"]:
                        try:
                            greeks[k] = float(v) if "." in v else int(v)
                        except ValueError:
                            greeks[k] = v
                    else:
                        quote[k] = v
                if greeks:
                    quote["option_greeks"] = greeks
                quotes.append(quote)
            else:
                quotes.append(json.loads(raw_val))
        return quotes
    except Exception as e:
        logger.error(f"Failed to retrieve batch quotes: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.websocket("/ws")
async def websocket_stream(websocket: WebSocket):
    """
    Persistent WebSocket feed streaming real-time normalized ticks fanned out 
    directly from the Redis Pub/Sub channel.
    """
    await websocket.accept()
    logger.info(f"WebSocket subscriber connected from client {websocket.client}")
    
    r = await get_redis_client()
    pubsub = r.pubsub()
    await pubsub.subscribe("md:stream:all")
    
    try:
        while True:
            # Check for incoming message or trigger keepalive/timeout
            try:
                # We listen to Redis PubSub messages asynchronously
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message["type"] == "message":
                    data = message["data"]
                    await websocket.send_text(data)
            except asyncio.TimeoutError:
                # Send a heartbeat/ping to keep the client WebSocket active
                await websocket.send_json({"type": "ping"})
            except Exception as e:
                logger.error(f"PubSub stream broadcasting error: {e}")
                break
    except WebSocketDisconnect:
        logger.info(f"WebSocket subscriber disconnected: {websocket.client}")
    finally:
        await pubsub.unsubscribe("md:stream:all")
        await pubsub.close()
