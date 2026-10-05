"""Deribit index-price feed (public, no key) — pushed spot for BTC/ETH.

Same source the fair-value model already uses over REST, but pushed roughly
once a second instead of pulled once a tick.

    spot = DeribitIndexFeed(["btc_usd", "eth_usd"]).start()
    spot.price("BTC")        -> 81234.5 or None
    spot.age_s("BTC")        -> seconds since last update

Protocol: JSON-RPC over wss://www.deribit.com/ws/api/v2
    {"jsonrpc":"2.0","id":1,"method":"public/subscribe","params":{"channels":["deribit_price_index.btc_usd"]}}
    {"jsonrpc":"2.0","method":"subscription","params":{"channel":"deribit_price_index.btc_usd",
                                                       "data":{"timestamp":ms,"price":81234.5,"index_name":"btc_usd"}}}
Heartbeat: we ask for one; server sends "heartbeat" type "test_request", we answer public/test.
"""
from __future__ import annotations

import asyncio
import json
import threading
import time
from typing import Any, Callable, Dict, Iterable, Optional

try:
    import websockets
except ImportError:  # pragma: no cover
    websockets = None

DERIBIT_WS = "wss://www.deribit.com/ws/api/v2"


def parse_index_message(m: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """subscription message -> {"sym": "BTC", "price": float, "ts": seconds} or None."""
    if m.get("method") != "subscription":
        return None
    params = m.get("params") or {}
    ch = params.get("channel", "")
    if not ch.startswith("deribit_price_index."):
        return None
    data = params.get("data") or {}
    name = data.get("index_name") or ch.split(".", 1)[1]
    sym = name.split("_")[0].upper()
    ts = data.get("timestamp")
    return {"sym": sym, "price": float(data["price"]), "ts": (ts / 1000.0) if ts else time.time()}


class DeribitIndexFeed:
    def __init__(self, indexes: Iterable[str] = ("btc_usd",), on_raw: Optional[Callable[[Dict[str, Any], float], None]] = None,
                 log: Callable[[str], None] = print) -> None:
        self.indexes = list(indexes)
        self.on_raw, self.log = on_raw, log
        self._lock = threading.Lock()
        self._px: Dict[str, Dict[str, float]] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop = threading.Event()
        self.stats = {"messages": 0, "reconnects": 0, "connected": False, "last_msg_ts": 0.0}

    def start(self) -> "DeribitIndexFeed":
        if websockets is None:
            raise RuntimeError("pip install websockets")
        threading.Thread(target=self._thread_main, name="deribit-ws", daemon=True).start()
        return self

    def stop(self) -> None:
        self._stop.set()

    def _thread_main(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self.run())
        finally:
            self._loop.close()

    async def run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                async with websockets.connect(DERIBIT_WS, ping_interval=20, ping_timeout=20) as ws:
                    self.stats["connected"] = True
                    backoff = 1.0
                    await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "public/set_heartbeat",
                                              "params": {"interval": 30}}))
                    await ws.send(json.dumps({"jsonrpc": "2.0", "id": 2, "method": "public/subscribe",
                                              "params": {"channels": [f"deribit_price_index.{i}" for i in self.indexes]}}))
                    async for raw in ws:
                        m = json.loads(raw)
                        if m.get("method") == "heartbeat" and (m.get("params") or {}).get("type") == "test_request":
                            await ws.send(json.dumps({"jsonrpc": "2.0", "id": 3, "method": "public/test", "params": {}}))
                            continue
                        self._handle(m)
            except Exception as e:  # noqa: BLE001
                self.log(f"[deribit] disconnected: {type(e).__name__}: {e}; retry in {backoff:.0f}s")
            finally:
                self.stats["connected"] = False
            if self._stop.is_set():
                break
            self.stats["reconnects"] += 1
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    def _handle(self, m: Dict[str, Any]) -> None:
        now = time.time()
        self.stats["messages"] += 1
        self.stats["last_msg_ts"] = now
        if self.on_raw:
            self.on_raw(m, now)
        px = parse_index_message(m)
        if px:
            with self._lock:
                self._px[px["sym"]] = {"price": px["price"], "ts": px["ts"], "recv_ts": now}

    def price(self, sym: str = "BTC") -> Optional[float]:
        with self._lock:
            d = self._px.get(sym.upper())
            return d["price"] if d else None

    def age_s(self, sym: str = "BTC") -> Optional[float]:
        with self._lock:
            d = self._px.get(sym.upper())
        return (time.time() - d["recv_ts"]) if d else None

    def snapshot(self) -> Dict[str, Dict[str, float]]:
        with self._lock:
            return {k: dict(v) for k, v in self._px.items()}
