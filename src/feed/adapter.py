"""LiveBooks — the one object both bots read market data from.

WebSocket first; REST fallback when the feed is stale or has no book for a ticker. Same return
shape as KalshiClient.orderbook(), so the bots' `_best_ask` / `_best_bid` code does not change.

Wiring (both bots, in the constructor):

    from src.feed.adapter import LiveBooks
    self.books = LiveBooks(self.client, env=cfg["env"], key_id=..., key_path=..., spot_syms=["BTC", "ETH"])
    self.books.start()

Then, in the tick / _take path:

    self.books.ensure(selected_tickers)                  # subscribe to this tick's markets (idempotent)
    ob = self.books.orderbook(ticker)                    # was: self.client.orderbook(ticker)
    spot = self.books.spot("BTC") or deribit_rest_spot() # was: REST every tick

Heartbeat line:  self.books.status()  ->  "ws: books 41/41 fresh, 0 fallbacks/min, deltas 1234/min, spot age 0.4s"
"""
from __future__ import annotations

import time
from typing import Any, Callable, Dict, Iterable, List, Optional

from .kalshi_ws import KalshiFeed
from .spot_ws import DeribitIndexFeed


class LiveBooks:
    def __init__(self, client: Any, env: str = "prod", key_id: Optional[str] = None, key_path: Optional[str] = None,
                 spot_syms: Iterable[str] = ("BTC",), max_age_s: float = 60.0, spot_max_age_s: float = 5.0,
                 url: Optional[str] = None, on_fill: Optional[Callable[[Dict[str, Any]], None]] = None,
                 log: Callable[[str], None] = print) -> None:
        self.client, self.max_age_s, self.spot_max_age_s, self.log = client, max_age_s, spot_max_age_s, log
        self.feed = KalshiFeed(env, key_id, key_path, tickers=[], channels=("orderbook_delta", "trade", "ticker", "fill"),
                               url=url, on_fill=on_fill, log=log)
        self.spot_feed = DeribitIndexFeed([s.lower() + "_usd" for s in spot_syms], log=log)
        self.fallbacks = 0
        self._fallback_ts: List[float] = []
        self._delta_mark = (time.time(), 0)

    def start(self) -> "LiveBooks":
        self.feed.start()
        self.spot_feed.start()
        return self

    def stop(self) -> None:
        self.feed.stop(); self.spot_feed.stop()

    # ---- market data
    def ensure(self, tickers: Iterable[str]) -> None:
        """Subscribe to any tickers not yet on the feed (call every tick with the selected markets)."""
        self.feed.add_tickers(tickers)

    def orderbook(self, ticker: str) -> Any:
        """WebSocket book if the socket is alive and has a snapshot for ticker, else client.orderbook(ticker).
        Shape: {"yes": [[price_c, qty], ...], "no": [...]} plus "source" and "age_s" (seconds since last change)."""
        ob = self.feed.orderbook(ticker)
        if ob is not None and self.feed.fresh(ticker, self.max_age_s):
            return {"yes": ob["yes"], "no": ob["no"], "source": "ws", "age_s": ob["age_s"]}
        self.fallbacks += 1
        self._fallback_ts.append(time.time())
        rest = self.client.orderbook(ticker)
        if isinstance(rest, dict):
            rest = dict(rest, source="rest")
        return rest

    def best(self, ticker: str) -> Optional[Dict[str, Any]]:
        b = self.feed.best(ticker)
        if b is not None and self.feed.fresh(ticker, self.max_age_s):
            return b
        ob = self.orderbook(ticker)
        if not isinstance(ob, dict):
            return None
        yes, no = ob.get("yes") or [], ob.get("no") or []
        yb = max((int(l[0]) for l in yes), default=None)
        nb = max((int(l[0]) for l in no), default=None)
        return {"yes_bid": yb, "yes_ask": (100 - nb) if nb is not None else None,
                "bid_size": next((float(l[1]) for l in yes if int(l[0]) == yb), 0.0) if yb is not None else 0.0,
                "ask_size": next((float(l[1]) for l in no if int(l[0]) == nb), 0.0) if nb is not None else 0.0,
                "age_s": None}

    def spot(self, sym: str = "BTC") -> Optional[float]:
        """Pushed Deribit index price, or None if stale (caller falls back to its REST spot)."""
        age = self.spot_feed.age_s(sym)
        if age is None or age > self.spot_max_age_s:
            return None
        return self.spot_feed.price(sym)

    def last_trade(self, ticker: str) -> Optional[Dict[str, Any]]:
        return self.feed.last_trade(ticker)

    # ---- diagnostics
    def status(self) -> str:
        now = time.time()
        self._fallback_ts = [t for t in self._fallback_ts if now - t < 60]
        t0, d0 = self._delta_mark
        rate = (self.feed.stats["deltas"] - d0) / max(now - t0, 1e-9) * 60
        self._delta_mark = (now, self.feed.stats["deltas"])
        ready = len(self.feed.ready_tickers())
        age = self.spot_feed.age_s("BTC")
        lag = self.feed.stats["lag_ms"]
        return (f"ws: books {ready}/{len(self.feed.tickers)} {'fresh' if self.feed.stats['connected'] else 'DISCONNECTED'}, "
                f"{len(self._fallback_ts)} fallbacks/min, deltas {rate:.0f}/min, gaps {self.feed.stats['seq_gaps']}, "
                f"reconn {self.feed.stats['reconnects']}, lag {'-' if lag is None else f'{lag:.0f}ms'}, "
                f"spot age {age if age is None else round(age, 1)}s")
