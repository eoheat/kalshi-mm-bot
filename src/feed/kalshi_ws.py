"""Kalshi WebSocket feed — keeps a live local order book per market.

Why this exists: the bots polled REST, so every decision used a book that was
seconds old (and one shape bug left them blind for days). This module keeps
the book updated the instant Kalshi pushes a change, and exposes it through
thread-safe getters so the existing *synchronous* bot loops can read it with
one call and fall back to REST when the feed is stale.

Protocol (Kalshi Trade API WS v2, same key + signature as REST):
    connect  wss://<host>/trade-api/ws/v2   headers KALSHI-ACCESS-KEY / -SIGNATURE / -TIMESTAMP(ms)
    sign     timestamp + "GET" + "/trade-api/ws/v2"   (RSA-PSS SHA256, base64)
    send     {"id": n, "cmd": "subscribe", "params": {"channels": [...], "market_tickers": [...]}}
    recv     {"id": n, "type": "subscribed", "msg": {"channel": "...", "sid": s}}
             {"type": "orderbook_snapshot", "sid": s, "seq": k, "msg": {"market_ticker": T, "yes": [[price, qty], ...], "no": [...]}}
             {"type": "orderbook_delta",    "sid": s, "seq": k, "msg": {"market_ticker": T, "price": p, "delta": d, "side": "yes"|"no"}}
             {"type": "trade" | "ticker" | "fill" | "market_lifecycle_v2", "sid": s, "msg": {...}}
             {"id": n, "type": "error", "msg": {"code": .., "msg": ".."}}

Book convention (same as REST): the "yes" side lists resting YES bids and the
"no" side lists resting NO bids. A NO bid at q is a YES ask at 100-q, so
best YES ask = 100 - best NO bid.

Field shapes: Kalshi has been migrating int-cent / int-count fields to
"*_dollars" strings ("0.4500") and "*_fp" strings ("3.00"). Every parser here
accepts all three shapes (the REST orderbook bug was exactly this). Verify the
real shape once with `python -m src.feed.record --probe` before trusting it.

No external dependency on the rest of the repo: signing is self-contained so
this file can be dropped in as-is.
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
import threading
import time
from typing import Any, Callable, Dict, Iterable, List, Optional

try:  # optional at import time so tests can run without it
    import websockets
except ImportError:  # pragma: no cover
    websockets = None

WS_PATH = "/trade-api/ws/v2"
WS_HOSTS = {
    "prod": "wss://api.elections.kalshi.com" + WS_PATH,
    "demo": "wss://demo-api.kalshi.co" + WS_PATH,
    # Dedicated Trade-API hosts (also documented; same auth). Use if the shared host misbehaves.
    "prod-dedicated": "wss://external-api-ws.kalshi.com" + WS_PATH,
    "demo-dedicated": "wss://external-api-ws.demo.kalshi.co" + WS_PATH,
}
PUBLIC_CHANNELS = ("ticker", "trade", "market_lifecycle_v2")
PRIVATE_CHANNELS = ("orderbook_delta", "fill", "market_positions")


# --------------------------------------------------------------------------- auth
def sign_ws_request(key_path: str, ts_ms: str, method: str = "GET", path: str = WS_PATH) -> str:
    """RSA-PSS SHA256 signature over ts + method + path, base64 — identical to the REST scheme."""
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding

    with open(key_path, "rb") as f:
        key = serialization.load_pem_private_key(f.read(), password=None)
    sig = key.sign(
        (ts_ms + method + path).encode(),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return base64.b64encode(sig).decode()


def auth_headers(key_id: str, key_path: str) -> Dict[str, str]:
    ts = str(int(time.time() * 1000))
    return {
        "KALSHI-ACCESS-KEY": key_id,
        "KALSHI-ACCESS-SIGNATURE": sign_ws_request(key_path, ts),
        "KALSHI-ACCESS-TIMESTAMP": ts,
    }


# --------------------------------------------------------------------------- parsing
def price_cents(value: Any, key: str = "") -> Optional[int]:
    """int cents | "45" | "0.4500" (dollars, 4dp) | 0.45 -> 45. Dollar-ness is decided by "dollars" in the
    key name or by a decimal point in a string; a bare int/float is cents."""
    if value is None or isinstance(value, bool):
        return None
    if "dollars" in key or (isinstance(value, str) and "." in value):
        return int(round(float(value) * 100))
    return int(round(float(value)))


def qty_float(value: Any) -> float:
    """int | "3.00" | 3.0 -> 3.0"""
    if value is None:
        return 0.0
    return float(value)


def _first(d: Dict[str, Any], *keys: str):
    for k in keys:
        if k in d and d[k] is not None:
            return k, d[k]
    return None, None


def _pick(d: Dict[str, Any], *prefixes: str, want_list: bool = False):
    """(key, value) for the first prefix that matches a key exactly or as key_<suffix> — so yes / yes_dollars /
    yes_fp / yes_dollars_fp (the shape seen on prod, Sep 2026) all resolve without a code change."""
    for pfx in prefixes:
        v = d.get(pfx)
        if v is not None and (not want_list or isinstance(v, list)):
            return pfx, v
    for pfx in prefixes:
        for k, v in d.items():
            if k.startswith(pfx + "_") and v is not None and (not want_list or isinstance(v, list)):
                return k, v
    return None, None


def msg_ts(msg: Dict[str, Any]) -> Optional[float]:
    """exchange timestamp in seconds from ts_ms / ts (ms int, s int, or ISO string)."""
    v = msg.get("ts_ms")
    if v is not None:
        return float(v) / 1000.0
    v = msg.get("ts")
    if isinstance(v, (int, float)):
        return float(v) / 1000.0 if v > 1e11 else float(v)
    if isinstance(v, str) and "T" in v:
        try:
            import datetime as _dt
            core, _, frac = v.replace("Z", "").partition(".")
            return _dt.datetime.fromisoformat(core + (("." + frac[:6]) if frac else "") + "+00:00").timestamp()
        except ValueError:
            return None
    return None


def parse_levels(levels: Any, key: str) -> Dict[int, float]:
    """[[price, qty], ...] or [{"price": p, "quantity": q}, ...] -> {price_cents: qty}."""
    out: Dict[int, float] = {}
    for lvl in levels or []:
        if isinstance(lvl, dict):
            pk, pv = _pick(lvl, "price")
            _, qv = _pick(lvl, "quantity", "count", "size")
            p = price_cents(pv, pk or key)
        else:
            p = price_cents(lvl[0], key)
            qv = lvl[1] if len(lvl) > 1 else 0
        if p is None:
            continue
        q = qty_float(qv)
        if q > 0:
            out[p] = out.get(p, 0.0) + q
    return out


def parse_snapshot(msg: Dict[str, Any]):
    """-> (ticker, yes_levels, no_levels). A side that is absent (no resting orders) is an empty dict."""
    ticker = msg.get("market_ticker") or msg.get("ticker")
    yk, yv = _pick(msg, "yes", want_list=True)
    nk, nv = _pick(msg, "no", want_list=True)
    return ticker, parse_levels(yv, yk or ""), parse_levels(nv, nk or "")


def parse_delta(msg: Dict[str, Any]):
    """-> (ticker, side, price_cents, signed_delta)"""
    ticker = msg.get("market_ticker") or msg.get("ticker")
    pk, pv = _pick(msg, "price")
    _, dv = _pick(msg, "delta")
    side = (msg.get("side") or "").lower()
    return ticker, side, price_cents(pv, pk or ""), qty_float(dv)


def parse_trade(msg: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a trade message to {ticker, yes_price (cents), count, taker_side, ts (exchange, seconds)}."""
    ticker = msg.get("market_ticker") or msg.get("ticker")
    pk, pv = _pick(msg, "yes_price", "price")
    _, cv = _pick(msg, "count")
    return {
        "ticker": ticker,
        "yes_price": price_cents(pv, pk or ""),
        "count": qty_float(cv),
        "taker_side": (msg.get("taker_side") or "").lower() or None,
        "ts": msg_ts(msg) or msg.get("created_time"),
    }


# --------------------------------------------------------------------------- book
class Book:
    __slots__ = ("yes", "no", "ts", "ready")

    def __init__(self) -> None:
        self.yes: Dict[int, float] = {}
        self.no: Dict[int, float] = {}
        self.ts: float = 0.0
        self.ready: bool = False

    def apply_delta(self, side: str, price: int, delta: float) -> None:
        d = self.yes if side == "yes" else self.no
        q = d.get(price, 0.0) + delta
        if q <= 1e-9:
            d.pop(price, None)
        else:
            d[price] = q

    def best(self) -> Dict[str, Optional[float]]:
        yb = max(self.yes) if self.yes else None
        nb = max(self.no) if self.no else None
        return {
            "yes_bid": yb,
            "yes_ask": (100 - nb) if nb is not None else None,
            "bid_size": self.yes.get(yb, 0.0) if yb is not None else 0.0,
            "ask_size": self.no.get(nb, 0.0) if nb is not None else 0.0,
        }

    def levels(self) -> Dict[str, List[List[float]]]:
        """REST-shaped normalized book: {"yes": [[price_cents, qty], ...], "no": [...]}, best first."""
        return {
            "yes": [[p, q] for p, q in sorted(self.yes.items(), reverse=True)],
            "no": [[p, q] for p, q in sorted(self.no.items(), reverse=True)],
        }


# --------------------------------------------------------------------------- feed
class KalshiFeed:
    """Runs the WebSocket in a daemon thread; the bot reads books via thread-safe getters.

        feed = KalshiFeed("prod", key_id, key_path, tickers=[...])
        feed.start()
        ob = feed.orderbook(t)          # {"yes": [[p,q],..], "no": [[p,q],..], "ts":..., "age_s":...} or None
        if not feed.fresh(t, 5.0): ob = client.orderbook(t)   # REST fallback

    Async users can `await feed.run()` directly instead of start().
    """

    def __init__(
        self,
        env: str = "prod",
        key_id: Optional[str] = None,
        key_path: Optional[str] = None,
        tickers: Iterable[str] = (),
        channels: Iterable[str] = ("orderbook_delta", "trade", "ticker"),
        url: Optional[str] = None,
        on_raw: Optional[Callable[[Dict[str, Any], float], None]] = None,
        on_trade: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_fill: Optional[Callable[[Dict[str, Any]], None]] = None,
        log: Callable[[str], None] = print,
    ) -> None:
        self.url = url or WS_HOSTS[env]
        self.key_id = key_id or os.environ.get("KALSHI_KEY_ID") or os.environ.get("KALSHI_API_KEY_ID")
        self.key_path = key_path or os.environ.get("KALSHI_KEY_PATH") or os.environ.get("KALSHI_PRIVATE_KEY_PATH")
        self.tickers: List[str] = list(dict.fromkeys(tickers))
        self.channels: List[str] = list(channels)
        self.on_raw, self.on_trade, self.on_fill, self.log = on_raw, on_trade, on_fill, log

        self._lock = threading.Lock()
        self._books: Dict[str, Book] = {}
        self._last_trade: Dict[str, Dict[str, Any]] = {}
        self._ticker_msgs: Dict[str, Dict[str, Any]] = {}
        self._subs: Dict[int, Dict[str, Any]] = {}   # sid -> {"channel": ch, "tickers": [...]} (batched)
        self._pending_subs: Dict[int, Dict[str, Any]] = {}   # request id -> {"channel", "tickers"} awaiting "subscribed"
        self._seq: Dict[int, int] = {}           # sid -> last seq
        self.batch = 100                         # tickers per subscription command
        self._next_id = 1
        self._ws = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self.stats = {"messages": 0, "snapshots": 0, "deltas": 0, "trades": 0, "tickers": 0, "fills": 0,
                      "reconnects": 0, "seq_gaps": 0, "errors": 0, "connected": False, "last_msg_ts": 0.0,
                      "lag_ms": None, "lag_ms_max": 0.0,        # exchange ts_ms -> our receipt, EWMA / max
                      "ticker_checks": 0, "ticker_mismatch": 0}  # ticker channel top-of-book vs our derived book

    # ----- lifecycle
    def start(self) -> "KalshiFeed":
        if websockets is None:
            raise RuntimeError("pip install websockets")
        if not (self.key_id and self.key_path):
            raise RuntimeError("KalshiFeed needs key_id and key_path (or KALSHI_KEY_ID / KALSHI_KEY_PATH)")
        self._thread = threading.Thread(target=self._thread_main, name="kalshi-ws", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        if self._loop and self._ws:
            asyncio.run_coroutine_threadsafe(self._ws.close(), self._loop)

    def _thread_main(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self.run())
        finally:
            self._loop.close()

    async def _connect(self, headers: Dict[str, str]):
        kw = dict(ping_interval=20, ping_timeout=20, max_size=8_000_000)
        try:
            return await websockets.connect(self.url, additional_headers=headers, **kw)   # websockets >= 13
        except TypeError:
            return await websockets.connect(self.url, extra_headers=headers, **kw)        # legacy API

    async def run(self) -> None:
        if not (self.key_id and self.key_path):
            raise RuntimeError("KalshiFeed needs key_id and key_path (or KALSHI_KEY_ID / KALSHI_KEY_PATH)")
        backoff = 1.0
        while not self._stop.is_set():
            try:
                headers = auth_headers(self.key_id, self.key_path)
                async with await self._connect(headers) as ws:
                    self._ws = ws
                    self.stats["connected"] = True
                    backoff = 1.0
                    with self._lock:
                        for b in self._books.values():
                            b.ready = False            # stale until a fresh snapshot arrives
                        self._subs.clear(); self._pending_subs.clear(); self._seq.clear()
                    await self._subscribe_all(ws)
                    async for raw in ws:
                        self._handle(raw)
            except Exception as e:  # noqa: BLE001 — reconnect on anything
                self.stats["errors"] += 1
                self.log(f"[ws] disconnected: {type(e).__name__}: {e}; retry in {backoff:.0f}s")
            finally:
                self._ws = None
                self.stats["connected"] = False
            if self._stop.is_set():
                break
            self.stats["reconnects"] += 1
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    # ----- subscriptions
    async def _send(self, ws, cmd: str, params: Dict[str, Any]) -> int:
        msg_id = self._next_id
        self._next_id += 1
        payload = {"id": msg_id, "cmd": cmd, "params": params}
        await ws.send(json.dumps(payload))
        if self.on_raw:
            self.on_raw({"_out": payload}, time.time())
        return msg_id

    MARKET_CHANNELS = ("orderbook_delta", "trade", "ticker")

    async def _subscribe(self, ws, ch: str, tickers: Optional[List[str]]) -> None:
        params: Dict[str, Any] = {"channels": [ch]}
        if tickers:
            params["market_tickers"] = list(tickers)
        req = await self._send(ws, "subscribe", params)
        self._pending_subs[req] = {"channel": ch, "tickers": list(tickers or [])}

    async def _subscribe_all(self, ws) -> None:
        with self._lock:
            tickers = list(self.tickers)
        for ch in self.channels:
            if ch in self.MARKET_CHANNELS and tickers:
                for i in range(0, len(tickers), self.batch):     # batched: one sid per <=batch tickers
                    await self._subscribe(ws, ch, tickers[i:i + self.batch])
            else:
                await self._subscribe(ws, ch, None)

    async def _resubscribe(self, ws, sid: int) -> None:
        sub = self._subs.pop(sid, None)
        await self._send(ws, "unsubscribe", {"sids": [sid]})
        self._seq.pop(sid, None)
        if sub:
            await self._subscribe(ws, sub["channel"], sub["tickers"] or None)

    def add_tickers(self, tickers: Iterable[str]) -> None:
        with self._lock:
            new = [t for t in dict.fromkeys(tickers) if t not in self.tickers]
            self.tickers.extend(new)
        if not new or not (self._loop and self._ws):
            return

        async def go():
            ws = self._ws
            if ws is None:
                return
            for ch in self.channels:
                if ch in self.MARKET_CHANNELS:
                    for i in range(0, len(new), self.batch):
                        await self._subscribe(ws, ch, new[i:i + self.batch])
        asyncio.run_coroutine_threadsafe(go(), self._loop)

    def remove_tickers(self, tickers: Iterable[str]) -> None:
        with self._lock:
            gone = [t for t in tickers if t in self.tickers]
            self.tickers = [t for t in self.tickers if t not in gone]
            for t in gone:
                self._books.pop(t, None)
        if not gone or not (self._loop and self._ws):
            return

        async def go():
            ws = self._ws
            if ws is None:
                return
            for sid, sub in list(self._subs.items()):
                mine = [t for t in sub["tickers"] if t in gone]
                if not mine:
                    continue
                sub["tickers"] = [t for t in sub["tickers"] if t not in gone]
                if sub["tickers"]:
                    await self._send(ws, "update_subscription", {"sids": [sid], "market_tickers": mine, "action": "delete_markets"})
                else:
                    await self._send(ws, "unsubscribe", {"sids": [sid]})
                    self._subs.pop(sid, None); self._seq.pop(sid, None)
        asyncio.run_coroutine_threadsafe(go(), self._loop)

    # ----- message handling (sync so it is unit-testable without a socket)
    def _handle(self, raw: Any) -> None:
        now = time.time()
        try:
            m = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
        except json.JSONDecodeError:
            self.stats["errors"] += 1
            return
        self.stats["messages"] += 1
        self.stats["last_msg_ts"] = now
        if self.on_raw:
            self.on_raw(m, now)
        mtype = m.get("type")
        msg = m.get("msg") or {}
        sid = m.get("sid")
        seq = m.get("seq")

        if mtype == "subscribed":
            pend = self._pending_subs.pop(m.get("id"), None) or {"channel": msg.get("channel", ""), "tickers": []}
            self._subs[msg.get("sid")] = pend
            return
        if mtype == "error":
            self.stats["errors"] += 1
            self.log(f"[ws] error: {msg}")
            return
        if sid is not None and seq is not None and mtype in ("orderbook_snapshot", "orderbook_delta"):
            last = self._seq.get(sid)
            if last is not None and seq != last + 1:
                self.stats["seq_gaps"] += 1
                self.log(f"[ws] seq gap on sid {sid}: {last} -> {seq}; resubscribing")
                self._seq.pop(sid, None)
                if self._loop and self._ws:
                    asyncio.run_coroutine_threadsafe(self._resubscribe(self._ws, sid), self._loop)
                return
            self._seq[sid] = seq

        if mtype in ("orderbook_delta", "trade"):
            xts = msg_ts(msg)
            if xts is not None:
                lag = (now - xts) * 1000.0
                self.stats["lag_ms"] = lag if self.stats["lag_ms"] is None else 0.9 * self.stats["lag_ms"] + 0.1 * lag
                self.stats["lag_ms_max"] = max(self.stats["lag_ms_max"], lag)

        if mtype == "orderbook_snapshot":
            t, yes, no = parse_snapshot(msg)
            with self._lock:
                b = self._books.setdefault(t, Book())
                b.yes, b.no, b.ts, b.ready = yes, no, now, True
            self.stats["snapshots"] += 1
        elif mtype == "orderbook_delta":
            t, side, p, d = parse_delta(msg)
            if p is None or side not in ("yes", "no"):
                return
            with self._lock:
                b = self._books.setdefault(t, Book())
                b.apply_delta(side, p, d)
                b.ts = now
            self.stats["deltas"] += 1
        elif mtype == "trade":
            tr = parse_trade(msg)
            tr["recv_ts"] = now
            with self._lock:
                self._last_trade[tr["ticker"]] = tr
            self.stats["trades"] += 1
            if self.on_trade:
                self.on_trade(tr)
        elif mtype == "ticker":
            t = msg.get("market_ticker") or msg.get("ticker")
            self.stats["tickers"] += 1
            with self._lock:
                self._ticker_msgs[t] = dict(msg, recv_ts=now)
                b = self._books.get(t)
                best = b.best() if b is not None and b.ready else None
            if best is not None:   # independent check of our derived top-of-book against Kalshi's own ticker
                bk, bv = _pick(msg, "yes_bid")
                ak, av = _pick(msg, "yes_ask")
                tb, ta = price_cents(bv, bk or ""), price_cents(av, ak or "")
                if tb is not None and ta is not None:
                    self.stats["ticker_checks"] += 1
                    ob = best["yes_bid"] if best["yes_bid"] is not None else 0
                    oa = best["yes_ask"] if best["yes_ask"] is not None else 100
                    if (tb, ta) != (ob, oa):
                        self.stats["ticker_mismatch"] += 1
                        self.stats["ticker_last_mismatch"] = f"{t}: ticker {tb}/{ta} vs book {ob}/{oa}"
        elif mtype == "fill":
            self.stats["fills"] += 1
            if self.on_fill:
                self.on_fill(dict(msg, recv_ts=now))

    # ----- getters (thread-safe copies)
    def orderbook(self, ticker: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            b = self._books.get(ticker)
            if b is None or not b.ready:
                return None
            out = b.levels()
            out["ts"] = b.ts
        out["age_s"] = time.time() - out["ts"]
        return out

    def best(self, ticker: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            b = self._books.get(ticker)
            if b is None or not b.ready:
                return None
            out = b.best()
            out["ts"] = b.ts
        out["age_s"] = time.time() - out["ts"]
        return out

    def fresh(self, ticker: str, max_quiet_s: float = 60.0) -> bool:
        """True if the socket is connected, we hold a snapshot for ticker, and the socket has spoken within
        max_quiet_s. A quiet book is still current while the connection is alive: the library's keepalive
        pings (20s interval / 20s timeout) close a dead socket, which flips connected to False."""
        with self._lock:
            b = self._books.get(ticker)
            ok = b is not None and b.ready
        return ok and self.stats["connected"] and (time.time() - self.stats["last_msg_ts"]) <= max_quiet_s

    def last_trade(self, ticker: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return dict(self._last_trade[ticker]) if ticker in self._last_trade else None

    def ticker_msg(self, ticker: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return dict(self._ticker_msgs[ticker]) if ticker in self._ticker_msgs else None

    def ready_tickers(self) -> List[str]:
        with self._lock:
            return [t for t, b in self._books.items() if b.ready]

    def ticker_check(self) -> str:
        """'checks N, mismatch M (x%)' — a few % is timing between channels; >20% means a convention bug."""
        n, m = self.stats["ticker_checks"], self.stats["ticker_mismatch"]
        return f"ticker-vs-book checks {n}, mismatch {m}" + (f" ({100.0 * m / n:.0f}%)" if n else "") + \
               (f" last: {self.stats.get('ticker_last_mismatch')}" if m else "")
