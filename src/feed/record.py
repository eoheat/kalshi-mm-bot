"""Record a replayable tape from the Kalshi + Deribit WebSocket feeds.

    # 1. verify the real message shapes (prints the first raw message of each type, then exits)
    python -m src.feed.record --env prod --series KXBTC --probe

    # 2. record (runs until Ctrl-C; rotates one file per UTC hour)
    python -m src.feed.record --env prod --series KXBTC KXETH --horizon-hours 3 --out data/tape

Tape line types (one JSON object per line, all with "t" = local receive time, epoch seconds):
    {"t", "type": "market", "ticker", "event", "series", "strike_type", "lo", "hi", "close_ts", "open_ts"}
    {"t", "type": "ws",     "raw": <verbatim Kalshi message>}            # snapshots, deltas, trades, tickers
    {"t", "type": "spot",   "sym": "BTC", "price": 81234.5, "ts": <deribit ts>}
    {"t", "type": "result", "ticker", "result": "yes"|"no", "close_ts"}   # fetched after settlement
The replay engine (src/replay/run.py) consumes exactly this.

Keys: --key-id/--key-path or env KALSHI_KEY_ID / KALSHI_KEY_PATH (also reads .env / .env.demo if present).
Market discovery uses the PUBLIC REST endpoints (no key needed) — the same call as the curl that produced strip.json.
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import os
import re
import signal
import sys
import threading
import time
from typing import Any, Dict, Iterable, List, Optional

import requests

from .kalshi_ws import KalshiFeed, WS_HOSTS
from .spot_ws import DeribitIndexFeed

REST_HOSTS = {"prod": "https://api.elections.kalshi.com/trade-api/v2", "demo": "https://demo-api.kalshi.co/trade-api/v2"}


def rest_base(env: str) -> str:
    return REST_HOSTS["demo" if env.startswith("demo") else "prod"]
SERIES_UNDERLYING = {"KXBTC": "BTC", "KXBTC15M": "BTC", "KXBTCD": "BTC", "KXETH": "ETH", "KXETH15M": "ETH", "KXETHD": "ETH"}


def _load_dotenv(path: str) -> None:
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def _iso_to_ts(s: Optional[str]) -> Optional[float]:
    """'2026-09-19T20:00:00.329404Z' -> epoch seconds (Python 3.9-safe: trims >6 fractional digits)."""
    if not s:
        return None
    s = s.strip().replace("Z", "+00:00")
    m = re.match(r"^(.*T\d\d:\d\d:\d\d)(\.\d+)?([+-]\d\d:\d\d)?$", s)
    if m:
        frac = (m.group(2) or "")[:7]
        s = m.group(1) + frac + (m.group(3) or "+00:00")
    return dt.datetime.fromisoformat(s).timestamp()


def _f(v: Any) -> Optional[float]:
    try:
        return None if v in (None, "") else float(v)
    except (TypeError, ValueError):
        return None


def market_meta(m: Dict[str, Any]) -> Dict[str, Any]:
    """REST market row -> tape 'market' record (bracket bounds as numbers; open ends as None)."""
    st = m.get("strike_type")
    lo, hi = _f(m.get("floor_strike")), _f(m.get("cap_strike"))
    if st == "greater":
        hi = None
    elif st == "less":
        # Kalshi uses cap_strike for "or below"; some rows carry it in floor_strike instead
        lo, hi = None, (hi if hi is not None else lo)
    return {
        "type": "market", "ticker": m["ticker"], "event": m.get("event_ticker"),
        "series": m.get("series_ticker") or m["ticker"].split("-")[0],
        "strike_type": st, "lo": lo, "hi": hi,
        "close_ts": _iso_to_ts(m.get("close_time")), "open_ts": _iso_to_ts(m.get("open_time")),
        "result": (m.get("result") or None), "status": m.get("status"),
    }


def rest_markets(env: str, **params: Any) -> List[Dict[str, Any]]:
    """Paginated public GET /markets."""
    out: List[Dict[str, Any]] = []
    cursor = None
    while True:
        q = dict(params, limit=params.get("limit", 1000))
        if cursor:
            q["cursor"] = cursor
        r = requests.get(rest_base(env) + "/markets", params=q, timeout=20)
        r.raise_for_status()
        body = r.json()
        out.extend(body.get("markets", []))
        cursor = body.get("cursor")
        if not cursor or not body.get("markets"):
            return out
        time.sleep(0.15)


def discover(env: str, series: Iterable[str], horizon_hours: float) -> List[Dict[str, Any]]:
    """Open markets closing within horizon_hours, as tape 'market' records."""
    now = time.time()
    metas: List[Dict[str, Any]] = []
    for s in series:
        for m in rest_markets(env, series_ticker=s, status="open"):
            meta = market_meta(m)
            if meta["close_ts"] and now - 120 <= meta["close_ts"] <= now + horizon_hours * 3600:
                metas.append(meta)
    metas.sort(key=lambda x: (x["close_ts"], x["ticker"]))
    return metas


def fetch_results(env: str, events: Iterable[str]) -> List[Dict[str, Any]]:
    out = []
    for ev in events:
        try:
            for m in rest_markets(env, event_ticker=ev):
                res = (m.get("result") or "").lower()
                if res in ("yes", "no"):
                    out.append({"type": "result", "ticker": m["ticker"], "result": res,
                                "close_ts": _iso_to_ts(m.get("close_time"))})
        except requests.RequestException as e:
            print(f"[rec] result fetch failed for {ev}: {e}")
    return out


class Tape:
    """JSONL writer, gzip-compressed, rotated per UTC hour (tape-YYYYMMDD-HH.jsonl.gz)."""

    def __init__(self, out_dir: str, compress: bool = True) -> None:
        os.makedirs(out_dir, exist_ok=True)
        self.out_dir, self.compress, self._fh, self._hour, self._lock = out_dir, compress, None, None, threading.Lock()
        self.lines = 0

    def write(self, rec: Dict[str, Any], t: Optional[float] = None) -> None:
        t = t if t is not None else time.time()
        hour = int(t // 3600)
        with self._lock:
            if hour != self._hour:
                if self._fh:
                    self._fh.close()
                name = dt.datetime.fromtimestamp(hour * 3600, dt.timezone.utc).strftime("tape-%Y%m%d-%H.jsonl")
                path = os.path.join(self.out_dir, name)
                self._fh = gzip.open(path + ".gz", "at") if self.compress else open(path, "a")
                self._hour = hour
            self._fh.write(json.dumps(dict(rec, t=t), separators=(",", ":")) + "\n")
            self.lines += 1

    def flush(self) -> None:
        with self._lock:
            if self._fh:
                self._fh.flush()


def probe(args: argparse.Namespace) -> None:
    """Connect, print the first raw message of each type (so field shapes can be checked), exit."""
    from .kalshi_ws import parse_snapshot
    metas = discover(args.env, args.series, args.horizon_hours)
    if not metas:
        print("no open markets found in horizon"); return
    spot = DeribitIndexFeed(["btc_usd", "eth_usd"]).start()
    for _ in range(50):
        if spot.price("BTC"):
            break
        time.sleep(0.1)
    px = spot.price("BTC")
    print("deribit BTC index:", px, "stats:", spot.stats)
    # the soonest settlement only, brackets nearest spot first (a strip is alphabetical = lowest strikes first)
    soonest = min(m["close_ts"] for m in metas)
    strip = [m for m in metas if m["close_ts"] == soonest]
    sym = "ETH" if "ETH" in strip[0]["series"] else "BTC"
    ref = spot.price(sym) or sorted(m["lo"] or m["hi"] for m in strip)[len(strip) // 2]

    def dist(m):
        mid = ((m["lo"] or m["hi"]) + (m["hi"] or m["lo"])) / 2.0
        return abs(mid - ref)
    strip.sort(key=dist)
    tickers = [m["ticker"] for m in strip[:40]]
    print(f"probing {len(tickers)} brackets of {strip[0]['event']} nearest {sym} {ref:.0f}: {tickers[0]} ... {tickers[-1]}")
    print("first market row from REST:", json.dumps(strip[0]))
    seen: Dict[str, int] = {}
    done = threading.Event()
    first_snapshot: Dict[str, Any] = {}

    def on_raw(m: Dict[str, Any], t: float) -> None:
        key = m.get("type") or ("out" if "_out" in m else "?")
        seen[key] = seen.get(key, 0) + 1
        if seen[key] == 1:
            print(f"--- first {key}:\n{json.dumps(m)[:1200]}")
            if key == "orderbook_snapshot":
                first_snapshot.update(m.get("msg") or {})
        if all(seen.get(k, 0) >= 1 for k in ("subscribed", "orderbook_snapshot", "orderbook_delta")) and seen.get("trade", 0) >= 1:
            done.set()

    feed = KalshiFeed(args.env, args.key_id, args.key_path, tickers, on_raw=on_raw, url=args.url)
    feed.batch = args.batch
    feed.start()
    done.wait(timeout=args.probe_seconds)
    print("message counts:", seen)
    if first_snapshot:
        raw_levels = sum(len(v) for k, v in first_snapshot.items() if isinstance(v, list))
        _, yes, no = parse_snapshot(first_snapshot)
        print(f"SELF-CHECK first snapshot: {raw_levels} raw levels -> parsed {len(yes)} yes + {len(no)} no "
              f"{'OK' if len(yes) + len(no) == raw_levels else '<< SHAPE BUG: parser missed levels'}")
    best = [(t, feed.best(t)) for t in tickers]
    live = [(t, b) for t, b in best if b and (b["yes_ask"] is not None or b["yes_bid"] is not None)]
    print(f"books ready: {len(feed.ready_tickers())}/{len(tickers)}; with a quote: {len(live)}")
    for t, b in live[:8]:
        bid = f"{b['yes_bid']} x{b['bid_size']:.0f}" if b["yes_bid"] is not None else "-"
        ask = f"{b['yes_ask']} x{b['ask_size']:.0f}" if b["yes_ask"] is not None else "-"
        print(f"  {t}: bid {bid:12s} ask {ask}")
    s = feed.stats
    lag = f"{s['lag_ms']:.0f}ms (max {s['lag_ms_max']:.0f}ms)" if s["lag_ms"] is not None else "n/a (no timestamped messages yet)"
    print(f"stats: {s}\nLAG exchange->you: {lag}\n{feed.ticker_check()}")
    feed.stop(); spot.stop()


def record(args: argparse.Namespace) -> None:
    tape = Tape(args.out, compress=not args.no_gzip)
    metas = discover(args.env, args.series, args.horizon_hours)
    known: Dict[str, Dict[str, Any]] = {}
    for m in metas:
        known[m["ticker"]] = m
        tape.write(m)
    tickers = list(known)
    print(f"[rec] {len(tickers)} markets in horizon; writing to {args.out}/")

    feed = KalshiFeed(args.env, args.key_id, args.key_path, tickers, url=args.url,
                      on_raw=lambda m, t: tape.write({"type": "ws", "raw": m}, t))
    feed.batch = args.batch
    feed.start()
    syms = sorted({SERIES_UNDERLYING.get(s, "BTC").lower() + "_usd" for s in args.series})
    spot = DeribitIndexFeed(syms, on_raw=lambda m, t: None).start()

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    last_refresh = last_spot = last_hb = time.time()
    pending_results: Dict[str, float] = {}   # event -> close_ts of settled events whose results we still need
    while not stop.is_set():
        now = time.time()
        if now - last_spot >= args.spot_every:
            for sym, d in spot.snapshot().items():
                tape.write({"type": "spot", "sym": sym, "price": d["price"], "ts": d["ts"]}, now)
            last_spot = now
        if now - last_refresh >= args.refresh:
            try:
                fresh = {m["ticker"]: m for m in discover(args.env, args.series, args.horizon_hours)}
                new = [t for t in fresh if t not in known]
                for t in new:
                    known[t] = fresh[t]; tape.write(fresh[t], now)
                if new:
                    feed.add_tickers(new); print(f"[rec] +{len(new)} markets (next close {min(fresh[t]['close_ts'] for t in new):.0f})")
                gone = [t for t, m in known.items() if m["close_ts"] and m["close_ts"] < now - 120]
                if gone:
                    feed.remove_tickers(gone)
                    for t in gone:
                        pending_results[known[t]["event"]] = known[t]["close_ts"]; known.pop(t)
                # results: settlement lands a few minutes after close; ask once >5 min old, retry until seen
                due = [ev for ev, c in pending_results.items() if now - c > 300]
                if due:
                    got = fetch_results(args.env, due)
                    for r in got:
                        tape.write(r, now)
                    for ev in due:
                        if any(r["ticker"].startswith(ev) for r in got):
                            pending_results.pop(ev, None)
                        elif now - pending_results[ev] > 2 * 3600:
                            print(f"[rec] no result after 2h for {ev}; giving up"); pending_results.pop(ev, None)
            except requests.RequestException as e:
                print(f"[rec] refresh failed: {e}")
            last_refresh = now
        if now - last_hb >= args.heartbeat:
            ready = feed.ready_tickers()
            s = feed.stats
            lag = f"{s['lag_ms']:.0f}" if s["lag_ms"] is not None else "-"
            print(f"[rec] {dt.datetime.utcnow():%H:%M:%S}Z lines={tape.lines} books={len(ready)}/{len(feed.tickers)} "
                  f"msgs={s['messages']} deltas={s['deltas']} trades={s['trades']} gaps={s['seq_gaps']} "
                  f"reconn={s['reconnects']} lag={lag}ms spot={spot.price('BTC')} conn={'Y' if s['connected'] else 'N'} | "
                  f"{feed.ticker_check()}")
            tape.flush(); last_hb = now
        time.sleep(0.2)
    feed.stop(); spot.stop(); tape.flush()
    print(f"[rec] stopped; {tape.lines} lines")


def main(argv: Optional[List[str]] = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", default="prod", choices=list(WS_HOSTS))
    ap.add_argument("--url", default=None, help="override WS url (e.g. the dedicated host)")
    ap.add_argument("--series", nargs="+", default=["KXBTC"])
    ap.add_argument("--horizon-hours", type=float, default=3.0, help="subscribe to markets closing within this many hours")
    ap.add_argument("--out", default="data/tape")
    ap.add_argument("--refresh", type=float, default=120, help="seconds between market re-discovery")
    ap.add_argument("--spot-every", type=float, default=1.0, help="seconds between spot samples written to tape")
    ap.add_argument("--heartbeat", type=float, default=60)
    ap.add_argument("--batch", type=int, default=100, help="tickers per subscription command")
    ap.add_argument("--no-gzip", action="store_true", help="write plain .jsonl instead of .jsonl.gz")
    ap.add_argument("--key-id", default=None)
    ap.add_argument("--key-path", default=None)
    ap.add_argument("--probe", action="store_true", help="print first raw message of each type and exit")
    ap.add_argument("--probe-seconds", type=float, default=20)
    args = ap.parse_args(argv)
    _load_dotenv(".env.demo" if args.env.startswith("demo") else ".env")
    _load_dotenv(".env")
    if args.probe:
        probe(args)
    else:
        record(args)


if __name__ == "__main__":
    main()
