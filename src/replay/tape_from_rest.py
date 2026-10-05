"""Build a replay tape from PUBLIC REST history (no key needed) — runs today over past days.

    python -m src.replay.tape_from_rest --series KXBTC --days 7 --out data/tape/rest-KXBTC-7d.jsonl
    python -m src.replay.tape_from_rest --series KXBTC --days 1 --probe        # print raw shapes, fetch nothing else

What it can and cannot give you
  * Settled markets with results, 1-MINUTE top-of-book (bid/ask close of each minute) and every trade with
    its taker side, plus 1-minute Coinbase BTC-USD / ETH-USD closes as spot.
  * NO level sizes and NO intra-minute book states: Kalshi does not serve historical order books. So on
    this tape the maker sim can only credit fills where a trade printed THROUGH your price (queue=back)
    or at it (queue=front), and taker fills are capped at --assume-size. The WebSocket tape from
    src/feed/record.py has everything; this one is for a first look at more days than you have recorded.
  * Selection: only brackets with volume > 0 are fetched (that is where a live bot would have been quoting
    too, but it is still a mild look-ahead — say so when you quote results).

Rate limits: ~10 public requests/s; the script sleeps between calls. 7 days of the daily strip is a few
hundred calls; 7 days of the HOURLY strip is thousands (use --max-per-event 20 and --days 2 first).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional

import requests

sys.path.insert(0, ".")
from src.feed.kalshi_ws import price_cents, qty_float  # noqa: E402
from src.feed.record import REST_HOSTS, market_meta, _iso_to_ts  # noqa: E402

COINBASE = "https://api.exchange.coinbase.com/products/{pair}/candles"
PAIRS = {"BTC": "BTC-USD", "ETH": "ETH-USD"}
SLEEP = 0.12


def get(url: str, params: Dict[str, Any], tries: int = 5) -> Any:
    for i in range(tries):
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(1.5 * (i + 1)); continue
        r.raise_for_status()
        time.sleep(SLEEP)
        return r.json()
    r.raise_for_status()


def _close(d: Any, key_hint: str = "") -> Optional[int]:
    """candle sub-object {open,low,high,close} (ints or dollar strings) -> close in cents"""
    if isinstance(d, dict):
        return price_cents(d.get("close"), key_hint)
    return price_cents(d, key_hint)


def candle_to_tob(c: Dict[str, Any], ticker: str) -> Optional[Dict[str, Any]]:
    """Kalshi candlestick -> tape 'tob' record at the minute's end. bid 0 / ask 100 mean 'no quote'."""
    ts = c.get("end_period_ts") or c.get("end_ts") or c.get("ts")
    if ts is None:
        return None
    bk = next((k for k in ("yes_bid", "yes_bid_dollars") if k in c), None)
    ak = next((k for k in ("yes_ask", "yes_ask_dollars") if k in c), None)
    bid = _close(c.get(bk), bk or "") if bk else None
    ask = _close(c.get(ak), ak or "") if ak else None
    if bid is not None and bid <= 0:
        bid = None
    if ask is not None and ask >= 100:
        ask = None
    return {"t": float(ts), "type": "tob", "ticker": ticker, "yes_bid": bid, "yes_ask": ask,
            "volume": qty_float(c.get("volume_fp", c.get("volume", 0)))}


def trade_to_rec(tr: Dict[str, Any], ticker: str) -> Optional[Dict[str, Any]]:
    ts = tr.get("created_time") or tr.get("ts")
    t = _iso_to_ts(ts) if isinstance(ts, str) else (float(ts) if ts is not None else None)
    if t is None:
        return None
    pk = next((k for k in ("yes_price_dollars", "yes_price") if k in tr), None)
    return {"t": t, "type": "trade", "ticker": ticker, "yes_price": price_cents(tr.get(pk), pk or ""),
            "count": qty_float(tr.get("count_fp", tr.get("count"))), "taker_side": (tr.get("taker_side") or "").lower() or None}


def coinbase_candles(pair: str, start_ts: float, end_ts: float) -> List[Dict[str, Any]]:
    """1-minute closes as tape 'spot' records, oldest first."""
    out: List[Dict[str, Any]] = []
    sym = pair.split("-")[0]
    t0 = start_ts
    while t0 < end_ts:
        t1 = min(t0 + 300 * 60, end_ts)
        body = get(COINBASE.format(pair=pair), {"granularity": 60,
                                                 "start": dt.datetime.fromtimestamp(t0, dt.timezone.utc).isoformat(),
                                                 "end": dt.datetime.fromtimestamp(t1, dt.timezone.utc).isoformat()})
        for row in sorted(body, key=lambda r: r[0]):      # [time, low, high, open, close, volume]
            out.append({"t": float(row[0]) + 60.0, "type": "spot", "sym": sym, "price": float(row[4]), "ts": float(row[0]) + 60.0})
        t0 = t1
    return out


def settled_markets(base: str, series: str, min_close_ts: float, max_close_ts: float) -> List[Dict[str, Any]]:
    out, cursor = [], None
    while True:
        params: Dict[str, Any] = {"series_ticker": series, "status": "settled", "limit": 1000,
                                  "min_close_ts": int(min_close_ts), "max_close_ts": int(max_close_ts)}
        if cursor:
            params["cursor"] = cursor
        body = get(base + "/markets", params)
        rows = body.get("markets", [])
        out.extend(m for m in rows if min_close_ts <= (_iso_to_ts(m.get("close_time")) or 0) <= max_close_ts)
        cursor = body.get("cursor")
        if not cursor or not rows:
            return out


def build(args: argparse.Namespace) -> None:
    base = REST_HOSTS["prod"]
    now = time.time()
    min_close, max_close = now - args.days * 86400, now
    lines: List[Dict[str, Any]] = []
    spot_syms = set()
    probe_rows: Dict[str, List[Dict[str, Any]]] = {}
    for series in args.series:
        sym = "ETH" if "ETH" in series else "BTC"
        spot_syms.add(sym)
        rows = settled_markets(base, series, min_close, max_close)
        by_event: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for m in rows:
            by_event[m.get("event_ticker", "?")].append(m)
        print(f"[tape] {series}: {len(rows)} settled markets in {len(by_event)} events over {args.days}d")
        if args.probe:
            probe_rows[series] = rows
            if rows:
                print("--- first settled market row:\n", json.dumps(rows[0])[:1500])
            continue
        n_c = n_t = 0
        for ev, ms in sorted(by_event.items()):
            ms = [m for m in ms if qty_float(m.get("volume_fp", m.get("volume", 0))) > 0]
            ms.sort(key=lambda m: -qty_float(m.get("volume_fp", m.get("volume", 0))))
            ms = ms[: args.max_per_event]
            for m in ms:
                meta = market_meta(m)
                if not meta["close_ts"]:
                    continue
                meta["t"] = (meta["open_ts"] or meta["close_ts"] - 86400) - 1
                meta["sym"] = sym
                lines.append(meta)
                start = int(meta["open_ts"] or meta["close_ts"] - 86400)
                try:
                    body = get(f"{base}/series/{series}/markets/{m['ticker']}/candlesticks",
                               {"start_ts": start, "end_ts": int(meta["close_ts"]) + 60, "period_interval": 1})
                    for c in body.get("candlesticks", []):
                        rec = candle_to_tob(c, m["ticker"])
                        if rec and (rec["yes_bid"] is not None or rec["yes_ask"] is not None):
                            lines.append(rec); n_c += 1
                except requests.RequestException as e:
                    print(f"[tape] candles failed {m['ticker']}: {e}")
                cursor = None
                while True:
                    try:
                        body = get(base + "/markets/trades", dict(ticker=m["ticker"], limit=1000, **({"cursor": cursor} if cursor else {})))
                    except requests.RequestException as e:
                        print(f"[tape] trades failed {m['ticker']}: {e}"); break
                    trs = body.get("trades", [])
                    for tr in trs:
                        rec = trade_to_rec(tr, m["ticker"])
                        if rec and rec["yes_price"] is not None:
                            lines.append(rec); n_t += 1
                    cursor = body.get("cursor")
                    if not cursor or not trs:
                        break
            print(f"[tape] {ev}: {len(ms)} brackets, candles so far {n_c}, trades {n_t}")
    if args.probe:
        m0 = next(((s, r[0]) for s, r in probe_rows.items() if r), None)
        if m0:
            series, m = m0
            body = get(f"{base}/series/{series}/markets/{m['ticker']}/candlesticks",
                       {"start_ts": int(_iso_to_ts(m["close_time"])) - 3600, "end_ts": int(_iso_to_ts(m["close_time"])), "period_interval": 1})
            cs = body.get("candlesticks", [])
            print(f"--- candlesticks: {len(cs)} rows; first:\n", json.dumps(cs[0])[:800] if cs else body)
            body = get(base + "/markets/trades", {"ticker": m["ticker"], "limit": 3})
            print("--- trades:\n", json.dumps(body)[:800])
        cb = get(COINBASE.format(pair="BTC-USD"), {"granularity": 60})
        print("--- coinbase candle row:", cb[0] if cb else cb)
        return
    for sym in sorted(spot_syms):
        sp = coinbase_candles(PAIRS[sym], min_close - 2 * 86400, max_close)
        print(f"[tape] {sym} spot: {len(sp)} minutes from Coinbase")
        lines.extend(sp)
    lines.sort(key=lambda r: r["t"])
    with open(args.out, "w") as f:
        for r in lines:
            f.write(json.dumps(r, separators=(",", ":")) + "\n")
    print(f"[tape] wrote {len(lines)} lines to {args.out}")


def main(argv: Optional[List[str]] = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--series", nargs="+", default=["KXBTC"])
    ap.add_argument("--days", type=float, default=3.0)
    ap.add_argument("--max-per-event", type=int, default=40, help="most-traded brackets per settlement to fetch")
    ap.add_argument("--out", default="data/tape/rest.jsonl")
    ap.add_argument("--probe", action="store_true")
    build(ap.parse_args(argv))


if __name__ == "__main__":
    main()
