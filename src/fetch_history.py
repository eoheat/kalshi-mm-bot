"""Fetch historical Kalshi + Deribit data for backtesting.

STDLIB-ONLY on purpose — run this in your normal macOS Terminal (which has
full network access), no venv needed:

    cd ~/Documents/kalshi-mm-bot
    python3 -m src.fetch_history --days 14

It discovers Kalshi's BTC price series, downloads settled markets +
1-minute candlesticks, plus Deribit BTC spot (1-min) and the DVOL implied-
vol index, and writes everything under data/history/. Public endpoints
only — no API key needed. Re-running skips files already downloaded.
"""
from __future__ import annotations

import argparse
import json
import os
import time
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timezone

KALSHI = "https://api.elections.kalshi.com/trade-api/v2"
DERIBIT = "https://www.deribit.com/api/v2"
OUT = os.path.join("data", "history")


_last_req = 0.0
MIN_INTERVAL_S = 0.75  # global throttle: stay politely under the rate limit


def get(url: str, params: dict | None = None, tries: int = 6) -> dict:
    global _last_req
    if params:
        url += "?" + urllib.parse.urlencode(params)
    for i in range(tries):
        gap = MIN_INTERVAL_S - (time.time() - _last_req)
        if gap > 0:
            time.sleep(gap)
        _last_req = time.time()
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "kalshi-mm-bot-backtest"})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read())
        except Exception as e:
            if i == tries - 1:
                raise
            is_429 = "429" in str(e)
            wait = (5 * (i + 1)) if is_429 else 2 ** i
            if not is_429 or i >= 1:
                print(f"    retry {i+1}: {e} (sleep {wait}s)")
            time.sleep(wait)
    raise RuntimeError("unreachable")


def save(name: str, obj) -> None:
    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT, name), "w") as f:
        json.dump(obj, f)
    print(f"  wrote {OUT}/{name}")


# ---------- Kalshi ----------

CANDIDATE_SERIES = ["KXBTC", "KXBTCD", "KXBTCMAX", "KXBTCMIN", "KXBTC15",
                    "KXBTCH", "BTCUSD", "BTC", "KXETH", "KXETHD"]


def discover_btc_series() -> str:
    """Find the BTC price series: try known tickers first, then deep-scan."""
    found: dict[str, int] = {}
    for s in CANDIDATE_SERIES:
        try:
            res = get(f"{KALSHI}/markets", {"series_ticker": s, "limit": 100, "status": "open"})
            n = len(res.get("markets", []))
            if n:
                found[s] = n
                print(f"  series {s}: {n} open markets")
        except Exception:
            pass
        time.sleep(0.1)
    if found:
        best = max(found, key=lambda k: found[k])
        print(f"candidate series with open markets: {found} -> using {best}")
        return best

    print("known tickers empty — deep-scanning all open markets (may take a minute)...")
    counts: Counter[str] = Counter()
    cursor = None
    for page in range(30):
        params = {"limit": 200, "status": "open"}
        if cursor:
            params["cursor"] = cursor
        res = get(f"{KALSHI}/markets", params)
        for m in res.get("markets", []):
            t = m.get("ticker", "")
            if "BTC" in t.upper():
                counts[t.split("-")[0]] += 1
        cursor = res.get("cursor")
        if not cursor:
            break
        if page % 5 == 4:
            print(f"  scanned {(page+1)*200} markets, BTC series so far: {dict(counts)}")
        time.sleep(0.1)
    if not counts:
        raise SystemExit(
            "still no open BTC markets found. Open kalshi.com, find an hourly Bitcoin market, "
            "copy the ticker prefix before the first '-' (e.g. KXBTCD from KXBTCD-26AUG30...), "
            "then rerun:  python3 -m src.fetch_history --days 14 --series <THAT_PREFIX>")
    print(f"BTC series found (open-market counts): {dict(counts)}")
    return counts.most_common(1)[0][0]


def fetch_settled_markets(series: str, days: int) -> list[dict]:
    since = time.time() - days * 86400
    rows, cursor = [], None
    while True:
        params = {"series_ticker": series, "status": "settled", "limit": 200}
        if cursor:
            params["cursor"] = cursor
        res = get(f"{KALSHI}/markets", params)
        batch = res.get("markets", [])
        rows += batch
        cursor = res.get("cursor")
        old = [m for m in batch if _ts(m.get("close_time")) and _ts(m["close_time"]) < since]
        if not cursor or old or not batch:
            break
    rows = [m for m in rows if _ts(m.get("close_time")) and _ts(m["close_time"]) >= since]
    print(f"{len(rows)} settled {series} markets in the last {days} days")
    return rows


def _ts(iso: str | None) -> float | None:
    if not iso:
        return None
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(timezone.utc).timestamp()


def fetch_candles(series: str, markets: list[dict]) -> None:
    for i, m in enumerate(markets):
        ticker = m["ticker"]
        fname = f"candles_{ticker}.json".replace("/", "_")
        if os.path.exists(os.path.join(OUT, fname)):
            continue
        start = int(_ts(m.get("open_time")) or (_ts(m["close_time"]) - 6 * 3600))
        end = int(_ts(m["close_time"]))
        try:
            res = get(f"{KALSHI}/series/{series}/markets/{ticker}/candlesticks",
                      {"start_ts": start, "end_ts": end, "period_interval": 1})
            save(fname, res)
        except Exception as e:
            print(f"  candles failed for {ticker}: {e}")
        if i % 20 == 0:
            print(f"  candles {i+1}/{len(markets)}")


# ---------- Deribit ----------

def fetch_spot(days: int) -> None:
    end = int(time.time() * 1000)
    chunk = 2 * 86400 * 1000  # 2 days of 1-min bars per request
    out = {"ticks": [], "close": []}
    t0 = end - days * 86400 * 1000
    while t0 < end:
        t1 = min(t0 + chunk, end)
        res = get(f"{DERIBIT}/public/get_tradingview_chart_data",
                  {"instrument_name": "BTC-PERPETUAL", "resolution": "1",
                   "start_timestamp": t0, "end_timestamp": t1})
        r = res.get("result", res)
        out["ticks"] += r.get("ticks", [])
        out["close"] += r.get("close", [])
        t0 = t1
        time.sleep(0.2)
    save("spot_1m.json", out)
    print(f"  {len(out['ticks'])} spot minutes")


def fetch_dvol(days: int) -> None:
    end = int(time.time() * 1000)
    start = end - days * 86400 * 1000
    res = get(f"{DERIBIT}/public/get_volatility_index_data",
              {"currency": "BTC", "resolution": "60",
               "start_timestamp": start, "end_timestamp": end})
    r = res.get("result", res)
    save("dvol_1m.json", r)
    print(f"  {len(r.get('data', []))} DVOL points")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--series", default=None, help="override series ticker (else auto-discover)")
    ap.add_argument("--max-markets", type=int, default=300)
    args = ap.parse_args()

    series = args.series or discover_btc_series()
    print(f"using series: {series}")
    markets = fetch_settled_markets(series, args.days)[: args.max_markets]
    save("markets.json", {"series": series, "markets": markets})
    fetch_candles(series, markets)
    print("fetching Deribit spot history...")
    fetch_spot(args.days)
    print("fetching DVOL history...")
    fetch_dvol(args.days)
    print("\ndone — data/history/ is ready for: python3 -m src.backtest")


if __name__ == "__main__":
    main()
