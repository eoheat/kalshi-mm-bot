"""SYNTHETIC tape for smoke-testing the pipeline only. Numbers from it say nothing about Kalshi.

    python -m src.replay.synth_tape --days 2 --hours-per-day 6 --out data/tape/synth.jsonl
    python -m src.replay.run data/tape/synth.jsonl --latency-ms 100 3000

World: GBM spot (sigma 45%/yr), an hourly $250-bracket strip, one market maker quoting the TRUE
lognormal fair value +/- 2c with random size, and Poisson takers who buy the side the coin flip says at
the touch. Because the maker knows the true vol and takers are uninformed, a maker joined at the touch
should look fine and a taker should bleed the spread+fee — that is the expected sanity result.
"""
from __future__ import annotations

import argparse
import json
import math
import random
from typing import Any, Dict, List

from .run import norm_cdf

YEAR_S = 365.0 * 86400.0


def p_bracket(S: float, lo: float, hi: float, tau_s: float, sigma: float) -> float:
    sd = sigma * math.sqrt(max(tau_s, 1.0) / YEAR_S)
    return norm_cdf(math.log(hi / S) / sd) - norm_cdf(math.log(lo / S) / sd)


def main(argv=None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=2)
    ap.add_argument("--hours-per-day", type=int, default=6)
    ap.add_argument("--sigma", type=float, default=0.45)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", default="data/tape/synth.jsonl")
    a = ap.parse_args(argv)
    rnd = random.Random(a.seed)
    T0 = 1_789_000_000.0 - 1_789_000_000.0 % 86400   # a UTC midnight
    S = 81000.0
    lines: List[Dict[str, Any]] = []
    dollar_shape = False
    for d in range(a.days):
        for h in range(a.hours_per_day):
            open_ts = T0 + d * 86400 + h * 3600
            close_ts = open_ts + 3600
            ev = f"KXSYN-{d}-{h}"
            center = round(S / 250) * 250
            mkts = []
            for i in range(-12, 13):
                lo, hi = center + i * 250, center + (i + 1) * 250
                tk = f"{ev}-B{int(lo + 125)}"
                mkts.append((tk, lo, hi))
                lines.append({"t": open_ts - 1, "type": "market", "ticker": tk, "event": ev, "series": "KXBTC",
                              "strike_type": "between", "lo": lo, "hi": hi, "close_ts": close_ts, "open_ts": open_ts})
            quotes: Dict[str, Dict[str, int]] = {}
            spot_path = []
            for sec in range(0, 3601):
                t = open_ts + sec
                S *= math.exp(rnd.gauss(0, a.sigma * math.sqrt(1 / YEAR_S)))
                spot_path.append(S)
                lines.append({"t": t, "type": "spot", "sym": "BTC", "price": round(S, 2), "ts": t})
                if sec % 60 == 0 and sec < 3600:
                    dollar_shape = not dollar_shape
                    for tk, lo, hi in mkts:
                        p = p_bracket(S, lo, hi, close_ts - t, a.sigma)
                        fair = 100 * p
                        bid, ask = int(math.floor(fair - 2)), int(math.ceil(fair + 2))
                        bid, ask = max(bid, 1), min(ask, 99)
                        if fair < 1.5:
                            bid = 0
                        bs, as_ = rnd.randint(20, 400), rnd.randint(20, 400)
                        yes_lv = [[bid, bs]] if bid >= 1 else []
                        no_lv = [[100 - ask, as_]] if ask <= 99 else []
                        if dollar_shape:
                            msg = {"market_ticker": tk, "yes_dollars": [[f"{p_ / 100:.4f}", f"{q:.2f}"] for p_, q in yes_lv],
                                   "no_dollars": [[f"{p_ / 100:.4f}", f"{q:.2f}"] for p_, q in no_lv]}
                        else:
                            msg = {"market_ticker": tk, "yes": yes_lv, "no": no_lv}
                        lines.append({"t": t + 0.01, "type": "ws", "raw": {"type": "orderbook_snapshot", "sid": 1, "seq": sec // 60 + 1, "msg": msg}})
                        quotes[tk] = {"bid": bid, "ask": ask}
                        # uninformed takers: Poisson, more active near the money
                        lam = 0.6 * max(p, 0.02)
                        if rnd.random() < lam:
                            side = "yes" if rnd.random() < 0.5 else "no"
                            price = ask if side == "yes" else bid
                            if 1 <= price <= 99:
                                lines.append({"t": t + rnd.uniform(0.02, 59.0), "type": "ws",
                                              "raw": {"type": "trade", "sid": 2, "msg": {"market_ticker": tk, "yes_price": price,
                                                                                         "count": rnd.randint(1, 30), "taker_side": side}}})
            settle = sum(spot_path[-60:]) / 60
            for tk, lo, hi in mkts:
                lines.append({"t": close_ts + 120, "type": "result", "ticker": tk, "result": "yes" if lo <= settle < hi else "no", "close_ts": close_ts})
    lines.sort(key=lambda r: r["t"])
    with open(a.out, "w") as f:
        for r in lines:
            f.write(json.dumps(r, separators=(",", ":")) + "\n")
    print(f"synthetic tape: {len(lines)} lines, {a.days} days x {a.hours_per_day} hours -> {a.out}")


if __name__ == "__main__":
    main()
