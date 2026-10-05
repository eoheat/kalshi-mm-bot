"""Replay backtest over a recorded tape: taker vs maker, same fair-value model, honest fills.

    python -m src.replay.run data/tape/*.jsonl                       # default grid, both queue modes
    python -m src.replay.run data/tape/*.jsonl --latency-ms 100 3000   # WebSocket-speed vs polling-speed
    python -m src.replay.run data/tape/*.jsonl --only maker_fair --csv out.csv

Tape = JSONL from src/feed/record.py (WebSocket tape: full books, 1s spot) or
src/replay/tape_from_rest.py (REST tape: 1-minute top-of-book candles + trades).

What is simulated
  * Fair value: lognormal, sigma = EWMA realized vol of the tape's own spot x k (plug your own in FairValue).
  * Taker: decides at t, order lands at t+latency, re-reads the book, re-checks edge at the live touch,
    fills at the resting ask up to its size (IOC), fee 0.07*p*(1-p)/contract.
  * Maker: desired quotes at t go live at t+latency (post_only: a crossing quote is rejected); a cancel
    takes latency too, so a stale quote can be picked off in that window. A resting order fills only when a
    real TRADE prints through it (price priority) or at it after the size that was ahead in the queue is
    consumed. queue=back uses the real level size at placement (pessimistic); queue=front assumes we are
    first (optimistic). Report both — the truth is in between.
  * Settlement: the tape's 'result' records (fetched from Kalshi); if missing, the 60s spot average before close.
  * Sizing: fixed contracts per order, so strategies compare on edge, not on bet size.

The winner by TRAIN days is a biased pick (you chose it because it won). Judge it by its TEST days.
"""
from __future__ import annotations

import argparse
import bisect
import csv
import datetime as dt
import glob
import gzip
import json
import zlib
import math
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

sys.path.insert(0, ".")
from src.feed.kalshi_ws import Book, parse_delta, parse_snapshot, parse_trade  # noqa: E402

TAKER_FEE = 0.07
YEAR_S = 365.0 * 86400.0


# --------------------------------------------------------------------------- data
@dataclass
class Market:
    ticker: str
    event: str
    series: str
    lo: Optional[float]
    hi: Optional[float]
    close_ts: float
    open_ts: Optional[float] = None
    result: Optional[str] = None
    sym: str = "BTC"
    settled_by_spot: bool = False

    @property
    def day(self) -> str:
        return dt.datetime.fromtimestamp(self.close_ts, dt.timezone.utc).strftime("%Y-%m-%d")


@dataclass
class Lot:
    side: str          # "yes" | "no"
    price_c: int       # what we paid per contract, in cents, for that side
    qty: float
    fee: float         # dollars, total for the lot
    t: float
    kind: str          # "take" | "make"


@dataclass
class Resting:
    side: str          # "bid" (we buy YES at price_c) | "ask" (we sell YES at price_c = buy NO at 100-price_c)
    price_c: int
    qty: float
    live_at: float
    dies_at: float = math.inf
    ahead: Optional[float] = None   # queue ahead of us at our price (None = unknown -> at-price prints never fill)


def fee_dollars(rate: float, price_c: int, qty: float) -> float:
    p = price_c / 100.0
    return rate * p * (1 - p) * qty


def fee_cents(rate: float, price_c: int) -> float:
    return fee_dollars(rate, price_c, 1) * 100


# --------------------------------------------------------------------------- fair value
def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


class FairValue:
    """Lognormal bracket probability with EWMA realized vol from the tape's spot samples.
    Replace .p_bracket with the repo's model (Deribit-implied / oracle) to backtest that instead."""

    def __init__(self, k: float = 0.8, halflife_s: float = 3600.0, prior_sigma: float = 0.45) -> None:
        self.k, self.prior = k, prior_sigma
        self.alpha = 1 - 0.5 ** (1.0 / max(halflife_s, 1.0))   # per-second decay -> applied per dt
        self.state: Dict[str, Dict[str, float]] = {}            # sym -> {price, ts, var_s, n}

    def on_spot(self, sym: str, price: float, ts: float) -> None:
        s = self.state.get(sym)
        if s is None:
            self.state[sym] = {"price": price, "ts": ts, "var_s": (self.prior ** 2) / YEAR_S, "n": 0}
            return
        dt_s = ts - s["ts"]
        if dt_s <= 0 or price <= 0 or s["price"] <= 0:
            return
        r2 = math.log(price / s["price"]) ** 2 / dt_s          # variance per second, this interval
        a = 1 - (1 - self.alpha) ** dt_s
        s["var_s"] = (1 - a) * s["var_s"] + a * r2
        s["price"], s["ts"], s["n"] = price, ts, s["n"] + 1

    def spot(self, sym: str) -> Optional[float]:
        s = self.state.get(sym)
        return s["price"] if s else None

    def sigma_annual(self, sym: str) -> float:
        s = self.state.get(sym)
        return self.prior if not s or s["n"] < 30 else math.sqrt(s["var_s"] * YEAR_S)

    def p_bracket(self, m: Market, ts: float) -> Optional[float]:
        S = self.spot(m.sym)
        if S is None:
            return None
        tau = max(m.close_ts - ts, 1.0) / YEAR_S
        sd = self.k * self.sigma_annual(m.sym) * math.sqrt(tau)
        if sd <= 0:
            return None
        hi = norm_cdf(math.log(m.hi / S) / sd) if m.hi else 1.0
        lo = norm_cdf(math.log(m.lo / S) / sd) if m.lo else 0.0
        return min(max(hi - lo, 0.0), 1.0)


# --------------------------------------------------------------------------- strategies
@dataclass
class Ctx:
    t: float
    mkt: Market
    p: float                      # fair probability of YES
    ttl: float                    # seconds to close
    yes_bid: Optional[int]
    yes_ask: Optional[int]
    bid_size: Optional[float]
    ask_size: Optional[float]
    inv: float                    # net YES contracts held (NO counts negative)
    takes: int                    # takes so far on this market
    jump: float                   # |log spot move| over the last 10s, in units of 10s-sigma


class Strategy:
    family = "base"

    def __init__(self, **params: Any) -> None:
        self.params = params

    @property
    def name(self) -> str:
        return self.family + "(" + ",".join(f"{k}={v}" for k, v in self.params.items()) + ")"

    def taker_fee_rate(self) -> float:
        return TAKER_FEE

    def decide(self, c: Ctx) -> Dict[str, Any]:
        """-> {"take": (side, limit_c, qty)} and/or {"bid": (price_c, qty)|None, "ask": (price_c, qty)|None}"""
        return {}


class TakerEdge(Strategy):
    """In the last [ttl_lo, ttl_hi] seconds, buy the side whose fair value beats the touch
    by min_edge cents after fee. Limit = touch + slippage (fills at the resting price if it is still there)."""
    family = "taker"

    def decide(self, c: Ctx) -> Dict[str, Any]:
        P = self.params
        if not (P["ttl_lo"] <= c.ttl <= P["ttl_hi"]) or c.takes >= P["max_takes"]:
            return {}
        best, side, limit = -1e9, None, None
        if c.yes_ask is not None and 1 <= c.yes_ask <= 99:
            e = 100 * c.p - c.yes_ask - fee_cents(TAKER_FEE, c.yes_ask)
            if e > best:
                best, side, limit = e, "yes", c.yes_ask + P["slippage"]
        if P["both_sides"] and c.yes_bid is not None and 1 <= c.yes_bid <= 99:
            no_ask = 100 - c.yes_bid
            e = 100 * (1 - c.p) - no_ask - fee_cents(TAKER_FEE, no_ask)
            if e > best:
                best, side, limit = e, "no", no_ask + P["slippage"]
        if side and best >= P["min_edge"]:
            return {"take": (side, limit, P["size"])}
        return {}


class MakerFair(Strategy):
    """Quote fair +/- half_spread with inventory skew; pull quotes on a spot jump or inside min_ttl."""
    family = "maker_fair"

    def decide(self, c: Ctx) -> Dict[str, Any]:
        P = self.params
        if c.ttl < P["min_ttl"] or not (P["p_lo"] <= c.p <= P["p_hi"]) or c.jump > P["jump_halt"]:
            return {"bid": None, "ask": None}
        fair_c = 100 * c.p - P["skew"] * c.inv
        bid = int(math.floor(fair_c - P["half_spread"]))
        ask = int(math.ceil(fair_c + P["half_spread"]))
        bid, ask = max(1, min(bid, 99)), max(1, min(ask, 99))
        if ask <= bid:
            ask = bid + 1
        out: Dict[str, Any] = {"bid": None, "ask": None}
        if c.inv < P["max_inv"] and bid >= 1:
            out["bid"] = (bid, P["size"])
        if c.inv > -P["max_inv"] and ask <= 99:
            out["ask"] = (ask, P["size"])
        return out


class MakerJoin(Strategy):
    """No model: join (improve=0) or step inside (improve=1) the current best bid/ask. Tests whether
    liquidity alone gets paid. Only quotes when both sides exist and the spread covers the improvement."""
    family = "maker_join"

    def decide(self, c: Ctx) -> Dict[str, Any]:
        P = self.params
        if c.ttl < P["min_ttl"] or c.jump > P["jump_halt"] or c.yes_bid is None or c.yes_ask is None:
            return {"bid": None, "ask": None}
        if c.yes_ask - c.yes_bid < P["min_spread"] or not (P["p_lo"] <= c.p <= P["p_hi"]):
            return {"bid": None, "ask": None}
        bid, ask = c.yes_bid + P["improve"], c.yes_ask - P["improve"]
        if ask - bid < 1:
            return {"bid": None, "ask": None}
        out: Dict[str, Any] = {"bid": None, "ask": None}
        if c.inv < P["max_inv"]:
            out["bid"] = (bid, P["size"])
        if c.inv > -P["max_inv"]:
            out["ask"] = (ask, P["size"])
        return out


def default_grid(size: int) -> List[Strategy]:
    g: List[Strategy] = []
    for e in (4, 6, 8, 12):
        g.append(TakerEdge(min_edge=e, ttl_lo=60, ttl_hi=600, max_takes=2, both_sides=True, slippage=2, size=size))
    g.append(TakerEdge(min_edge=6, ttl_lo=60, ttl_hi=1800, max_takes=2, both_sides=True, slippage=2, size=size))
    for hs in (1, 2, 3, 4):
        g.append(MakerFair(half_spread=hs, skew=0.2, max_inv=4 * size, min_ttl=120, p_lo=0.05, p_hi=0.95,
                           jump_halt=3.0, size=size))
    g.append(MakerFair(half_spread=2, skew=0.5, max_inv=2 * size, min_ttl=300, p_lo=0.10, p_hi=0.90, jump_halt=2.0, size=size))
    for imp in (0, 1):
        g.append(MakerJoin(improve=imp, min_spread=3, max_inv=4 * size, min_ttl=120, p_lo=0.03, p_hi=0.97,
                           jump_halt=3.0, size=size))
    return g


# --------------------------------------------------------------------------- tape
def load_tape(paths: Iterable[str], near_brackets: Optional[float] = 12.0) -> List[Tuple[float, str, Dict[str, Any]]]:
    """-> [(t, kind, payload)] sorted by t. kinds: market, spot, snapshot, delta, trade, tob, result.
    Reads .jsonl or .jsonl.gz. near_brackets drops book/trade events for brackets more than that many bracket-widths
    from the latest spot (far-OTM 1c/99c churn is most of a raw tape and no strategy here quotes it)."""
    ev: List[Tuple[float, str, Dict[str, Any]]] = []
    files = [f for p in paths for f in sorted(glob.glob(p))]
    mk: Dict[str, Tuple[Optional[float], Optional[float], str]] = {}   # ticker -> (lo, hi, sym)
    spot: Dict[str, float] = {}
    dropped = 0

    def near(ticker: str) -> bool:
        if near_brackets is None:
            return True
        m = mk.get(ticker)
        if m is None:
            return True
        lo, hi, sym = m
        S = spot.get(sym)
        if S is None or lo is None or hi is None:
            return True
        return abs((lo + hi) / 2.0 - S) <= near_brackets * (hi - lo)

    for f in files:
        opener = gzip.open if f.endswith(".gz") else open
        n_lines = 0
        try:
            with opener(f, "rt") as fh:
              for line in fh:
                  line = line.strip()
                  if not line:
                      continue
                  try:
                      r = json.loads(line)
                  except json.JSONDecodeError:
                      continue                                   # half-written last line of a live file
                  n_lines += 1
                  t, typ = float(r["t"]), r.get("type")
                  if typ == "ws":
                      raw = r["raw"]
                      rt, msg = raw.get("type"), raw.get("msg") or {}
                      if rt not in ("orderbook_snapshot", "orderbook_delta", "trade"):
                          continue
                      tk = msg.get("market_ticker") or msg.get("ticker")
                      if rt != "trade" and not near(tk):
                          dropped += 1; continue
                      if rt == "orderbook_snapshot":
                          ev.append((t, "snapshot", msg))
                      elif rt == "orderbook_delta":
                          ev.append((t, "delta", msg))
                      else:
                          ev.append((t, "trade", parse_trade(msg)))
                  elif typ == "market":
                      mk[r["ticker"]] = (r.get("lo"), r.get("hi"), "ETH" if "ETH" in (r.get("series") or r["ticker"]) else "BTC")
                      ev.append((t, typ, r))
                  elif typ == "spot":
                      spot[r["sym"].upper()] = float(r["price"])
                      ev.append((t, typ, r))
                  elif typ in ("tob", "trade"):
                      if typ == "tob" and not near(r["ticker"]):
                          dropped += 1; continue
                      ev.append((t, typ, r))
                  elif typ == "result":
                      ev.append((t, typ, r))
        except (EOFError, OSError, zlib.error) as e:           # gzip member not finished: recorder still writing it
            print(f"tape: {f} is still being written ({type(e).__name__}); using the {n_lines} complete lines it has")
    ev.sort(key=lambda e: e[0])
    if dropped:
        print(f"tape: dropped {dropped} far-from-spot book events (>{near_brackets} brackets away); --near-brackets 0 keeps all")
    return ev


# --------------------------------------------------------------------------- engine
class Sim:
    RESULT_WAIT_S = 900.0   # settle from the spot average only if no 'result' record arrives within this after close

    def __init__(self, strat: Strategy, fv: FairValue, latency_s: float, queue: str, maker_fee: float,
                 tick_every: float, assume_size: float) -> None:
        self.s, self.fv, self.lat, self.queue, self.maker_fee = strat, fv, latency_s, queue, maker_fee
        self.tick_every, self.assume_size = tick_every, assume_size
        self.markets: Dict[str, Market] = {}
        self.books: Dict[str, Book] = {}
        self.tob: Dict[str, Dict[str, Optional[float]]] = {}   # REST tapes: top of book, sizes unknown
        self.lots: Dict[str, List[Lot]] = defaultdict(list)
        self.resting: Dict[str, Dict[str, Resting]] = defaultdict(dict)   # ticker -> {"bid": .., "ask": ..}
        self.pending: List[Tuple[float, int, str, Dict[str, Any]]] = []   # (due, seq, kind, payload)
        self.takes: Dict[str, int] = defaultdict(int)
        self.last_tick: Dict[str, float] = {}
        self.spot_hist: Dict[str, List[Tuple[float, float]]] = defaultdict(list)
        self.settled: Dict[str, Dict[str, Any]] = {}
        self.fills: List[Dict[str, Any]] = []
        self._seq = 0
        self.dead: set = set()
        self.inflight: Dict[str, Dict[str, int]] = defaultdict(dict)   # ticker -> side -> price being placed
        self.unsettled: List[str] = []
        self.active: List[str] = []
        self._last_active = -1e9
        self._last_sweep = -1e9
        self._close_q: List[Tuple[float, str]] = []

    # ---- book helpers
    def best(self, t: str) -> Dict[str, Optional[float]]:
        b = self.books.get(t)
        if b is not None and b.ready:
            return b.best()
        return self.tob.get(t, {"yes_bid": None, "yes_ask": None, "bid_size": None, "ask_size": None})

    def level_size(self, t: str, side: str, price_c: int) -> Optional[float]:
        """size resting on OUR side at our price: 'bid' -> YES bids at price; 'ask' -> NO bids at 100-price."""
        b = self.books.get(t)
        if b is None or not b.ready:
            return None
        return b.yes.get(price_c, 0.0) if side == "bid" else b.no.get(100 - price_c, 0.0)

    def inv(self, t: str) -> float:
        return sum(l.qty if l.side == "yes" else -l.qty for l in self.lots[t])

    def jump(self, sym: str, now: float) -> float:
        h = self.spot_hist[sym]
        if len(h) < 2:
            return 0.0
        i = bisect.bisect_left(h, (now - 10.0, -1.0))
        if i >= len(h):
            i = len(h) - 1
        p0, p1 = h[i][1], h[-1][1]
        sig10 = self.fv.sigma_annual(sym) * math.sqrt(10.0 / YEAR_S)
        return abs(math.log(p1 / p0)) / sig10 if p0 > 0 and sig10 > 0 else 0.0

    # ---- scheduling
    def schedule(self, due: float, kind: str, payload: Dict[str, Any]) -> None:
        self._seq += 1
        bisect.insort(self.pending, (due, self._seq, kind, payload))

    def run_due(self, now: float) -> None:
        while self.pending and self.pending[0][0] <= now:
            due, _, kind, pl = self.pending.pop(0)
            if kind == "take":
                self.exec_take(due, pl)
            elif kind == "place":
                self.exec_place(due, pl)

    # ---- taker execution (re-read, edge re-check, marketable limit)
    def exec_take(self, now: float, pl: Dict[str, Any]) -> None:
        t, side, limit, qty = pl["ticker"], pl["side"], pl["limit"], pl["qty"]
        if t in self.dead:
            return
        m, b = self.markets[t], self.best(t)
        p = self.fv.p_bracket(m, now)
        if p is None:
            return
        if side == "yes":
            touch, size = b["yes_ask"], b["ask_size"]
            edge = (100 * p - touch - fee_cents(TAKER_FEE, touch)) if touch is not None else -1
        else:
            touch = (100 - b["yes_bid"]) if b["yes_bid"] is not None else None
            size = b["bid_size"]
            edge = (100 * (1 - p) - touch - fee_cents(TAKER_FEE, touch)) if touch is not None else -1
        if touch is None or touch > limit or edge < self.s.params.get("min_edge", 0):
            self.fills.append({"t": now, "ticker": t, "kind": "take_cancel", "side": side, "qty": 0})
            self.takes[t] -= 1                            # IOC cancelled: the bot may try again next tick
            return
        avail = size if size is not None else self.assume_size
        q = min(qty, avail)
        if q <= 0:
            self.takes[t] -= 1
            return
        fee = fee_dollars(TAKER_FEE, touch, q)
        self.lots[t].append(Lot(side, touch, q, fee, now, "take"))
        self.fills.append({"t": now, "ticker": t, "kind": "take", "side": side, "price": touch, "qty": q, "fee": fee, "p": p})

    # ---- maker placement / cancellation with latency
    def want_quotes(self, now: float, t: str, want: Dict[str, Any]) -> None:
        for side in ("bid", "ask"):
            if side not in want:
                continue
            cur = self.resting[t].get(side)
            w = want[side]
            if cur is None and w is None:
                continue
            if cur is not None and w is not None and cur.price_c == w[0] and cur.dies_at == math.inf:
                continue                                   # unchanged: keep queue position
            if cur is not None and cur.dies_at == math.inf:
                cur.dies_at = now + self.lat                # cancel lands after latency
            if w is not None and self.inflight[t].get(side) != w[0]:
                self.inflight[t][side] = w[0]
                self.schedule(now + self.lat, "place", {"ticker": t, "side": side, "price": w[0], "qty": w[1]})

    def exec_place(self, now: float, pl: Dict[str, Any]) -> None:
        t, side, price, qty = pl["ticker"], pl["side"], pl["price"], pl["qty"]
        if self.inflight[t].get(side) == price:
            self.inflight[t].pop(side, None)
        if t in self.dead:
            return
        b = self.best(t)
        # post_only: reject if it would cross
        if side == "bid" and b["yes_ask"] is not None and price >= b["yes_ask"]:
            return
        if side == "ask" and b["yes_bid"] is not None and price <= b["yes_bid"]:
            return
        old = self.resting[t].get(side)
        if old is not None and old.dies_at > now:
            old.dies_at = now                              # replaced
        ahead = 0.0 if self.queue == "front" else self.level_size(t, side, price)
        self.resting[t][side] = Resting(side, price, qty, live_at=now, ahead=ahead)

    def sweep_dead(self, t: str, now: float) -> None:
        for side in ("bid", "ask"):
            r = self.resting[t].get(side)
            if r is not None and r.dies_at <= now:
                self.resting[t].pop(side, None)

    # ---- fills from real trades
    def on_trade(self, now: float, tr: Dict[str, Any]) -> None:
        t, P, C, taker = tr.get("ticker"), tr.get("yes_price"), tr.get("count") or 0.0, tr.get("taker_side")
        if t not in self.markets or P is None or C <= 0:
            return
        if taker not in ("yes", "no"):
            b = self.best(t)
            if b["yes_ask"] is not None and P >= b["yes_ask"]:
                taker = "yes"
            elif b["yes_bid"] is not None and P <= b["yes_bid"]:
                taker = "no"
            else:
                return                                     # ambiguous print: no fill (pessimistic)
        side = "ask" if taker == "yes" else "bid"          # taker buying YES lifts our ASK; selling YES hits our BID
        r = self.resting[t].get(side)
        if r is None or not (r.live_at <= now < r.dies_at) or r.qty <= 0:
            return
        through = (P > r.price_c) if side == "ask" else (P < r.price_c)
        q = 0.0
        if through:
            q = min(r.qty, C)
        elif P == r.price_c:
            if r.ahead is None:
                q = 0.0                                    # unknown queue -> at-price prints never fill
            else:
                use = min(C, r.ahead)
                r.ahead -= use
                q = min(r.qty, C - use)
        if q <= 0:
            return
        r.qty -= q
        if side == "bid":
            lot_side, paid = "yes", r.price_c
        else:
            lot_side, paid = "no", 100 - r.price_c
        fee = fee_dollars(self.maker_fee, paid, q)
        self.lots[t].append(Lot(lot_side, paid, q, fee, now, "make"))
        self.fills.append({"t": now, "ticker": t, "kind": "make", "side": lot_side, "price": paid, "qty": q, "fee": fee,
                           "p": self.fv.p_bracket(self.markets[t], now)})
        if r.qty <= 1e-9:
            self.resting[t].pop(side, None)

    # ---- strategy tick
    def tick(self, now: float, t: str) -> None:
        m = self.markets[t]
        if t in self.dead or now >= m.close_ts:
            return
        p = self.fv.p_bracket(m, now)
        if p is None:
            return
        b = self.best(t)
        c = Ctx(now, m, p, m.close_ts - now, b["yes_bid"], b["yes_ask"], b["bid_size"], b["ask_size"],
                self.inv(t), self.takes[t], self.jump(m.sym, now))
        d = self.s.decide(c)
        if "take" in d:
            side, limit, qty = d["take"]
            self.takes[t] += 1                             # counts scheduled takes so max_takes gates the loop
            self.schedule(now + self.lat, "take", {"ticker": t, "side": side, "limit": limit, "qty": qty})
        if "bid" in d or "ask" in d:
            self.want_quotes(now, t, d)

    def refresh_active(self, now: float) -> None:
        """Markets worth evaluating: open, and within 5 sigma of spot (or open-ended). Recomputed every 30s."""
        self._last_active = now
        act: List[str] = []
        for t, m in self.markets.items():
            if t in self.dead or now >= m.close_ts:
                continue
            S = self.fv.spot(m.sym)
            if S is None:
                continue
            sd = max(self.fv.k * self.fv.sigma_annual(m.sym) * math.sqrt(max(m.close_ts - now, 1.0) / YEAR_S), 1e-6)
            if m.lo is None or m.hi is None:
                act.append(t); continue
            mid = (m.lo + m.hi) / 2.0
            if abs(math.log(mid / S)) < 5 * sd:
                act.append(t)
        self.active = act

    def tick_all(self, now: float, sym: Optional[str] = None) -> None:
        if now - self._last_active >= 30.0:
            self.refresh_active(now)
        for t in self.active:
            m = self.markets[t]
            if t in self.dead or now >= m.close_ts or (sym and m.sym != sym):
                continue
            if now - self.last_tick.get(t, -1e9) < self.tick_every:
                continue
            self.last_tick[t] = now
            self.tick(now, t)

    # ---- settlement
    def settle(self, t: str, now: float, result: Optional[str]) -> None:
        m = self.markets[t]
        if t in self.settled:
            return
        if result not in ("yes", "no"):
            h = self.spot_hist[m.sym]
            i = bisect.bisect_left(h, (m.close_ts - 60.0, -1.0))
            window = [px for ts, px in h[i:] if ts <= m.close_ts]
            if not window:
                self.dead.add(t); self.resting.pop(t, None)
                if self.lots[t]:
                    self.unsettled.append(t)
                return
            avg = sum(window) / len(window)
            result = "yes" if (m.lo is None or avg >= m.lo) and (m.hi is None or avg < m.hi) else "no"
            m.settled_by_spot = True
        m.result = result
        self.resting.pop(t, None)
        self.dead.add(t)
        gross = fees = 0.0
        for l in self.lots[t]:
            win = (l.side == result)
            gross += (1.0 if win else 0.0) * l.qty - l.price_c / 100.0 * l.qty
            fees += l.fee
        self.settled[t] = {"day": m.day, "gross": gross, "fees": fees, "net": gross - fees,
                           "contracts": sum(l.qty for l in self.lots[t]), "lots": len(self.lots[t]),
                           "by_spot": m.settled_by_spot}

    # ---- main loop
    def run(self, events: List[Tuple[float, str, Dict[str, Any]]]) -> None:
        for t, kind, pl in events:
            self.run_due(t)
            if kind == "market":
                if pl.get("close_ts") is None:
                    continue
                m = Market(pl["ticker"], pl.get("event") or "", pl.get("series") or "", pl.get("lo"), pl.get("hi"),
                           float(pl["close_ts"]), pl.get("open_ts"), pl.get("result"),
                           "ETH" if "ETH" in (pl.get("series") or pl["ticker"]) else "BTC")
                if m.ticker not in self.markets:
                    self.markets[m.ticker] = m
                    bisect.insort(self._close_q, (m.close_ts, m.ticker))
                if pl.get("result") in ("yes", "no"):
                    self.markets[m.ticker].result = pl["result"]
            elif kind == "spot":
                sym = pl["sym"].upper()
                self.fv.on_spot(sym, float(pl["price"]), float(pl.get("ts") or t))
                self.spot_hist[sym].append((t, float(pl["price"])))
                self.tick_all(t, sym)
            elif kind == "snapshot":
                tk, yes, no = parse_snapshot(pl)
                if tk in self.markets:
                    b = self.books.setdefault(tk, Book())
                    b.yes, b.no, b.ts, b.ready = yes, no, t, True
                    self.tick(t, tk)
            elif kind == "delta":
                tk, side, price, d = parse_delta(pl)
                if tk in self.markets and price is not None and side in ("yes", "no"):
                    b = self.books.setdefault(tk, Book())
                    b.apply_delta(side, price, d)
                    b.ts = t
            elif kind == "tob":
                tk = pl["ticker"]
                if tk in self.markets:
                    self.tob[tk] = {"yes_bid": pl.get("yes_bid"), "yes_ask": pl.get("yes_ask"),
                                    "bid_size": pl.get("bid_size"), "ask_size": pl.get("ask_size")}
                    self.tick(t, tk)
            elif kind == "trade":
                tr = pl if "taker_side" in pl and "yes_price" in pl else parse_trade(pl)
                self.on_trade(t, tr)
            elif kind == "result":
                tk = pl["ticker"]
                if tk in self.markets:
                    self.settle(tk, t, (pl.get("result") or "").lower())
            # once a second: drop expired quotes, settle markets 90s past close
            if t - self._last_sweep >= 1.0:
                self._last_sweep = t
                for tk in list(self.resting):
                    self.sweep_dead(tk, t)
                while self._close_q and self._close_q[0][0] + self.RESULT_WAIT_S <= t:
                    _, tk = self._close_q.pop(0)
                    if tk not in self.dead:
                        self.settle(tk, t, self.markets[tk].result)
        last_t = events[-1][0] if events else 0.0
        self.run_due(last_t)
        for close_ts, tk in list(self._close_q):          # closed before the tape ended: settle (by spot if no result)
            if tk in self.dead:
                continue
            if close_ts <= last_t:
                self.settle(tk, last_t, self.markets[tk].result)
            elif self.lots[tk]:
                self.unsettled.append(tk)                 # still open when the tape ended: position left hanging

    # ---- results
    def summary(self) -> Dict[str, Any]:
        by_day: Dict[str, float] = defaultdict(float)
        gross = fees = contracts = 0.0
        n_mkts = wins = 0
        for s in self.settled.values():
            if s["lots"] == 0:
                continue
            by_day[s["day"]] += s["net"]
            gross += s["gross"]; fees += s["fees"]; contracts += s["contracts"]
            n_mkts += 1; wins += 1 if s["net"] > 0 else 0
        return {"gross": gross, "fees": fees, "net": gross - fees, "contracts": contracts, "markets_traded": n_mkts,
                "markets_won": wins, "by_day": dict(sorted(by_day.items())),
                "fills": sum(1 for f in self.fills if f["kind"] in ("take", "make")),
                "cancels": sum(1 for f in self.fills if f["kind"] == "take_cancel"),
                "by_spot": sum(1 for s in self.settled.values() if s["by_spot"] and s["lots"]),
                "unsettled": len(self.unsettled)}


# --------------------------------------------------------------------------- driver
def split_days(days: List[str], train_frac: float) -> Tuple[List[str], List[str]]:
    """Chronological split; with 2+ days at least one day is always held out."""
    days = sorted(days)
    if len(days) < 2:
        return days, []
    n_train = max(1, min(len(days) - 1, int(math.ceil(len(days) * train_frac))))
    return days[:n_train], days[n_train:]


def run_grid(events, strategies: List[Strategy], latencies_ms: List[float], queues: List[str], k: float,
             maker_fee: float, tick_every: float, assume_size: float, train_frac: float, log=print) -> List[Dict[str, Any]]:
    all_days = sorted({dt.datetime.fromtimestamp(float(pl["close_ts"]), dt.timezone.utc).strftime("%Y-%m-%d")
                       for _, kind, pl in events if kind == "market" and pl.get("close_ts")})
    train, test = split_days(all_days, train_frac)
    rows = []
    total = len(strategies) * len(latencies_ms) * len(queues)
    i = 0
    for strat in strategies:
        for lat in latencies_ms:
            for q in (queues if strat.family != "taker" else ["-"]):
                i += 1
                fv = FairValue(k=k)
                sim = Sim(strat, fv, lat / 1000.0, q if q != "-" else "back", maker_fee, tick_every, assume_size)
                sim.run(events)
                s = sim.summary()
                tr_net = sum(v for d, v in s["by_day"].items() if d in train)
                te_net = sum(v for d, v in s["by_day"].items() if d in test)
                daily = list(s["by_day"].values())
                rows.append({"strategy": strat.name, "family": strat.family, "latency_ms": lat, "queue": q,
                             "fills": s["fills"], "cancels": s["cancels"], "contracts": s["contracts"],
                             "gross": s["gross"], "fees": s["fees"], "net": s["net"],
                             "net_per_contract_c": (100 * s["net"] / s["contracts"]) if s["contracts"] else 0.0,
                             "days": len(daily), "worst_day": min(daily) if daily else 0.0,
                             "train_net": tr_net, "test_net": te_net, "by_spot": s["by_spot"]})
                log(f"[{i}/{total}] {strat.name} lat={lat:.0f}ms q={q}: fills={s['fills']} net=${s['net']:+.2f} "
                    f"train=${tr_net:+.2f} test=${te_net:+.2f}")
    return rows, train, test


def print_report(rows: List[Dict[str, Any]], train: List[str], test: List[str]) -> None:
    print("\n=== REPLAY REPORT ===")
    print(f"train days: {train}\ntest days:  {test if test else '(none — need >=2 days for an out-of-sample number)'}")
    hdr = f"{'strategy':60s} {'lat':>5s} {'q':>5s} {'fills':>5s} {'ctr':>6s} {'net$':>8s} {'c/ctr':>6s} {'worst$':>7s} {'train$':>8s} {'test$':>8s}"
    print(hdr); print("-" * len(hdr))
    for r in sorted(rows, key=lambda r: (r["family"], -r["train_net"])):
        print(f"{r['strategy'][:60]:60s} {r['latency_ms']:5.0f} {r['queue']:>5s} {r['fills']:5d} {r['contracts']:6.0f} "
              f"{r['net']:8.2f} {r['net_per_contract_c']:6.1f} {r['worst_day']:7.2f} {r['train_net']:8.2f} {r['test_net']:8.2f}")
    print()
    for fam in ("taker", "maker_fair", "maker_join"):
        fr = [r for r in rows if r["family"] == fam and (fam == "taker" or r["queue"] == "back")]
        if not fr:
            continue
        best = max(fr, key=lambda r: r["train_net"])
        opt = next((r for r in rows if r["strategy"] == best["strategy"] and r["latency_ms"] == best["latency_ms"]
                    and r["queue"] == "front"), None)
        line = f"best {fam:10s} by TRAIN: {best['strategy']} lat={best['latency_ms']:.0f}ms -> train ${best['train_net']:+.2f}, TEST ${best['test_net']:+.2f} (pessimistic fills)"
        if opt:
            line += f"; optimistic-queue test ${opt['test_net']:+.2f}"
        print(line)
    n_by_spot = sum(r["by_spot"] for r in rows[:1])
    if n_by_spot:
        print(f"note: {n_by_spot} traded markets had no 'result' record and were settled from the spot average — fetch results for exactness")
    print("\nRead the TEST column. The train winner was selected because it won, so its train number is biased upward.\n"
          "If test is empty or a different config wins each day, the tape is too short to trust any of this.")


def main(argv: Optional[List[str]] = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("tape", nargs="+", help="tape files or globs")
    ap.add_argument("--latency-ms", type=float, nargs="+", default=[150.0], help="decision->exchange latency to simulate")
    ap.add_argument("--queue", nargs="+", default=["back", "front"], choices=["back", "front"])
    ap.add_argument("--k", type=float, default=0.8, help="vol scale on realized vol (repo backtests fitted 0.8)")
    ap.add_argument("--maker-fee", type=float, default=0.0175, help="maker fee rate; set 0 if your fills show none")
    ap.add_argument("--size", type=int, default=5, help="contracts per order")
    ap.add_argument("--tick-every", type=float, default=1.0, help="min seconds between strategy re-evaluations per market")
    ap.add_argument("--assume-size", type=float, default=20, help="touch depth to assume when the tape has no sizes (REST tape)")
    ap.add_argument("--train-frac", type=float, default=0.7)
    ap.add_argument("--only", choices=["taker", "maker_fair", "maker_join"], default=None)
    ap.add_argument("--csv", default=None)
    ap.add_argument("--near-brackets", type=float, default=12, help="keep book events within N bracket-widths of spot (0 = all)")
    args = ap.parse_args(argv)

    events = load_tape(args.tape, None if args.near_brackets <= 0 else args.near_brackets)
    n_m = sum(1 for e in events if e[1] == "market")
    n_tr = sum(1 for e in events if e[1] == "trade")
    n_res = sum(1 for e in events if e[1] == "result")
    print(f"tape: {len(events)} events, {n_m} markets, {n_tr} trades, {n_res} results, "
          f"{(events[-1][0] - events[0][0]) / 3600:.1f}h span" if events else "empty tape")
    if not events:
        return
    grid = [s for s in default_grid(args.size) if not args.only or s.family == args.only]
    rows, train, test = run_grid(events, grid, args.latency_ms, args.queue, args.k, args.maker_fee,
                                 args.tick_every, args.assume_size, args.train_frac)
    print_report(rows, train, test)
    if args.csv:
        with open(args.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader(); w.writerows(rows)
        print(f"wrote {args.csv}")


if __name__ == "__main__":
    main()
