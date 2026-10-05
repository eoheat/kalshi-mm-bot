"""Offline tests: message shapes, book derivation, seq gaps, spot parsing, market meta, and the replay
engine's fill/latency/queue/settlement semantics. Run: python -m tests.test_feed_replay"""
from __future__ import annotations

import json
import math
import sys

sys.path.insert(0, ".")
from src.feed.kalshi_ws import Book, KalshiFeed, msg_ts, parse_delta, parse_snapshot, parse_trade, price_cents  # noqa: E402
from src.feed.spot_ws import parse_index_message  # noqa: E402
from src.feed.record import market_meta  # noqa: E402
from src.replay.run import (FairValue, MakerFair, Sim, Strategy, TakerEdge, fee_cents, fee_dollars,  # noqa: E402
                             load_tape)

CHECKS = 0


def ok(cond: bool, what: str) -> None:
    global CHECKS
    CHECKS += 1
    if not cond:
        raise AssertionError(what)


# --------------------------------------------------------------------------- parsers
def test_price_shapes() -> None:
    ok(price_cents(45) == 45, "int cents")
    ok(price_cents("45") == 45, "str cents")
    ok(price_cents("0.4500") == 45, "dollar string")
    ok(price_cents("0.4500", "yes_price_dollars") == 45, "dollar key")
    ok(price_cents(1, "price_dollars") == 100, "int under dollar key is dollars")
    ok(price_cents("1.0000") == 100, "one dollar")
    ok(price_cents(None) is None, "none")


def test_snapshot_shapes_agree() -> None:
    legacy = {"market_ticker": "T", "yes": [[20, 100], [19, 50]], "no": [[77, 40]]}
    dollars = {"market_ticker": "T", "yes_dollars": [["0.2000", "100.00"], ["0.1900", "50.00"]], "no_dollars": [["0.7700", "40.00"]]}
    objects = {"market_ticker": "T", "yes": [{"price": 20, "quantity": 100}, {"price_dollars": "0.1900", "quantity_fp": "50.00"}],
               "no": [{"price": 77, "quantity": 40}]}
    books = []
    for shape in (legacy, dollars, objects):
        t, yes, no = parse_snapshot(shape)
        b = Book(); b.yes, b.no, b.ready = yes, no, True
        books.append(b)
        ok(t == "T", "ticker")
    for b in books:
        best = b.best()
        ok(best["yes_bid"] == 20 and best["bid_size"] == 100, f"best bid {best}")
        ok(best["yes_ask"] == 23 and best["ask_size"] == 40, f"best ask = 100 - best NO bid {best}")
        ok(b.levels()["yes"] == [[20, 100.0], [19, 50.0]], "levels sorted best-first")


def test_prod_shapes_sep2026() -> None:
    """Exact raw messages from the Sep 19 2026 prod probe: keys are *_dollars_fp, and a side with no
    resting orders is simply absent. The first parser missed these and loaded EMPTY books."""
    snap = {"market_ticker": "KXBTC-26SEP1918-B68750", "market_id": "f5325078-6dca-4737-a1c6-9a6577d48954",
            "no_dollars_fp": [["0.0400", "12.00"], ["0.0500", "5.00"], ["0.0900", "2.00"], ["0.1000", "2.00"],
                              ["0.2100", "1.00"], ["0.2800", "1.00"], ["0.3100", "2.00"], ["0.4900", "2.00"],
                              ["0.6500", "1.00"], ["0.7600", "1.00"], ["0.9900", "33105.00"]]}
    t, yes, no = parse_snapshot(snap)
    b = Book(); b.yes, b.no, b.ready = yes, no, True
    ok(len(yes) == 0 and len(no) == 11, f"all 11 NO levels parsed, no YES side: {len(yes)}/{len(no)}")
    ok(b.best() == {"yes_bid": None, "yes_ask": 1, "bid_size": 0.0, "ask_size": 33105.0}, f"1c ask x33105: {b.best()}")
    d = {"market_ticker": "KXBTC-26SEP1918-B71950", "market_id": "918c681e", "price_dollars": "0.6500", "delta_fp": "1.00",
         "side": "no", "ts": "2026-09-19T21:03:51.004488Z", "ts_ms": 1789851831004}
    ok(parse_delta(d) == ("KXBTC-26SEP1918-B71950", "no", 65, 1.0), f"prod delta {parse_delta(d)}")
    ok(msg_ts(d) == 1789851831.004, "ts_ms preferred, in seconds")
    ok(abs(msg_ts({"ts": "2026-09-19T21:03:51.004488Z"}) - 1789851831.004488) < 1e-6, "ISO ts fallback")
    yes_only = {"market_ticker": "T", "yes_dollars_fp": [["0.2000", "3.00"]]}
    t, yes, no = parse_snapshot(yes_only)
    ok(yes == {20: 3.0} and no == {}, "yes-only snapshot")
    feed = KalshiFeed("prod", "k", "p", ["T"], log=lambda s: None)
    feed._handle(json.dumps({"type": "orderbook_snapshot", "sid": 1, "seq": 1, "msg": snap}))
    feed._handle(json.dumps({"type": "orderbook_delta", "sid": 1, "seq": 2, "msg": d}))
    feed._handle(json.dumps({"type": "orderbook_delta", "sid": 1, "seq": 3,
                             "msg": {"market_ticker": "KXBTC-26SEP1918-B68750", "price_dollars": "0.9900", "delta_fp": "-33105.00", "side": "no", "ts_ms": 1789851831004}}))
    ok(feed.best("KXBTC-26SEP1918-B68750")["yes_ask"] == 24, "after the 99c NO level is pulled, best ask is 100-76")
    ok(feed.stats["lag_ms"] is not None and feed.stats["lag_ms_max"] >= feed.stats["lag_ms"], "lag measured from ts_ms")


def test_delta_shapes_and_removal() -> None:
    b = Book(); b.yes = {20: 100.0}; b.no = {77: 40.0}; b.ready = True
    for msg in ({"market_ticker": "T", "price": 20, "delta": -60, "side": "yes"},
                {"market_ticker": "T", "price_dollars": "0.2000", "delta_fp": "-40.00", "side": "yes"}):
        t, side, p, d = parse_delta(msg)
        b.apply_delta(side, p, d)
    ok(20 not in b.yes, "level removed at zero")
    t, side, p, d = parse_delta({"market_ticker": "T", "price": 78, "delta": 5, "side": "no"})
    b.apply_delta(side, p, d)
    ok(b.best()["yes_ask"] == 22, "new best NO bid 78 -> yes ask 22")


def test_trade_shapes() -> None:
    a = parse_trade({"market_ticker": "T", "yes_price": 42, "count": 3, "taker_side": "yes", "ts": 1})
    b = parse_trade({"market_ticker": "T", "yes_price_dollars": "0.4200", "count_fp": "3.00", "taker_side": "YES", "created_time": "x"})
    ok(a["yes_price"] == b["yes_price"] == 42 and a["count"] == b["count"] == 3.0, "trade normalized")
    ok(a["taker_side"] == b["taker_side"] == "yes", "taker side lowercased")


def test_feed_handle_and_seq_gap() -> None:
    feed = KalshiFeed("prod", "k", "p", ["T"], log=lambda s: None)
    feed._handle(json.dumps({"id": 1, "type": "subscribed", "msg": {"channel": "orderbook_delta", "sid": 7}}))
    feed._handle(json.dumps({"type": "orderbook_snapshot", "sid": 7, "seq": 1,
                             "msg": {"market_ticker": "T", "yes": [[20, 100]], "no": [[77, 40]]}}))
    ok(feed.best("T")["yes_ask"] == 23, "snapshot applied")
    feed._handle(json.dumps({"type": "orderbook_delta", "sid": 7, "seq": 2, "msg": {"market_ticker": "T", "price": 78, "delta": 10, "side": "no"}}))
    ok(feed.best("T")["yes_ask"] == 22 and feed.orderbook("T")["no"][0] == [78, 10.0], "delta applied")
    feed._handle(json.dumps({"type": "orderbook_delta", "sid": 7, "seq": 4, "msg": {"market_ticker": "T", "price": 79, "delta": 10, "side": "no"}}))
    ok(feed.stats["seq_gaps"] == 1 and feed.best("T")["yes_ask"] == 22, "gap detected, message dropped")
    feed._handle(json.dumps({"type": "trade", "sid": 8, "msg": {"market_ticker": "T", "yes_price": 22, "count": 5, "taker_side": "yes"}}))
    ok(feed.last_trade("T")["count"] == 5.0, "trade stored")
    ok(feed.fresh("T") is False, "not fresh while disconnected")


class _FakeWS:
    def __init__(self):
        self.sent = []

    async def send(self, raw):
        self.sent.append(json.loads(raw))


def test_subscription_batching_and_resubscribe() -> None:
    import asyncio
    feed = KalshiFeed("prod", "k", "p", [f"T{i}" for i in range(250)], channels=("orderbook_delta", "trade", "fill"), log=lambda s: None)
    feed.batch = 100
    ws = _FakeWS()
    asyncio.run(feed._subscribe_all(ws))
    subs = [m for m in ws.sent if m["cmd"] == "subscribe"]
    ok(len(subs) == 7, f"3 batches x 2 market channels + 1 fill = 7 subscribe commands, got {len(subs)}")
    ok(all(len(m["params"].get("market_tickers", [])) <= 100 for m in subs), "no command carries more than the batch")
    ok(sorted(t for m in subs if m["params"]["channels"] == ["orderbook_delta"] for t in m["params"]["market_tickers"]) == sorted(feed.tickers), "every ticker subscribed once")
    # server answers each request id with a sid; a gap on one sid resubscribes only that batch
    for i, m in enumerate(subs):
        feed._handle(json.dumps({"id": m["id"], "type": "subscribed", "msg": {"channel": m["params"]["channels"][0], "sid": 10 + i}}))
    ok(feed._subs[10]["tickers"] == feed.tickers[:100] and feed._subs[16]["channel"] == "fill", "sid -> batch mapping kept")
    ws2 = _FakeWS()
    asyncio.run(feed._resubscribe(ws2, 11))
    ok(ws2.sent[0]["cmd"] == "unsubscribe" and ws2.sent[0]["params"]["sids"] == [11], "unsubscribes the gapped sid")
    ok(ws2.sent[1]["cmd"] == "subscribe" and ws2.sent[1]["params"]["market_tickers"] == feed.tickers[100:200], "re-subscribes exactly that batch")


def test_ticker_cross_check() -> None:
    feed = KalshiFeed("prod", "k", "p", ["T"], log=lambda s: None)
    feed._handle(json.dumps({"type": "orderbook_snapshot", "sid": 1, "seq": 1, "msg": {"market_ticker": "T", "yes_dollars_fp": [["0.2300", "51.00"]], "no_dollars_fp": [["0.7500", "301.00"]]}}))
    feed._handle(json.dumps({"type": "ticker", "sid": 3, "msg": {"market_ticker": "T", "yes_bid_dollars": "0.2300", "yes_ask_dollars": "0.2500", "ts_ms": 1}}))
    feed._handle(json.dumps({"type": "ticker", "sid": 3, "msg": {"market_ticker": "T", "yes_bid_dollars": "0.2300", "yes_ask_dollars": "0.2600", "ts_ms": 2}}))
    ok(feed.stats["ticker_checks"] == 2 and feed.stats["ticker_mismatch"] == 1, f"one of two tickers disagreed: {feed.stats}")
    ok("T: ticker 23/26 vs book 23/25" in feed.ticker_check(), feed.ticker_check())


def test_load_tape_gzip_and_near_filter(tmp="/tmp/_tape_test.jsonl.gz") -> None:
    import gzip
    lines = [{"t": 1.0, "type": "market", "ticker": "NEAR", "series": "KXBTC", "lo": 81000, "hi": 81100, "close_ts": 100},
             {"t": 1.0, "type": "market", "ticker": "FAR", "series": "KXBTC", "lo": 60000, "hi": 60100, "close_ts": 100},
             {"t": 2.0, "type": "spot", "sym": "BTC", "price": 81050.0, "ts": 2.0},
             {"t": 3.0, "type": "ws", "raw": {"type": "orderbook_delta", "sid": 1, "seq": 1, "msg": {"market_ticker": "NEAR", "price_dollars": "0.2000", "delta_fp": "1.00", "side": "yes"}}},
             {"t": 3.0, "type": "ws", "raw": {"type": "orderbook_delta", "sid": 1, "seq": 2, "msg": {"market_ticker": "FAR", "price_dollars": "0.9900", "delta_fp": "1.00", "side": "no"}}},
             {"t": 4.0, "type": "ws", "raw": {"type": "trade", "sid": 2, "msg": {"market_ticker": "FAR", "yes_price_dollars": "0.0100", "count_fp": "1.00", "taker_side": "yes"}}}]
    with gzip.open(tmp, "wt") as f:
        for l in lines:
            f.write(json.dumps(l) + "\n")
    ev = load_tape([tmp], near_brackets=12)
    kinds = [(k, p.get("market_ticker") or p.get("ticker")) for _, k, p in ev]
    ok(("delta", "NEAR") in kinds and ("delta", "FAR") not in kinds, f"far bracket's book events dropped: {kinds}")
    ok(("trade", "FAR") in kinds, "trades are always kept")
    ok(len(load_tape([tmp], near_brackets=None)) == 6, "no filter keeps everything")


def test_deribit_parse() -> None:
    m = {"jsonrpc": "2.0", "method": "subscription", "params": {"channel": "deribit_price_index.btc_usd",
                                                                "data": {"timestamp": 1700000000000, "price": 81234.5, "index_name": "btc_usd"}}}
    px = parse_index_message(m)
    ok(px["sym"] == "BTC" and px["price"] == 81234.5 and px["ts"] == 1700000000.0, "deribit index parsed")
    ok(parse_index_message({"jsonrpc": "2.0", "id": 1, "result": ["x"]}) is None, "non-subscription ignored")


def test_market_meta_from_real_rows() -> None:
    rows = [
        {"ticker": "KXBTC-26SEP1917-B81125", "event_ticker": "KXBTC-26SEP1917", "strike_type": "between",
         "floor_strike": 81000, "cap_strike": 81249.99, "close_time": "2026-09-19T21:00:00Z", "open_time": "2026-09-18T20:00:00Z", "result": ""},
        {"ticker": "KXBTC-26SEP1917-T87749.99", "event_ticker": "KXBTC-26SEP1917", "strike_type": "greater",
         "floor_strike": 87749.99, "close_time": "2026-09-19T21:00:00Z", "result": "no"},
        {"ticker": "KXBTC-26SEP1917-T68249.99", "event_ticker": "KXBTC-26SEP1917", "strike_type": "less",
         "cap_strike": 68249.99, "close_time": "2026-09-19T21:00:00Z"},
        {"ticker": "X-T1", "event_ticker": "X", "strike_type": "less", "floor_strike": 68249.99, "close_time": "2026-09-19T21:00:00Z"},
    ]
    b, g, l, l2 = (market_meta(r) for r in rows)
    ok(b["lo"] == 81000 and b["hi"] == 81249.99 and b["series"] == "KXBTC" and b["result"] is None, f"between {b}")
    ok(g["lo"] == 87749.99 and g["hi"] is None and g["result"] == "no", f"greater {g}")
    ok(l["lo"] is None and l["hi"] == 68249.99, f"less via cap {l}")
    ok(l2["lo"] is None and l2["hi"] == 68249.99, f"less via floor {l2}")
    ok(abs(b["close_ts"] - 1789851600.0) < 1, f"close ts {b['close_ts']}")


# --------------------------------------------------------------------------- replay engine
class FixedFV(FairValue):
    """Deterministic fair value for engine tests."""

    def __init__(self, p: float) -> None:
        super().__init__(k=1.0)
        self.fixed = p

    def on_spot(self, sym, price, ts):
        self.state.setdefault(sym, {"price": price, "ts": ts, "var_s": 0.0, "n": 0})
        self.state[sym]["price"] = price

    def sigma_annual(self, sym):
        return 0.5

    def p_bracket(self, m, ts):
        return self.fixed


def market_ev(ticker="KXBTC-TEST-B81125", lo=81000.0, hi=81250.0, close=600.0):
    return (0.0, "market", {"ticker": ticker, "event": "KXBTC-TEST", "series": "KXBTC", "lo": lo, "hi": hi, "close_ts": close})


def spot_evs(t0, t1, price=81100.0):
    return [(float(t), "spot", {"sym": "BTC", "price": price, "ts": float(t)}) for t in range(int(t0), int(t1) + 1)]


def snapshot_ev(t, yes, no, ticker="KXBTC-TEST-B81125"):
    return (t, "snapshot", {"market_ticker": ticker, "yes": yes, "no": no})


def trade_ev(t, price, count, taker, ticker="KXBTC-TEST-B81125"):
    return (t, "trade", {"ticker": ticker, "yes_price": price, "count": count, "taker_side": taker})


def result_ev(t, result, ticker="KXBTC-TEST-B81125"):
    return (t, "result", {"ticker": ticker, "result": result})


def run(strat, events, p=0.40, latency=0.15, queue="back", maker_fee=0.0175, tick_every=1.0, assume_size=20):
    sim = Sim(strat, FixedFV(p), latency, queue, maker_fee, tick_every, assume_size)
    sim.run(sorted(events, key=lambda e: e[0]))
    return sim


def test_taker_fills_with_latency_and_max_takes() -> None:
    strat = TakerEdge(min_edge=6, ttl_lo=60, ttl_hi=600, max_takes=2, both_sides=True, slippage=2, size=5)
    ev = [market_ev(), snapshot_ev(0.0, [[20, 100]], [[77, 50]])] + spot_evs(0, 700) + [result_ev(650.0, "yes")]
    sim = run(strat, ev)
    takes = [f for f in sim.fills if f["kind"] == "take"]
    ok(len(takes) == 2 and all(f["price"] == 23 and f["qty"] == 5 for f in takes), f"two takes at the 23c ask: {takes}")
    ok(takes[0]["t"] >= 0.15, "first fill lands after latency")
    s = sim.settled["KXBTC-TEST-B81125"]
    ok(abs(s["gross"] - 10 * 0.77) < 1e-9, f"gross {s['gross']}")
    ok(abs(s["fees"] - 10 * fee_dollars(0.07, 23, 1)) < 1e-9, f"taker fee 7% formula {s['fees']}")
    ok(abs(fee_cents(0.07, 21) - 1.1613) < 1e-3, "fee on the real 21c fill matches the 1.16c Kalshi charged")


def test_taker_ioc_cancels_when_touch_moves() -> None:
    strat = TakerEdge(min_edge=6, ttl_lo=60, ttl_hi=600, max_takes=1, both_sides=False, slippage=2, size=5)
    ev = [market_ev(), snapshot_ev(0.0, [[20, 100]], [[77, 50]]),
          (0.5, "delta", {"market_ticker": "KXBTC-TEST-B81125", "price": 77, "delta": -50, "side": "no"}),
          (0.6, "delta", {"market_ticker": "KXBTC-TEST-B81125", "price": 60, "delta": 50, "side": "no"})]  # ask jumps 23 -> 40
    ev += spot_evs(0, 700) + [result_ev(650.0, "yes")]
    sim = run(strat, ev, latency=1.0)
    first = sim.fills[0]
    ok(first["kind"] == "take_cancel", f"IOC cancelled: ask moved past limit before it landed {first}")
    ok(all(f["kind"] == "take_cancel" for f in sim.fills), "40c ask never passes the edge check at p=0.40")


def test_taker_no_side() -> None:
    strat = TakerEdge(min_edge=6, ttl_lo=60, ttl_hi=600, max_takes=1, both_sides=True, slippage=2, size=5)
    ev = [market_ev(), snapshot_ev(0.0, [[70, 100]], [[25, 50]])] + spot_evs(0, 700) + [result_ev(650.0, "no")]  # bid 70 ask 75, p=0.40
    sim = run(strat, ev)
    takes = [f for f in sim.fills if f["kind"] == "take"]
    ok(len(takes) == 1 and takes[0]["side"] == "no" and takes[0]["price"] == 30, f"buys NO at 100-bid=30: {takes}")
    ok(abs(sim.settled["KXBTC-TEST-B81125"]["gross"] - 5 * 0.70) < 1e-9, "NO lot pays on NO settle")


class ScriptedMaker(Strategy):
    family = "maker_fair"

    def __init__(self, script):
        super().__init__(script="s")
        self.script = script   # list of (from_t, quotes)

    def decide(self, c):
        q = {"bid": None, "ask": None}
        for t0, quotes in self.script:
            if c.t >= t0:
                q = quotes
        return dict(q)


def test_maker_fills_through_and_at_price_queue_back() -> None:
    strat = ScriptedMaker([(0.0, {"bid": (38, 5), "ask": (42, 5)})])
    book = snapshot_ev(0.0, [[38, 7], [35, 100]], [[55, 50]])            # our bid joins a 7-lot level at 38; ask 45
    ev = [market_ev(), book] + spot_evs(0, 700)
    ev += [trade_ev(10.0, 42, 3, "yes"),      # at our ask, nobody ahead -> 3 filled (sell YES 42 = buy NO 58)
           trade_ev(20.0, 38, 10, "no"),      # at our bid with 7 ahead -> 3 filled
           trade_ev(30.0, 37, 10, "no"),      # through our bid -> remaining 2 filled
           result_ev(650.0, "yes")]
    sim = run(strat, ev, queue="back")
    makes = [f for f in sim.fills if f["kind"] == "make"]
    ok([(f["side"], f["price"], f["qty"]) for f in makes] == [("no", 58, 3.0), ("yes", 38, 3.0), ("yes", 38, 2.0)], f"fills {makes}")
    s = sim.settled["KXBTC-TEST-B81125"]
    ok(abs(s["gross"] - (5 * 0.62 - 3 * 0.58)) < 1e-9, f"gross {s['gross']}")
    ok(abs(s["fees"] - (fee_dollars(0.0175, 38, 5) + fee_dollars(0.0175, 58, 3))) < 1e-9, "maker fee")


def test_maker_queue_front_is_optimistic() -> None:
    strat = ScriptedMaker([(0.0, {"bid": (38, 5), "ask": None})])
    ev = [market_ev(), snapshot_ev(0.0, [[38, 7]], [[55, 50]])] + spot_evs(0, 700) + [trade_ev(20.0, 38, 6, "no"), result_ev(650.0, "yes")]
    back = run(strat, ev, queue="back")
    front = run(strat, ev, queue="front")
    ok(sum(f["qty"] for f in back.fills if f["kind"] == "make") == 0, "back of a 7-lot queue: 6 traded, none ours")
    ok(sum(f["qty"] for f in front.fills if f["kind"] == "make") == 5, "front of queue: filled")


def test_maker_latency_blocks_early_fill_and_allows_pickoff() -> None:
    strat = ScriptedMaker([(0.0, {"bid": (38, 5), "ask": None}), (10.0, {"bid": None, "ask": None})])   # pull quote at t=10
    ev = [market_ev(), snapshot_ev(0.0, [[35, 100]], [[55, 50]])] + spot_evs(0, 700)
    ev += [trade_ev(1.0, 30, 5, "no"),        # before our order is live (latency 3s) -> no fill
           trade_ev(11.0, 30, 5, "no"),       # cancel sent at 10, lands at 13 -> picked off at 11
           result_ev(650.0, "no")]
    sim = run(strat, ev, latency=3.0)
    makes = [f for f in sim.fills if f["kind"] == "make"]
    ok(len(makes) == 1 and makes[0]["t"] == 11.0, f"only the pick-off fills: {makes}")
    fast = run(strat, ev, latency=0.1)
    ok([f["t"] for f in fast.fills if f["kind"] == "make"] == [1.0], "100ms latency: live in time for the t=1 trade, cancelled before the t=11 pick-off")


def test_post_only_rejects_crossing_quote() -> None:
    strat = ScriptedMaker([(0.0, {"bid": (50, 5), "ask": None})])           # ask is 45 -> a 50c bid would cross
    ev = [market_ev(), snapshot_ev(0.0, [[38, 7]], [[55, 50]])] + spot_evs(0, 700) + [trade_ev(20.0, 45, 6, "no"), result_ev(650.0, "yes")]
    sim = run(strat, ev)
    ok(not sim.resting["KXBTC-TEST-B81125"] and not [f for f in sim.fills if f["kind"] == "make"], "crossing post_only rejected")


def test_maker_fair_quotes_and_skew() -> None:
    strat = MakerFair(half_spread=2, skew=0.2, max_inv=20, min_ttl=120, p_lo=0.05, p_hi=0.95, jump_halt=3.0, size=5)
    ev = [market_ev(), snapshot_ev(0.0, [[35, 100]], [[55, 50]])] + spot_evs(0, 700) + [result_ev(650.0, "yes")]
    sim = run(strat, ev)
    r = sim.resting.get("KXBTC-TEST-B81125") or {}
    # market settled -> resting cleared; check via fills-free run: inspect quotes at t=5 instead
    sim2 = Sim(strat, FixedFV(0.40), 0.15, "back", 0.0175, 1.0, 20)
    sim2.run(sorted([market_ev(), snapshot_ev(0.0, [[35, 100]], [[55, 50]])] + spot_evs(0, 5), key=lambda e: e[0]))
    r = sim2.resting["KXBTC-TEST-B81125"]
    ok(r["bid"].price_c == 38 and r["ask"].price_c == 42, f"fair 40 +/- 2: {r}")
    sim3 = Sim(strat, FixedFV(0.40), 0.15, "back", 0.0175, 1.0, 20)
    sim3.run(sorted([market_ev(), snapshot_ev(0.0, [[35, 100]], [[55, 50]])] + spot_evs(0, 5) + [trade_ev(2.0, 37, 5, "no")], key=lambda e: e[0]))
    r = sim3.resting["KXBTC-TEST-B81125"]
    ok(sim3.inv("KXBTC-TEST-B81125") == 5 and r["bid"].price_c == 37 and r["ask"].price_c == 41, f"skew after +5 inv: {r}")


def test_settle_by_spot_when_result_missing() -> None:
    strat = TakerEdge(min_edge=6, ttl_lo=60, ttl_hi=600, max_takes=1, both_sides=False, slippage=2, size=5)
    ev = [market_ev(), snapshot_ev(0.0, [[20, 100]], [[77, 50]])] + spot_evs(0, 800, price=81100.0)
    sim = run(strat, ev)
    s = sim.settled["KXBTC-TEST-B81125"]
    ok(s["by_spot"] and abs(s["gross"] - 5 * 0.77) < 1e-9, "60s spot average inside bracket -> YES")


def test_tob_tape_unknown_sizes() -> None:
    strat = ScriptedMaker([(0.0, {"bid": (38, 5), "ask": None})])
    ev = [market_ev(), (0.0, "tob", {"ticker": "KXBTC-TEST-B81125", "yes_bid": 38, "yes_ask": 45})] + spot_evs(0, 700)
    ev += [trade_ev(20.0, 38, 50, "no"), trade_ev(30.0, 37, 2, "no"), result_ev(650.0, "yes")]
    sim = run(strat, ev, queue="back")
    ok([f["qty"] for f in sim.fills if f["kind"] == "make"] == [2.0], "no sizes: at-price never fills, through does")
    tk = TakerEdge(min_edge=6, ttl_lo=60, ttl_hi=600, max_takes=1, both_sides=False, slippage=2, size=50)
    sim = run(tk, [market_ev(), (0.0, "tob", {"ticker": "KXBTC-TEST-B81125", "yes_bid": 20, "yes_ask": 23})] + spot_evs(0, 700) + [result_ev(650.0, "yes")],
              assume_size=20)
    ok([f["qty"] for f in sim.fills if f["kind"] == "take"] == [20.0], "taker capped at assumed depth")


def test_fair_value_lognormal() -> None:
    fv = FairValue(k=1.0, halflife_s=600)
    ts = 0.0
    import random
    rnd = random.Random(1)
    px = 80000.0
    for i in range(3600):
        px *= math.exp(rnd.gauss(0, 0.5 * math.sqrt(1 / (365 * 86400))))
        fv.on_spot("BTC", px, float(i))
    sig = fv.sigma_annual("BTC")
    ok(0.35 < sig < 0.65, f"recovers ~50% annual vol from 1s samples: {sig:.2f}")
    from src.replay.run import Market
    m = Market("T", "E", "KXBTC", px - 125, px + 125, 3600.0 + 3600.0)
    p_atm = fv.p_bracket(m, 3600.0)
    far = Market("T2", "E", "KXBTC", px + 1000, px + 1250, 3600.0 + 3600.0)
    ok(0 < fv.p_bracket(far, 3600.0) < p_atm < 1, "ATM bracket likelier than a far one")
    tot = sum(fv.p_bracket(Market("x", "E", "S", px - 20000 + 250 * i, px - 20000 + 250 * (i + 1), 7200.0), 3600.0) for i in range(160))
    ok(abs(tot - 1) < 0.01, f"bracket probabilities sum to 1: {tot:.3f}")


def test_load_tape_roundtrip(tmp="/tmp/_tape_test.jsonl") -> None:
    lines = [{"t": 1.0, "type": "market", "ticker": "T", "event": "E", "series": "KXBTC", "lo": 1, "hi": 2, "close_ts": 100},
             {"t": 2.0, "type": "ws", "raw": {"type": "orderbook_snapshot", "sid": 1, "seq": 1, "msg": {"market_ticker": "T", "yes": [[1, 1]], "no": []}}},
             {"t": 3.0, "type": "ws", "raw": {"type": "trade", "sid": 2, "msg": {"market_ticker": "T", "yes_price_dollars": "0.4200", "count_fp": "3.00", "taker_side": "yes"}}},
             {"t": 2.5, "type": "spot", "sym": "BTC", "price": 1.5, "ts": 2.5},
             {"t": 4.0, "type": "ws", "raw": {"id": 1, "type": "subscribed", "msg": {}}},
             {"t": 5.0, "type": "result", "ticker": "T", "result": "yes"}]
    with open(tmp, "w") as f:
        for l in lines:
            f.write(json.dumps(l) + "\n")
    ev = load_tape([tmp])
    ok([k for _, k, _ in ev] == ["market", "snapshot", "spot", "trade", "result"], f"sorted+normalized: {[k for _, k, _ in ev]}")
    ok(ev[3][2]["yes_price"] == 42 and ev[3][2]["count"] == 3.0, "trade normalized on load")


def main() -> None:
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
        print(f"  ok  {t.__name__}")
    print(f"\n{len(tests)} tests, {CHECKS} checks: all green")


if __name__ == "__main__":
    main()
