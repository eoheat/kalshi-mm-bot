"""Paper broker — a virtual account the maker trades against REAL market data.

Demo can't test this maker: the demo crypto books are empty, so nothing fills.
Instead we read LIVE prod books (public, no key, no real order) and SIMULATE
fills against a virtual account with equity. That lets us watch a P&L curve
under real conditions.

Accounting (all cents, integer): every maker quote is a BUY of a fully
collateralised binary — a 'yes' order buys YES at its price, a 'no' order buys
NO at its price (NO price = 100 - the YES ask). Positions are long YES and/or
long NO per ticker; nothing is sold intraday, so realised P&L is booked only at
settlement, when the winning side pays 100 and the other pays 0.

    cash        = bankroll - cost(filled buys) + payouts(settlements)
    available   = cash - collateral locked by resting orders
    equity      = cash + mark-to-market of open positions

FILL MODEL (honest about its optimism): a resting order fills the moment the
live top-of-book crosses its price — a YES bid at p fills when yes_ask <= p, a
NO buy at p fills when yes_bid >= 100 - p — for up to the size resting at that
touch. This assumes we were first in the queue and filled at our own price with
no latency, so simulated P&L is an UPPER BOUND on what a real maker would earn;
real queue position, partial fills and adverse selection all make it worse. Use
it to see the SIGN and shape of the edge, not to trust the exact number.
"""
from __future__ import annotations

import time


class PaperBroker:
    def __init__(self, bankroll_cents: int, now_fn=time.time):
        self.cash = int(bankroll_cents)
        self.start_cash = int(bankroll_cents)
        self.now = now_fn
        self.pos: dict[str, dict[str, list]] = {}   # ticker -> {"yes":[qty,cost], "no":[qty,cost]}
        self.resting: dict[str, dict] = {}          # oid -> {ticker, side, price, count}
        self._oid = 0
        self.realized = 0                            # cumulative settlement P&L (cents)
        self.fills: list[dict] = []
        self.settlements: list[dict] = []
        self.equity_curve: list[dict] = []

    # ---- account views --------------------------------------------------
    def _slot(self, ticker: str) -> dict:
        return self.pos.setdefault(ticker, {"yes": [0, 0], "no": [0, 0]})

    def locked(self) -> int:
        return sum(o["count"] * o["price"] for o in self.resting.values())

    def available(self) -> int:
        return max(self.cash - self.locked(), 0)

    def net_position(self, ticker: str) -> int:
        """Net YES-equivalent contracts (long NO counts as short YES) — what the
        quoter's inventory skew and per-market cap reason about."""
        p = self.pos.get(ticker)
        return 0 if not p else p["yes"][0] - p["no"][0]

    def underlying_exposure_usd(self, prefix: str) -> float:
        """Rough $ exposure across a coin's held tickers (0.5/contract, matching
        the live bot's pos_exposure), for the quoter's exposure cap."""
        return sum(0.5 * (p["yes"][0] + p["no"][0])
                   for t, p in self.pos.items() if t.split("-")[0] == prefix)

    # ---- order lifecycle ------------------------------------------------
    def place(self, ticker: str, side: str, price_cents: int, count: int) -> str:
        assert side in ("yes", "no"), side
        assert 1 <= price_cents <= 99, price_cents
        assert count > 0, count
        self._oid += 1
        oid = f"paper-{self._oid}"
        self.resting[oid] = {"ticker": ticker, "side": side,
                             "price": int(price_cents), "count": int(count)}
        return oid

    def cancel_ticker(self, ticker: str) -> None:
        for oid in [k for k, o in self.resting.items() if o["ticker"] == ticker]:
            del self.resting[oid]

    def cancel_all(self) -> None:
        self.resting.clear()

    # ---- fills against the live book ------------------------------------
    def try_fills(self, books: dict) -> list[dict]:
        """books: {ticker: {yes_bid, yes_ask, yes_bid_size, yes_ask_size}} in cents.
        Fill any resting order the current top-of-book has crossed."""
        booked = []
        for oid in list(self.resting):
            o = self.resting.get(oid)
            if o is None:
                continue
            b = books.get(o["ticker"])
            if not b:
                continue
            side, p, cnt = o["side"], o["price"], o["count"]
            fill = 0
            if side == "yes":                                    # buy YES at p
                ask = b.get("yes_ask")
                if ask is not None and ask <= p:                 # a seller crossed to <= our bid
                    fill = min(cnt, b.get("yes_ask_size") or cnt)
            else:                                                # buy NO at p  (== sell YES at 100-p)
                bid = b.get("yes_bid")
                if bid is not None and bid >= 100 - p:           # a buyer crossed up to our YES ask
                    fill = min(cnt, b.get("yes_bid_size") or cnt)
            if fill > 0:
                self._book_fill(o["ticker"], side, p, fill)
                booked.append({"ticker": o["ticker"], "side": side, "price": p, "qty": fill})
                if fill >= cnt:
                    del self.resting[oid]
                else:
                    o["count"] = cnt - fill
        return booked

    def _book_fill(self, ticker: str, side: str, price: int, qty: int) -> None:
        self.cash -= qty * price                     # fully-collateralised buy: pay upfront
        slot = self._slot(ticker)[side]
        slot[0] += qty
        slot[1] += qty * price
        self.fills.append({"ts": self.now(), "ticker": ticker, "side": side,
                           "price": price, "qty": qty})

    # ---- settlement -----------------------------------------------------
    def settle(self, ticker: str, result: str) -> "dict | None":
        """Resolve a held position: the winning side pays 100/contract, the
        other 0. result is 'yes' or 'no'."""
        p = self.pos.pop(ticker, None)
        self.cancel_ticker(ticker)
        if not p:
            return None
        yq, yc = p["yes"]
        nq, nc = p["no"]
        payout = (yq * 100 if result == "yes" else 0) + (nq * 100 if result == "no" else 0)
        cost = yc + nc
        self.cash += payout
        self.realized += payout - cost
        rec = {"ts": self.now(), "ticker": ticker, "result": result,
               "payout": payout, "cost": cost, "pnl": payout - cost}
        self.settlements.append(rec)
        return rec

    # ---- marking + equity ----------------------------------------------
    def mark(self, books: dict) -> float:
        """Mark-to-market of open positions at the market MID (not our own fair,
        which could be biased). Falls back to cost when there's no live mid."""
        total = 0.0
        for ticker, p in self.pos.items():
            b = books.get(ticker) or {}
            bid, ask = b.get("yes_bid"), b.get("yes_ask")
            yq, yc = p["yes"]
            nq, nc = p["no"]
            if bid is not None and ask is not None:
                yes_mid = (bid + ask) / 2.0
                total += yq * yes_mid + nq * (100 - yes_mid)
            else:
                total += yc + nc                     # no market -> hold at cost (no P&L recognised)
        return total

    def equity(self, books: dict) -> float:
        return self.cash + self.mark(books)

    def snapshot(self, books: dict) -> float:
        eq = self.equity(books)
        self.equity_curve.append({"ts": self.now(), "equity": round(eq, 2),
                                  "cash": self.cash, "realized": self.realized,
                                  "open_positions": len(self.pos), "resting": len(self.resting)})
        return eq

    # ---- reporting ------------------------------------------------------
    def summary(self, books: dict | None = None) -> dict:
        eq = self.equity(books or {})
        wins = sum(1 for s in self.settlements if s["pnl"] > 0)
        n = len(self.settlements)
        return {
            "start_equity": self.start_cash,
            "equity": round(eq, 2),
            "pnl": round(eq - self.start_cash, 2),
            "realized": self.realized,
            "unrealized": round(eq - self.cash, 2),
            "fills": len(self.fills),
            "settled": n,
            "win_rate": round(wins / n, 3) if n else None,
            "open_positions": len(self.pos),
        }
