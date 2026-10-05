"""Minimal Kalshi trade-api v2 client: RSA-PSS auth, markets, orders.

Auth model (per docs as of Aug 2026 — verify if you get 401s):
  headers KALSHI-ACCESS-KEY / KALSHI-ACCESS-TIMESTAMP / KALSHI-ACCESS-SIGNATURE,
  signature = base64(RSA-PSS-SHA256 over f"{timestamp_ms}{METHOD}{path}").

CLIs:
    python -m src.kalshi_client --check           # auth + balance
    python -m src.kalshi_client --discover BTC    # list open markets matching a string
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import time
import uuid

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from dotenv import load_dotenv

BASES = {
    "prod": "https://api.elections.kalshi.com/trade-api/v2",
    # NOTE: demo is external-api.demo.kalshi.co (the ".com" spelling and the
    # old demo-api host are wrong). Demo also needs its OWN account and API
    # key from demo.kalshi.co/sign-up — production credentials 401 here.
    "demo": "https://external-api.demo.kalshi.co/trade-api/v2",
}
# Kalshi's current docs give the portfolio/shard endpoints under
# external-api.kalshi.com (demo: external-api.demo.kalshi.co) while the
# market-data host above still serves reads. If a portfolio call 404s,
# set KALSHI_API_BASE to the host from the docs and re-run — don't guess.
ALT_BASES = {
    "prod": "https://external-api.kalshi.com/trade-api/v2",
    "demo": "https://external-api.demo.kalshi.co/trade-api/v2",
}
# Signature spec (a 401 usually means one of these is off):
#   sign  f"{timestamp_ms}{METHOD}{path}"  where path INCLUDES /trade-api/v2
#   and EXCLUDES the query string; RSA-PSS, SHA-256, salt = DIGEST_LENGTH.


class KalshiClient:
    def __init__(self, env: str = "demo", dry_run: bool = True):
        # .env holds production; .env.demo (if present) overrides it for demo.
        # Demo and prod are SEPARATE Kalshi accounts with separate keys — a
        # crossed key is a 401, so keep them in separate files.
        load_dotenv()
        env_file = f".env.{env}"
        if os.path.exists(env_file):
            load_dotenv(env_file, override=True)
            self.env_file = env_file
        else:
            self.env_file = ".env"
        self.base = os.environ.get("KALSHI_API_BASE") or BASES[env]
        self.alt_base = ALT_BASES[env]
        # Public market-data reads can come from a different host than the
        # portfolio calls: the demo exchange has NO quotes on its crypto
        # strips (every book is 0.00/1.00), so a shadow run that never orders
        # points its reads at prod. set_data_env("prod") flips it.
        self.data_base = self.base
        self.env = env
        self.dry_run = dry_run
        self.key_id = os.environ.get("KALSHI_API_KEY_ID", "")
        pem_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH", "")
        self._key = None
        if pem_path and os.path.exists(pem_path):
            with open(pem_path, "rb") as f:
                self._key = serialization.load_pem_private_key(f.read(), password=None)

    # ---------- auth ----------
    def _headers(self, method: str, path: str) -> dict:
        if self._key is None:
            raise RuntimeError(f"no private key loaded — set KALSHI_PRIVATE_KEY_PATH in {self.env_file}")
        if not self.key_id or "paste" in self.key_id.lower():
            raise RuntimeError(f"KALSHI_API_KEY_ID is unset or still a placeholder in {self.env_file}")
        ts = str(int(time.time() * 1000))
        msg = f"{ts}{method.upper()}{'/trade-api/v2' + path}".encode()
        sig = self._key.sign(
            msg,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
            "Content-Type": "application/json",
        }

    def set_data_env(self, env: str) -> None:
        self.data_base = BASES[env]

    def _req(self, method: str, path: str, *, auth: bool = True, **kw):
        headers = self._headers(method, path) if auth else {}
        base = self.base if auth else self.data_base
        r = requests.request(method, base + path, headers=headers, timeout=10, **kw)
        if r.status_code == 401:
            raise requests.HTTPError(
                f"401 Unauthorized for {method} {path} on {self.base}.\n"
                f"  env: {self.env} | credentials from: {self.env_file} | "
                f"key id set: {bool(self.key_id)}\n"
                "  Most common cause: demo and production have SEPARATE accounts and\n"
                "  keys — a production key 401s against demo (and vice versa).\n"
                "  Sign up for demo at demo.kalshi.co/sign-up and make a key there,\n"
                "  or run against --env prod with your production key.",
                response=r,
            )
        # A plain-text 404 is the router (no such route -> maybe wrong host).
        # A JSON 404 is the application (route exists; the resource doesn't).
        if (r.status_code == 404 and self.base != self.alt_base
                and path.startswith("/portfolio") and not r.text.lstrip().startswith("{")):
            raise requests.HTTPError(
                f"404 for {path} on {self.base}. The portfolio/shard endpoints may "
                f"live on {self.alt_base} — set KALSHI_API_BASE to that host and retry.",
                response=r,
            )
        if r.status_code >= 400:
            body = r.text.strip().replace("\n", " ")[:400]
            raise requests.HTTPError(
                f"{r.status_code} for {method} {path} on {self.base}\n  body: {body}",
                response=r,
            )
        return r.json()

    # ---------- public ----------
    @staticmethod
    def normalize_market(m: dict) -> dict:
        """V2 market rows carry prices as dollar strings (yes_ask_dollars) and
        sizes as fp strings (yes_ask_size_fp). Add the integer-cent fields the
        bots read (yes_bid / yes_ask, plus yes_bid_size / yes_ask_size), and
        blank out an EMPTY book: bid 0.00 means no bid, ask 1.00 (or 0.00)
        means no ask — not a 100c offer."""
        def cents(key_int, key_dollars):
            v = m.get(key_int)
            if v is None and m.get(key_dollars) is not None:
                try:
                    v = int(round(float(m[key_dollars]) * 100))
                except (TypeError, ValueError):
                    v = None
            return v
        def size(key_int, key_fp):
            v = m.get(key_int)
            if v is None and m.get(key_fp) is not None:
                try:
                    v = int(round(float(m[key_fp])))
                except (TypeError, ValueError):
                    v = None
            return v
        bid, ask = cents("yes_bid", "yes_bid_dollars"), cents("yes_ask", "yes_ask_dollars")
        m["yes_bid"] = bid if bid and 1 <= bid <= 99 else None
        m["yes_ask"] = ask if ask and 1 <= ask <= 99 else None
        m["yes_bid_size"] = size("yes_bid_size", "yes_bid_size_fp") if m["yes_bid"] else 0
        m["yes_ask_size"] = size("yes_ask_size", "yes_ask_size_fp") if m["yes_ask"] else 0
        return m

    def markets(self, series_ticker: str | None = None, status: str = "open",
                max_pages: int = 10, **params) -> list[dict]:
        """All matching markets, following the cursor.

        One page is 200 rows; a single KXBTC hour lists ~200 brackets and
        several hours are open at once, so the near-the-money brackets are
        routinely NOT on the first page. Not paginating was a real bug."""
        params = {"status": status, "limit": 200, **params}
        if series_ticker:
            params["series_ticker"] = series_ticker
        out, cursor = [], None
        for _ in range(max_pages):
            if cursor:
                params["cursor"] = cursor
            res = self._req("GET", "/markets", auth=False, params=params)
            batch = res.get("markets", [])
            out.extend(self.normalize_market(m) for m in batch)
            cursor = res.get("cursor")
            if not cursor or not batch:
                break
        return out

    def orderbook(self, ticker: str) -> dict:
        """{"yes": [[price, count], ...], "no": [...]} — resting bids per side.
        V2 may send the levels under yes_dollars/no_dollars (price as a dollar
        string, count as an fp string) or as objects; normalise to pairs the
        bot already parses (cents ints or dollar strings both work there)."""
        res = self._req("GET", f"/markets/{ticker}/orderbook", auth=False)
        # V2 nests the book under "orderbook" (cents ints) OR "orderbook_fp"
        # (fixed-point: price a dollar string, count an fp string). PROD sends
        # ONLY orderbook_fp with per-side keys yes_dollars/no_dollars — the old
        # code looked for "orderbook"/"yes"/"no" and silently read an EMPTY book,
        # so every taker skipped "no ask on the live book". Try all shapes.
        raw = res.get("orderbook") or res.get("orderbook_fp") or res or {}
        out = {}
        for side in ("yes", "no"):
            levels = raw.get(side)
            if levels is None:
                levels = raw.get(f"{side}_dollars")
            if levels is None:
                levels = raw.get(f"{side}_fp")
            pairs = []
            for lvl in levels or []:
                if isinstance(lvl, dict):
                    px = lvl.get("price", lvl.get("price_dollars", lvl.get("yes_price")))
                    cnt = lvl.get("count", lvl.get("count_fp", lvl.get("quantity")))
                    pairs.append([px, cnt])
                else:
                    pairs.append(list(lvl))
            out[side] = pairs
        return out

    # ---------- private ----------
    def balance(self) -> dict:
        return self._req("GET", "/portfolio/balance")

    @staticmethod
    def _position_contracts(p: dict) -> int:
        """Signed net contracts, whatever the venue calls the field. PROD sends
        position_fp (a string); older shapes send position. Reading only the
        legacy name gave 0 on prod -> inventory caps and skew silently off."""
        for k in ("position_fp", "position", "position_cents"):
            if p.get(k) is not None:
                try:
                    return int(round(float(p[k])))
                except (TypeError, ValueError):
                    pass
        return 0

    @staticmethod
    def _position_realized_cents(p: dict) -> int:
        """Realized P&L in CENTS. PROD sends realized_pnl_dollars; legacy sends
        realized_pnl (cents). Reading only the legacy name gave 0 on prod ->
        the daily-loss kill switch was DEAD (see sniper._realized_cents)."""
        if p.get("realized_pnl_dollars") is not None:
            try:
                return int(round(float(p["realized_pnl_dollars"]) * 100))
            except (TypeError, ValueError):
                pass
        for k in ("realized_pnl", "realized_pnl_cents"):
            if p.get(k) is not None:
                try:
                    return int(p[k])
                except (TypeError, ValueError):
                    pass
        return 0

    def positions(self) -> list[dict]:
        res = self._req("GET", "/portfolio/positions")
        rows: list[dict] = []
        for k in ("market_positions", "positions"):
            if isinstance(res.get(k), list):
                rows = res[k]
                break
        # Canonicalize the fields the maker reads (position, realized_pnl-cents)
        # from whatever V2/legacy names the venue used. Raw fields are kept so
        # the sniper's own readers (which prefer the *_dollars/*_fp names) still
        # work. Without this the maker read prod rows as all-zero.
        for p in rows:
            p["position"] = self._position_contracts(p)
            p["realized_pnl"] = self._position_realized_cents(p)
        return rows

    # ---------- orders (V2, June 2026) ----------
    # VERIFIED on demo 2026-09-03 with --probe:
    #   GET    /portfolio/orders              200  (list; survives)
    #   POST   /portfolio/orders              410  deprecated_v1_order_endpoint
    #   POST   /portfolio/events/orders       JSON not_found for a bogus ticker
    #                                         = the route is LIVE (a missing
    #                                         route answers plain-text 404)
    # So: list on /portfolio/orders, mutate on /portfolio/events/orders.
    # V2 speaks YES-side economics directly:
    # side = "bid" (buy YES) | "ask" (sell YES), price = dollar string.
    # Internally we keep the same normalized order shape everywhere:
    #   {order_id, ticker, book_side, yes_price_cents, remaining_count}
    # A bid locks yes_price per contract; an ask locks (100 - yes_price).

    @staticmethod
    def _normalize(o: dict) -> dict:
        """Accept both the V2 field names and the deprecated ones."""
        book = o.get("book_side")
        if book not in ("bid", "ask"):
            # legacy: action=buy + side=yes -> bid; action=buy + side=no -> ask
            side = o.get("side") or o.get("outcome_side")
            book = "bid" if side == "yes" else "ask"
        yp = o.get("yes_price_dollars")
        if yp is not None:
            yes_cents = int(round(float(yp) * 100))
        elif o.get("yes_price") is not None:
            yes_cents = int(o["yes_price"])
        elif o.get("no_price") is not None:
            yes_cents = 100 - int(o["no_price"])
        else:
            yes_cents = 0
        rem = o.get("remaining_count_fp", o.get("remaining_count", o.get("count", 0)))
        return {
            "order_id": o.get("order_id"), "ticker": o.get("ticker"),
            "book_side": book, "yes_price_cents": yes_cents,
            "remaining_count": int(round(float(rem or 0))),
            "client_order_id": o.get("client_order_id"),
        }

    def resting_orders(self, ticker: str | None = None) -> list[dict]:
        params = {"status": "resting", "limit": 1000}
        if ticker:
            params["ticker"] = ticker
        out, cursor = [], None
        for _ in range(10):
            if cursor:
                params["cursor"] = cursor
            res = self._req("GET", "/portfolio/orders", params=params)
            out += [self._normalize(o) for o in res.get("orders", [])]
            cursor = res.get("cursor")
            if not cursor:
                break
        return out

    def place_limit(self, ticker: str, side: str, price_cents: int, count: int) -> dict:
        """Post a resting limit order. Maker-only is ENFORCED by post_only —
        the exchange rejects it rather than letting it cross.

        side keeps the quoter's vocabulary: 'yes' = bid for YES at price_cents;
        'no' = buy NO at price_cents, i.e. an ask on YES at 100 - price_cents.
        """
        if side == "yes":
            book, yes_cents = "bid", int(price_cents)
        elif side == "no":
            book, yes_cents = "ask", 100 - int(price_cents)
        elif side in ("bid", "ask"):
            book, yes_cents = side, int(price_cents)
        else:
            raise ValueError(f"unknown side {side!r}")
        if not (1 <= yes_cents <= 99):
            raise ValueError(f"yes price {yes_cents}c out of range")
        order = {
            "ticker": ticker,
            "side": book,
            "count": f"{int(count)}.00",
            "price": f"{yes_cents / 100:.4f}",
            "time_in_force": "good_till_canceled",
            "self_trade_prevention_type": "taker_at_cross",   # our resting order wins
            "post_only": True,
            "cancel_order_on_pause": True,
            # uuid, not a ms timestamp: a bid and ask posted back-to-back land
            # in the same millisecond and the second is rejected as a duplicate
            "client_order_id": f"mm-{uuid.uuid4().hex[:20]}",
            "exchange_index": -1,   # auto-route by ticker (crypto = shard 2)
        }
        if self.dry_run:
            print(f"[dry-run] PLACE {json.dumps(order)}")
            return {"dry_run": True, "order": order}
        return self._req("POST", "/portfolio/events/orders", json=order)

    def place_taker(self, ticker: str, side: str, price_cents: int, count: int,
                    time_in_force: str = "immediate_or_cancel",
                    max_price_cents: int | None = None) -> dict:
        """Cross the spread: a limit at the ask that must fill NOW or die.

        Used by the sniper only. post_only=False by design, and the order is
        immediate-or-cancel so nothing is ever left resting where a faster
        bot can pick it off. side vocabulary matches place_limit: 'yes' =
        buy YES at price_cents, 'no' = buy NO at price_cents.

        price_cents is the touch we saw; max_price_cents (>= price_cents) is
        the limit actually sent. On a limit-order book a crossing order fills
        at the RESTING price, so the extra cents are only a ceiling for when
        the touch has moved by the time the order lands (it had, both times,
        on the first live night: IOC at the stale touch = cancelled, 0 fills).

        Returns {"filled", "fill_price_cents", "fee_cents", "verified", ...}.
        If the venue rejects the TIF (400 mentioning time_in_force), falls
        back to GTC + immediate cancel."""
        limit = int(max_price_cents if max_price_cents is not None else price_cents)
        limit = max(limit, int(price_cents))
        if side == "yes":
            book, yes_cents = "bid", limit
        elif side == "no":
            book, yes_cents = "ask", 100 - limit
        else:
            raise ValueError(f"unknown side {side!r}")
        if not (1 <= yes_cents <= 99):
            raise ValueError(f"yes price {yes_cents}c out of range")
        order = {
            "ticker": ticker,
            "side": book,
            "count": f"{int(count)}.00",
            "price": f"{yes_cents / 100:.4f}",
            "time_in_force": time_in_force,
            "self_trade_prevention_type": "taker_at_cross",
            "post_only": False,
            "client_order_id": f"sn-{uuid.uuid4().hex[:20]}",
            "exchange_index": -1,
        }
        if self.dry_run:
            print(f"[dry-run] TAKE {json.dumps(order)}")
            return {"dry_run": True, "order": order, "filled": 0, "remaining": int(count)}
        try:
            res = self._req("POST", "/portfolio/events/orders", json=order)
        except requests.HTTPError as e:
            body = (getattr(e, "response", None) is not None and e.response.text) or ""
            if e.response is not None and e.response.status_code == 400 \
                    and "time_in_force" in body and time_in_force != "good_till_canceled":
                order["time_in_force"] = "good_till_canceled"
                order["client_order_id"] = f"sn-{uuid.uuid4().hex[:20]}"
                res = self._req("POST", "/portfolio/events/orders", json=order)
                o = res.get("order", res)
                oid = o.get("order_id")
                if oid:
                    try:
                        self.cancel(oid, ticker)
                    except requests.HTTPError:
                        pass   # already fully filled or already gone
            else:
                raise
        o = res.get("order", res) if isinstance(res, dict) else {}
        oid = o.get("order_id")
        filled = self.filled_count(o, count)
        status = str(o.get("status") or o.get("order_status") or "").lower()
        _TERMINAL = ("executed", "filled", "canceled", "cancelled")
        # An EXPLICIT fill count is the exchange stating the outcome, and is
        # confirmation on its own — the POST create response reports the fill
        # via fill_count / average_fill_price but sends NO status word, so
        # requiring a status here threw away a real 5-contract fill on prod
        # 2026-09-04 (booked 0). A status word alone also confirms. What must
        # NEVER pass as verified is an initial-minus-remaining GUESS.
        verified = filled is not None and (self._has_explicit_fill(o) or status in _TERMINAL)
        if not verified and oid:
            # No explicit outcome in the create response. Ask the order row.
            for _ in range(3):
                try:
                    o2 = self.order(oid)
                except requests.HTTPError:
                    o2 = {}
                f2 = self.filled_count(o2, count)
                st2 = str(o2.get("status") or o2.get("order_status") or "").lower()
                if f2 is not None and (self._has_explicit_fill(o2) or st2 in _TERMINAL):
                    o, filled, status, verified = {**o, **o2}, f2, st2, True
                    break
                time.sleep(0.3)
        if not verified:
            filled = 0                       # unknown == none; never book a phantom fill
        avg, fee_c = self.fill_economics(o, int(filled), side, int(price_cents))
        return {"order": o, "filled": int(filled), "remaining": max(int(count) - int(filled), 0),
                "order_id": oid, "status": status, "verified": verified,
                "fill_price_cents": avg, "fee_cents": fee_c, "limit_cents": limit, "raw": res}

    def order(self, order_id: str) -> dict:
        """One order's current state from the exchange (V2 field names)."""
        res = self._req("GET", f"/portfolio/orders/{order_id}")
        return res.get("order", res)

    def fills(self, ticker: str | None = None, limit: int = 100) -> list[dict]:
        params = {"limit": limit}
        if ticker:
            params["ticker"] = ticker
        res = self._req("GET", "/portfolio/fills", params=params)
        return res.get("fills", [])

    @staticmethod
    def filled_count(o: dict, count: int | None = None) -> int | None:
        """Contracts actually filled on an order row, from whichever V2/legacy
        field carries it. None when the row does not say — callers must treat
        None as ZERO, never as 'all of it' (the first live night recorded two
        phantom fills by defaulting a missing remaining-count to 0)."""
        # V2 (verified on prod 2026-09-03): the row carries fill_count_fp, and a
        # cancelled IOC has remaining_count_fp "0.00" — so initial - remaining
        # is 1 for an order that filled NOTHING. Read the fill count field;
        # only fall back to the subtraction on a live/resting order.
        for k in ("fill_count_fp", "fill_count", "filled_count_fp", "filled_count",
                  "taker_fill_count_fp", "taker_fill_count"):
            if o.get(k) is not None:
                try:
                    return int(round(float(o[k])))
                except (TypeError, ValueError):
                    pass
        status = str(o.get("status") or o.get("order_status") or "").lower()
        if status in ("canceled", "cancelled"):
            return None
        init = o.get("initial_count_fp", o.get("initial_count", o.get("count_fp", o.get("count"))))
        rem = o.get("remaining_count_fp", o.get("remaining_count"))
        if init is None and rem is not None and count is not None:
            init = count                     # the venue said what is left; we know what we sent
        if init is not None and rem is not None:
            try:
                return max(int(round(float(init))) - int(round(float(rem))), 0)
            except (TypeError, ValueError):
                pass
        return None

    @staticmethod
    def _has_explicit_fill(o: dict) -> bool:
        """True when the response STATES a fill count — the POST create response
        via fill_count / average_fill_price, an order row via fill_count_fp. This
        is authoritative, unlike an initial-minus-remaining subtraction (a guess
        that booked two phantom fills on the first live night)."""
        return any(o.get(k) is not None for k in
                   ("fill_count", "fill_count_fp", "filled_count", "filled_count_fp",
                    "taker_fill_count", "taker_fill_count_fp", "average_fill_price"))

    @staticmethod
    def fill_economics(o: dict, filled: int, side: str, price_cents: int) -> tuple[int, int]:
        """(our per-contract COST in cents for the side we bought, fee in cents).

        Two V2 shapes carry this: the order ROW as totals
        (taker_fill_cost_dollars / taker_fees_dollars) and the POST create
        response as per-contract averages on the YES price (average_fill_price /
        average_fee_paid). Prefer the unambiguous total; else convert the YES
        average to our side's cost. Fall back to the limit and the 7% formula."""
        avg = None
        cost = 0.0
        for k in ("taker_fill_cost_dollars", "maker_fill_cost_dollars"):
            try:
                cost += float(o.get(k) or 0.0)
            except (TypeError, ValueError):
                pass
        if filled > 0 and cost > 0:
            avg = int(round(cost * 100 / filled))       # total we paid / contracts
        if avg is None and o.get("average_fill_price") is not None:
            try:
                yes_c = float(o["average_fill_price"]) * 100    # Kalshi quotes the YES price
                avg = int(round(yes_c if side == "yes" else 100 - yes_c))
            except (TypeError, ValueError):
                avg = None
        if avg is None:
            avg = price_cents
        fee = 0.0
        for k in ("taker_fees_dollars", "maker_fees_dollars"):
            try:
                fee += float(o.get(k) or 0.0)
            except (TypeError, ValueError):
                pass
        if fee <= 0 and o.get("average_fee_paid") is not None and filled > 0:
            try:
                fee = float(o["average_fee_paid"]) * filled     # create response: per-contract
            except (TypeError, ValueError):
                fee = 0.0
        fee_c = int(round(fee * 100)) if fee > 0 else -1
        return avg, fee_c

    def market(self, ticker: str) -> dict:
        """One market row (has `result` once settled: 'yes' | 'no')."""
        return self.normalize_market(self._req("GET", f"/markets/{ticker}", auth=False).get("market", {}))

    def cancel(self, order_id: str, ticker: str | None = None) -> dict:
        if self.dry_run:
            print(f"[dry-run] CANCEL {order_id}")
            return {"dry_run": True}
        params = {"market_ticker": ticker} if ticker else {"exchange_index": -1}
        return self._req("DELETE", f"/portfolio/events/orders/{order_id}", params=params)

    def cancel_all(self, ticker: str | None = None) -> int:
        """Cancel resting orders. With a ticker: that market's, one by one.
        Without: the exchange's own cancel-everything call (one request,
        every shard) — the right tool for halts and shutdown."""
        if self.dry_run:
            print(f"[dry-run] CANCEL-ALL ticker={ticker}")
            return 0
        n = 0
        if ticker is None:
            # Fast path: the exchange's own cancel-everything. But it can answer
            # 200 with canceled_count=0, or hit only one shard — so we DON'T
            # trust it as the guarantee; the per-order sweep below is.
            for path in ("/portfolio/events/orders/batched", "/portfolio/orders"):
                try:
                    res = self._req("DELETE", path)
                    n = int(res.get("canceled_count") or res.get("count") or 0)
                    break
                except requests.HTTPError:
                    continue
        # Always sweep whatever is still resting, one by one, resilient to a
        # single cancel failing (transient 500 / already gone) — a maker must
        # never leave an orphan resting on the book. Best-effort throughout:
        # if we can't even list, there's nothing to cancel from here.
        try:
            remaining = self.resting_orders(ticker)
        except Exception:
            remaining = []
        for o in remaining:
            try:
                self.cancel(o["order_id"], o.get("ticker"))
                n += 1
            except Exception:
                continue   # keep cancelling the rest; a stuck one retries next call
        return n


CANDIDATE_SERIES = ["KXBTC", "KXBTCD", "KXBTCMAX", "KXBTCMIN", "KXBTC15", "KXBTC15M",
                    "KXETH", "KXETHD", "KXETH15M", "KXSOL", "KXSOLD", "KXSOL15M",
                    "KXXRP", "KXXRPD", "KXXRP15M", "KXDOGE", "KXDOGED"]


def _discover(client: KalshiClient, needle: str, max_pages: int = 25):
    """Find matching open markets. Tries known series tickers first (demo
    carries far fewer markets, so a single unfiltered page usually misses
    them), then pages through everything as a fallback."""
    hits, seen = [], set()

    for s in CANDIDATE_SERIES:
        try:
            for m in client.markets(series_ticker=s):
                if m["ticker"] not in seen:
                    seen.add(m["ticker"]); hits.append(m)
        except Exception:
            pass
    if hits:
        print(f"found via known series tickers: "
              f"{sorted({m['ticker'].split('-')[0] for m in hits})}")

    if not hits:
        print("known series empty here — paging through all open markets...")
        cursor, scanned = None, 0
        for page in range(max_pages):
            params = {"status": "open", "limit": 200}
            if cursor:
                params["cursor"] = cursor
            res = client._req("GET", "/markets", auth=False, params=params)
            batch = res.get("markets", [])
            scanned += len(batch)
            for m in batch:
                blob = (m.get("ticker", "") + " " + m.get("title", "")).upper()
                if needle.upper() in blob and m["ticker"] not in seen:
                    seen.add(m["ticker"]); hits.append(m)
            cursor = res.get("cursor")
            if not cursor or not batch:
                break
        print(f"scanned {scanned} open markets")

    for m in sorted(hits, key=lambda m: m.get("close_time", ""))[:40]:
        print(f"{m['ticker']:<30} close={m.get('close_time','?'):<22} "
              f"yes_bid={m.get('yes_bid','-'):>3} yes_ask={m.get('yes_ask','-'):>3} "
              f"vol={m.get('volume', 0):>7}")
    print(f"\n{len(hits)} open markets matched '{needle}'"
          + (f" (showing first 40)" if len(hits) > 40 else "") + ".")
    if hits:
        series = sorted({m["ticker"].split("-")[0] for m in hits})
        print(f"series ticker(s) for config.yaml: {series}")


def _probe(client: KalshiClient) -> None:
    """Ask the exchange itself which order endpoints exist.

    The POST probes use a V2 body on a ticker that does not exist, so nothing
    can ever fill: a real V2 route answers with a validation error (4xx with
    a message about the ticker), a missing route 404s, a V1 route 410s."""
    v2_body = {"ticker": "PROBE-NO-SUCH-MARKET-XYZ", "side": "bid", "count": "1.00",
               "price": "0.0100", "time_in_force": "good_till_canceled",
               "self_trade_prevention_type": "taker_at_cross", "post_only": True,
               "client_order_id": f"probe-{uuid.uuid4().hex[:12]}", "exchange_index": -1}
    hosts = {
        "demo": ["https://external-api.demo.kalshi.co/trade-api/v2",
                 "https://demo-api.kalshi.co/trade-api/v2"],
        "prod": ["https://external-api.kalshi.com/trade-api/v2",
                 "https://api.elections.kalshi.com/trade-api/v2"],
    }[client.env]
    candidates = [
        ("GET",  "/portfolio/orders", {"status": "resting", "limit": 5}, None),
        ("POST", "/portfolio/orders", None, v2_body),
        ("POST", "/portfolio/events/orders", None, v2_body),
    ]
    for base in hosts:
        print(f"===== {base} =====")
        for method, path, params, body in candidates:
            try:
                headers = client._headers(method, path)
                r = requests.request(method, base + path, headers=headers,
                                     params=params, json=body, timeout=10)
                text = r.text.strip().replace("\n", " ")[:220]
                print(f"{r.status_code}  {method} {path}\n      {text}")
            except Exception as e:
                print(f"ERR  {method} {path}: {e!r}")
        print()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="demo", choices=["demo", "prod"])
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--discover", metavar="NEEDLE")
    ap.add_argument("--probe", action="store_true",
                    help="try each candidate order endpoint and print status + body")
    ap.add_argument("--order", metavar="ORDER_ID", help="print one order's state from the exchange")
    ap.add_argument("--fills", action="store_true", help="print recent fills from the exchange")
    ap.add_argument("--open", action="store_true",
                    help="print the exchange's view: balances per shard, positions, resting orders, recent fills")
    args = ap.parse_args()
    c = KalshiClient(env=args.env, dry_run=True)
    if args.check:
        print(json.dumps(c.balance(), indent=2))
    elif args.discover:
        _discover(c, args.discover)
    elif args.probe:
        _probe(c)
    elif args.order:
        o = c.order(args.order)
        print(json.dumps(o, indent=2))
        print(f"\nfilled_count -> {c.filled_count(o)}  status -> {o.get('status') or o.get('order_status')}")
    elif args.fills:
        for f in c.fills():
            print(json.dumps(f))
        print(f"\n{len(c.fills())} fills")
    elif args.open:
        from .shards import shard_balances
        print("balances per shard (cents):", shard_balances(c))
        pos = c.positions()
        print(f"positions: {len(pos)}")
        for p in pos:
            print("  ", json.dumps({k: p.get(k) for k in ("ticker", "position", "position_fp",
                                                          "market_exposure_dollars", "realized_pnl_dollars") if k in p}))
        rest = c.resting_orders()
        print(f"resting orders: {len(rest)}")
        for o in rest:
            print("  ", json.dumps(o))
        fills = c.fills()
        print(f"fills (most recent {min(len(fills), 20)} of {len(fills)}):")
        for f in fills[:20]:
            print("  ", json.dumps({k: f.get(k) for k in ("ticker", "side", "action", "count_fp", "yes_price_dollars",
                                                          "no_price_dollars", "created_time", "is_taker") if k in f}))
    else:
        ap.print_help()
