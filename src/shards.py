"""Kalshi exchange shards — keeping collateral where the orders are.

Kalshi splits trading across several matching engines ("exchange shards"),
and **balances are held per shard**. Deposited cash lands on shard 0; our
BTC hourly brackets are the Crypto category, which lives on **shard 2**.
An order rejects if the collateral is still sitting on shard 0, so it has to
be moved there first. Kalshi's own docs put it plainly: "Programmatic
traders must preallocate collateral on a given exchange shard before order
placement."

Shard map (docs.kalshi.com/getting_started/exchange_sharding):
    0   default / catch-all
    1   exotics (combos)
    2   crypto          <-- this bot
    3   sports tagged tennis or baseball
Never hardcode a market's shard: every market/event carries an authoritative
`exchange_index` field. `market_shard()` reads it.

Two traps this module exists to absorb:

  * **Units.** GET /portfolio/balance reports CENTS. The transfer endpoint
    takes CENTICENTS (1 cent = 100 centicents). Everything below is cents
    until the API boundary, where it is multiplied exactly once.
  * **Cross-shard transfers are non-atomic and non-idempotent.** Kalshi
    warns a failure "may leave funds in the primary account", and the
    endpoint takes no client-supplied idempotency key. So a failed transfer
    is NEVER retried automatically here — we re-read balances and report.
    Every attempt is appended to data/transfers.jsonl before it is sent.

    OBSERVED, 2026-09-02 (demo): a transfer returned 504 Gateway Timeout and
    the balances afterwards did NOT clearly show it either settling or not —
    the outcome was simply unknowable from the client side. That ambiguity is
    the whole argument for the rule: on a timeout, read balances and decide
    by hand, never retry blind.

  * **Wire unit: VERIFIED 2026-09-02 on demo via `--calibrate`.** 1000 wire
    units moved exactly $0.10, so 1 cent = 100 units (centicents), matching
    the docs. Re-run `--calibrate` if Kalshi ever changes the endpoint.

CLI:
    python3 -m src.shards --status
    python3 -m src.shards --ensure 2 --usd 200
    python3 -m src.shards --sweep 2
Add --live to actually move money (default is dry-run).
"""
from __future__ import annotations

import argparse
import json
import os
import time

from .kalshi_client import KalshiClient

CENTICENTS_PER_CENT = 100

SHARD_NAMES = {
    0: "default",
    1: "exotics/combos",
    2: "crypto",
    3: "sports (tennis, baseball)",
}
CRYPTO_SHARD = 2

# event_contract = the binary event-contract engine (our brackets).
# margined = the margined/perpetuals engine. Don't mix them up.
EVENT_CONTRACT = "event_contract"

TRANSFER_LOG = os.path.join("data", "transfers.jsonl")


class ShardError(RuntimeError):
    pass


def _log(entry: dict) -> None:
    os.makedirs(os.path.dirname(TRANSFER_LOG), exist_ok=True)
    with open(TRANSFER_LOG, "a") as f:
        f.write(json.dumps({"ts": time.time(), **entry}) + "\n")


# ---------------------------------------------------------------- balances

def shard_balances(client: KalshiClient) -> dict[int, int]:
    """{exchange_index: available cents}.

    UNIT TRAP (cost us an evening): the top-level `balance` is in CENTS, but
    inside `balance_breakdown` the per-shard `balance` is a DOLLAR string
    ("18.0000" means $18.00, not 18 cents). Convert, don't cast.
    """
    res = client._req("GET", "/portfolio/balance")
    out: dict[int, int] = {}
    for row in res.get("balance_breakdown") or []:
        out[int(row["exchange_index"])] = int(round(float(row["balance"]) * 100))
    if not out:
        # breakdown is omitted for subaccount-restricted keys — fall back to
        # the flat balance and attribute it to shard 0 rather than guessing.
        out[0] = int(res.get("balance", 0))
    return out


def shard_balance(client: KalshiClient, shard: int) -> int:
    """Available cents on one shard, asked for directly."""
    res = client._req("GET", "/portfolio/balance", params={"exchange_index": shard})
    return int(res.get("balance", 0))


def market_shard(client: KalshiClient, ticker: str) -> int:
    """The authoritative shard for a market, from its own exchange_index."""
    res = client._req("GET", f"/markets/{ticker}", auth=False)
    market = res.get("market", res)
    idx = market.get("exchange_index")
    if idx is None:
        raise ShardError(
            f"{ticker} has no exchange_index — cannot confirm its shard. "
            "Refusing to guess; check the market payload."
        )
    return int(idx)


# ---------------------------------------------------------------- transfer

def transfer(client: KalshiClient, amount_cents: int, from_shard: int,
             to_shard: int, from_subaccount: int = 0,
             to_subaccount: int = 0) -> dict:
    """Move collateral between shards. Single attempt, never retried.

    amount_cents is CENTS; the wire format is centicents.
    """
    if amount_cents <= 0:
        raise ShardError(f"refusing to transfer a non-positive amount ({amount_cents}c)")
    if from_shard == to_shard and from_subaccount == to_subaccount:
        raise ShardError("source and destination are the same")

    body = {
        "source": EVENT_CONTRACT,
        "destination": EVENT_CONTRACT,
        "amount": int(amount_cents) * CENTICENTS_PER_CENT,
        "source_exchange_shard": int(from_shard),
        "destination_exchange_shard": int(to_shard),
        "source_subaccount": int(from_subaccount),
        "destination_subaccount": int(to_subaccount),
    }
    attempt = {"action": "transfer", "amount_cents": amount_cents,
               "from_shard": from_shard, "to_shard": to_shard,
               "dry_run": client.dry_run}
    _log(attempt)  # logged BEFORE sending: a timeout still leaves a record

    if client.dry_run:
        print(f"[dry-run] transfer ${amount_cents/100:.2f} "
              f"shard {from_shard} -> {to_shard}")
        return {"dry_run": True, "body": body}

    try:
        res = client._req("POST", "/portfolio/intra_exchange_instance_transfer",
                          json=body)
    except Exception as e:
        _log({**attempt, "action": "transfer_failed", "error": repr(e)})
        raise ShardError(
            f"transfer failed: {e!r}\n"
            "NOT retrying — cross-shard transfers are non-atomic and take no "
            "idempotency key, so a retry can double-move. Re-read balances "
            "with `python3 -m src.shards --status` and decide from there."
        ) from e
    _log({**attempt, "action": "transfer_ok", "transfer_id": res.get("transfer_id")})
    return res


# ---------------------------------------------------------------- the main entry

def ensure_collateral(client: KalshiClient, shard: int, need_cents: int,
                      buffer_cents: int = 0, source_shard: int = 0,
                      max_transfer_cents: int = 100_000,
                      settle_wait_s: float = 2.0) -> dict:
    """Make sure `shard` holds at least need_cents, moving the shortfall from
    `source_shard` if not.

    Returns a dict describing what happened; raises ShardError if the target
    still isn't funded afterwards. Never moves more than max_transfer_cents
    in one call (default $1,000) and never moves more than the source holds.
    """
    target = int(need_cents) + int(buffer_cents)
    before = shard_balances(client)
    have = before.get(shard, 0)
    result = {"shard": shard, "shard_name": SHARD_NAMES.get(shard, "?"),
              "target_cents": target, "before": dict(before), "moved_cents": 0}

    if have >= target:
        result["status"] = "already_funded"
        return result

    shortfall = target - have
    available = before.get(source_shard, 0)
    if available <= 0:
        raise ShardError(
            f"shard {shard} has ${have/100:.2f}, needs ${target/100:.2f}, but "
            f"shard {source_shard} holds nothing to move. Deposit first, or "
            f"sweep an idle shard: {dict(before)}"
        )

    amount = min(shortfall, available, max_transfer_cents)
    if amount < shortfall:
        print(f"note: moving ${amount/100:.2f} of the ${shortfall/100:.2f} "
              f"shortfall (source balance / per-call cap)")

    transfer(client, amount, source_shard, shard)
    result["moved_cents"] = amount

    if client.dry_run:
        result["status"] = "dry_run"
        return result

    time.sleep(settle_wait_s)
    after = shard_balances(client)
    result["after"] = dict(after)
    now = after.get(shard, 0)
    if now < have + amount * 0.99:  # allow rounding, not silent loss
        raise ShardError(
            f"transfer reported success but shard {shard} shows "
            f"${now/100:.2f} (expected ~${(have + amount)/100:.2f}). "
            "Cross-shard transfers are non-atomic — funds may be mid-flight "
            "or back in the primary account. Check --status before trading."
        )
    result["status"] = "funded" if now >= target else "partially_funded"
    return result


def sweep_home(client: KalshiClient, from_shard: int, to_shard: int = 0,
               keep_cents: int = 0) -> dict:
    """Move idle collateral off a shard (e.g. after the trading session)."""
    bal = shard_balances(client).get(from_shard, 0)
    amount = bal - keep_cents
    if amount <= 0:
        return {"status": "nothing_to_sweep", "balance_cents": bal}
    transfer(client, amount, from_shard, to_shard)
    return {"status": "swept", "moved_cents": amount,
            "from": from_shard, "to": to_shard}


# ---------------------------------------------------------------- CLI

def _print_status(client: KalshiClient) -> None:
    bal = shard_balances(client)
    total = sum(bal.values())
    print(f"{'shard':<6} {'name':<28} {'balance':>12}")
    for idx in sorted(set(list(bal) + list(SHARD_NAMES))):
        cents = bal.get(idx, 0)
        mark = "  <-- this bot" if idx == CRYPTO_SHARD else ""
        print(f"{idx:<6} {SHARD_NAMES.get(idx, '?'):<28} ${cents/100:>10,.2f}{mark}")
    print(f"{'':<6} {'total':<28} ${total/100:>10,.2f}")


def calibrate_units(client: KalshiClient, wire_amount: int = 1000,
                    from_shard: int = 2, to_shard: int = 0,
                    settle_wait_s: float = 4.0) -> dict:
    """Empirically determine what one unit of the API's `amount` field is.

    Sends a transfer of a KNOWN raw wire amount and measures how much money
    actually moved. Deliberately bypasses CENTICENTS_PER_CENT so the constant
    can't bias its own measurement. Demo only — this moves money.

    With the default wire_amount=1000: if the unit is centicents, $0.10 moves;
    if it is a tenth of a cent, $1.00 moves. Either fits in a $2 shard.
    """
    if client.dry_run:
        raise ShardError("calibration must run with --live (it measures a real transfer)")
    before = shard_balances(client)
    if before.get(from_shard, 0) < 200:
        raise ShardError(
            f"shard {from_shard} holds ${before.get(from_shard,0)/100:.2f}; "
            "calibration needs at least $2.00 there to be safe under either hypothesis"
        )
    body = {
        "source": EVENT_CONTRACT, "destination": EVENT_CONTRACT,
        "amount": int(wire_amount),
        "source_exchange_shard": int(from_shard),
        "destination_exchange_shard": int(to_shard),
        "source_subaccount": 0, "destination_subaccount": 0,
    }
    _log({"action": "calibrate", "wire_amount": wire_amount,
          "from_shard": from_shard, "to_shard": to_shard})
    timed_out = False
    try:
        client._req("POST", "/portfolio/intra_exchange_instance_transfer", json=body)
    except Exception as e:
        timed_out = True
        print(f"request errored ({repr(e)[:90]}) — measuring anyway, "
              "since a timeout here has been seen to settle regardless")
    time.sleep(settle_wait_s)
    after = shard_balances(client)
    moved_cents = before.get(from_shard, 0) - after.get(from_shard, 0)
    out = {"wire_amount": wire_amount, "moved_cents": moved_cents,
           "errored": timed_out, "before": before, "after": after}
    if moved_cents <= 0:
        out["verdict"] = "nothing moved — inconclusive; re-run or check balances"
        return out
    units_per_cent = wire_amount / moved_cents
    out["units_per_cent"] = round(units_per_cent, 4)
    out["verdict"] = (
        f"{wire_amount} wire units moved ${moved_cents/100:.2f} "
        f"=> 1 cent = {units_per_cent:g} units. "
        f"Set CENTICENTS_PER_CENT = {units_per_cent:g} in src/shards.py "
        f"(currently {CENTICENTS_PER_CENT})."
    )
    return out


def _print_raw(client: KalshiClient) -> None:
    """Dump every balance view the API offers, unparsed.

    Use when the UI and `--status` disagree: the money is usually real but
    sitting in a view the default call doesn't report (another subaccount,
    the margined instance, or a shard the breakdown omits)."""
    print("=== GET /portfolio/balance (no params) ===")
    try:
        print(json.dumps(client._req("GET", "/portfolio/balance"), indent=2))
    except Exception as e:
        print("  failed:", repr(e)[:200])

    for idx in range(4):
        print(f"\n=== GET /portfolio/balance?exchange_index={idx} ===")
        try:
            print(json.dumps(client._req("GET", "/portfolio/balance",
                                         params={"exchange_index": idx}), indent=2))
        except Exception as e:
            print("  failed:", repr(e)[:160])

    for sub in (0, 1):
        print(f"\n=== GET /portfolio/balance?subaccount={sub} ===")
        try:
            print(json.dumps(client._req("GET", "/portfolio/balance",
                                         params={"subaccount": sub}), indent=2))
        except Exception as e:
            print("  failed:", repr(e)[:160])

    for path in ("/portfolio/positions", "/portfolio/settlements"):
        print(f"\n=== GET {path} (first 400 chars) ===")
        try:
            print(json.dumps(client._req("GET", path))[:400])
        except Exception as e:
            print("  failed:", repr(e)[:160])


def main() -> None:
    ap = argparse.ArgumentParser(description="Inspect and move Kalshi shard collateral.")
    ap.add_argument("--raw", action="store_true",
                    help="dump every balance view the API offers (use when the UI disagrees)")
    ap.add_argument("--calibrate", action="store_true",
                    help="measure the wire unit of `amount` with one known transfer (demo, --live)")
    ap.add_argument("--env", default="demo", choices=["demo", "prod"])
    ap.add_argument("--live", action="store_true",
                    help="actually move money (default: dry-run)")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--ensure", type=int, metavar="SHARD",
                    help="ensure SHARD holds --usd (default shard 2, crypto)")
    ap.add_argument("--usd", type=float, default=200.0)
    ap.add_argument("--buffer-usd", type=float, default=0.0)
    ap.add_argument("--sweep", type=int, metavar="SHARD",
                    help="move everything off SHARD back to shard 0")
    ap.add_argument("--shard-of", metavar="TICKER",
                    help="print the authoritative shard for a market")
    args = ap.parse_args()

    client = KalshiClient(env=args.env, dry_run=not args.live)
    if args.live:
        print(f"*** LIVE on {args.env} — this moves real collateral ***")

    if args.raw:
        _print_raw(client)
    elif args.calibrate:
        print(json.dumps(calibrate_units(client), indent=2))
    elif args.shard_of:
        idx = market_shard(client, args.shard_of)
        print(f"{args.shard_of} -> shard {idx} ({SHARD_NAMES.get(idx, '?')})")
    elif args.ensure is not None:
        out = ensure_collateral(client, args.ensure, int(round(args.usd * 100)),
                                int(round(args.buffer_usd * 100)))
        print(json.dumps(out, indent=2))
    elif args.sweep is not None:
        print(json.dumps(sweep_home(client, args.sweep), indent=2))
    else:
        _print_status(client)


if __name__ == "__main__":
    main()
