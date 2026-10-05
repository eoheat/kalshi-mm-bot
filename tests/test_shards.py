"""Offline tests for the shard collateral logic — no network, no real money.

A fake client records what would have been sent, so we can assert the two
things most likely to cost real dollars: the cents -> centicents conversion,
and the refusal to move money in situations that should be refused.

    python3 -m tests.test_shards
"""
import json
import os
import shutil
import tempfile

from src import shards
from src.shards import ShardError, ensure_collateral, sweep_home, transfer


class FakeClient:
    """Stands in for KalshiClient. Balances in cents, keyed by shard."""

    def __init__(self, balances, dry_run=False, fail_transfer=False):
        self.balances = dict(balances)
        self.dry_run = dry_run
        self.fail_transfer = fail_transfer
        self.sent = []

    def _req(self, method, path, *, auth=True, **kw):
        if path == "/portfolio/balance":
            idx = (kw.get("params") or {}).get("exchange_index")
            if idx is not None:
                return {"balance": self.balances.get(int(idx), 0)}
            # Real API shape: top-level `balance` is CENTS, but each
            # breakdown row's `balance` is a DOLLAR string ("18.0000").
            return {
                "balance": sum(self.balances.values()),
                "balance_dollars": f"{sum(self.balances.values())/100:.4f}",
                "balance_breakdown": [
                    {"exchange_index": k, "balance": f"{v/100:.4f}"}
                    for k, v in self.balances.items()
                ],
            }
        if path == "/portfolio/intra_exchange_instance_transfer":
            body = kw["json"]
            self.sent.append(body)
            if self.fail_transfer:
                raise RuntimeError("503 upstream")
            cents = body["amount"] // shards.CENTICENTS_PER_CENT
            src, dst = body["source_exchange_shard"], body["destination_exchange_shard"]
            self.balances[src] = self.balances.get(src, 0) - cents
            self.balances[dst] = self.balances.get(dst, 0) + cents
            return {"transfer_id": "tid_test"}
        if path.startswith("/markets/"):
            return {"market": {"ticker": path.split("/")[-1], "exchange_index": 2}}
        raise AssertionError("unexpected path " + path)


def test_units():
    c = FakeClient({0: 50_000, 2: 0})
    transfer(c, 20_000, 0, 2)          # $200 in cents
    body = c.sent[0]
    assert body["amount"] == 2_000_000, body           # centicents
    assert body["source"] == "event_contract"
    assert body["destination"] == "event_contract"
    assert body["source_exchange_shard"] == 0
    assert body["destination_exchange_shard"] == 2
    assert c.balances[2] == 20_000 and c.balances[0] == 30_000
    print("units: cents -> centicents (x100), balances move  OK")


def test_ensure_moves_only_the_shortfall():
    c = FakeClient({0: 100_000, 2: 5_000})
    out = ensure_collateral(c, 2, need_cents=20_000, buffer_cents=0, settle_wait_s=0)
    assert out["moved_cents"] == 15_000, out
    assert out["status"] == "funded"
    assert c.balances[2] == 20_000
    print("ensure: moves exactly the shortfall  OK")


def test_ensure_noop_when_funded():
    c = FakeClient({0: 100_000, 2: 30_000})
    out = ensure_collateral(c, 2, need_cents=20_000, settle_wait_s=0)
    assert out["status"] == "already_funded" and out["moved_cents"] == 0
    assert c.sent == []
    print("ensure: no transfer when already funded  OK")


def test_caps_and_refusals():
    # never moves more than the source holds
    c = FakeClient({0: 3_000, 2: 0})
    out = ensure_collateral(c, 2, need_cents=20_000, settle_wait_s=0)
    assert out["moved_cents"] == 3_000 and out["status"] == "partially_funded"

    # per-call cap respected
    c2 = FakeClient({0: 500_000, 2: 0})
    out2 = ensure_collateral(c2, 2, need_cents=400_000,
                             max_transfer_cents=100_000, settle_wait_s=0)
    assert out2["moved_cents"] == 100_000, out2

    # empty source is an error, not a silent no-op
    c3 = FakeClient({0: 0, 2: 0})
    try:
        ensure_collateral(c3, 2, need_cents=10_000, settle_wait_s=0)
        raise AssertionError("should have raised")
    except ShardError as e:
        assert "Deposit first" in str(e)

    # non-positive and same-shard transfers refused
    c4 = FakeClient({0: 10_000})
    for bad in (lambda: transfer(c4, 0, 0, 2), lambda: transfer(c4, -5, 0, 2),
                lambda: transfer(c4, 100, 2, 2)):
        try:
            bad(); raise AssertionError("should have raised")
        except ShardError:
            pass
    assert c4.sent == []
    print("caps: source limit, per-call cap, empty source, bad amounts  OK")


def test_failure_is_not_retried():
    c = FakeClient({0: 100_000, 2: 0}, fail_transfer=True)
    try:
        ensure_collateral(c, 2, need_cents=20_000, settle_wait_s=0)
        raise AssertionError("should have raised")
    except ShardError as e:
        assert "NOT retrying" in str(e)
    assert len(c.sent) == 1, "a failed transfer must be attempted exactly once"
    print("failure: one attempt, no auto-retry  OK")


def test_dry_run_moves_nothing():
    c = FakeClient({0: 100_000, 2: 0}, dry_run=True)
    out = ensure_collateral(c, 2, need_cents=20_000, settle_wait_s=0)
    assert out["status"] == "dry_run"
    assert c.sent == [] and c.balances[2] == 0
    print("dry run: nothing sent, nothing moved  OK")


def test_sweep():
    c = FakeClient({0: 1_000, 2: 47_500})
    out = sweep_home(c, 2, keep_cents=0)
    assert out["moved_cents"] == 47_500 and c.balances[0] == 48_500
    assert sweep_home(c, 2)["status"] == "nothing_to_sweep"
    print("sweep: empties the shard, no-ops when empty  OK")


def test_transfer_log():
    tmp = tempfile.mkdtemp()
    orig = shards.TRANSFER_LOG
    shards.TRANSFER_LOG = os.path.join(tmp, "data", "transfers.jsonl")
    try:
        c = FakeClient({0: 50_000, 2: 0})
        transfer(c, 10_000, 0, 2)
        rows = [json.loads(l) for l in open(shards.TRANSFER_LOG)]
        assert rows[0]["action"] == "transfer" and rows[0]["amount_cents"] == 10_000
        assert rows[-1]["action"] == "transfer_ok"
        print("log: attempt written before send, result after  OK")
    finally:
        shards.TRANSFER_LOG = orig
        shutil.rmtree(tmp)


if __name__ == "__main__":
    test_units()
    test_ensure_moves_only_the_shortfall()
    test_ensure_noop_when_funded()
    test_caps_and_refusals()
    test_failure_is_not_retried()
    test_dry_run_moves_nothing()
    test_sweep()
    test_transfer_log()
    print("\nall shard tests passed")
