"""Risk gates: jump kill switch, staleness, caps, daily loss limit.

Everything here answers one question: "are we allowed to have quotes up
right now, and how big?" The quoter never overrides a risk answer.
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field


@dataclass
class RiskState:
    # per-underlying: a BTC jump must not halt ETH quoting, and vice-versa
    spot_window: dict = field(default_factory=dict)     # key -> deque[(ts, spot)]
    cooldown_until: dict = field(default_factory=dict)  # key -> ts
    halted_reason: dict = field(default_factory=dict)   # key -> str
    last_spot: dict = field(default_factory=dict)        # key -> last spot value seen
    last_spot_change_ts: dict = field(default_factory=dict)  # key -> ts spot last CHANGED
    realized_pnl_today: float = 0.0                     # global: one account
    day_stamp: str = ""


class RiskManager:
    def __init__(self, cfg: dict, now_fn=time.time):
        # One clock for the whole bot. Using time.time() here while the loop
        # uses another source meant the cooldown could never be reasoned
        # about (or tested) against the loop's own timeline.
        self.now = now_fn
        r = cfg["risk"]
        self.jump_bps = r["jump_bps"]
        self.jump_window_s = r["jump_window_s"]
        self.jump_cooldown_s = r["jump_cooldown_s"]
        self.daily_loss_limit = r["daily_loss_limit_usd"]
        self.stale_surface_max_s = r["stale_surface_max_s"]
        # A feed can return HTTP 200 with FROZEN data: the fetch succeeds (so the
        # per-market staleness clock, which uses fetch time, never trips) and the
        # spot doesn't move (so the jump switch never fires). Halt a coin whose
        # spot has not changed at all for this long. 0 disables (sniper default).
        self.feed_freeze_s = r.get("feed_freeze_s", 0)
        s = cfg["sizing"]
        self.max_contracts_per_market = s["max_contracts_per_market"]
        self.max_total_exposure = s["max_total_exposure_usd"]
        # Live collateral available on the trading shard, in cents. None until
        # the first balance poll; once set it is a hard ceiling on size — you
        # cannot post an order you cannot collateralize.
        self.available_cents: int | None = None
        self._day_baseline_cents = 0
        self.state = RiskState()

    def set_available(self, cents: int) -> None:
        self.available_cents = max(int(cents), 0)

    # ---- feeds ----
    def on_spot(self, spot: float, key: str = "BTC") -> None:
        now = self.now()
        st = self.state
        prev = st.last_spot.get(key)
        if prev is None or spot != prev:      # feed is alive only when the value moves
            st.last_spot[key] = spot
            st.last_spot_change_ts[key] = now
        win = st.spot_window.setdefault(key, deque(maxlen=600))
        win.append((now, spot))
        old = [p for t, p in win if now - t <= self.jump_window_s]
        if len(old) >= 2:
            move_bps = abs(spot / old[0] - 1.0) * 1e4
            if move_bps > self.jump_bps:
                st.cooldown_until[key] = now + self.jump_cooldown_s
                st.halted_reason[key] = f"{key} spot jump {move_bps:.0f}bps > {self.jump_bps}bps"

    def on_fill_pnl(self, realized_usd: float) -> None:
        st = self.state
        day = time.strftime("%Y-%m-%d", time.gmtime(self.now()))
        if st.day_stamp != day:
            st.day_stamp, st.realized_pnl_today = day, 0.0
        st.realized_pnl_today += realized_usd

    def set_realized_total(self, realized_cents_total: int) -> None:
        """Feed the exchange's own running realized P&L (sum over positions).

        The exchange reports a cumulative figure, so today's P&L is the change
        since the first reading of the day. This is what makes the daily loss
        limit real — before it was wired up, nothing ever called on_fill_pnl.
        """
        st = self.state
        day = time.strftime("%Y-%m-%d", time.gmtime(self.now()))
        if st.day_stamp != day:
            st.day_stamp = day
            st.realized_pnl_today = 0.0
            self._day_baseline_cents = realized_cents_total
        st.realized_pnl_today = (realized_cents_total - self._day_baseline_cents) / 100.0

    # ---- gates ----
    def quoting_allowed(self, surface_age_s: float = 0.0,
                        key: str | None = None) -> tuple[bool, str]:
        """key=None checks only the account-wide gates (daily loss, data age);
        a key adds that underlying's jump cooldown."""
        now = self.now()
        st = self.state
        if key is not None and now < st.cooldown_until.get(key, 0.0):
            return False, f"cooldown ({st.halted_reason.get(key, '')})"
        if key is not None and self.feed_freeze_s > 0:
            last_chg = st.last_spot_change_ts.get(key)
            if last_chg is not None and now - last_chg > self.feed_freeze_s:
                return False, f"feed frozen ({key} spot unchanged {now - last_chg:.0f}s)"
        if surface_age_s > self.stale_surface_max_s:
            return False, f"stale surface ({surface_age_s:.0f}s)"
        if st.realized_pnl_today <= -self.daily_loss_limit:
            return False, f"daily loss limit hit ({st.realized_pnl_today:+.2f})"
        return True, ""

    def cap_size(self, desired: int, current_position: int, exposure_usd: float,
                 price_cents: int, direction: int = +1,
                 available_cents: int | None = None,
                 exposure_cap_usd: float | None = None) -> int:
        """Largest order size every limit allows.

        direction: +1 buys YES (position grows), -1 sells YES (position
        shrinks). The per-market cap is on the position AFTER the trade —
        so a maxed-out long can always post the ask that unwinds it. Using
        abs(position) for both sides (the old code) froze exactly the order
        that reduces risk.

        available_cents overrides the polled balance for this call, so the
        caller can hand each order the collateral left after the ones it has
        already decided to post.
        """
        if direction >= 0:
            room_market = self.max_contracts_per_market - current_position
        else:
            room_market = self.max_contracts_per_market + current_position
        room_market = max(room_market, 0)
        cap_usd = exposure_cap_usd if exposure_cap_usd is not None else self.max_total_exposure
        room_usd = max(cap_usd - exposure_usd, 0.0)
        room_by_usd = int(room_usd / max(price_cents / 100.0, 0.01))
        caps = [desired, room_market, room_by_usd]
        avail = available_cents if available_cents is not None else self.available_cents
        if avail is not None:
            caps.append(int(avail / max(price_cents, 1)))  # each contract locks its price
        return max(min(caps), 0)
