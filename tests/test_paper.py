"""Paper broker: simulated fills, settlement, and equity — all offline.

    python3 -m tests.test_paper
"""
from paper.broker import PaperBroker


def _book(bid=None, ask=None, bsz=None, asz=None):
    return {"yes_bid": bid, "yes_ask": ask, "yes_bid_size": bsz, "yes_ask_size": asz}


def test_yes_bid_fills_when_ask_crosses_and_settles():
    b = PaperBroker(20000)                       # $200
    b.place("T", "yes", 40, 10)                  # rest buy YES @40c x10
    assert b.locked() == 400 and b.available() == 19600
    b.try_fills({"T": _book(bid=38, ask=41, bsz=500, asz=500)})   # ask 41 > 40: no cross
    assert not b.fills and b.cash == 20000
    b.try_fills({"T": _book(bid=39, ask=40, bsz=500, asz=500)})   # ask crosses to 40: fill
    assert b.cash == 19600 and b.pos["T"]["yes"] == [10, 400] and not b.resting
    assert b.settle("T", "yes")["pnl"] == 600    # 10*100 - 400
    assert b.cash == 20600 and b.realized == 600
    print("yes bid: fills when ask crosses; YES settle pays 100 -> +$6.00  OK")


def test_no_buy_fills_when_bid_crosses_and_loses_when_yes_wins():
    b = PaperBroker(20000)
    b.place("T", "no", 30, 5)                    # buy NO @30 (== sell YES @70); fills when yes_bid >= 70
    b.try_fills({"T": _book(bid=69, ask=75, bsz=500, asz=500)})   # 69 < 70: no cross
    assert not b.fills
    b.try_fills({"T": _book(bid=72, ask=78, bsz=500, asz=500)})   # 72 >= 70: fill
    assert b.cash == 19850 and b.pos["T"]["no"] == [5, 150]
    assert b.settle("T", "yes")["pnl"] == -150   # YES won -> our NO pays 0
    print("no buy: fills when bid crosses; NO loses when YES wins -> -$1.50  OK")


def test_fill_is_capped_to_touch_size_and_partial_rests():
    b = PaperBroker(20000)
    b.place("T", "yes", 50, 10)
    b.try_fills({"T": _book(bid=49, ask=50, asz=3)})   # only 3 offered at the touch
    assert b.pos["T"]["yes"] == [3, 150]
    assert list(b.resting.values())[0]["count"] == 7   # the other 7 stay resting
    print("fill: capped to the size at the touch (3 of 10), remainder rests  OK")


def test_no_cross_and_missing_book_are_safe():
    b = PaperBroker(20000)
    b.place("T", "yes", 30, 5)
    b.try_fills({"T": _book(bid=20, ask=45)})    # ask 45 > 30: no cross
    b.try_fills({})                               # no book for T at all
    assert not b.fills and b.available() == 20000 - 150
    print("no cross / missing book: nothing fills, no crash  OK")


def test_equity_marks_open_position_to_mid_else_cost():
    b = PaperBroker(20000)
    b.place("T", "yes", 40, 10)
    b.try_fills({"T": _book(bid=39, ask=40, asz=500)})       # fill 10 @40 -> cash 19600
    assert b.equity({"T": _book(bid=60, ask=64)}) == 19600 + 620   # mid 62 * 10 = 620
    assert b.equity({}) == 19600 + 400                       # no live mid -> mark at cost
    print("equity: open position marked to the market mid, cost as fallback  OK")


def test_cancel_frees_locked_and_summary_reports_win_rate():
    b = PaperBroker(20000)
    b.place("T", "yes", 40, 10)
    b.place("U", "no", 25, 4)
    assert b.locked() == 400 + 100
    b.cancel_ticker("T")
    assert b.locked() == 100
    b.cancel_all()
    assert b.locked() == 0 and b.available() == 20000
    b.place("A", "yes", 40, 1); b.try_fills({"A": _book(bid=39, ask=40, asz=9)}); b.settle("A", "yes")
    b.place("B", "yes", 40, 1); b.try_fills({"B": _book(bid=39, ask=40, asz=9)}); b.settle("B", "no")
    s = b.summary({})
    assert s["settled"] == 2 and s["win_rate"] == 0.5 and s["fills"] == 2
    print("cancel frees locked collateral; summary win-rate over settlements  OK")


if __name__ == "__main__":
    test_yes_bid_fills_when_ask_crosses_and_settles()
    test_no_buy_fills_when_bid_crosses_and_loses_when_yes_wins()
    test_fill_is_capped_to_touch_size_and_partial_rests()
    test_no_cross_and_missing_book_are_safe()
    test_equity_marks_open_position_to_mid_else_cost()
    test_cancel_frees_locked_and_summary_reports_win_rate()
    print("\nall paper-broker tests passed")
