"""Unit tests for portfolio.execution_rules — T+1 sell restriction."""

from __future__ import annotations

from datetime import date

from portfolio.execution_rules import can_sell_t1


class TestCanSellT1:
    def test_buy_before_trade_day(self) -> None:
        """Bought on Monday, trying to sell on Tuesday → allowed."""
        assert can_sell_t1("2025-08-11", date(2025, 8, 12)) is True

    def test_buy_same_day_blocked(self) -> None:
        """Bought today → cannot sell today (T+1)."""
        assert can_sell_t1("2025-08-11", date(2025, 8, 11)) is False

    def test_no_buy_date_allows_sell(self) -> None:
        """No buy date recorded → assume allowed."""
        assert can_sell_t1(None, date(2025, 8, 12)) is True

    def test_buy_friday_sell_monday(self) -> None:
        """Bought Friday, sell Monday → allowed (T+1 is calendar‑based)."""
        assert can_sell_t1("2025-08-08", date(2025, 8, 11)) is True
