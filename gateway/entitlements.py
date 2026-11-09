"""Who may see what.

This runs on the server because anything sent to a browser can be read there,
even if the page never displays it. Fund rows are built from an allow-list, so
a column added upstream stays private until someone chooses to publish it.
"""

# Toy login: the token decides the scope, so a client can't simply ask for
# another fund. None means the desk, which sees everything.
TOKENS = {
    "desk": None,
    "fund-cp1": "CP-1",
    "fund-cp2": "CP-2",
    "fund-cp3": "CP-3",
    "fund-cp4": "CP-4",
    "fund-cp5": "CP-5",
}

OPPOSITE_SIDE = {"BUY": "SELL", "SELL": "BUY"}


def fund_trade(row: dict) -> dict:
    """The same trade from the fund's side: when the desk buys, the fund sells."""
    return {
        "trade_id": row["trade_id"],
        "version": row["version"],
        "status": row["status"],
        "instrument_id": row["instrument_id"],
        "instrument_name": row["instrument_name"],
        "side": OPPOSITE_SIDE[row["side"]],
        "qty": row["qty"],
        "trade_price": row["trade_price"],
        "live_price": row["live_price"],
        "mtm": 0 - row["mtm"],  # 0 - x, because -x would show a cancelled trade as -0.0
    }


def fund_credit(row: dict) -> dict:
    return {
        "counterparty_id": row["counterparty_id"],
        "counterparty_name": row["counterparty_name"],
        "exposure": row["exposure"],
        "credit_limit": row["credit_limit"],
        "breached": row["breached"],
    }


def view(fund: str | None, topic: str, row: dict) -> dict | None:
    """Return the row as this viewer may see it, or None if they may not see it."""
    if fund is None:
        return row
    if row["counterparty_id"] != fund:
        return None
    return fund_trade(row) if topic == "trade_view" else fund_credit(row)
