"""Show each counterparty's current exposure against its credit limit.

Usage: .venv/bin/python scripts/exposure_check.py [topic]
"""
import sys

from stale_check import read_current


def main() -> None:
    topic = sys.argv[1] if len(sys.argv) > 1 else "counterparty_view"
    for _, r in sorted(read_current(topic).items()):
        limit = r["credit_limit"]
        used = f"{r['exposure'] / limit:.0%}" if limit else "?"
        state = {True: "BREACH", False: "ok", None: "NO LIMIT"}[r["breached"]]
        print(f"{r['counterparty_id']}  {r['counterparty_name']:<22} exposure {r['exposure']:>14,.0f}  limit {limit or 0:>14,.0f}  {used:>7}  {state}")


if __name__ == "__main__":
    main()
