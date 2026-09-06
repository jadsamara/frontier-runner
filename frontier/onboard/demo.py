from __future__ import annotations

from pathlib import Path

JAFFLE_HINT = """\
This looks like the Jaffle fixture.

1. Comment-only change (no semantic difference):
   Add `-- frontier demo` above the customer_summary SELECT and open a PR.

2. Known filter change (semantic difference):
   In models/marts/int_customer_orders.sql (or the model that filters order status),
   change the closed-order filter from a single status to IN ('F', 'O').
   Open a PR. Do not commit or push until you review the diff.
"""

GENERIC_HINT = """\
Do not let Frontier rewrite SQL automatically.

1. Comment-only change (verify no semantic difference):
   Add a SQL comment to one dbt model, commit, and open a pull request.

2. User-selected supported filter change:
   Change a WHERE filter on a staging or intermediate model you own,
   then open a second PR. Frontier will compare compiled SQL and prove impact.

Never commit or push without reviewing the diff yourself.
"""


def demo_change_instructions(project_dir: Path, project_name: str | None = None) -> str:
    name = (project_name or "").strip().lower()
    dbt = project_dir / "dbt_project.yml"
    text = dbt.read_text() if dbt.is_file() else ""
    is_jaffle = name == "jaffle_shop" or "name: jaffle_shop" in text
    if is_jaffle:
        return JAFFLE_HINT.strip()
    return GENERIC_HINT.strip()
