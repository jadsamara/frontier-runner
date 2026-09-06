from __future__ import annotations

import sys
from typing import Callable


def is_interactive() -> bool:
    return sys.stdin.isatty() and sys.stdout.isatty()


def prompt_text(
    label: str,
    *,
    default: str | None = None,
    assume: str | None = None,
    reader: Callable[[str], str] | None = None,
) -> str:
    if assume is not None:
        return assume
    if default is not None and not is_interactive():
        return default
    suffix = f" [{default}]" if default else ""
    raw = (reader or input)(f"{label}{suffix}: ").strip()
    if raw:
        return raw
    if default is not None:
        return default
    return ""


def prompt_yes_no(
    label: str,
    *,
    default: bool = True,
    assume_yes: bool = False,
    reader: Callable[[str], str] | None = None,
) -> bool:
    if assume_yes or not is_interactive():
        return default if not assume_yes else True
    hint = "Y/n" if default else "y/N"
    raw = (reader or input)(f"{label} ({hint}): ").strip().lower()
    if not raw:
        return default
    return raw in {"y", "yes"}


def prompt_choice(
    label: str,
    options: list[str],
    *,
    assume_index: int | None = None,
    reader: Callable[[str], str] | None = None,
) -> str:
    if not options:
        raise ValueError("options required")
    if assume_index is not None:
        return options[assume_index]
    if len(options) == 1 or not is_interactive():
        return options[0]
    print(label)
    for index, option in enumerate(options, start=1):
        print(f"  {index}. {option}")
    raw = (reader or input)("Select [1]: ").strip() or "1"
    try:
        choice = int(raw)
    except ValueError as error:
        raise ValueError("Enter a number") from error
    if choice < 1 or choice > len(options):
        raise ValueError("Selection out of range")
    return options[choice - 1]
