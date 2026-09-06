from __future__ import annotations

import re

from frontier import __version__


def parse_version(value: str) -> tuple[int, int, int]:
    match = re.match(r"^v?(\d+)\.(\d+)\.(\d+)", value.strip())
    if not match:
        return (0, 0, 0)
    return (int(match.group(1)), int(match.group(2)), int(match.group(3)))


def version_at_least(current: str, minimum: str) -> bool:
    return parse_version(current) >= parse_version(minimum)


def current_runner_version() -> str:
    return __version__
