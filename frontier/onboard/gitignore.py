from __future__ import annotations

from pathlib import Path

from frontier.onboard.constants import GITIGNORE_ENTRIES


def ensure_gitignore(project_dir: Path, entries: tuple[str, ...] = GITIGNORE_ENTRIES) -> Path:
    path = project_dir / ".gitignore"
    existing = path.read_text() if path.is_file() else ""
    lines = existing.splitlines()
    missing = [entry for entry in entries if entry not in lines]
    if not missing:
        return path
    prefix = "" if not existing or existing.endswith("\n") else "\n"
    addition = "\n".join(missing) + "\n"
    if path.is_file():
        path.write_text(existing + prefix + addition)
    else:
        path.write_text(addition)
    return path
