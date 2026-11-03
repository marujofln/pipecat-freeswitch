#
# Copyright (c) 2026, Marcelo Araujo
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Print the GitHub release notes of a version, taken from its CHANGELOG.md section.

GitHub renders every newline in release notes as a line break, so the hard-wrapped lines of the
CHANGELOG are joined back into one line per paragraph or list item. Exits with an error if the
version has no section, so a release cannot go out without notes.

    python .github/scripts/release_notes.py 0.2.0 > notes.md
"""

import re
import sys
from pathlib import Path

CHANGELOG = Path(__file__).resolve().parents[2] / "CHANGELOG.md"


def section(changelog: str, version: str) -> str:
    """Body of the ``## [version]`` section, without its heading and link references."""
    match = re.search(
        rf"^## \[{re.escape(version)}\][^\n]*\n(.*?)(?=^## \[|^\[[^\]]+\]: |\Z)",
        changelog,
        flags=re.M | re.S,
    )
    if not match or not match.group(1).strip():
        raise SystemExit(f"CHANGELOG.md has no notes for version {version}")
    return match.group(1).strip()


def unwrap(text: str) -> str:
    """Join hard-wrapped lines: one line per paragraph, list item or heading."""
    lines: list[str] = []
    for line in text.splitlines():
        previous = lines[-1] if lines else ""
        continues = (
            previous.strip()
            and not previous.startswith("#")
            and line.strip()
            and not line.lstrip().startswith(("- ", "* ", "#"))
        )
        if continues:
            lines[-1] = f"{previous} {line.strip()}"
        else:
            lines.append(line)
    return "\n".join(lines)


def main():
    """Print the notes of the version given as the only argument."""
    if len(sys.argv) != 2:
        raise SystemExit("usage: release_notes.py <version>")
    version = sys.argv[1].removeprefix("v")
    notes = unwrap(section(CHANGELOG.read_text(), version))
    print(notes)
    print()
    print(
        f"**PyPI:** https://pypi.org/project/pipecat-freeswitch/{version}/"
        f" — `pip install pipecat-freeswitch=={version}`"
    )


if __name__ == "__main__":
    main()
