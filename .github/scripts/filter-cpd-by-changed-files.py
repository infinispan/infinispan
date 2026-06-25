#!/usr/bin/env python3
"""Filter Maven PMD-CPD reports to retain only duplications that reference
at least one Java file changed by the current PR.

CPD is inherently cross-file (a duplication, by definition, involves at
least two files), so the Maven goal must scan whole modules. This script
narrows the *output* to PR-scoped signal without giving up that detection.

Usage:
    filter-cpd-by-changed-files.py <changed-files-list>

<changed-files-list> is a text file with one repo-root-relative path per line.
"""
from __future__ import annotations

import sys
from pathlib import Path
from xml.etree import ElementTree as ET


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(
            "Usage: filter-cpd-by-changed-files.py <changed-files-list>",
            file=sys.stderr,
        )
        return 2

    changed = {
        str(Path(line.strip()).resolve())
        for line in Path(argv[1]).read_text().splitlines()
        if line.strip()
    }
    if not changed:
        print("No changed files listed; nothing to filter.")
        return 0

    reports = sorted(Path(".").rglob("target/cpd.xml"))
    if not reports:
        print("No cpd.xml reports found.")
        return 0

    for report in reports:
        try:
            tree = ET.parse(report)
        except ET.ParseError as exc:
            print(f"{report}: parse error ({exc}); leaving file untouched")
            continue
        root = tree.getroot()
        kept = 0
        dropped = 0
        for dup in list(root.findall("duplication")):
            paths = {
                str(Path(f.get("path", "")).resolve())
                for f in dup.findall("file")
            }
            if paths & changed:
                kept += 1
            else:
                root.remove(dup)
                dropped += 1
        if kept == 0:
            report.unlink()
            print(f"{report}: removed (no PR-relevant duplications)")
        else:
            tree.write(report, encoding="UTF-8", xml_declaration=True)
            print(f"{report}: kept {kept}, dropped {dropped}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
