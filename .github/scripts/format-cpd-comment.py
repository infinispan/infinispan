#!/usr/bin/env python3
"""Format the filtered Maven PMD-CPD reports as a single Markdown body that
can be posted as a pull-request comment.

Reads every ``**/target/cpd.xml`` under the current working directory
(``filter-cpd-by-changed-files.py`` runs first and removes reports that
have no PR-relevant duplications, so this script only sees what should
be reported). Emits the rendered Markdown on stdout, including a
leading HTML marker (``<!-- cpd-comment -->``) so the workflow's
sticky-comment logic can find and update the existing comment instead
of appending a new one on every push.

A hard cap (``MAX_BODY_CHARS``) prevents the body from approaching
GitHub's 65 535-character comment limit; codefragments are also
individually capped (``MAX_FRAGMENT_LINES`` / ``MAX_FRAGMENT_CHARS``)
so a single very long duplication cannot dominate the comment.
"""
from __future__ import annotations

import sys
from pathlib import Path
from xml.etree import ElementTree as ET

MARKER = "<!-- cpd-comment -->"
MAX_BODY_CHARS = 60000
MAX_FRAGMENT_LINES = 12
MAX_FRAGMENT_CHARS = 600


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def truncate_fragment(text: str) -> str:
    if not text:
        return ""
    lines = text.splitlines()
    truncated = False
    if len(lines) > MAX_FRAGMENT_LINES:
        lines = lines[:MAX_FRAGMENT_LINES]
        truncated = True
    body = "\n".join(lines)
    if len(body) > MAX_FRAGMENT_CHARS:
        body = body[:MAX_FRAGMENT_CHARS].rstrip()
        truncated = True
    if truncated:
        body += "\n…  (truncated; see uploaded `cpd-reports` artifact for full fragment)"
    return body


def render_duplication(dup: ET.Element, idx: int) -> str:
    tokens = dup.get("tokens", "?")
    lines = dup.get("lines", "?")
    files = [el for el in dup if local_name(el.tag) == "file"]
    fragments = [el for el in dup if local_name(el.tag) == "codefragment"]

    parts: list[str] = []
    parts.append(f"#### Duplication {idx} — {lines} lines / {tokens} tokens")
    if files:
        parts.append("Files:")
        for f in files:
            path = f.get("path", "(unknown)")
            start = f.get("line", "?")
            end = f.get("endline") or f.get("endLine") or start
            parts.append(f"- `{path}` (lines {start}–{end})")
    if fragments and fragments[0].text:
        parts.append("")
        parts.append("```java")
        parts.append(truncate_fragment(fragments[0].text))
        parts.append("```")
    return "\n".join(parts)


def render_report(report: Path) -> tuple[int, str]:
    try:
        tree = ET.parse(report)
    except ET.ParseError as exc:
        return 0, f"_Could not parse `{report}`: {exc}_"
    root = tree.getroot()
    dups = [el for el in list(root) if local_name(el.tag) == "duplication"]
    if not dups:
        return 0, ""

    # The report path looks like ``<module>/target/cpd.xml``; use the
    # nearest non-"target" ancestor as a human-readable module label.
    module_dir = report.parent.parent
    module = module_dir.name if module_dir.name else "(root)"

    body: list[str] = []
    body.append(f"### Module: `{module}` — {len(dups)} duplication(s)")
    for i, dup in enumerate(dups, start=1):
        body.append("")
        body.append(render_duplication(dup, i))
    return len(dups), "\n".join(body)


def main() -> int:
    reports = sorted(Path(".").rglob("target/cpd.xml"))

    header = [
        MARKER,
        "## PMD CPD findings",
        "",
        "_Filtered to duplications that involve at least one file changed by this PR._",
        "",
    ]

    if not reports:
        body = "\n".join(header + ["**No PR-relevant CPD findings.**"])
        sys.stdout.write(body)
        return 0

    sections: list[str] = []
    total = 0
    for report in reports:
        count, section = render_report(report)
        if count:
            sections.append(section)
            total += count

    if not sections:
        body = "\n".join(header + ["**No PR-relevant CPD findings.**"])
        sys.stdout.write(body)
        return 0

    header.append(f"**Total:** {total} duplication(s) across {len(sections)} module(s).")
    header.append("")
    parts = header + sections

    body = "\n\n".join(parts)
    if len(body) > MAX_BODY_CHARS:
        truncated_note = (
            "\n\n---\n_Comment truncated — see uploaded `cpd-reports` artifact for the full report._"
        )
        body = body[: MAX_BODY_CHARS - len(truncated_note)] + truncated_note

    sys.stdout.write(body)
    return 0


if __name__ == "__main__":
    sys.exit(main())
