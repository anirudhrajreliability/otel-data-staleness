"""Extract the *documented version* of RAG / documentation content.

Version-currency (``VersionFreshnessProbe``) needs to know which release the
content describes. Rather than hard-coding it, these helpers pull it from the
content itself — the common places docs record their version:

- YAML/Markdown **frontmatter** (`version:` field)
- a **semver token** in text (optionally after a keyword like "Version")
- a **JSON field** (dotted path, e.g. package.json `version`)
- an **HTML `<meta>`** tag
- a docs-site **URL path** segment (`/docs/v1.28/...`)

All are pure functions returning the version string or ``None`` (never a
fabricated value). ``first_of`` composes several into one resolver for the
probe's ``documented_version`` argument.
"""
from __future__ import annotations

import json
import re
from typing import Callable, Optional

_SEMVER = r"v?\d+(?:\.\d+){1,3}(?:[-.][0-9A-Za-z.]+)?"
_SEMVER_RE = re.compile(_SEMVER)


def extract_frontmatter_version(text: str, field: str = "version") -> Optional[str]:
    m = re.match(r"^﻿?---\s*\n(.*?)\n---\s*(?:\n|$)", text, re.DOTALL)
    if not m:
        return None
    fm = re.search(rf"^\s*{re.escape(field)}\s*:\s*[\"']?([^\"'\n#]+)", m.group(1), re.MULTILINE)
    return fm.group(1).strip() if fm else None


def extract_semver(text: str, keyword: Optional[str] = None) -> Optional[str]:
    if keyword:
        m = re.search(re.escape(keyword) + r"\s*[:=]?\s*(" + _SEMVER + r")", text, re.IGNORECASE)
        return m.group(1) if m else None
    m = _SEMVER_RE.search(text)
    return m.group(0) if m else None


def extract_json_version(text: str, field: str = "version") -> Optional[str]:
    try:
        doc = json.loads(text)
    except ValueError:
        return None
    for part in field.split("."):
        if isinstance(doc, dict) and part in doc:
            doc = doc[part]
        else:
            return None
    return str(doc) if isinstance(doc, (str, int, float)) else None


def extract_html_meta_version(html: str, name: str = "version") -> Optional[str]:
    m = re.search(r"<meta[^>]+name=[\"']" + re.escape(name) + r"[\"'][^>]+content=[\"']([^\"']+)", html, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    m2 = re.search(r"<meta[^>]+content=[\"']([^\"']+)[\"'][^>]+name=[\"']" + re.escape(name) + r"[\"']", html, re.IGNORECASE)
    return m2.group(1).strip() if m2 else None


def extract_url_path_version(url: str) -> Optional[str]:
    m = re.search(r"/(v?\d+(?:\.\d+){0,2})(?:/|$)", url)
    return m.group(1) if m else None


def first_of(*resolvers: Callable[[], Optional[str]]) -> Callable[[], Optional[str]]:
    """Compose zero-arg resolvers; return the first that yields a value.

    Failing resolvers are skipped so a missing file or malformed page does not
    break the others. Returns ``None`` if none match (the probe then raises a
    visible error rather than fabricating a version).
    """

    def run() -> Optional[str]:
        for r in resolvers:
            try:
                v = r()
            except Exception:
                v = None
            if v:
                return v
        return None

    return run
