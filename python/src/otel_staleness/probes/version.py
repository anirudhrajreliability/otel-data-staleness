"""Version-currency staleness for RAG / documentation content.

Time-freshness ("when was this crawled?") misses the freshness question that
matters for version-specific docs: *does the content describe the current
release?* This probe compares the version your corpus documents against the
current release from a package registry, and emits:

    data.staleness.records.behind   # number of releases behind
    data.staleness.age              # now - release time of the oldest release
                                    # newer than the documented one (how long
                                    # newer info has existed unreflected); 0 if current

plus attributes data.staleness.version.documented / .version.current and
method=version_drift.

The comparison logic is pure and unit-tested; registry access is provided by
thin helper constructors (from_pypi / from_npm / from_github_releases /
from_dockerhub) that fetch over HTTP.
"""
from __future__ import annotations

import json
import time
import urllib.request
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

from ..core import FreshnessReading, StalenessProbe
from .. import conventions as sc
from .._timeutil import parse_iso_epoch as _iso

try:  # robust PEP440/semver parsing when available
    from packaging.version import Version, InvalidVersion

    def _parse(v: str):
        try:
            return Version(str(v).strip().lstrip("vV"))
        except InvalidVersion:
            return None

    def _is_prerelease(pv) -> bool:
        return bool(pv.is_prerelease)

except Exception:  # pragma: no cover - fallback if packaging is absent
    import re

    class _V(tuple):
        pass

    def _parse(v: str):
        s = str(v).strip().lstrip("vV")
        parts = re.findall(r"\d+", s)
        if not parts:
            return None
        pv = _V(int(p) for p in parts[:4])
        pv.pre = bool(re.search(r"[A-Za-z]", s))  # crude prerelease flag
        return pv

    def _is_prerelease(pv) -> bool:
        return getattr(pv, "pre", False)


@dataclass
class VersionInfo:
    """Registry snapshot: the current version and known release timestamps."""

    current: str
    releases: Dict[str, float] = field(default_factory=dict)  # version -> Unix seconds


def compute_version_staleness(
    documented: str,
    info: VersionInfo,
    now: float,
    include_prereleases: bool = False,
) -> Tuple[int, Optional[float], str]:
    """Return (versions_behind, age_seconds_or_None, current_version).

    Raises ValueError if the documented version cannot be parsed — a visible
    failure rather than a fabricated "0 behind".
    """
    doc_pv = _parse(documented)
    if doc_pv is None:
        raise ValueError(f"undocumented/unparseable version: {documented!r}")

    # Candidate versions: the release list (filtered) PLUS the reported current,
    # which may not appear in `releases` — without this, a `current` newer than
    # `documented` but absent from the release map would be missed and the probe
    # would wrongly report "up to date".
    candidates: Dict[str, Tuple[object, Optional[float]]] = {}
    for ver, ts in info.releases.items():
        pv = _parse(ver)
        if pv is None:
            continue
        if not include_prereleases and _is_prerelease(pv):
            continue
        candidates[ver] = (pv, ts)

    cur_pv = _parse(info.current)
    if cur_pv is not None and (include_prereleases or not _is_prerelease(cur_pv)):
        if info.current not in candidates:
            candidates[info.current] = (cur_pv, info.releases.get(info.current))

    newer = [(pv, ver, ts) for ver, (pv, ts) in candidates.items() if pv > doc_pv]
    versions_behind = len(newer)

    age: Optional[float] = 0.0
    if newer:
        # oldest version strictly newer than documented (how long newer info has existed)
        oldest = min(newer, key=lambda t: t[0])
        ts = oldest[2]
        age = max(0.0, now - ts) if ts else None

    return versions_behind, age, info.current


class VersionFreshnessProbe(StalenessProbe):
    def __init__(
        self,
        documented_version,
        fetch_current: Callable[[], VersionInfo],
        *,
        source_name: str,
        system: str = sc.System.DOCS,
        namespace: Optional[str] = None,
        include_prereleases: bool = False,
        sla_threshold_seconds: Optional[float] = None,
        now_fn: Callable[[], float] = time.time,
    ):
        self._documented = documented_version
        self._fetch = fetch_current
        self._name = source_name
        self._system = system
        self._namespace = namespace
        self._include_pre = include_prereleases
        self._sla = sla_threshold_seconds
        self._now = now_fn

    def _resolve_documented(self) -> str:
        d = self._documented
        return d() if callable(d) else d

    def read(self) -> List[FreshnessReading]:
        documented = self._resolve_documented()
        info = self._fetch()  # may raise on network failure -> isolated by monitor
        behind, age, current = compute_version_staleness(
            documented, info, self._now(), self._include_pre
        )
        return [
            FreshnessReading(
                source_system=self._system,
                source_name=self._name,
                namespace=self._namespace,
                method=sc.Method.VERSION_DRIFT,
                pipeline_stage="serve",
                age_seconds=age,
                records_behind=behind,
                sla_threshold_seconds=self._sla,
                extra_attributes={
                    sc.ATTR_VERSION_DOCUMENTED: str(documented),
                    sc.ATTR_VERSION_CURRENT: str(current),
                },
            )
        ]

    # --- registry helpers (network) ---------------------------------------
    @classmethod
    def from_pypi(cls, package: str, documented_version, **kw) -> "VersionFreshnessProbe":
        def fetch() -> VersionInfo:
            doc = _get_json(f"https://pypi.org/pypi/{package}/json")
            current = doc["info"]["version"]
            releases: Dict[str, float] = {}
            for ver, files in doc.get("releases", {}).items():
                for f in files:
                    ts = _iso(f.get("upload_time_iso_8601") or f.get("upload_time"))
                    if ts is not None:
                        releases[ver] = ts
                        break
            return VersionInfo(current=current, releases=releases)

        return cls(documented_version, fetch, source_name=package, **kw)

    @classmethod
    def from_npm(cls, package: str, documented_version, **kw) -> "VersionFreshnessProbe":
        def fetch() -> VersionInfo:
            doc = _get_json(f"https://registry.npmjs.org/{package}")
            current = doc.get("dist-tags", {}).get("latest", "")
            times = doc.get("time", {})
            releases = {v: _iso(t) for v, t in times.items()
                        if v not in ("created", "modified") and _iso(t) is not None}
            return VersionInfo(current=current, releases=releases)

        return cls(documented_version, fetch, source_name=package, **kw)

    @classmethod
    def from_github_releases(cls, repo: str, documented_version, **kw) -> "VersionFreshnessProbe":
        # repo = "owner/name"
        def fetch() -> VersionInfo:
            rels = _get_json(f"https://api.github.com/repos/{repo}/releases?per_page=100")
            releases: Dict[str, float] = {}
            current = ""
            for r in rels:
                if r.get("draft"):
                    continue
                tag = r.get("tag_name", "")
                ts = _iso(r.get("published_at"))
                if tag and ts is not None:
                    releases[tag] = ts
                    if not r.get("prerelease") and not current:
                        current = tag
            if not current and rels:
                current = rels[0].get("tag_name", "")
            return VersionInfo(current=current, releases=releases)

        return cls(documented_version, fetch, source_name=repo, **kw)

    @classmethod
    def from_dockerhub(cls, repo: str, documented_version, **kw) -> "VersionFreshnessProbe":
        # repo = "library/redis" or "org/image"
        def fetch() -> VersionInfo:
            doc = _get_json(f"https://hub.docker.com/v2/repositories/{repo}/tags?page_size=100")
            releases: Dict[str, float] = {}
            for t in doc.get("results", []):
                name = t.get("name", "")
                ts = _iso(t.get("last_updated"))
                if name and ts is not None and name not in ("latest", "stable"):
                    releases[name] = ts
            current = ""
            if releases:
                # highest parseable tag is treated as current
                best = max((p for p in ((_parse(v), v) for v in releases) if p[0] is not None),
                           key=lambda t: t[0], default=(None, ""))
                current = best[1]
            return VersionInfo(current=current, releases=releases)

        return cls(documented_version, fetch, source_name=repo, **kw)


def _get_json(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": "otel-staleness"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))



