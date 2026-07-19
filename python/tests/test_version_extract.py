from otel_staleness.version_extract import (
    extract_frontmatter_version, extract_semver, extract_json_version,
    extract_html_meta_version, extract_url_path_version, first_of,
)


def test_frontmatter():
    doc = "---\ntitle: Guide\nversion: 1.28.3\n---\n# Body\n"
    assert extract_frontmatter_version(doc) == "1.28.3"
    assert extract_frontmatter_version(doc, field="title") == "Guide"
    assert extract_frontmatter_version("no frontmatter here") is None


def test_semver():
    assert extract_semver("Applies to Kubernetes 1.31.0 and later") == "1.31.0"
    assert extract_semver("built for v2.4 release", keyword="for") == "v2.4"
    assert extract_semver("no version") is None


def test_json_version():
    assert extract_json_version('{"name":"x","version":"3.2.1"}') == "3.2.1"
    assert extract_json_version('{"pkg":{"version":"9.9"}}', field="pkg.version") == "9.9"
    assert extract_json_version("{}") is None
    assert extract_json_version("not json") is None


def test_html_meta():
    assert extract_html_meta_version('<meta name="version" content="1.5.0">') == "1.5.0"
    assert extract_html_meta_version('<meta content="2.0" name="version"/>') == "2.0"
    assert extract_html_meta_version("<html></html>") is None


def test_url_path():
    assert extract_url_path_version("https://docs.example.com/docs/v1.28/intro") == "v1.28"
    assert extract_url_path_version("https://x/docs/1.31/guide") == "1.31"
    assert extract_url_path_version("https://x/docs/latest/guide") is None


def test_first_of_skips_failures_and_none():
    def boom():
        raise RuntimeError("nope")
    resolver = first_of(
        boom,
        lambda: extract_frontmatter_version("no fm"),        # None
        lambda: extract_semver("see version 4.5.6"),          # hit
    )
    assert resolver() == "4.5.6"
    assert first_of(lambda: None)() is None


def test_extractor_feeds_version_probe():
    from otel_staleness.probes import VersionFreshnessProbe, VersionInfo
    NOW = 1_000_000.0
    info = VersionInfo(current="1.31.0", releases={
        "1.28.0": NOW - 300_000, "1.29.0": NOW - 200_000,
        "1.30.0": NOW - 150_000, "1.31.0": NOW - 100_000})
    page = '<html><head><meta name="version" content="1.28.0"></head></html>'
    probe = VersionFreshnessProbe(
        documented_version=first_of(lambda: extract_html_meta_version(page)),
        fetch_current=lambda: info, source_name="docs", now_fn=lambda: NOW)
    r = probe.read()[0]
    assert r.records_behind == 3
    from otel_staleness import conventions as sc
    assert r.attributes()[sc.ATTR_VERSION_DOCUMENTED] == "1.28.0"
