package datastalenessreceiver

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"
)

// version-drift sources have no timestamp -> must NOT emit last_update.timestamp (bug: emitted 0 = epoch 1970)
func TestVersionDriftEmitsNoLastUpdate(t *testing.T) {
	defer withFakeSR(5, nil)()
	r := testReceiver(&Config{
		CollectionInterval: time.Second,
		Sources:            []SourceConfig{srcfg("2")},
	})
	md := r.collect(context.Background())
	if _, ok := metricByName(md, metricLastUpdate); ok {
		t.Fatal("version drift must not emit data.staleness.last_update.timestamp")
	}
	// but records.behind must be present
	if _, ok := metricByName(md, metricRecordsBehind); !ok {
		t.Fatal("expected records.behind for version drift")
	}
}

func TestPerTypeValidation(t *testing.T) {
	cases := []struct {
		name string
		src  SourceConfig
	}{
		{"file needs path", SourceConfig{Type: "file", Name: "x"}},
		{"http needs url", SourceConfig{Type: "http", Name: "x"}},
		{"static needs value", SourceConfig{Type: "static", Name: "x"}},
	}
	for _, c := range cases {
		cfg := &Config{CollectionInterval: time.Second, Sources: []SourceConfig{c.src}}
		if err := cfg.Validate(); err == nil {
			t.Fatalf("%s: expected validation error", c.name)
		}
	}
	// valid ones pass
	ok := &Config{CollectionInterval: time.Second, Sources: []SourceConfig{
		{Type: "file", Name: "x", Path: "/tmp/x"},
		{Type: "http", Name: "y", URL: "http://x"},
		{Type: "static", Name: "z", AgeSeconds: 5},
	}}
	if err := ok.Validate(); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestStaticNoValueIsVisibleError(t *testing.T) {
	r := scrapeStatic(SourceConfig{Type: "static", Name: "x"}, time.Unix(int64(fixedNow), 0))
	if r.ok || r.errType != "no_value" {
		t.Fatalf("expected no_value error, got ok=%v err=%q", r.ok, r.errType)
	}
}

func TestStaticExplicitAgePreferredOverLastUpdate(t *testing.T) {
	// When both are set, explicit age_seconds wins over a derived age — matches
	// the Python SDK and the explicit_age_preferred conformance vector.
	cfg := SourceConfig{Type: "static", Name: "x",
		LastUpdateEpoch: float64(fixedNow) - 999, AgeSeconds: 12.5}
	r := scrapeStatic(cfg, time.Unix(int64(fixedNow), 0))
	if !r.ok || !r.hasAge || r.ageSeconds != 12.5 {
		t.Fatalf("expected explicit age 12.5, got ok=%v hasAge=%v age=%v", r.ok, r.hasAge, r.ageSeconds)
	}
}

func TestParseTimeStringRejectsDateEncodedInt(t *testing.T) {
	// 20240101 must NOT be read as epoch seconds (would be Aug 1970)
	if _, ok := toEpoch("20240101", true); ok {
		t.Fatal("date-encoded int 20240101 should not parse as an epoch")
	}
	// a real epoch-seconds string still works
	if v, ok := toEpoch("1700000000", true); !ok || v != 1700000000 {
		t.Fatalf("real epoch should parse, got %v %v", v, ok)
	}
	// epoch milliseconds are converted to seconds
	if v, ok := toEpoch("1700000000000", true); !ok || v != 1700000000 {
		t.Fatalf("epoch ms should convert to seconds, got %v %v", v, ok)
	}
}

func TestDBMigrationUnparseableVersion(t *testing.T) {
	resetDBCache()
	fakeRegistry["mv"] = fakeResult{cols: []string{"v"}, vals: []driver.Value{"not-a-version"}}
	cfg := SourceConfig{Type: "db_migration", Name: "db", Driver: "fakesql", DSN: "mv",
		VersionQuery: "SELECT version FROM x", CurrentVersion: "20240115"}
	rs := scrapeDBMigration(context.Background(), cfg, time.Unix(int64(fixedNow), 0))
	if rs[0].ok || rs[0].errType != "unparseable_version" {
		t.Fatalf("expected unparseable_version, got ok=%v err=%q", rs[0].ok, rs[0].errType)
	}
}
