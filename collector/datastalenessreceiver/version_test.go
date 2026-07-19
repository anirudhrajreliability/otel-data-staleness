package datastalenessreceiver

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"
	"time"
)

func TestVersionCompare(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"3", "5", -1}, {"5", "5", 0}, {"6", "5", 1},
		{"1.28.0", "1.31.0", -1}, {"1.31.0", "1.28.0", 1}, {"1.31.0", "1.31.0", 0},
		{"20240101", "20240115", -1}, {"v2", "v10", -1},
	}
	for _, c := range cases {
		if got := versionCompare(c.a, c.b); got != c.want {
			t.Fatalf("versionCompare(%q,%q)=%d want %d", c.a, c.b, got, c.want)
		}
	}
}

// --- Schema Registry ---
type fakeSR struct {
	latest int
	err    error
}

func (f *fakeSR) latestVersion(context.Context, string) (int, error) { return f.latest, f.err }
func (f *fakeSR) close()                                             {}

func withFakeSR(latest int, err error) func() {
	prev := openSchemaRegistry
	openSchemaRegistry = func(SourceConfig) (schemaRegistryClient, error) { return &fakeSR{latest, err}, nil }
	return func() { openSchemaRegistry = prev }
}

func srcfg(documented string) SourceConfig {
	return SourceConfig{Type: "schema_registry", Name: "orders-value", System: "kafka",
		RegistryURL: "http://sr:8081", Subject: "orders-value", DocumentedVersion: documented}
}

func TestSchemaRegistryDrift(t *testing.T) {
	defer withFakeSR(5, nil)()
	rs := scrapeSchemaRegistry(context.Background(), srcfg("3"), time.Unix(int64(fixedNow), 0))
	r := rs[0]
	if !r.ok || r.method != methodVersionDrift {
		t.Fatalf("expected version_drift ok, got %+v", r)
	}
	if !r.hasRecordsBehind || r.recordsBehind != 2 {
		t.Fatalf("expected 2 versions behind, got %v", r.recordsBehind)
	}
	if r.hasAge {
		t.Fatal("version drift must not emit age")
	}
	if r.extraAttrs[attrVersionDocumented] != "3" || r.extraAttrs[attrVersionCurrent] != "5" {
		t.Fatalf("version attrs wrong: %v", r.extraAttrs)
	}
}

func TestSchemaRegistryCurrent(t *testing.T) {
	defer withFakeSR(5, nil)()
	rs := scrapeSchemaRegistry(context.Background(), srcfg("5"), time.Unix(int64(fixedNow), 0))
	if rs[0].recordsBehind != 0 {
		t.Fatalf("expected 0 behind, got %v", rs[0].recordsBehind)
	}
}

func TestSchemaRegistryError(t *testing.T) {
	defer withFakeSR(0, errors.New("404"))()
	rs := scrapeSchemaRegistry(context.Background(), srcfg("1"), time.Unix(int64(fixedNow), 0))
	if rs[0].ok || rs[0].errType != "registry_failed" {
		t.Fatalf("expected registry_failed, got %+v", rs[0])
	}
}

func TestSchemaRegistryBadDocumented(t *testing.T) {
	defer withFakeSR(5, nil)()
	rs := scrapeSchemaRegistry(context.Background(), srcfg("not-an-int"), time.Unix(int64(fixedNow), 0))
	if rs[0].ok || rs[0].errType != "bad_documented_version" {
		t.Fatalf("expected bad_documented_version, got %+v", rs[0])
	}
}

// --- DB migration (reuses the fakesql driver from sql_test.go) ---
func TestDBMigrationBehind(t *testing.T) {
	resetDBCache()
	fakeRegistry["m1"] = fakeResult{cols: []string{"v"}, vals: []driver.Value{int64(20240101)}}
	cfg := SourceConfig{Type: "db_migration", Name: "app-db", System: "postgresql",
		Driver: "fakesql", DSN: "m1",
		VersionQuery:   "SELECT MAX(version) FROM schema_migrations",
		CurrentVersion: "20240115"}
	rs := scrapeDBMigration(context.Background(), cfg, time.Unix(int64(fixedNow), 0))
	r := rs[0]
	if !r.ok || r.recordsBehind != 1 {
		t.Fatalf("expected behind=1, got %+v", r)
	}
	if r.extraAttrs[attrVersionDocumented] != "20240101" || r.extraAttrs[attrVersionCurrent] != "20240115" {
		t.Fatalf("version attrs wrong: %v", r.extraAttrs)
	}
	if r.hasAge {
		t.Fatal("migration drift must not emit age")
	}
}

func TestDBMigrationUpToDate(t *testing.T) {
	resetDBCache()
	fakeRegistry["m2"] = fakeResult{cols: []string{"v"}, vals: []driver.Value{int64(20240115)}}
	cfg := SourceConfig{Type: "db_migration", Name: "app-db", Driver: "fakesql", DSN: "m2",
		VersionQuery: "SELECT MAX(version) FROM schema_migrations", CurrentVersion: "20240115"}
	rs := scrapeDBMigration(context.Background(), cfg, time.Unix(int64(fixedNow), 0))
	if rs[0].recordsBehind != 0 {
		t.Fatalf("expected 0 behind, got %v", rs[0].recordsBehind)
	}
}

func TestDBMigrationConfigValidation(t *testing.T) {
	bad := &Config{CollectionInterval: time.Second, Sources: []SourceConfig{{Type: "db_migration", Name: "x"}}}
	if err := bad.Validate(); err == nil {
		t.Fatal("expected error for incomplete db_migration")
	}
	badSR := &Config{CollectionInterval: time.Second, Sources: []SourceConfig{{Type: "schema_registry", Name: "x"}}}
	if err := badSR.Validate(); err == nil {
		t.Fatal("expected error for incomplete schema_registry")
	}
}

func TestReceiverEmitsVersionDriftWithoutAge(t *testing.T) {
	defer withFakeSR(5, nil)()
	r := testReceiver(&Config{
		CollectionInterval: time.Second,
		Sources:            []SourceConfig{srcfg("2")},
	})
	md := r.collect(context.Background())
	rb, ok := metricByName(md, metricRecordsBehind)
	if !ok || rb.Gauge().DataPoints().At(0).IntValue() != 3 {
		t.Fatalf("expected records.behind=3, got ok=%v", ok)
	}
	// version attributes present
	attrs := rb.Gauge().DataPoints().At(0).Attributes().AsRaw()
	if attrs[attrVersionDocumented] != "2" || attrs[attrVersionCurrent] != "5" {
		t.Fatalf("version attrs missing: %v", attrs)
	}
	// NO age metric for version drift
	if _, hasAge := metricByName(md, metricAge); hasAge {
		t.Fatal("version drift must not emit data.staleness.age")
	}
}
