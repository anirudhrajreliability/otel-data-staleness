package datastalenessreceiver

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"time"
)

const (
	attrVersionDocumented = "data.staleness.version.documented"
	attrVersionCurrent    = "data.staleness.version.current"
	methodVersionDrift    = "version_drift"
)

var _digits = regexp.MustCompile(`\d+`)

// versionCompare compares two version-ish strings by numeric components:
// returns -1 if a<b, 0 if equal, 1 if a>b. Handles integers (schema-registry,
// sequential migrations) and dotted semver (1.28.0). Non-numeric suffixes are
// ignored, which is fine for schema/migration versions.
func versionCompare(a, b string) int {
	pa, pb := _digits.FindAllString(a, -1), _digits.FindAllString(b, -1)
	n := len(pa)
	if len(pb) > n {
		n = len(pb)
	}
	for i := 0; i < n; i++ {
		var x, y int
		if i < len(pa) {
			x, _ = strconv.Atoi(pa[i])
		}
		if i < len(pb) {
			y, _ = strconv.Atoi(pb[i])
		}
		if x != y {
			if x < y {
				return -1
			}
			return 1
		}
	}
	return 0
}

func versionDriftReading(cfg SourceConfig, behind int64, documented, current string) reading {
	return reading{
		cfg:              cfg,
		ok:               true,
		method:           methodVersionDrift,
		lastUpdateEpoch:  -1, // no timestamp for version drift -> do not emit last_update
		recordsBehind:    behind,
		hasRecordsBehind: true,
		extraAttrs: map[string]string{
			attrVersionDocumented: documented,
			attrVersionCurrent:    current,
		},
	}
}

// ---- Schema Registry (Confluent) version drift ---------------------------

type schemaRegistryClient interface {
	latestVersion(ctx context.Context, subject string) (int, error)
	close()
}

var openSchemaRegistry = func(cfg SourceConfig) (schemaRegistryClient, error) {
	return nil, fmt.Errorf("schema registry client not built into this collector")
}

// scrapeSchemaRegistry reports how many schema versions a subject is behind the
// registry's latest, relative to a pinned/contract version. No timestamps are
// available from the Schema Registry API, so no age is emitted — alert on
// records.behind > 0.
func scrapeSchemaRegistry(ctx context.Context, cfg SourceConfig, _ time.Time) []reading {
	documented, err := strconv.Atoi(cfg.DocumentedVersion)
	if err != nil {
		return []reading{{cfg: cfg, method: methodVersionDrift, errType: "bad_documented_version"}}
	}
	client, err := openSchemaRegistry(cfg)
	if err != nil {
		return []reading{{cfg: cfg, method: methodVersionDrift, errType: "connect_failed"}}
	}
	defer client.close()

	timeout := cfg.QueryTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	dctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	latest, err := client.latestVersion(dctx, cfg.Subject)
	if err != nil {
		et := "registry_failed"
		if dctx.Err() == context.DeadlineExceeded {
			et = "timeout"
		}
		return []reading{{cfg: cfg, method: methodVersionDrift, errType: et}}
	}
	behind := int64(latest - documented)
	if behind < 0 {
		behind = 0
	}
	return []reading{versionDriftReading(cfg, behind, cfg.DocumentedVersion, strconv.Itoa(latest))}
}

// ---- DB migration version drift ------------------------------------------

// scrapeDBMigration reads the applied schema-migration version from the database
// and compares it to the expected (deployed) version. behind = 1 if the DB is
// behind the expected version (migrations lag the code), else 0. Migration
// numbering is not necessarily contiguous, so this is a boolean "is behind"
// rather than a count.
func scrapeDBMigration(ctx context.Context, cfg SourceConfig, _ time.Time) []reading {
	db, err := getDB(cfg.Driver, cfg.DSN)
	if err != nil {
		return []reading{{cfg: cfg, method: methodVersionDrift, errType: "connect_failed"}}
	}
	timeout := cfg.QueryTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	qctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	rows, err := db.QueryContext(qctx, cfg.VersionQuery)
	if err != nil {
		et := "query_failed"
		if qctx.Err() == context.DeadlineExceeded {
			et = "timeout"
		}
		return []reading{{cfg: cfg, method: methodVersionDrift, errType: et}}
	}
	defer rows.Close()
	if !rows.Next() {
		return []reading{{cfg: cfg, method: methodVersionDrift, errType: "no_rows"}}
	}
	var raw any
	if err := rows.Scan(&raw); err != nil {
		return []reading{{cfg: cfg, method: methodVersionDrift, errType: "scan_failed"}}
	}
	if raw == nil {
		return []reading{{cfg: cfg, method: methodVersionDrift, errType: "null_version"}}
	}
	applied := fmt.Sprintf("%v", raw)
	if b, ok := raw.([]byte); ok {
		applied = string(b)
	}
	if !_digits.MatchString(applied) {
		return []reading{{cfg: cfg, method: methodVersionDrift, errType: "unparseable_version"}}
	}
	behind := int64(0)
	if versionCompare(applied, cfg.CurrentVersion) < 0 {
		behind = 1
	}
	return []reading{versionDriftReading(cfg, behind, applied, cfg.CurrentVersion)}
}
