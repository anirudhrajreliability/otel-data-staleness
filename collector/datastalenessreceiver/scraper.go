package datastalenessreceiver

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// reading is a normalized freshness observation produced by a scraper.
type reading struct {
	cfg              SourceConfig
	lastUpdateEpoch  float64 // Unix seconds; <0 means unavailable
	ageSeconds       float64 // resolved age; <0 means unavailable
	recordsBehind    int64
	hasRecordsBehind bool
	method           string // overrides methodForType when set
	partition        string // per-partition sources (e.g. kafka)
	lagSeconds       float64
	hasLag           bool
	hasAge           bool
	extraAttrs       map[string]string
	ok               bool
	errType          string
}

// scrapeSource dispatches to the scraper implied by cfg.Type. now is injected
// for deterministic testing.
// scrapeSourceMulti dispatches sources that can yield multiple readings (Kafka,
// per partition). All other source types yield exactly one reading.
func scrapeSourceMulti(ctx context.Context, cfg SourceConfig, now time.Time, httpGet httpGetter) []reading {
	if cfg.Type == "kafka" {
		return scrapeKafka(ctx, cfg, now)
	}
	if cfg.Type == "kinesis" {
		return scrapeKinesis(ctx, cfg, now)
	}
	if cfg.Type == "schema_registry" {
		return scrapeSchemaRegistry(ctx, cfg, now)
	}
	if cfg.Type == "db_migration" {
		return scrapeDBMigration(ctx, cfg, now)
	}
	return []reading{scrapeSource(ctx, cfg, now, httpGet)}
}

func scrapeSource(ctx context.Context, cfg SourceConfig, now time.Time, httpGet httpGetter) reading {
	switch cfg.Type {
	case "static":
		return scrapeStatic(cfg, now)
	case "file":
		return scrapeFile(cfg, now)
	case "http":
		return scrapeHTTP(ctx, cfg, now, httpGet)
	case "sql":
		return scrapeSQL(ctx, cfg, now)
	default:
		return reading{cfg: cfg, errType: "unknown_type"}
	}
}

func resolve(cfg SourceConfig, now time.Time, lastUpdate float64, hasLast bool) reading {
	r := reading{cfg: cfg, lastUpdateEpoch: -1, ageSeconds: -1}
	if cfg.AgeSeconds > 0 && !hasLast {
		r.ageSeconds = cfg.AgeSeconds
		r.hasAge = true
		r.ok = true
		return r
	}
	if hasLast {
		r.lastUpdateEpoch = lastUpdate
		age := float64(now.UnixNano())/1e9 - lastUpdate
		if age < 0 {
			age = 0
		}
		r.ageSeconds = age
		r.hasAge = true
		r.ok = true
	}
	return r
}

func scrapeStatic(cfg SourceConfig, now time.Time) reading {
	if cfg.LastUpdateEpoch > 0 {
		return resolve(cfg, now, cfg.LastUpdateEpoch, true)
	}
	if cfg.AgeSeconds > 0 {
		return resolve(cfg, now, 0, false) // resolve reads AgeSeconds
	}
	return reading{cfg: cfg, errType: "no_value"}
}

func scrapeFile(cfg SourceConfig, now time.Time) reading {
	info, err := os.Stat(cfg.Path)
	if err != nil {
		return reading{cfg: cfg, errType: "stat_failed"}
	}
	return resolve(cfg, now, float64(info.ModTime().UnixNano())/1e9, true)
}

type httpGetter func(ctx context.Context, url string) (*http.Response, error)

func defaultHTTPGet(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}

func scrapeHTTP(ctx context.Context, cfg SourceConfig, now time.Time, get httpGetter) reading {
	if get == nil {
		get = defaultHTTPGet
	}
	resp, err := get(ctx, cfg.URL)
	if err != nil {
		return reading{cfg: cfg, errType: "request_failed"}
	}
	defer resp.Body.Close()
	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		if t, err := http.ParseTime(lm); err == nil {
			return resolve(cfg, now, float64(t.Unix()), true)
		}
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64))
	if v, err := strconv.ParseFloat(strings.TrimSpace(string(body)), 64); err == nil {
		return resolve(cfg, now, v, true)
	}
	return reading{cfg: cfg, errType: "unparseable_response"}
}

// ---- SQL scraper ----------------------------------------------------------

var (
	dbCacheMu sync.Mutex
	dbCache   = map[string]*sql.DB{}
)

// getDB returns a pooled *sql.DB for the driver/DSN, opening it once.
func getDB(driver, dsn string) (*sql.DB, error) {
	key := driver + "\x00" + dsn
	dbCacheMu.Lock()
	defer dbCacheMu.Unlock()
	if db, ok := dbCache[key]; ok {
		return db, nil
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	// Freshness probing is light and periodic; keep the pool tiny.
	db.SetMaxOpenConns(2)
	db.SetConnMaxIdleTime(5 * time.Minute)
	dbCache[key] = db
	return db, nil
}

// closeDBs closes and clears all pooled connections (called on receiver shutdown).
func closeDBs() {
	dbCacheMu.Lock()
	defer dbCacheMu.Unlock()
	for k, db := range dbCache {
		_ = db.Close()
		delete(dbCache, k)
	}
}

func scrapeSQL(ctx context.Context, cfg SourceConfig, now time.Time) reading {
	query := cfg.Query
	method := cfg.Method
	if query == "" {
		if cfg.Table == "" || cfg.TimestampColumn == "" {
			return reading{cfg: cfg, errType: "config_incomplete"}
		}
		tbl := cfg.Table
		if cfg.Namespace != "" {
			tbl = cfg.Namespace + "." + cfg.Table
		}
		query = fmt.Sprintf("SELECT MAX(%s) FROM %s", cfg.TimestampColumn, tbl)
		if method == "" {
			method = "max_timestamp"
		}
	} else if method == "" {
		method = "watermark"
	}

	db, err := getDB(cfg.Driver, cfg.DSN)
	if err != nil {
		return reading{cfg: cfg, errType: "connect_failed", method: method}
	}
	timeout := cfg.QueryTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	qctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	rows, err := db.QueryContext(qctx, query)
	if err != nil {
		if qctx.Err() == context.DeadlineExceeded {
			return reading{cfg: cfg, errType: "timeout", method: method}
		}
		return reading{cfg: cfg, errType: "query_failed", method: method}
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	if !rows.Next() {
		return reading{cfg: cfg, errType: "no_rows", method: method}
	}
	var tsRaw any
	var cnt sql.NullInt64
	if len(cols) >= 2 {
		if err := rows.Scan(&tsRaw, &cnt); err != nil {
			return reading{cfg: cfg, errType: "scan_failed", method: method}
		}
	} else {
		if err := rows.Scan(&tsRaw); err != nil {
			return reading{cfg: cfg, errType: "scan_failed", method: method}
		}
	}

	epoch, ok := toEpoch(tsRaw, !cfg.AssumeLocalTime)
	if !ok {
		// NULL/empty MAX() or an unparseable value is a real, visible failure
		// rather than a fabricated "infinitely fresh/stale" number.
		return reading{cfg: cfg, errType: "null_timestamp", method: method}
	}

	r := resolve(cfg, now, epoch, true)
	r.method = method
	if cnt.Valid {
		r.recordsBehind = cnt.Int64
		r.hasRecordsBehind = true
	}
	return r
}

// toEpoch converts a scanned value to Unix seconds. It is deliberately liberal
// about driver return types (time.Time, string/[]byte, or a numeric epoch) so
// the same scraper works across Postgres, MySQL, SQLite, and warehouses.
func toEpoch(v any, assumeUTC bool) (float64, bool) {
	switch x := v.(type) {
	case nil:
		return 0, false
	case time.Time:
		// The driver has already resolved the zone; use the absolute instant.
		// (For naive columns, set the driver session TZ to UTC, e.g. mysql
		// loc=UTC, so the stored value is read as UTC.)
		return float64(x.UnixNano()) / 1e9, true
	case []byte:
		return parseTimeString(string(x), assumeUTC)
	case string:
		return parseTimeString(x, assumeUTC)
	case int64:
		return float64(x), true
	case float64:
		return x, true
	default:
		return 0, false
	}
}

func parseTimeString(s string, assumeUTC bool) (float64, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	// Numeric epoch, but only when the magnitude is a plausible timestamp, so a
	// date-encoded integer like 20240101 is not silently read as epoch 1970.
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		if f >= 1e8 && f < 1e11 { // ~1973..5138 in seconds
			return f, true
		}
		if f >= 1e11 && f < 1e14 { // plausible milliseconds
			return f / 1000.0, true
		}
		// otherwise fall through to date-layout parsing (and error if none match)
	}
	loc := time.UTC
	if !assumeUTC {
		loc = time.Local
	}
	layouts := []string{
		time.RFC3339Nano, time.RFC3339,
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, l := range layouts {
		// Parse honors an embedded offset; ParseInLocation applies loc to naive values.
		if t, err := time.Parse(l, s); err == nil && (strings.Contains(l, "07:00") || strings.Contains(l, "Z07")) {
			return float64(t.UnixNano()) / 1e9, true
		}
		if t, err := time.ParseInLocation(l, s, loc); err == nil {
			return float64(t.UnixNano()) / 1e9, true
		}
	}
	return 0, false
}
