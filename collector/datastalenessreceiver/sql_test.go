package datastalenessreceiver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"
	"time"
)

// --- hermetic in-process SQL driver (no external DB, no cgo) ---------------
// Exercises the real database/sql path: QueryContext -> Columns -> Scan.

type fakeResult struct {
	cols     []string
	vals     []driver.Value
	empty    bool // query returns zero rows
	queryErr bool
}

var fakeRegistry = map[string]fakeResult{}

type fakeDriver struct{}

func (fakeDriver) Open(dsn string) (driver.Conn, error) { return &fakeConn{dsn: dsn}, nil }

type fakeConn struct{ dsn string }

func (c *fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unused") }
func (c *fakeConn) Close() error                        { return nil }
func (c *fakeConn) Begin() (driver.Tx, error)           { return nil, errors.New("no tx") }

func (c *fakeConn) QueryContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	r := fakeRegistry[c.dsn]
	if r.queryErr {
		return nil, errors.New("boom")
	}
	return &fakeRows{r: r}, nil
}

type fakeRows struct {
	r    fakeResult
	done bool
}

func (f *fakeRows) Columns() []string { return f.r.cols }
func (f *fakeRows) Close() error      { return nil }
func (f *fakeRows) Next(dest []driver.Value) error {
	if f.done || f.r.empty {
		return io.EOF
	}
	f.done = true
	copy(dest, f.r.vals)
	return nil
}

func init() { sql.Register("fakesql", fakeDriver{}) }

func resetDBCache() {
	dbCacheMu.Lock()
	dbCache = map[string]*sql.DB{}
	dbCacheMu.Unlock()
}

func sqlCfg(dsn string) SourceConfig {
	return SourceConfig{Type: "sql", Name: "t", System: "postgresql", Driver: "fakesql", DSN: dsn}
}

// --- tests -----------------------------------------------------------------

func TestSQLColumnMaxAge(t *testing.T) {
	resetDBCache()
	fakeRegistry["c1"] = fakeResult{cols: []string{"m"}, vals: []driver.Value{time.Unix(int64(fixedNow-90), 0)}}
	cfg := sqlCfg("c1")
	cfg.Table, cfg.TimestampColumn = "events", "updated_at"
	r := scrapeSQL(context.Background(), cfg, time.Unix(int64(fixedNow), 0))
	if !r.ok {
		t.Fatalf("expected ok, err=%s", r.errType)
	}
	if r.ageSeconds < 89 || r.ageSeconds > 91 {
		t.Fatalf("age ~90 expected, got %v", r.ageSeconds)
	}
	if r.method != "max_timestamp" {
		t.Fatalf("method=%q", r.method)
	}
}

func TestSQLWatermarkRecordsBehind(t *testing.T) {
	resetDBCache()
	fakeRegistry["c2"] = fakeResult{cols: []string{"ts", "behind"},
		vals: []driver.Value{int64(fixedNow - 10), int64(98)}}
	cfg := sqlCfg("c2")
	cfg.Query = "SELECT last_loaded_at, expected-loaded FROM audit"
	r := scrapeSQL(context.Background(), cfg, time.Unix(int64(fixedNow), 0))
	if !r.ok {
		t.Fatalf("expected ok, err=%s", r.errType)
	}
	if r.ageSeconds < 9 || r.ageSeconds > 11 {
		t.Fatalf("age ~10 expected, got %v", r.ageSeconds)
	}
	if r.method != "watermark" {
		t.Fatalf("method=%q", r.method)
	}
	if !r.hasRecordsBehind || r.recordsBehind != 98 {
		t.Fatalf("records_behind=98 expected, got %v", r.recordsBehind)
	}
}

func TestSQLNullTimestampIsVisibleError(t *testing.T) {
	resetDBCache()
	// MAX() over an empty table returns one row whose value is NULL.
	fakeRegistry["c3"] = fakeResult{cols: []string{"m"}, vals: []driver.Value{nil}}
	cfg := sqlCfg("c3")
	cfg.Table, cfg.TimestampColumn = "events", "updated_at"
	r := scrapeSQL(context.Background(), cfg, time.Unix(int64(fixedNow), 0))
	if r.ok {
		t.Fatal("expected failure on NULL, not a fabricated age")
	}
	if r.errType != "null_timestamp" {
		t.Fatalf("expected null_timestamp, got %q", r.errType)
	}
}

func TestSQLStringTimestampParsed(t *testing.T) {
	resetDBCache()
	iso := time.Unix(int64(fixedNow-45), 0).UTC().Format(time.RFC3339)
	fakeRegistry["c4"] = fakeResult{cols: []string{"m"}, vals: []driver.Value{iso}}
	cfg := sqlCfg("c4")
	cfg.Table, cfg.TimestampColumn = "t2", "ts"
	r := scrapeSQL(context.Background(), cfg, time.Unix(int64(fixedNow), 0))
	if !r.ok || r.ageSeconds < 44 || r.ageSeconds > 46 {
		t.Fatalf("age ~45 from RFC3339 expected, got %v (err=%s)", r.ageSeconds, r.errType)
	}
}

func TestSQLNoRows(t *testing.T) {
	resetDBCache()
	fakeRegistry["c5"] = fakeResult{cols: []string{"m"}, empty: true}
	cfg := sqlCfg("c5")
	cfg.Query = "SELECT ts FROM audit WHERE 1=0"
	r := scrapeSQL(context.Background(), cfg, time.Unix(int64(fixedNow), 0))
	if r.ok || r.errType != "no_rows" {
		t.Fatalf("expected no_rows, got ok=%v err=%s", r.ok, r.errType)
	}
}

func TestSQLQueryError(t *testing.T) {
	resetDBCache()
	fakeRegistry["c6"] = fakeResult{queryErr: true}
	cfg := sqlCfg("c6")
	cfg.Query = "SELECT boom"
	r := scrapeSQL(context.Background(), cfg, time.Unix(int64(fixedNow), 0))
	if r.ok || r.errType != "query_failed" {
		t.Fatalf("expected query_failed, got ok=%v err=%s", r.ok, r.errType)
	}
}

func TestSQLConfigIncomplete(t *testing.T) {
	resetDBCache()
	r := scrapeSQL(context.Background(), sqlCfg("c7"), time.Unix(int64(fixedNow), 0))
	if r.ok || r.errType != "config_incomplete" {
		t.Fatalf("expected config_incomplete, got ok=%v err=%s", r.ok, r.errType)
	}
}

func TestToEpochFormats(t *testing.T) {
	cases := []struct {
		in   any
		want float64
	}{
		{int64(1000), 1000},
		{float64(1500.5), 1500.5},
		{"1700000000", 1700000000},
		{time.Unix(1234, 0).UTC(), 1234},
	}
	for _, c := range cases {
		got, ok := toEpoch(c.in, true)
		if !ok || got != c.want {
			t.Fatalf("toEpoch(%v)=%v,%v want %v", c.in, got, ok, c.want)
		}
	}
	if _, ok := toEpoch(nil, true); ok {
		t.Fatal("nil must be not-ok")
	}
}
