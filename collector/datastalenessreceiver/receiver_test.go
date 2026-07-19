package datastalenessreceiver

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

const fixedNow = 1_000_000.0

func testReceiver(cfg *Config) *stalenessReceiver {
	r := newReceiver(cfg, component.TelemetrySettings{Logger: zap.NewNop()}, consumertest.NewNop())
	r.now = func() time.Time { return time.Unix(int64(fixedNow), 0) }
	r.startTime = r.now()
	return r
}

func metricByName(md pmetric.Metrics, name string) (pmetric.Metric, bool) {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				if ms.At(k).Name() == name {
					return ms.At(k), true
				}
			}
		}
	}
	return pmetric.Metric{}, false
}

func TestStaticScraperAgeAndSLA(t *testing.T) {
	r := testReceiver(&Config{
		CollectionInterval: time.Second,
		Sources: []SourceConfig{{
			Type: "static", Name: "orders", System: "postgresql",
			LastUpdateEpoch: fixedNow - 120, SLAThresholdSeconds: 60,
		}},
	})
	md := r.collect(context.Background())
	m, ok := metricByName(md, metricAge)
	if !ok || m.Gauge().DataPoints().At(0).DoubleValue() != 120 {
		t.Fatalf("expected age 120, got %v (ok=%v)", m, ok)
	}
	b, _ := metricByName(md, metricBreached)
	if b.Gauge().DataPoints().At(0).IntValue() != 1 {
		t.Fatal("expected breach=1")
	}
}

func TestFileScraperUsesModTime(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "data.parquet")
	if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	modAge := 90.0
	os.Chtimes(p, time.Unix(int64(fixedNow-modAge), 0), time.Unix(int64(fixedNow-modAge), 0))
	r := testReceiver(&Config{
		CollectionInterval: time.Second,
		Sources:            []SourceConfig{{Type: "file", Name: "lake", System: "s3", Path: p}},
	})
	md := r.collect(context.Background())
	m, ok := metricByName(md, metricAge)
	if !ok {
		t.Fatal("no age metric")
	}
	got := m.Gauge().DataPoints().At(0).DoubleValue()
	if got < 89 || got > 91 {
		t.Fatalf("expected ~90, got %v", got)
	}
	if m.Gauge().DataPoints().At(0).Attributes().AsRaw()[attrMethod] != "object_mtime" {
		t.Fatal("expected method=object_mtime")
	}
}

func TestHTTPScraperLastModified(t *testing.T) {
	r := testReceiver(&Config{
		CollectionInterval: time.Second,
		Sources:            []SourceConfig{{Type: "http", Name: "feed", System: "http", URL: "http://x"}},
	})
	r.httpGet = func(ctx context.Context, url string) (*http.Response, error) {
		h := http.Header{}
		h.Set("Last-Modified", time.Unix(int64(fixedNow-45), 0).UTC().Format(http.TimeFormat))
		return &http.Response{StatusCode: 200, Header: h, Body: http.NoBody}, nil
	}
	md := r.collect(context.Background())
	m, _ := metricByName(md, metricAge)
	got := m.Gauge().DataPoints().At(0).DoubleValue()
	if got < 44 || got > 46 {
		t.Fatalf("expected ~45, got %v", got)
	}
}

func TestErrorEmitsProbeErrorCounter(t *testing.T) {
	r := testReceiver(&Config{
		CollectionInterval: time.Second,
		Sources:            []SourceConfig{{Type: "file", Name: "missing", System: "s3", Path: "/no/such/file"}},
	})
	md := r.collect(context.Background())
	m, ok := metricByName(md, metricProbeErr)
	if !ok {
		t.Fatal("expected probe.errors metric")
	}
	dp := m.Sum().DataPoints().At(0)
	if dp.IntValue() != 1 {
		t.Fatalf("expected count 1, got %v", dp.IntValue())
	}
	if dp.Attributes().AsRaw()[attrErrorType] != "stat_failed" {
		t.Fatal("expected error.type=stat_failed")
	}
	// second scrape -> cumulative 2
	md2 := r.collect(context.Background())
	m2, _ := metricByName(md2, metricProbeErr)
	if m2.Sum().DataPoints().At(0).IntValue() != 2 {
		t.Fatal("expected cumulative count 2")
	}
}

func TestConfigValidate(t *testing.T) {
	if err := (&Config{}).Validate(); err == nil {
		t.Fatal("expected error for zero interval")
	}
	if err := (&Config{CollectionInterval: time.Second}).Validate(); err == nil {
		t.Fatal("expected error for no sources")
	}
	bad := &Config{CollectionInterval: time.Second, Sources: []SourceConfig{{Type: "bogus", Name: "x"}}}
	if err := bad.Validate(); err == nil {
		t.Fatal("expected error for bad type")
	}
	good := &Config{CollectionInterval: time.Second, Sources: []SourceConfig{{Type: "static", Name: "x", AgeSeconds: 5}}}
	if err := good.Validate(); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
}

func TestFactoryDefaultAndStartShutdown(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.Sources = []SourceConfig{{Type: "static", Name: "x", System: "redis", LastUpdateEpoch: fixedNow - 1}}
	if cfg.CollectionInterval <= 0 {
		t.Fatal("expected default interval")
	}
	r := newReceiver(cfg, component.TelemetrySettings{Logger: zap.NewNop()}, consumertest.NewNop())
	if err := r.Start(context.Background(), componenttestNopHost{}); err != nil {
		t.Fatal(err)
	}
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type componenttestNopHost struct{}

func (componenttestNopHost) GetExtensions() map[component.ID]component.Component { return nil }
