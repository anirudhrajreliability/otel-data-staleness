package datastalenessprocessor

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

const fixedNow = 1_000_000.0 // Unix seconds

func newTestProc(cfg *Config) *stalenessProcessor {
	p := newProcessor(cfg, zap.NewNop())
	p.now = func() time.Time { return time.Unix(int64(fixedNow), 0) }
	return p
}

func inputWithLastUpdate(system, name string, ts float64) pmetric.Metrics {
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName(metricLastUpdate)
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(ts)
	dp.Attributes().PutStr(attrSourceSystem, system)
	dp.Attributes().PutStr(attrSourceName, name)
	return md
}

func findMetric(md pmetric.Metrics, name string) (pmetric.Metric, bool) {
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

func TestDeriveAgeFromLastUpdate(t *testing.T) {
	cfg := &Config{ComputeAgeFromLastUpdate: true}
	p := newTestProc(cfg)
	md := inputWithLastUpdate("postgresql", "orders", fixedNow-120) // 120s old
	out, err := p.processMetrics(context.Background(), md)
	if err != nil {
		t.Fatal(err)
	}
	m, ok := findMetric(out, metricAge)
	if !ok {
		t.Fatal("expected derived age metric")
	}
	got := m.Gauge().DataPoints().At(0).DoubleValue()
	if got != 120 {
		t.Fatalf("age: want 120, got %v", got)
	}
}

func TestAgeNeverNegative(t *testing.T) {
	cfg := &Config{ComputeAgeFromLastUpdate: true}
	p := newTestProc(cfg)
	md := inputWithLastUpdate("s3", "raw/", fixedNow+50) // future timestamp
	out, _ := p.processMetrics(context.Background(), md)
	m, _ := findMetric(out, metricAge)
	if v := m.Gauge().DataPoints().At(0).DoubleValue(); v != 0 {
		t.Fatalf("want 0, got %v", v)
	}
}

func TestSLABreachEvaluation(t *testing.T) {
	cfg := &Config{
		ComputeAgeFromLastUpdate: true,
		EvaluateSLA:              true,
		SLAs:                     []SLARule{{SourceName: "orders", Threshold: 60 * time.Second}},
	}
	p := newTestProc(cfg)
	md := inputWithLastUpdate("postgresql", "orders", fixedNow-120) // 120 > 60 -> breach
	out, _ := p.processMetrics(context.Background(), md)

	bm, ok := findMetric(out, metricBreached)
	if !ok {
		t.Fatal("expected breached metric")
	}
	if v := bm.Gauge().DataPoints().At(0).IntValue(); v != 1 {
		t.Fatalf("breached: want 1, got %v", v)
	}
	tm, ok := findMetric(out, metricThreshold)
	if !ok || tm.Gauge().DataPoints().At(0).DoubleValue() != 60 {
		t.Fatal("expected threshold gauge = 60")
	}
}

func TestSLANotBreached(t *testing.T) {
	cfg := &Config{
		ComputeAgeFromLastUpdate: true,
		EvaluateSLA:              true,
		DefaultThreshold:         5 * time.Minute,
	}
	p := newTestProc(cfg)
	md := inputWithLastUpdate("dbt", "model_a", fixedNow-30) // 30 < 300
	out, _ := p.processMetrics(context.Background(), md)
	bm, _ := findMetric(out, metricBreached)
	if v := bm.Gauge().DataPoints().At(0).IntValue(); v != 0 {
		t.Fatalf("want 0, got %v", v)
	}
}

func TestFactoryDefault(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	if !cfg.ComputeAgeFromLastUpdate || !cfg.EvaluateSLA {
		t.Fatal("expected defaults enabled")
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}
}
