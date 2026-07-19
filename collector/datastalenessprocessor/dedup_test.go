package datastalenessprocessor

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// helper: add a gauge datapoint with a source attribute to a scope
func addGauge(sm pmetric.ScopeMetrics, name, system, sysName string, val float64) {
	m := sm.Metrics().AppendEmpty()
	m.SetName(name)
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(val)
	dp.Attributes().PutStr(attrSourceSystem, system)
	dp.Attributes().PutStr(attrSourceName, sysName)
}

func countPoints(md pmetric.Metrics, name string) int {
	n := 0
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				if ms.At(k).Name() == name {
					n += ms.At(k).Gauge().DataPoints().Len()
				}
			}
		}
	}
	return n
}

// If a series already has an age, deriving from last_update must NOT add a duplicate.
func TestNoDuplicateDerivedAge(t *testing.T) {
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	addGauge(sm, metricAge, "postgresql", "orders", 42)                 // existing age
	addGauge(sm, metricLastUpdate, "postgresql", "orders", fixedNow-42) // same series' timestamp

	p := newProcessor(&Config{ComputeAgeFromLastUpdate: true, EvaluateSLA: false}, zap.NewNop())
	p.now = func() time.Time { return time.Unix(int64(fixedNow), 0) }
	out, _ := p.processMetrics(context.Background(), md)

	if got := countPoints(out, metricAge); got != 1 {
		t.Fatalf("expected exactly 1 age point (no duplicate derived), got %d", got)
	}
}

// If a series already has an SLA verdict, the processor must not add another.
func TestNoDuplicateSLA(t *testing.T) {
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	addGauge(sm, metricAge, "postgresql", "orders", 200)
	addGauge(sm, metricBreached, "postgresql", "orders", 1) // already evaluated upstream

	p := newProcessor(&Config{EvaluateSLA: true, DefaultThreshold: time.Minute}, zap.NewNop())
	p.now = func() time.Time { return time.Unix(int64(fixedNow), 0) }
	out, _ := p.processMetrics(context.Background(), md)

	if got := countPoints(out, metricBreached); got != 1 {
		t.Fatalf("expected exactly 1 breached point (no duplicate), got %d", got)
	}
}
