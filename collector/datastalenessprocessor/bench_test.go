package datastalenessprocessor

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

func buildBatch(n int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	for i := 0; i < n; i++ {
		m := sm.Metrics().AppendEmpty()
		m.SetName(metricLastUpdate)
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetDoubleValue(1000000 - float64(i))
		dp.Attributes().PutStr(attrSourceSystem, "postgresql")
		dp.Attributes().PutStr(attrSourceName, "orders")
	}
	return md
}

func BenchmarkProcess1000(b *testing.B) {
	cfg := &Config{ComputeAgeFromLastUpdate: true, EvaluateSLA: true, DefaultThreshold: time.Minute}
	p := newProcessor(cfg, zap.NewNop())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.processMetrics(context.Background(), buildBatch(1000))
	}
}
