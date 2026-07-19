package datastalenessprocessor

import (
	"context"
	"sort"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// Convention constants (mirror spec/semantic-conventions.md).
const (
	metricLastUpdate = "data.staleness.last_update.timestamp"
	metricAge        = "data.staleness.age"
	metricThreshold  = "data.staleness.sla.threshold"
	metricBreached   = "data.staleness.sla.breached"

	attrSourceSystem = "data.source.system"
	attrSourceName   = "data.source.name"
	attrMethod       = "data.staleness.method"

	unitSeconds = "s"
	unitBool    = "1"
)

type stalenessProcessor struct {
	cfg    *Config
	logger *zap.Logger
	now    func() time.Time
}

func newProcessor(cfg *Config, logger *zap.Logger) *stalenessProcessor {
	return &stalenessProcessor{cfg: cfg, logger: logger, now: time.Now}
}

// processMetrics derives data.staleness.age from last_update timestamps and
// evaluates SLA breaches, appending new metrics within each scope.
func (p *stalenessProcessor) processMetrics(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	nowSec := float64(p.now().UnixNano()) / 1e9
	nowTs := pcommon.NewTimestampFromTime(p.now())

	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()

			// Collect (age, attributes) we know about in this scope, either
			// from an existing age metric or derived from last_update.
			type ageEntry struct {
				attrs pcommon.Map
				age   float64
			}
			var ages []ageEntry
			existingAge := map[string]bool{}
			existingBreached := map[string]bool{}

			// Pass 1: read existing age metrics, and note series that already
			// have an SLA verdict (so we don't double-emit it).
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				switch m.Name() {
				case metricAge:
					if m.Type() == pmetric.MetricTypeGauge {
						dps := m.Gauge().DataPoints()
						for d := 0; d < dps.Len(); d++ {
							ages = append(ages, ageEntry{dps.At(d).Attributes(), valueOf(dps.At(d))})
							existingAge[attrKey(dps.At(d).Attributes())] = true
						}
					}
				case metricBreached:
					if m.Type() == pmetric.MetricTypeGauge {
						dps := m.Gauge().DataPoints()
						for d := 0; d < dps.Len(); d++ {
							existingBreached[attrKey(dps.At(d).Attributes())] = true
						}
					}
				}
			}

			// Pass 2: derive age from last_update.timestamp.
			if p.cfg.ComputeAgeFromLastUpdate {
				var derived []ageEntry
				for k := 0; k < ms.Len(); k++ {
					m := ms.At(k)
					if m.Name() != metricLastUpdate || m.Type() != pmetric.MetricTypeGauge {
						continue
					}
					dps := m.Gauge().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						if existingAge[attrKey(dps.At(d).Attributes())] {
							continue // an age already exists for this series
						}
						ts := valueOf(dps.At(d))
						age := nowSec - ts
						if age < 0 {
							age = 0
						}
						derived = append(derived, ageEntry{dps.At(d).Attributes(), age})
					}
				}
				for _, e := range derived {
					nm := ms.AppendEmpty()
					nm.SetName(metricAge)
					nm.SetUnit(unitSeconds)
					nm.SetDescription("Derived: now - data.staleness.last_update.timestamp.")
					g := nm.SetEmptyGauge()
					dp := g.DataPoints().AppendEmpty()
					e.attrs.CopyTo(dp.Attributes())
					dp.SetDoubleValue(e.age)
					dp.SetTimestamp(nowTs)
					ages = append(ages, ageEntry{dp.Attributes(), e.age})
					existingAge[attrKey(dp.Attributes())] = true
				}
			}

			// Pass 3: evaluate SLA breaches against the known ages.
			if p.cfg.EvaluateSLA {
				for _, e := range ages {
					if existingBreached[attrKey(e.attrs)] {
						continue // SLA already evaluated for this series upstream
					}
					system, _ := getStr(e.attrs, attrSourceSystem)
					name, _ := getStr(e.attrs, attrSourceName)
					thr, ok := p.cfg.thresholdFor(system, name)
					if !ok {
						continue
					}
					// threshold gauge
					tm := ms.AppendEmpty()
					tm.SetName(metricThreshold)
					tm.SetUnit(unitSeconds)
					tm.SetDescription("Configured maximum acceptable age (freshness SLA).")
					tdp := tm.SetEmptyGauge().DataPoints().AppendEmpty()
					e.attrs.CopyTo(tdp.Attributes())
					tdp.SetDoubleValue(thr)
					tdp.SetTimestamp(nowTs)

					// breached gauge {0,1}
					bm := ms.AppendEmpty()
					bm.SetName(metricBreached)
					bm.SetUnit(unitBool)
					bm.SetDescription("1 if age exceeds the SLA threshold, else 0.")
					bdp := bm.SetEmptyGauge().DataPoints().AppendEmpty()
					e.attrs.CopyTo(bdp.Attributes())
					breached := int64(0)
					if e.age > thr {
						breached = 1
					}
					bdp.SetIntValue(breached)
					bdp.SetTimestamp(nowTs)
				}
			}
		}
	}
	return md, nil
}

// attrKey is a deterministic identity for a datapoint's attribute set.
func attrKey(m pcommon.Map) string {
	parts := make([]string, 0, m.Len())
	m.Range(func(k string, v pcommon.Value) bool {
		parts = append(parts, k+"="+v.AsString())
		return true
	})
	sort.Strings(parts)
	return strings.Join(parts, "\x00")
}

// valueOf returns a numeric data point value as float64 regardless of int/double.
func valueOf(dp pmetric.NumberDataPoint) float64 {
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		return float64(dp.IntValue())
	case pmetric.NumberDataPointValueTypeDouble:
		return dp.DoubleValue()
	default:
		return 0
	}
}

func getStr(m pcommon.Map, key string) (string, bool) {
	v, ok := m.Get(key)
	if !ok {
		return "", false
	}
	return v.AsString(), true
}
