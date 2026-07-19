package datastalenessreceiver

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// Convention constants (mirror spec/semantic-conventions.md).
const (
	metricAge           = "data.staleness.age"
	metricLag           = "data.staleness.lag"
	metricLastUpdate    = "data.staleness.last_update.timestamp"
	metricThreshold     = "data.staleness.sla.threshold"
	metricBreached      = "data.staleness.sla.breached"
	metricProbeErr      = "data.staleness.probe.errors"
	metricRecordsBehind = "data.staleness.records.behind"

	attrSourceSystem    = "data.source.system"
	attrSourceName      = "data.source.name"
	attrSourceNamespace = "data.source.namespace"
	attrMethod          = "data.staleness.method"
	attrErrorType       = "error.type"
	attrPartition       = "data.staleness.partition"

	unitSeconds = "s"
	unitBool    = "1"
	unitError   = "{error}"
	unitRecords = "{record}"
)

// methodForType maps a scraper type to a data.staleness.method value.
func methodForType(t string) string {
	switch t {
	case "file":
		return "object_mtime"
	case "http":
		return "heartbeat"
	case "sql":
		return "max_timestamp"
	default:
		return "max_timestamp"
	}
}

type stalenessReceiver struct {
	cfg      *Config
	consumer consumer.Metrics
	logger   *zap.Logger
	now      func() time.Time
	httpGet  httpGetter

	startTime time.Time
	errCounts map[string]int64
	mu        sync.Mutex

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newReceiver(cfg *Config, set component.TelemetrySettings, next consumer.Metrics) *stalenessReceiver {
	return &stalenessReceiver{
		cfg:       cfg,
		consumer:  next,
		logger:    set.Logger,
		now:       time.Now,
		errCounts: make(map[string]int64),
	}
}

func (r *stalenessReceiver) Start(_ context.Context, _ component.Host) error {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.startTime = r.now()
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(r.cfg.CollectionInterval)
		defer ticker.Stop()
		r.scrapeAndEmit(ctx) // emit immediately on start
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.scrapeAndEmit(ctx)
			}
		}
	}()
	return nil
}

func (r *stalenessReceiver) Shutdown(context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
	closeDBs()
	return nil
}

func (r *stalenessReceiver) scrapeAndEmit(ctx context.Context) {
	md := r.collect(ctx)
	if md.MetricCount() == 0 {
		return
	}
	if err := r.consumer.ConsumeMetrics(ctx, md); err != nil {
		r.logger.Warn("failed to push staleness metrics", zap.Error(err))
	}
}

// collect scrapes all sources and builds the metrics payload. Exposed
// (lower-case, same package) for unit testing.
func (r *stalenessReceiver) collect(ctx context.Context) pmetric.Metrics {
	now := r.now()
	ts := pcommon.NewTimestampFromTime(now)
	startTs := pcommon.NewTimestampFromTime(r.startTime)

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("github.com/otel-data-staleness/datastalenessreceiver")
	ms := sm.Metrics()

	setAttrs := func(m pcommon.Map, s SourceConfig, rd reading) {
		m.PutStr(attrSourceSystem, s.System)
		m.PutStr(attrSourceName, s.Name)
		if s.Namespace != "" {
			m.PutStr(attrSourceNamespace, s.Namespace)
		}
		method := rd.method
		if method == "" {
			method = methodForType(s.Type)
		}
		m.PutStr(attrMethod, method)
		if rd.partition != "" {
			m.PutStr(attrPartition, rd.partition)
		}
		for k, v := range rd.extraAttrs {
			m.PutStr(k, v)
		}
	}

	emit := func(s SourceConfig, rd reading) {
		if !rd.ok {
			key := s.Name + "/" + rd.partition
			r.mu.Lock()
			r.errCounts[key]++
			count := r.errCounts[key]
			r.mu.Unlock()

			em := ms.AppendEmpty()
			em.SetName(metricProbeErr)
			em.SetUnit(unitError)
			em.SetDescription("Count of failed freshness measurement attempts.")
			sum := em.SetEmptySum()
			sum.SetIsMonotonic(true)
			sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			dp := sum.DataPoints().AppendEmpty()
			setAttrs(dp.Attributes(), s, rd)
			if rd.errType != "" {
				dp.Attributes().PutStr(attrErrorType, rd.errType)
			}
			dp.SetIntValue(count)
			dp.SetStartTimestamp(startTs)
			dp.SetTimestamp(ts)
			return
		}

		// age (only when a time-based freshness value exists)
		if rd.hasAge {
			am := ms.AppendEmpty()
			am.SetName(metricAge)
			am.SetUnit(unitSeconds)
			am.SetDescription("Current data staleness (now - event time of freshest record).")
			adp := am.SetEmptyGauge().DataPoints().AppendEmpty()
			setAttrs(adp.Attributes(), s, rd)
			adp.SetDoubleValue(rd.ageSeconds)
			adp.SetTimestamp(ts)
		}

		// last_update.timestamp
		if rd.lastUpdateEpoch >= 0 {
			lm := ms.AppendEmpty()
			lm.SetName(metricLastUpdate)
			lm.SetUnit(unitSeconds)
			lm.SetDescription("Unix timestamp of the most recent successful update.")
			ldp := lm.SetEmptyGauge().DataPoints().AppendEmpty()
			setAttrs(ldp.Attributes(), s, rd)
			ldp.SetDoubleValue(rd.lastUpdateEpoch)
			ldp.SetTimestamp(ts)
		}

		// records.behind (optional, e.g. completeness backlog)
		if rd.hasRecordsBehind {
			rm := ms.AppendEmpty()
			rm.SetName(metricRecordsBehind)
			rm.SetUnit(unitRecords)
			rm.SetDescription("Backlog between produced and consumed/loaded positions.")
			rdp := rm.SetEmptyGauge().DataPoints().AppendEmpty()
			setAttrs(rdp.Attributes(), s, rd)
			rdp.SetIntValue(rd.recordsBehind)
			rdp.SetTimestamp(ts)
		}

		// lag (consumer time-lag: now - timestamp at committed offset)
		if rd.hasLag {
			gm := ms.AppendEmpty()
			gm.SetName(metricLag)
			gm.SetUnit(unitSeconds)
			gm.SetDescription("Processing/consumer time-lag of the most recently processed record.")
			gdp := gm.SetEmptyGauge().DataPoints().AppendEmpty()
			setAttrs(gdp.Attributes(), s, rd)
			gdp.SetDoubleValue(rd.lagSeconds)
			gdp.SetTimestamp(ts)
		}

		// SLA threshold + breached (age-based; not for version-drift)
		if s.SLAThresholdSeconds > 0 && rd.hasAge {
			tm := ms.AppendEmpty()
			tm.SetName(metricThreshold)
			tm.SetUnit(unitSeconds)
			tm.SetDescription("Configured maximum acceptable age (freshness SLA).")
			tdp := tm.SetEmptyGauge().DataPoints().AppendEmpty()
			setAttrs(tdp.Attributes(), s, rd)
			tdp.SetDoubleValue(s.SLAThresholdSeconds)
			tdp.SetTimestamp(ts)

			bm := ms.AppendEmpty()
			bm.SetName(metricBreached)
			bm.SetUnit(unitBool)
			bm.SetDescription("1 if age exceeds the SLA threshold, else 0.")
			bdp := bm.SetEmptyGauge().DataPoints().AppendEmpty()
			setAttrs(bdp.Attributes(), s, rd)
			breached := int64(0)
			if rd.ageSeconds > s.SLAThresholdSeconds {
				breached = 1
			}
			bdp.SetIntValue(breached)
			bdp.SetTimestamp(ts)
		}
	}

	for _, s := range r.cfg.Sources {
		for _, rd := range scrapeSourceMulti(ctx, s, now, r.httpGet) {
			emit(s, rd)
		}
	}
	return md
}
