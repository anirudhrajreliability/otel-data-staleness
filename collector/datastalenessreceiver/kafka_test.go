package datastalenessreceiver

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeKafka struct {
	parts []kPartition
	err   error
}

func (f *fakeKafka) describe(context.Context) ([]kPartition, error) { return f.parts, f.err }
func (f *fakeKafka) close()                                         {}

func withFakeKafka(parts []kPartition, err error) func() {
	prev := openKafka
	openKafka = func(SourceConfig) (kafkaClient, error) { return &fakeKafka{parts: parts, err: err}, nil }
	return func() { openKafka = prev }
}

func kcfg() SourceConfig {
	return SourceConfig{Type: "kafka", Name: "events", System: "kafka",
		Brokers: []string{"b:9092"}, Topic: "events", ConsumerGroup: "g1",
		SLAThresholdSeconds: 60}
}

func readingByPartition(rs []reading, p string) (reading, bool) {
	for _, r := range rs {
		if r.partition == p {
			return r, true
		}
	}
	return reading{}, false
}

func TestKafkaPerPartitionFreshnessAndLag(t *testing.T) {
	nowMs := int64(fixedNow) * 1000
	defer withFakeKafka([]kPartition{
		{partition: 0, endOffset: 100, maxTimestampMs: nowMs - 10_000, committed: 95, hasCommitted: true},   // 10s old, 5 behind
		{partition: 1, endOffset: 200, maxTimestampMs: nowMs - 120_000, committed: 150, hasCommitted: true}, // 120s old, 50 behind
	}, nil)()

	rs := scrapeKafka(context.Background(), kcfg(), time.Unix(int64(fixedNow), 0))
	if len(rs) != 2 {
		t.Fatalf("expected 2 partition readings, got %d", len(rs))
	}
	p0, _ := readingByPartition(rs, "0")
	if !p0.ok || p0.ageSeconds < 9 || p0.ageSeconds > 11 {
		t.Fatalf("p0 age ~10 expected, got %v", p0.ageSeconds)
	}
	if !p0.hasRecordsBehind || p0.recordsBehind != 5 {
		t.Fatalf("p0 records_behind=5 expected, got %v", p0.recordsBehind)
	}
	if p0.method != "consumer_lag" {
		t.Fatalf("method=%q", p0.method)
	}
	p1, _ := readingByPartition(rs, "1")
	if p1.ageSeconds < 119 || p1.ageSeconds > 121 || p1.recordsBehind != 50 {
		t.Fatalf("p1 age~120/behind50 expected, got age=%v behind=%v", p1.ageSeconds, p1.recordsBehind)
	}
}

func TestKafkaEmptyPartitionIsVisibleError(t *testing.T) {
	defer withFakeKafka([]kPartition{
		{partition: 0, endOffset: 0, empty: true},
	}, nil)()
	rs := scrapeKafka(context.Background(), kcfg(), time.Unix(int64(fixedNow), 0))
	if len(rs) != 1 || rs[0].ok {
		t.Fatalf("expected 1 error reading, got %+v", rs)
	}
	if rs[0].errType != "empty_partition" {
		t.Fatalf("expected empty_partition, got %q", rs[0].errType)
	}
}

func TestKafkaNoConsumerGroupOmitsLag(t *testing.T) {
	nowMs := int64(fixedNow) * 1000
	defer withFakeKafka([]kPartition{
		{partition: 0, endOffset: 10, maxTimestampMs: nowMs - 5_000, hasCommitted: false},
	}, nil)()
	cfg := kcfg()
	cfg.ConsumerGroup = ""
	rs := scrapeKafka(context.Background(), cfg, time.Unix(int64(fixedNow), 0))
	if !rs[0].ok || rs[0].hasRecordsBehind {
		t.Fatalf("expected age-only reading without lag, got %+v", rs[0])
	}
	if rs[0].ageSeconds < 4 || rs[0].ageSeconds > 6 {
		t.Fatalf("age ~5 expected, got %v", rs[0].ageSeconds)
	}
}

func TestKafkaDescribeError(t *testing.T) {
	defer withFakeKafka(nil, errors.New("broker down"))()
	rs := scrapeKafka(context.Background(), kcfg(), time.Unix(int64(fixedNow), 0))
	if len(rs) != 1 || rs[0].ok || rs[0].errType != "describe_failed" {
		t.Fatalf("expected describe_failed, got %+v", rs)
	}
}

func TestKafkaConnectError(t *testing.T) {
	prev := openKafka
	openKafka = func(SourceConfig) (kafkaClient, error) { return nil, errors.New("no client") }
	defer func() { openKafka = prev }()
	rs := scrapeKafka(context.Background(), kcfg(), time.Unix(int64(fixedNow), 0))
	if len(rs) != 1 || rs[0].errType != "connect_failed" {
		t.Fatalf("expected connect_failed, got %+v", rs)
	}
}

func TestKafkaConfigValidation(t *testing.T) {
	bad := &Config{CollectionInterval: time.Second, Sources: []SourceConfig{{Type: "kafka", Name: "x"}}}
	if err := bad.Validate(); err == nil {
		t.Fatal("expected error: kafka needs brokers+topic")
	}
	badAuth := &Config{CollectionInterval: time.Second, Sources: []SourceConfig{
		{Type: "kafka", Name: "x", Brokers: []string{"b"}, Topic: "t", SASLMechanism: "bogus"}}}
	if err := badAuth.Validate(); err == nil {
		t.Fatal("expected error: bad sasl_mechanism")
	}
	good := &Config{CollectionInterval: time.Second, Sources: []SourceConfig{
		{Type: "kafka", Name: "x", Brokers: []string{"b"}, Topic: "t", SASLMechanism: "scram-sha-512"}}}
	if err := good.Validate(); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
}

func TestKafkaConsumerTimeLag(t *testing.T) {
	nowMs := int64(fixedNow) * 1000
	defer withFakeKafka([]kPartition{
		{partition: 0, endOffset: 100, maxTimestampMs: nowMs - 5_000,
			committed: 90, hasCommitted: true,
			committedTsMs: nowMs - 45_000, hasCommittedTs: true}, // consumer 45s behind in time
	}, nil)()
	rs := scrapeKafka(context.Background(), kcfg(), time.Unix(int64(fixedNow), 0))
	r := rs[0]
	if !r.hasLag || r.lagSeconds < 44 || r.lagSeconds > 46 {
		t.Fatalf("expected time-lag ~45s, got %v (has=%v)", r.lagSeconds, r.hasLag)
	}
	if r.ageSeconds < 4 || r.ageSeconds > 6 {
		t.Fatalf("age (topic freshness) ~5s expected, got %v", r.ageSeconds)
	}
}
