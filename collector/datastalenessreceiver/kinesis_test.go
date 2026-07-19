package datastalenessreceiver

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeKinesis struct {
	shards []kShard
	err    error
}

func (f *fakeKinesis) describe(context.Context) ([]kShard, error) { return f.shards, f.err }
func (f *fakeKinesis) close()                                     {}

func withFakeKinesis(shards []kShard, err error) func() {
	prev := openKinesis
	openKinesis = func(SourceConfig) (kinesisClient, error) { return &fakeKinesis{shards: shards, err: err}, nil }
	return func() { openKinesis = prev }
}

func kincfg() SourceConfig {
	return SourceConfig{Type: "kinesis", Name: "events", System: "kinesis",
		StreamName: "events", AWSRegion: "us-east-1", SLAThresholdSeconds: 60}
}

func TestKinesisPerShardFreshness(t *testing.T) {
	nowMs := int64(fixedNow) * 1000
	defer withFakeKinesis([]kShard{
		{shardID: "shardId-000000000000", latestArrivalMs: nowMs - 8_000},   // 8s old
		{shardID: "shardId-000000000001", latestArrivalMs: nowMs - 200_000}, // 200s old
	}, nil)()
	rs := scrapeKinesis(context.Background(), kincfg(), time.Unix(int64(fixedNow), 0))
	if len(rs) != 2 {
		t.Fatalf("expected 2 shard readings, got %d", len(rs))
	}
	s0, _ := readingByPartition(rs, "shardId-000000000000")
	if !s0.ok || s0.ageSeconds < 7 || s0.ageSeconds > 9 {
		t.Fatalf("shard0 age ~8 expected, got %v", s0.ageSeconds)
	}
	if s0.method != "max_timestamp" {
		t.Fatalf("method=%q", s0.method)
	}
	s1, _ := readingByPartition(rs, "shardId-000000000001")
	if s1.ageSeconds < 199 || s1.ageSeconds > 201 {
		t.Fatalf("shard1 age ~200 expected, got %v", s1.ageSeconds)
	}
}

func TestKinesisNoRecentRecordsIsVisible(t *testing.T) {
	defer withFakeKinesis([]kShard{
		{shardID: "shardId-000000000000", empty: true},
	}, nil)()
	rs := scrapeKinesis(context.Background(), kincfg(), time.Unix(int64(fixedNow), 0))
	if len(rs) != 1 || rs[0].ok || rs[0].errType != "no_recent_records" {
		t.Fatalf("expected no_recent_records error, got %+v", rs)
	}
}

func TestKinesisDescribeError(t *testing.T) {
	defer withFakeKinesis(nil, errors.New("throttled"))()
	rs := scrapeKinesis(context.Background(), kincfg(), time.Unix(int64(fixedNow), 0))
	if len(rs) != 1 || rs[0].errType != "describe_failed" {
		t.Fatalf("expected describe_failed, got %+v", rs)
	}
}

func TestKinesisConnectError(t *testing.T) {
	prev := openKinesis
	openKinesis = func(SourceConfig) (kinesisClient, error) { return nil, errors.New("no creds") }
	defer func() { openKinesis = prev }()
	rs := scrapeKinesis(context.Background(), kincfg(), time.Unix(int64(fixedNow), 0))
	if len(rs) != 1 || rs[0].errType != "connect_failed" {
		t.Fatalf("expected connect_failed, got %+v", rs)
	}
}

func TestKinesisConfigValidation(t *testing.T) {
	bad := &Config{CollectionInterval: time.Second, Sources: []SourceConfig{{Type: "kinesis", Name: "x"}}}
	if err := bad.Validate(); err == nil {
		t.Fatal("expected error: kinesis needs stream_name + aws_region")
	}
	good := &Config{CollectionInterval: time.Second, Sources: []SourceConfig{
		{Type: "kinesis", Name: "x", StreamName: "s", AWSRegion: "us-east-1"}}}
	if err := good.Validate(); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
}
