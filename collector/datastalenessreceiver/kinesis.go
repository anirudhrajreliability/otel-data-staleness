package datastalenessreceiver

import (
	"context"
	"fmt"
	"time"
)

// kShard is a normalized per-shard freshness snapshot from a Kinesis stream.
type kShard struct {
	shardID         string
	latestArrivalMs int64 // newest record ApproximateArrivalTimestamp (ms); <=0 = none in window
	empty           bool  // no records within the lookback window
	err             error
}

// kinesisClient abstracts the stream so the freshness LOGIC is testable without
// AWS. The real implementation uses aws-sdk-go-v2 (see kinesis_client.go).
type kinesisClient interface {
	describe(ctx context.Context) ([]kShard, error)
	close()
}

var openKinesis = func(cfg SourceConfig) (kinesisClient, error) {
	return nil, fmt.Errorf("kinesis client not built into this collector")
}

// scrapeKinesis measures stream freshness per shard (age from the newest
// record's arrival time). Kinesis does not expose consumer position without the
// KCL checkpoint store, so consumer lag is out of scope here; the honest,
// probe-derivable signal is stream freshness.
func scrapeKinesis(ctx context.Context, cfg SourceConfig, now time.Time) []reading {
	method := cfg.Method
	if method == "" {
		method = "max_timestamp"
	}
	client, err := openKinesis(cfg)
	if err != nil {
		return []reading{{cfg: cfg, method: method, errType: "connect_failed"}}
	}
	defer client.close()

	timeout := cfg.QueryTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	dctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	shards, err := client.describe(dctx)
	if err != nil {
		et := "describe_failed"
		if dctx.Err() == context.DeadlineExceeded {
			et = "timeout"
		}
		return []reading{{cfg: cfg, method: method, errType: et}}
	}
	if len(shards) == 0 {
		return []reading{{cfg: cfg, method: method, errType: "no_shards"}}
	}

	out := make([]reading, 0, len(shards))
	for _, sh := range shards {
		if sh.err != nil {
			out = append(out, reading{cfg: cfg, method: method, partition: sh.shardID, errType: "shard_error"})
			continue
		}
		// No record within the lookback window: we cannot state an exact age
		// (the newest record is older than lookback), so this is a visible
		// condition, not a fabricated value. It itself signals staleness.
		if sh.empty || sh.latestArrivalMs <= 0 {
			out = append(out, reading{cfg: cfg, method: method, partition: sh.shardID, errType: "no_recent_records"})
			continue
		}
		r := resolve(cfg, now, float64(sh.latestArrivalMs)/1000.0, true)
		r.method = method
		r.partition = sh.shardID
		out = append(out, r)
	}
	return out
}
