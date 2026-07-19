package datastalenessreceiver

import (
	"context"
	"fmt"
	"time"
)

// kPartition is a normalized per-partition snapshot from a Kafka cluster.
type kPartition struct {
	partition      int32
	endOffset      int64 // log-end offset (next offset to be written)
	maxTimestampMs int64 // timestamp of the newest record (ms); <=0 = unknown
	committed      int64 // consumer-group committed offset
	hasCommitted   bool
	committedTsMs  int64 // timestamp of the record at the committed offset (ms)
	hasCommittedTs bool
	empty          bool  // partition has no records
	err            error // per-partition load error
}

// kafkaClient abstracts the cluster so the freshness LOGIC is testable without
// a broker. The real implementation is franz-go (see kafka_client.go).
type kafkaClient interface {
	describe(ctx context.Context) ([]kPartition, error)
	close()
}

// openKafka builds a real client; overridden in tests with a fake. Replaced by
// the franz-go implementation in kafka_client.go via init().
var openKafka = func(cfg SourceConfig) (kafkaClient, error) {
	return nil, fmt.Errorf("kafka client not built into this collector")
}

// scrapeKafka measures freshness (age from the newest record timestamp) and
// consumer lag (records.behind) per partition. It returns one reading per
// partition; the topic-level aggregate (max age, total backlog) is left to the
// backend to compute, per the convention's "derive, don't emit" principle.
func scrapeKafka(ctx context.Context, cfg SourceConfig, now time.Time) []reading {
	method := cfg.Method
	if method == "" {
		method = "consumer_lag"
	}
	client, err := openKafka(cfg)
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

	parts, err := client.describe(dctx)
	if err != nil {
		et := "describe_failed"
		if dctx.Err() == context.DeadlineExceeded {
			et = "timeout"
		}
		return []reading{{cfg: cfg, method: method, errType: et}}
	}
	if len(parts) == 0 {
		return []reading{{cfg: cfg, method: method, errType: "no_partitions"}}
	}

	out := make([]reading, 0, len(parts))
	for _, p := range parts {
		pstr := fmt.Sprintf("%d", p.partition)
		if p.err != nil {
			out = append(out, reading{cfg: cfg, method: method, partition: pstr, errType: "partition_error"})
			continue
		}
		// An empty partition has no record to age; report it as a visible
		// condition rather than a fabricated freshness value.
		if p.empty || p.maxTimestampMs <= 0 {
			out = append(out, reading{cfg: cfg, method: method, partition: pstr, errType: "empty_partition"})
			continue
		}
		r := resolve(cfg, now, float64(p.maxTimestampMs)/1000.0, true)
		r.method = method
		r.partition = pstr
		if p.hasCommitted {
			behind := p.endOffset - p.committed
			if behind < 0 {
				behind = 0
			}
			r.recordsBehind = behind
			r.hasRecordsBehind = true
		}
		if p.hasCommittedTs && p.committedTsMs > 0 {
			lag := float64(now.UnixNano())/1e9 - float64(p.committedTsMs)/1000.0
			if lag < 0 {
				lag = 0
			}
			r.lagSeconds = lag
			r.hasLag = true
		}
		out = append(out, r)
	}
	return out
}
