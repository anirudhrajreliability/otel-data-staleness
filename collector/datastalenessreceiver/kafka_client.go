package datastalenessreceiver

import (
	"context"
	"crypto/tls"
	"os"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Wire the real franz-go client as the default kafkaClient factory.
func init() { openKafka = newFranzClient }

type franzClient struct {
	cl             *kgo.Client
	adm            *kadm.Client
	topic          string
	group          string
	measureTimeLag bool
	opts           []kgo.Opt // reused to build a short-lived consumer for time-lag
}

func newFranzClient(cfg SourceConfig) (kafkaClient, error) {
	opts := []kgo.Opt{kgo.SeedBrokers(cfg.Brokers...)}

	// TLS defaults on when SASL is configured; overridable via `tls`.
	useTLS := cfg.SASLMechanism != ""
	if cfg.TLS != nil {
		useTLS = *cfg.TLS
	}
	if useTLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}

	switch cfg.SASLMechanism {
	case "plain":
		opts = append(opts, kgo.SASL(plain.Auth{User: cfg.Username, Pass: cfg.Password}.AsMechanism()))
	case "scram-sha-256":
		opts = append(opts, kgo.SASL(scram.Auth{User: cfg.Username, Pass: cfg.Password}.AsSha256Mechanism()))
	case "scram-sha-512":
		opts = append(opts, kgo.SASL(scram.Auth{User: cfg.Username, Pass: cfg.Password}.AsSha512Mechanism()))
	case "aws-msk-iam":
		// Credentials from the standard AWS environment (access key/secret/
		// session token). Set AWS_REGION for SigV4. On EC2/EKS, inject creds
		// via the environment (e.g. from the instance/pod role).
		opts = append(opts, kgo.SASL(aws.ManagedStreamingIAM(func(context.Context) (aws.Auth, error) {
			return aws.Auth{
				AccessKey:    os.Getenv("AWS_ACCESS_KEY_ID"),
				SecretKey:    os.Getenv("AWS_SECRET_ACCESS_KEY"),
				SessionToken: os.Getenv("AWS_SESSION_TOKEN"),
			}, nil
		})))
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &franzClient{cl: cl, adm: kadm.NewClient(cl), topic: cfg.Topic, group: cfg.ConsumerGroup, measureTimeLag: cfg.MeasureTimeLag, opts: opts}, nil
}

func (f *franzClient) describe(ctx context.Context) ([]kPartition, error) {
	ends, err := f.adm.ListEndOffsets(ctx, f.topic)
	if err != nil {
		return nil, err
	}
	// Newest record timestamp per partition (KIP-734 MAX_TIMESTAMP); admin-only,
	// no consumer group join or record reads.
	maxts, err := f.adm.ListMaxTimestampOffsets(ctx, f.topic)
	if err != nil {
		return nil, err
	}

	var committed kadm.OffsetResponses
	if f.group != "" {
		committed, err = f.adm.FetchOffsets(ctx, f.group)
		if err != nil {
			return nil, err
		}
	}

	var parts []kPartition
	ends.Each(func(lo kadm.ListedOffset) {
		kp := kPartition{partition: lo.Partition, endOffset: lo.Offset}
		if lo.Err != nil {
			kp.err = lo.Err
			parts = append(parts, kp)
			return
		}
		if lo.Offset == 0 {
			kp.empty = true
		}
		if mt, ok := maxts.Lookup(f.topic, lo.Partition); ok && mt.Err == nil {
			kp.maxTimestampMs = mt.Timestamp
		}
		if f.group != "" {
			if co, ok := committed.Lookup(f.topic, lo.Partition); ok && co.Err == nil {
				kp.committed = co.At
				kp.hasCommitted = true
			}
		}
		parts = append(parts, kp)
	})

	if f.measureTimeLag && f.group != "" {
		f.fillCommittedTimestamps(ctx, parts)
	}
	return parts, nil
}

// fillCommittedTimestamps reads the record at the committed offset (offset-1,
// the last consumed record) for each partition to derive the consumer time-lag.
// Best-effort: failures leave time-lag unset rather than erroring the scrape.
func (f *franzClient) fillCommittedTimestamps(ctx context.Context, parts []kPartition) {
	offsets := make(map[int32]kgo.Offset)
	for _, p := range parts {
		if p.hasCommitted && p.committed > 0 {
			offsets[p.partition] = kgo.NewOffset().At(p.committed - 1)
		}
	}
	if len(offsets) == 0 {
		return
	}
	consumeOpts := append([]kgo.Opt{}, f.opts...)
	consumeOpts = append(consumeOpts, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{f.topic: offsets}))
	cc, err := kgo.NewClient(consumeOpts...)
	if err != nil {
		return
	}
	defer cc.Close()

	tsByPart := make(map[int32]int64)
	remaining := len(offsets)
	// Bounded: give up after a small number of polls so a partition whose
	// committed offset is unreadable (e.g. aged out by retention) cannot make us
	// read a large volume from the other partitions until the context deadline.
	const maxPolls = 50
	for polls := 0; remaining > 0 && polls < maxPolls; polls++ {
		fs := cc.PollFetches(ctx)
		if fs.IsClientClosed() || ctx.Err() != nil {
			break
		}
		var perr bool
		fs.EachError(func(string, int32, error) { perr = true })
		fs.EachRecord(func(r *kgo.Record) {
			if _, seen := tsByPart[r.Partition]; !seen {
				tsByPart[r.Partition] = r.Timestamp.UnixMilli()
				remaining--
			}
		})
		if perr || fs.NumRecords() == 0 {
			break
		}
	}
	for i := range parts {
		if ts, ok := tsByPart[parts[i].partition]; ok {
			parts[i].committedTsMs = ts
			parts[i].hasCommittedTs = true
		}
	}
}

func (f *franzClient) close() {
	if f.cl != nil {
		f.cl.Close()
	}
}
