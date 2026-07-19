package datastalenessreceiver

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func init() { openKinesis = newKinesisClient }

type kinClient struct {
	cl       *kinesis.Client
	stream   string
	lookback time.Duration
	now      func() time.Time // injectable for determinism
}

func newKinesisClient(cfg SourceConfig) (kinesisClient, error) {
	// Standard AWS credential chain (env, shared config, or the EC2/EKS role via
	// IMDS/IRSA); region from config.
	awscfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(cfg.AWSRegion))
	if err != nil {
		return nil, err
	}
	lb := cfg.Lookback
	if lb <= 0 {
		lb = time.Hour
	}
	return &kinClient{cl: kinesis.NewFromConfig(awscfg), stream: cfg.StreamName, lookback: lb, now: time.Now}, nil
}

func (k *kinClient) describe(ctx context.Context) ([]kShard, error) {
	// List ALL shards, following pagination (streams can exceed one page).
	var allShards []types.Shard
	input := &kinesis.ListShardsInput{StreamName: aws.String(k.stream)}
	for {
		ls, err := k.cl.ListShards(ctx, input)
		if err != nil {
			return nil, err
		}
		allShards = append(allShards, ls.Shards...)
		if ls.NextToken == nil || *ls.NextToken == "" {
			break
		}
		// When paging, ListShards requires NextToken WITHOUT StreamName.
		input = &kinesis.ListShardsInput{NextToken: ls.NextToken}
	}

	nowFn := k.now
	if nowFn == nil {
		nowFn = time.Now
	}
	start := nowFn().Add(-k.lookback)

	var shards []kShard
	for _, sh := range allShards {
		ks := kShard{shardID: aws.ToString(sh.ShardId)}

		itOut, err := k.cl.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
			StreamName:        aws.String(k.stream),
			ShardId:           sh.ShardId,
			ShardIteratorType: types.ShardIteratorTypeAtTimestamp,
			Timestamp:         &start,
		})
		if err != nil {
			ks.err = err
			shards = append(shards, ks)
			continue
		}

		it := itOut.ShardIterator
		var latest int64
		// Read forward toward the tip, bounded by an iteration cap, the context
		// timeout, and a "caught up" check, tracking the newest arrival time.
		for i := 0; it != nil && i < 20; i++ {
			out, err := k.cl.GetRecords(ctx, &kinesis.GetRecordsInput{ShardIterator: it, Limit: aws.Int32(1000)})
			if err != nil {
				if latest == 0 {
					ks.err = err
				}
				break
			}
			for j := range out.Records {
				if t := out.Records[j].ApproximateArrivalTimestamp; t != nil {
					if ms := t.UnixMilli(); ms > latest {
						latest = ms
					}
				}
			}
			it = out.NextShardIterator
			// Caught up to within 1s of the tip, or nothing more to read.
			if len(out.Records) == 0 || aws.ToInt64(out.MillisBehindLatest) < 1000 {
				break
			}
			if ctx.Err() != nil {
				break
			}
		}
		ks.latestArrivalMs = latest
		ks.empty = latest == 0
		shards = append(shards, ks)
	}
	return shards, nil
}

func (k *kinClient) close() {}
