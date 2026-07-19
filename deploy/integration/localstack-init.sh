#!/bin/bash
# LocalStack init hook. Runs INSIDE the localstack container when Kinesis is
# ready (mounted at /etc/localstack/init/ready.d/). Creates the stream and puts
# records so the Collector's kinesis scraper (endpoint -> LocalStack) measures
# real stream freshness. `awslocal` targets LocalStack with dummy creds itself,
# so no endpoint flag or credentials are needed here.
set -e
awslocal kinesis create-stream --stream-name clickstream --shard-count 1 || true
for i in $(seq 1 30); do
  st=$(awslocal kinesis describe-stream-summary --stream-name clickstream \
        --query 'StreamDescriptionSummary.StreamStatus' --output text 2>/dev/null || echo "")
  [ "$st" = "ACTIVE" ] && break
  sleep 1
done
for i in $(seq 1 10); do
  awslocal kinesis put-record --stream-name clickstream \
    --partition-key "pk-$i" --data "event-$i" >/dev/null 2>&1 || true
done
echo "init-kinesis: clickstream ready with records"
