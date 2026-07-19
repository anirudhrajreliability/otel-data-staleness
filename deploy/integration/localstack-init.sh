#!/usr/bin/env bash
# Creates a real Kinesis stream in LocalStack and puts records, so the Collector's
# kinesis scraper (endpoint override -> LocalStack) measures real stream freshness.
# Runs on the amazon/aws-cli image (aws + base64, no curl needed).
set -e
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
EP=http://localstack:4566
echo "localstack-init: waiting for LocalStack Kinesis..."
for i in $(seq 1 40); do
  if aws --endpoint-url=$EP kinesis create-stream --stream-name clickstream --shard-count 1 2>/dev/null; then
    echo "  stream create issued"; break
  fi
  if aws --endpoint-url=$EP kinesis describe-stream-summary --stream-name clickstream >/dev/null 2>&1; then
    echo "  stream already exists"; break
  fi
  sleep 3
done
echo "localstack-init: waiting for stream ACTIVE..."
for i in $(seq 1 30); do
  st=$(aws --endpoint-url=$EP kinesis describe-stream-summary --stream-name clickstream \
        --query 'StreamDescriptionSummary.StreamStatus' --output text 2>/dev/null || echo "")
  [ "$st" = "ACTIVE" ] && break
  sleep 2
done
echo "localstack-init: putting records into clickstream"
for i in $(seq 1 10); do
  aws --endpoint-url=$EP kinesis put-record --stream-name clickstream \
    --partition-key "pk-$i" --data "$(printf 'event-%s' "$i" | base64)" >/dev/null 2>&1 || true
done
echo "localstack-init: done (clickstream has recent records)"
