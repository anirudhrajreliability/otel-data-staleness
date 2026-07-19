#!/usr/bin/env bash
# Registers two schema versions for subject "orders-value" in Confluent Schema
# Registry. The Collector's schema_registry source pins documented_version="1",
# so with latest=2 it reports 1 version behind (version_drift, records.behind=1).
set -e
SR=http://schema-registry:8081
echo "sr-seed: waiting for Schema Registry..."
until curl -sf "$SR/subjects" >/dev/null 2>&1; do sleep 3; done

reg() {  # $1 = escaped avro schema string
  curl -sf -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data "{\"schema\": \"$1\"}" "$SR/subjects/orders-value/versions" >/dev/null
}

# v1: {id}
reg '{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}'
# v2: add an optional field (backward compatible) -> new version
reg '{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"total\",\"type\":[\"null\",\"double\"],\"default\":null}]}'

echo "sr-seed: registered versions ->"
curl -s "$SR/subjects/orders-value/versions" || true
echo
