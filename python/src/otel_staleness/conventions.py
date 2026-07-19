"""Constants for the Data Staleness semantic conventions (v0.4.0).

These mirror ``spec/semantic-conventions.md`` and are the single source of
truth for metric names, units and attribute keys used by this SDK and by the
companion OpenTelemetry Collector processor.
"""

CONVENTION_VERSION = "0.4.0"

# --- Metric names ----------------------------------------------------------
METRIC_AGE = "data.staleness.age"
METRIC_LAG = "data.staleness.lag"
METRIC_LAST_UPDATE = "data.staleness.last_update.timestamp"
METRIC_RECORDS_BEHIND = "data.staleness.records.behind"
METRIC_SLA_THRESHOLD = "data.staleness.sla.threshold"
METRIC_SLA_BREACHED = "data.staleness.sla.breached"
METRIC_SLA_BREACHES = "data.staleness.sla.breaches"
METRIC_PROBE_ERRORS = "data.staleness.probe.errors"
METRIC_UPDATE_INTERVAL = "data.staleness.update.interval"
METRIC_AGE_PEAK = "data.staleness.age.peak"
METRIC_PARTITION_SKEW = "data.staleness.partition.skew"

# --- Units (UCUM) ----------------------------------------------------------
UNIT_SECONDS = "s"
UNIT_RECORDS = "{record}"
UNIT_BOOL = "1"
UNIT_BREACH = "{breach}"
UNIT_ERROR = "{error}"

# --- Attribute keys --------------------------------------------------------
ATTR_SOURCE_SYSTEM = "data.source.system"
ATTR_SOURCE_NAME = "data.source.name"
ATTR_SOURCE_NAMESPACE = "data.source.namespace"
ATTR_METHOD = "data.staleness.method"
ATTR_PARTITION = "data.staleness.partition"
ATTR_PIPELINE_STAGE = "data.pipeline.stage"
ATTR_RELATIVE_TO = "data.staleness.relative_to"
ATTR_VERSION_DOCUMENTED = "data.staleness.version.documented"
ATTR_VERSION_CURRENT = "data.staleness.version.current"
ATTR_ERROR_TYPE = "error.type"


# --- Enumerations ----------------------------------------------------------
class Method:
    MAX_TIMESTAMP = "max_timestamp"
    WATERMARK = "watermark"
    CONSUMER_LAG = "consumer_lag"
    RUN_COMPLETION = "run_completion"
    OBJECT_MTIME = "object_mtime"
    TTL_AGE = "ttl_age"
    HEARTBEAT = "heartbeat"
    WRITETIME = "writetime"
    SNAPSHOT = "snapshot"
    REPLICATION_LAG = "replication_lag"
    INDEX_LAG = "index_lag"
    VERSION_DRIFT = "version_drift"
    END_TO_END = "end_to_end"


class System:
    # relational / warehouse
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SNOWFLAKE = "snowflake"
    REDSHIFT = "redshift"
    BIGQUERY = "bigquery"
    # streaming
    KAFKA = "kafka"
    KINESIS = "kinesis"
    # pipelines
    DBT = "dbt"
    AIRFLOW = "airflow"
    # caches / object storage
    REDIS = "redis"
    S3 = "s3"
    GCS = "gcs"
    # nosql / graph
    MONGODB = "mongodb"
    DYNAMODB = "dynamodb"
    CASSANDRA = "cassandra"
    COUCHBASE = "couchbase"
    NEO4J = "neo4j"
    # time-series
    INFLUXDB = "influxdb"
    TIMESCALEDB = "timescaledb"
    # lakehouse table formats
    ICEBERG = "iceberg"
    DELTA = "delta"
    HUDI = "hudi"
    # search / vector
    ELASTICSEARCH = "elasticsearch"
    OPENSEARCH = "opensearch"
    PINECONE = "pinecone"
    WEAVIATE = "weaviate"
    MILVUS = "milvus"
    QDRANT = "qdrant"
    PGVECTOR = "pgvector"
    # feature store / cdc / feeds
    FEAST = "feast"
    DEBEZIUM = "debezium"
    HTTP = "http"
    DOCS = "docs"
