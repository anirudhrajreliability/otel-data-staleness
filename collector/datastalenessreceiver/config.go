package datastalenessreceiver

import (
	"fmt"
	"time"
)

// SourceConfig describes one data source to scrape for freshness.
//
// Type selects the scraper:
//   - "static": uses LastUpdateEpoch / AgeSeconds from config (demo/testing).
//   - "file":   uses the modification time of Path as the last-update time.
//   - "http":   GETs URL and reads the Last-Modified header (or epoch body).
//   - "sql":    measures freshness from a database (see the SQL fields).
type SourceConfig struct {
	Type      string `mapstructure:"type"`
	Name      string `mapstructure:"name"`
	System    string `mapstructure:"system"`
	Namespace string `mapstructure:"namespace"`

	SLAThresholdSeconds float64 `mapstructure:"sla_threshold_seconds"`

	// static
	LastUpdateEpoch float64 `mapstructure:"last_update_epoch"`
	AgeSeconds      float64 `mapstructure:"age_seconds"`
	// file
	Path string `mapstructure:"path"`
	// http
	URL string `mapstructure:"url"`

	// --- sql -------------------------------------------------------------
	// Driver/DSN identify the database (driver must be registered in the
	// build; postgres and mysql are registered by default, see drivers.go).
	Driver string `mapstructure:"driver"`
	DSN    string `mapstructure:"dsn"`
	// Mode A (convenience): SELECT MAX(TimestampColumn) FROM [Namespace.]Table.
	Table           string `mapstructure:"table"`
	TimestampColumn string `mapstructure:"timestamp_column"`
	// Mode B (trustworthy): a custom query returning one row of
	//   (freshness_timestamp)  or  (freshness_timestamp, records_behind).
	// Point this at a load-audit/watermark table rather than scanning data.
	Query string `mapstructure:"query"`
	// QueryTimeout bounds each query (default 5s).
	QueryTimeout time.Duration `mapstructure:"query_timeout"`
	// AssumeLocalTime interprets naive DB timestamps in the server's local
	// zone instead of UTC. Default (false) = treat naive timestamps as UTC,
	// which is the safe default for warehouse `updated_at` columns.
	AssumeLocalTime bool `mapstructure:"assume_local_time"`
	// Method overrides the emitted data.staleness.method attribute.
	Method string `mapstructure:"method"`

	// --- kafka -----------------------------------------------------------
	Brokers       []string `mapstructure:"brokers"`
	Topic         string   `mapstructure:"topic"`
	ConsumerGroup string   `mapstructure:"consumer_group"`
	// Security: sasl_mechanism one of "", "plain", "scram-sha-256",
	// "scram-sha-512", "aws-msk-iam". TLS is enabled automatically for SASL
	// unless tls is explicitly set.
	SASLMechanism string `mapstructure:"sasl_mechanism"`
	Username      string `mapstructure:"username"`
	Password      string `mapstructure:"password"`
	TLS           *bool  `mapstructure:"tls"`
	// MeasureTimeLag reads the record at the committed offset to emit
	// data.staleness.lag (now - that record's timestamp). Does one fetch per
	// partition; off by default to keep the scraper admin-only.
	MeasureTimeLag bool `mapstructure:"measure_time_lag"`

	// --- kinesis ---------------------------------------------------------
	StreamName string `mapstructure:"stream_name"`
	AWSRegion  string `mapstructure:"aws_region"`
	// Lookback bounds how far back the scraper reads to find the newest record
	// (default 1h). Set it comfortably larger than the freshness SLA.
	Lookback time.Duration `mapstructure:"lookback"`

	// --- schema_registry (Confluent Schema Registry version drift) -------
	RegistryURL       string `mapstructure:"registry_url"`
	Subject           string `mapstructure:"subject"`
	DocumentedVersion string `mapstructure:"documented_version"` // pinned/contract version

	// --- db_migration (applied schema-migration version drift) -----------
	VersionQuery   string `mapstructure:"version_query"`   // SQL returning the applied version
	CurrentVersion string `mapstructure:"current_version"` // expected latest version
}

// Config is the receiver configuration.
type Config struct {
	CollectionInterval time.Duration  `mapstructure:"collection_interval"`
	Sources            []SourceConfig `mapstructure:"sources"`
}

func (c *Config) Validate() error {
	if c.CollectionInterval <= 0 {
		return fmt.Errorf("collection_interval must be > 0")
	}
	if len(c.Sources) == 0 {
		return fmt.Errorf("at least one source must be configured")
	}
	for i, s := range c.Sources {
		if s.Name == "" {
			return fmt.Errorf("sources[%d].name is required", i)
		}
		switch s.Type {
		case "static":
			if s.LastUpdateEpoch == 0 && s.AgeSeconds == 0 {
				return fmt.Errorf("sources[%d]: static requires last_update_epoch or age_seconds", i)
			}
		case "file":
			if s.Path == "" {
				return fmt.Errorf("sources[%d]: file requires path", i)
			}
		case "http":
			if s.URL == "" {
				return fmt.Errorf("sources[%d]: http requires url", i)
			}
		case "schema_registry":
			if s.RegistryURL == "" || s.Subject == "" || s.DocumentedVersion == "" {
				return fmt.Errorf("sources[%d]: schema_registry requires registry_url, subject, documented_version", i)
			}
		case "db_migration":
			if s.Driver == "" || s.DSN == "" || s.VersionQuery == "" || s.CurrentVersion == "" {
				return fmt.Errorf("sources[%d]: db_migration requires driver, dsn, version_query, current_version", i)
			}
		case "kinesis":
			if s.StreamName == "" || s.AWSRegion == "" {
				return fmt.Errorf("sources[%d]: kinesis requires stream_name and aws_region", i)
			}
		case "kafka":
			if len(s.Brokers) == 0 || s.Topic == "" {
				return fmt.Errorf("sources[%d]: kafka requires brokers and topic", i)
			}
			switch s.SASLMechanism {
			case "", "plain", "scram-sha-256", "scram-sha-512", "aws-msk-iam":
			default:
				return fmt.Errorf("sources[%d]: unknown sasl_mechanism %q", i, s.SASLMechanism)
			}
		case "sql":
			if s.Driver == "" || s.DSN == "" {
				return fmt.Errorf("sources[%d]: sql requires driver and dsn", i)
			}
			if s.Query == "" && (s.Table == "" || s.TimestampColumn == "") {
				return fmt.Errorf("sources[%d]: sql requires either query, or table + timestamp_column", i)
			}
		default:
			return fmt.Errorf("sources[%d].type %q is not one of static|file|http|sql|kafka|kinesis|schema_registry|db_migration", i, s.Type)
		}
	}
	return nil
}
