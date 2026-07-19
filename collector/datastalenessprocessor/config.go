package datastalenessprocessor

import (
	"fmt"
	"time"
)

// SLARule defines a freshness SLA for sources matching the given selectors.
// Empty selector fields act as wildcards. The first matching rule wins; if no
// rule matches, DefaultThreshold (when > 0) is used.
type SLARule struct {
	SourceSystem string        `mapstructure:"source_system"`
	SourceName   string        `mapstructure:"source_name"`
	Threshold    time.Duration `mapstructure:"threshold"`
}

// Config controls how the processor derives and evaluates data-staleness
// metrics defined by the data-staleness semantic conventions.
type Config struct {
	// ComputeAgeFromLastUpdate derives data.staleness.age from incoming
	// data.staleness.last_update.timestamp points (age = now - timestamp),
	// so lightweight producers can emit only a timestamp.
	ComputeAgeFromLastUpdate bool `mapstructure:"compute_age_from_last_update"`

	// EvaluateSLA emits data.staleness.sla.breached (and the threshold gauge)
	// for points whose age exceeds the matching SLA threshold.
	EvaluateSLA bool `mapstructure:"evaluate_sla"`

	// DefaultThreshold applies when no SLA rule matches a source.
	DefaultThreshold time.Duration `mapstructure:"default_threshold"`

	// SLAs is an ordered list of per-source threshold rules.
	SLAs []SLARule `mapstructure:"slas"`
}

func (c *Config) Validate() error {
	if c.DefaultThreshold < 0 {
		return fmt.Errorf("default_threshold must be >= 0")
	}
	for i, r := range c.SLAs {
		if r.Threshold <= 0 {
			return fmt.Errorf("slas[%d].threshold must be > 0", i)
		}
	}
	return nil
}

// thresholdFor returns the configured threshold (seconds) for a source, and
// whether one was found.
func (c *Config) thresholdFor(system, name string) (float64, bool) {
	for _, r := range c.SLAs {
		if (r.SourceSystem == "" || r.SourceSystem == system) &&
			(r.SourceName == "" || r.SourceName == name) {
			return r.Threshold.Seconds(), true
		}
	}
	if c.DefaultThreshold > 0 {
		return c.DefaultThreshold.Seconds(), true
	}
	return 0, false
}
