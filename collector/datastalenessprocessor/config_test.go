package datastalenessprocessor

import (
	"testing"
	"time"
)

func TestConfigValidate(t *testing.T) {
	c := &Config{DefaultThreshold: time.Minute, SLAs: []SLARule{{SourceName: "orders", Threshold: 30 * time.Second}}}
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bad := &Config{SLAs: []SLARule{{Threshold: 0}}}
	if err := bad.Validate(); err == nil {
		t.Fatal("expected error for zero threshold")
	}
	badDef := &Config{DefaultThreshold: -1}
	if err := badDef.Validate(); err == nil {
		t.Fatal("expected error for negative default_threshold")
	}
}

func TestThresholdFor(t *testing.T) {
	c := &Config{
		DefaultThreshold: 5 * time.Minute,
		SLAs: []SLARule{
			{SourceSystem: "kafka", Threshold: 30 * time.Second},
			{SourceName: "orders", Threshold: 10 * time.Second},
		},
	}
	if v, ok := c.thresholdFor("kafka", "anything"); !ok || v != 30 {
		t.Fatalf("kafka rule: got %v %v", v, ok)
	}
	if v, ok := c.thresholdFor("postgresql", "orders"); !ok || v != 10 {
		t.Fatalf("name rule: got %v %v", v, ok)
	}
	if v, ok := c.thresholdFor("mysql", "users"); !ok || v != 300 {
		t.Fatalf("default: got %v %v", v, ok)
	}
	noDef := &Config{}
	if _, ok := noDef.thresholdFor("x", "y"); ok {
		t.Fatal("expected no threshold when none configured")
	}
}
