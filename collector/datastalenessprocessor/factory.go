package datastalenessprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

var componentType = component.MustNewType("datastaleness")

// NewFactory returns a factory for the data-staleness processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		componentType,
		createDefaultConfig,
		processor.WithMetrics(createMetricsProcessor, component.StabilityLevelDevelopment),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		ComputeAgeFromLastUpdate: true,
		EvaluateSLA:              true,
		DefaultThreshold:         5 * time.Minute,
	}
}

func createMetricsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	next consumer.Metrics,
) (processor.Metrics, error) {
	pCfg := cfg.(*Config)
	sp := newProcessor(pCfg, set.Logger)
	return processorhelper.NewMetricsProcessor(
		ctx, set, cfg, next,
		sp.processMetrics,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}),
	)
}
