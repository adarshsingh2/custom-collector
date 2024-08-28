package metricsreceiver

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

var (
	typeStr = component.MustNewType("metricsreceiver")
)

func createDefaultConfig() component.Config {
	return &Config{
		Endpoint: string(""),
	}
}

func createMetricsReceiver(_ context.Context, params receiver.Settings, baseCfg component.Config, consumer consumer.Metrics) (receiver.Metrics, error) {

	logger := params.Logger
	metricsCfg := baseCfg.(*Config)

	metricsReceiver := &metricsReceiver{
		logger:       logger,
		nextConsumer: consumer,
		config:       metricsCfg,
	}

	return metricsReceiver, nil
}

// NewFactory creates a factory for metrics receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		typeStr,
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiver, component.StabilityLevelAlpha))
}
