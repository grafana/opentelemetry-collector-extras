package partialtracesamplerprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

type partialTraceSampler struct {
	logger       *zap.Logger
	nextConsumer consumer.Traces
}

func newPartialTraceSampler(
	set processor.Settings,
	_ *Config,
	nextConsumer consumer.Traces,
) *partialTraceSampler {
	return &partialTraceSampler{
		logger:       set.Logger,
		nextConsumer: nextConsumer,
	}
}

func (p *partialTraceSampler) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	return p.nextConsumer.ConsumeTraces(ctx, td)
}

func (p *partialTraceSampler) Start(_ context.Context, _ component.Host) error {
	return nil
}

func (p *partialTraceSampler) Shutdown(_ context.Context) error {
	return nil
}

func (p *partialTraceSampler) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
