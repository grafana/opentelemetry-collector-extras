package partialtracesamplerprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"
)

func TestProcessor(t *testing.T) {
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "test-svc")
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("test-span")
	span.SetTraceID([16]byte{1})
	span.SetSpanID([8]byte{1})

	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)

	require.Len(t, sink.AllTraces(), 1)
	got := sink.AllTraces()[0]
	assert.Equal(t, 1, got.SpanCount())
	assert.Equal(t, "test-span", got.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name())
}

func TestProcessorCapabilities(t *testing.T) {
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	assert.False(t, proc.Capabilities().MutatesData)
}

func TestProcessorStartShutdown(t *testing.T) {
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	require.NoError(t, proc.Start(context.Background(), nil))
	require.NoError(t, proc.Shutdown(context.Background()))
}
