// Copyright Grafana Labs
// SPDX-License-Identifier: Apache-2.0

package partialtracesamplerprocessor

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"
)

func newTestTraces(spans int) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "test-svc")
	ss := rs.ScopeSpans().AppendEmpty()
	for i := 0; i < spans; i++ {
		span := ss.Spans().AppendEmpty()
		span.SetName("test-span")
		// Use a unique trace ID per span.
		var traceID [16]byte
		traceID[0] = byte(i >> 8)
		traceID[1] = byte(i)
		span.SetTraceID(pcommon.TraceID(traceID))
		span.SetSpanID([8]byte{byte(i)})
	}
	return td
}

func TestDefaultZeroDropsAll(t *testing.T) {
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{DefaultSamplingPercentage: 0}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := newTestTraces(10)
	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)
	assert.Empty(t, sink.AllTraces(), "0%% sampling should drop all spans")
}

func TestDefault100PassesAll(t *testing.T) {
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{DefaultSamplingPercentage: 100}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := newTestTraces(10)
	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)

	require.Len(t, sink.AllTraces(), 1)
	assert.Equal(t, 10, sink.AllTraces()[0].SpanCount())
}

func TestDeterministic(t *testing.T) {
	factory := NewFactory()
	cfg := &Config{DefaultSamplingPercentage: 50}

	// Run twice with the same trace ID, expect same result.
	for run := 0; run < 2; run++ {
		sink := new(consumertest.TracesSink)
		proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
		require.NoError(t, err)

		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		span.SetName("deterministic")
		span.SetTraceID([16]byte{0xAA, 0xBB, 0xCC, 0xDD})
		span.SetSpanID([8]byte{1})

		_ = proc.ConsumeTraces(context.Background(), td)
	}
	// Both runs should produce the same outcome (both sampled or both dropped).
	// Since we can't easily compare across processor instances (sink is per-run),
	// we verify by running the same trace ID through a single processor twice.
	sink := new(consumertest.TracesSink)
	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		span.SetName("deterministic")
		span.SetTraceID([16]byte{0xAA, 0xBB, 0xCC, 0xDD})
		span.SetSpanID([8]byte{1})
		_ = proc.ConsumeTraces(context.Background(), td)
	}
	// Both calls should have same result: either both passed or both dropped.
	traces := sink.AllTraces()
	if len(traces) > 0 {
		assert.Len(t, traces, 2, "deterministic: same trace ID should always be sampled the same way")
	}
	// If len==0, both were dropped, which is also deterministic.
}

func TestStatistical(t *testing.T) {
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{DefaultSamplingPercentage: 50}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	const numTraces = 10000
	sampled := 0
	for i := 0; i < numTraces; i++ {
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		span.SetName("stat-test")
		var traceID [16]byte
		traceID[0] = byte(i >> 24)
		traceID[1] = byte(i >> 16)
		traceID[2] = byte(i >> 8)
		traceID[3] = byte(i)
		span.SetTraceID(pcommon.TraceID(traceID))
		span.SetSpanID([8]byte{1})

		_ = proc.ConsumeTraces(context.Background(), td)
	}
	sampled = len(sink.AllTraces())
	ratio := float64(sampled) / float64(numTraces)
	assert.InDelta(t, 0.50, ratio, 0.05, "expected ~50%% sampling rate, got %f", ratio)
}

func TestHighestPercentageWins(t *testing.T) {
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		DefaultSamplingPercentage: 0,
		Rules: []RuleConfig{
			{SamplingPercentage: 50, Condition: `name == "target"`},
			{SamplingPercentage: 100, Condition: `name == "target"`},
		},
	}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("target")
	span.SetTraceID([16]byte{1, 2, 3, 4})
	span.SetSpanID([8]byte{1})

	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)

	// 100% rule should win, so the span should always be sampled.
	require.Len(t, sink.AllTraces(), 1)
	assert.Equal(t, 1, sink.AllTraces()[0].SpanCount())
}

func TestUnmatchedSpansUseDefault(t *testing.T) {
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		DefaultSamplingPercentage: 100,
		Rules: []RuleConfig{
			{SamplingPercentage: 0, Condition: `name == "never-matches"`},
		},
	}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := newTestTraces(5)
	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)

	// No rules match, so default 100% applies.
	require.Len(t, sink.AllTraces(), 1)
	assert.Equal(t, 5, sink.AllTraces()[0].SpanCount())
}

func TestRuleWith100AlwaysKeeps(t *testing.T) {
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		DefaultSamplingPercentage: 0,
		Rules: []RuleConfig{
			{SamplingPercentage: 100, Condition: `attributes["keep"] == "yes"`},
		},
	}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()

	// Span that matches the rule.
	kept := ss.Spans().AppendEmpty()
	kept.SetName("kept")
	kept.SetTraceID([16]byte{1})
	kept.SetSpanID([8]byte{1})
	kept.Attributes().PutStr("keep", "yes")

	// Span that does NOT match (default 0% applies).
	dropped := ss.Spans().AppendEmpty()
	dropped.SetName("dropped")
	dropped.SetTraceID([16]byte{2})
	dropped.SetSpanID([8]byte{2})

	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)

	require.Len(t, sink.AllTraces(), 1)
	got := sink.AllTraces()[0]
	assert.Equal(t, 1, got.SpanCount())
	assert.Equal(t, "kept", got.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name())
}

func TestDifferentHashSeed(t *testing.T) {
	factory := NewFactory()
	const numTraces = 1000

	// Collect sampled trace IDs for two different seeds at 50%.
	sampleSets := [2]map[[16]byte]bool{}
	for seedIdx, seed := range []uint32{0, 12345} {
		sampleSets[seedIdx] = make(map[[16]byte]bool)
		sink := new(consumertest.TracesSink)
		cfg := &Config{DefaultSamplingPercentage: 50, HashSeed: seed}
		proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
		require.NoError(t, err)

		for i := 0; i < numTraces; i++ {
			td := ptrace.NewTraces()
			rs := td.ResourceSpans().AppendEmpty()
			span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
			span.SetName("seed-test")
			var traceID [16]byte
			traceID[0] = byte(i >> 8)
			traceID[1] = byte(i)
			span.SetTraceID(pcommon.TraceID(traceID))
			span.SetSpanID([8]byte{1})

			_ = proc.ConsumeTraces(context.Background(), td)
		}

		for _, tr := range sink.AllTraces() {
			for ri := 0; ri < tr.ResourceSpans().Len(); ri++ {
				rss := tr.ResourceSpans().At(ri)
				for si := 0; si < rss.ScopeSpans().Len(); si++ {
					sss := rss.ScopeSpans().At(si)
					for spi := 0; spi < sss.Spans().Len(); spi++ {
						sampleSets[seedIdx][sss.Spans().At(spi).TraceID()] = true
					}
				}
			}
		}
	}

	// The two sample sets should differ (not identical).
	differ := false
	for tid := range sampleSets[0] {
		if !sampleSets[1][tid] {
			differ = true
			break
		}
	}
	if !differ {
		for tid := range sampleSets[1] {
			if !sampleSets[0][tid] {
				differ = true
				break
			}
		}
	}
	assert.True(t, differ, "different hash seeds should produce different sampling decisions")
}

func TestMutatesData(t *testing.T) {
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{DefaultSamplingPercentage: 100}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	assert.True(t, proc.Capabilities().MutatesData)
}

func TestOTTLResourceAccess(t *testing.T) {
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		DefaultSamplingPercentage: 0,
		Rules: []RuleConfig{
			{SamplingPercentage: 100, Condition: `resource.attributes["service.name"] == "keep-me"`},
		},
	}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := ptrace.NewTraces()

	// Resource that matches.
	rs1 := td.ResourceSpans().AppendEmpty()
	rs1.Resource().Attributes().PutStr("service.name", "keep-me")
	span1 := rs1.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span1.SetName("match")
	span1.SetTraceID([16]byte{1})
	span1.SetSpanID([8]byte{1})

	// Resource that doesn't match (default 0%).
	rs2 := td.ResourceSpans().AppendEmpty()
	rs2.Resource().Attributes().PutStr("service.name", "drop-me")
	span2 := rs2.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span2.SetName("no-match")
	span2.SetTraceID([16]byte{2})
	span2.SetSpanID([8]byte{2})

	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)

	require.Len(t, sink.AllTraces(), 1)
	got := sink.AllTraces()[0]
	assert.Equal(t, 1, got.SpanCount())
	assert.Equal(t, "match", got.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name())
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{
			name:    "negative default percentage",
			cfg:     Config{DefaultSamplingPercentage: -1},
			wantErr: "default_sampling_percentage must be between 0 and 100",
		},
		{
			name:    "default percentage over 100",
			cfg:     Config{DefaultSamplingPercentage: 101},
			wantErr: "default_sampling_percentage must be between 0 and 100",
		},
		{
			name: "negative rule percentage",
			cfg: Config{
				Rules: []RuleConfig{{SamplingPercentage: -5, Condition: `name == "x"`}},
			},
			wantErr: "rule[0]: sampling_percentage must be between 0 and 100",
		},
		{
			name: "rule percentage over 100",
			cfg: Config{
				Rules: []RuleConfig{{SamplingPercentage: 150, Condition: `name == "x"`}},
			},
			wantErr: "rule[0]: sampling_percentage must be between 0 and 100",
		},
		{
			name: "empty condition",
			cfg: Config{
				Rules: []RuleConfig{{SamplingPercentage: 50, Condition: ""}},
			},
			wantErr: "rule[0]: condition must not be empty",
		},
		{
			name: "valid config",
			cfg: Config{
				DefaultSamplingPercentage: 10,
				Rules:                     []RuleConfig{{SamplingPercentage: 100, Condition: `name == "x"`}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestInvalidOTTLCondition(t *testing.T) {
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		DefaultSamplingPercentage: 10,
		Rules: []RuleConfig{
			{SamplingPercentage: 100, Condition: `this is not valid OTTL !!!`},
		},
	}

	_, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parsing condition")
}

func TestComputeHash(t *testing.T) {
	data := []byte{1, 2, 3, 4}
	h1 := computeHash(data, 0)
	h2 := computeHash(data, 0)
	assert.Equal(t, h1, h2, "same input should produce same hash")

	h3 := computeHash(data, 42)
	assert.NotEqual(t, h1, h3, "different seed should produce different hash")
}

func TestScaledThresholdBoundary(t *testing.T) {
	// Verify that percentageScaleFactor computation is correct.
	assert.Equal(t, uint32(numHashBuckets), uint32(100*percentageScaleFactor))
	assert.InDelta(t, 163.84, percentageScaleFactor, 0.01)

	// Verify that 100% maps to numHashBuckets.
	threshold100 := uint32(float64(100) * percentageScaleFactor)
	assert.True(t, threshold100 >= numHashBuckets)

	// Verify that 0% maps to 0.
	threshold0 := uint32(float64(0) * percentageScaleFactor)
	assert.Equal(t, uint32(0), threshold0)
}

func TestEmptyTracesInput(t *testing.T) {
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{DefaultSamplingPercentage: 100}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := ptrace.NewTraces()
	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)
	assert.Empty(t, sink.AllTraces())
}

// helper: count spans across all collected traces in a sink.
func countSpansInSink(sink *consumertest.TracesSink) int {
	total := 0
	for _, t := range sink.AllTraces() {
		total += t.SpanCount()
	}
	return total
}

func TestGhostSpanConversion(t *testing.T) {
	// Create a fully populated span and convert it to a ghost span.
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "test-svc")
	ss := rs.ScopeSpans().AppendEmpty()
	span := ss.Spans().AppendEmpty()

	traceID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	spanID := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	parentSpanID := [8]byte{8, 7, 6, 5, 4, 3, 2, 1}

	span.SetTraceID(pcommon.TraceID(traceID))
	span.SetSpanID(pcommon.SpanID(spanID))
	span.SetParentSpanID(pcommon.SpanID(parentSpanID))
	span.SetName("original-name")
	span.Attributes().PutStr("http.method", "GET")
	span.Attributes().PutInt("http.status_code", 200)
	span.Events().AppendEmpty().SetName("event1")
	span.Links().AppendEmpty().SetTraceID([16]byte{99})
	span.Status().SetCode(ptrace.StatusCodeError)
	span.Status().SetMessage("something failed")
	span.SetStartTimestamp(1000)
	span.SetEndTimestamp(2000)
	span.SetKind(ptrace.SpanKindServer)
	span.SetFlags(1)
	span.SetDroppedAttributesCount(5)
	span.SetDroppedEventsCount(3)
	span.SetDroppedLinksCount(2)
	span.TraceState().FromRaw("key=value")

	convertToGhostSpan(span)

	// Structural IDs preserved.
	assert.Equal(t, pcommon.TraceID(traceID), span.TraceID())
	assert.Equal(t, pcommon.SpanID(spanID), span.SpanID())
	assert.Equal(t, pcommon.SpanID(parentSpanID), span.ParentSpanID())

	// Timestamps and kinds are useful still.
	assert.Equal(t, pcommon.Timestamp(1000), span.StartTimestamp())
	assert.Equal(t, pcommon.Timestamp(2000), span.EndTimestamp())
	assert.Equal(t, ptrace.SpanKindServer, span.Kind())
	assert.Equal(t, uint32(1), span.Flags())

	// Name and attributes.
	assert.Equal(t, "unsampled", span.Name())
	assert.Equal(t, 1, span.Attributes().Len())
	v, ok := span.Attributes().Get(ghostSpanAttributeKey)
	assert.True(t, ok)
	assert.True(t, v.Bool())

	// Cleared fields.
	assert.Equal(t, 0, span.Events().Len())
	assert.Equal(t, 0, span.Links().Len())
	assert.Equal(t, ptrace.StatusCodeUnset, span.Status().Code())
	assert.Equal(t, "", span.Status().Message())
	assert.Equal(t, uint32(0), span.DroppedAttributesCount())
	assert.Equal(t, uint32(0), span.DroppedEventsCount())
	assert.Equal(t, uint32(0), span.DroppedLinksCount())
	assert.Equal(t, "", span.TraceState().AsRaw())
}

func TestGhostSpanAtIntermediateRate(t *testing.T) {
	// Default 0%, rule at 100% for attributes["important"]=="yes".
	// Non-matching spans: effective=0%, max=100%. All should become ghosts.
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		DefaultSamplingPercentage: 0,
		GhostSpans:                true,
		Rules: []RuleConfig{
			{SamplingPercentage: 100, Condition: `attributes["important"] == "yes"`},
		},
	}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := newTestTraces(10)
	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)

	require.Len(t, sink.AllTraces(), 1)
	got := sink.AllTraces()[0]
	assert.Equal(t, 10, got.SpanCount())

	// All spans should be ghosts.
	spans := got.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	for i := 0; i < spans.Len(); i++ {
		s := spans.At(i)
		assert.Equal(t, "unsampled", s.Name())
		v, ok := s.Attributes().Get(ghostSpanAttributeKey)
		assert.True(t, ok)
		assert.True(t, v.Bool())
	}
}

func TestGhostSpanPreservesTraceStructure(t *testing.T) {
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		DefaultSamplingPercentage: 0,
		GhostSpans:                true,
		Rules: []RuleConfig{
			{SamplingPercentage: 100, Condition: `attributes["important"] == "yes"`},
		},
	}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	traceID := [16]byte{0xAA, 0xBB, 0xCC, 0xDD}
	spanID := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	parentSpanID := [8]byte{8, 7, 6, 5, 4, 3, 2, 1}

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("original")
	span.SetTraceID(pcommon.TraceID(traceID))
	span.SetSpanID(pcommon.SpanID(spanID))
	span.SetParentSpanID(pcommon.SpanID(parentSpanID))

	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)

	require.Len(t, sink.AllTraces(), 1)
	got := sink.AllTraces()[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	assert.Equal(t, pcommon.TraceID(traceID), got.TraceID())
	assert.Equal(t, pcommon.SpanID(spanID), got.SpanID())
	assert.Equal(t, pcommon.SpanID(parentSpanID), got.ParentSpanID())
}

func TestNoGhostWhenMaxThresholdEqualsEffective(t *testing.T) {
	// Default 10%, no rules. Effective == max, so no ghosts possible.
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		DefaultSamplingPercentage: 10,
		GhostSpans:                true,
	}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	const numTraces = 10000
	for i := 0; i < numTraces; i++ {
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		span.SetName("no-ghost-test")
		var traceID [16]byte
		traceID[0] = byte(i >> 24)
		traceID[1] = byte(i >> 16)
		traceID[2] = byte(i >> 8)
		traceID[3] = byte(i)
		span.SetTraceID(pcommon.TraceID(traceID))
		span.SetSpanID([8]byte{1})
		_ = proc.ConsumeTraces(context.Background(), td)
	}

	// All kept spans should be real spans, not ghosts.
	for _, tr := range sink.AllTraces() {
		for ri := 0; ri < tr.ResourceSpans().Len(); ri++ {
			for si := 0; si < tr.ResourceSpans().At(ri).ScopeSpans().Len(); si++ {
				spans := tr.ResourceSpans().At(ri).ScopeSpans().At(si).Spans()
				for spi := 0; spi < spans.Len(); spi++ {
					assert.NotEqual(t, "unsampled", spans.At(spi).Name(), "should not produce ghost spans when max == effective")
				}
			}
		}
	}
}

func TestNoGhostWhenAllZero(t *testing.T) {
	// Default 0%, no rules. Max threshold is 0, everything drops.
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		DefaultSamplingPercentage: 0,
		GhostSpans:                true,
	}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := newTestTraces(10)
	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)
	assert.Empty(t, sink.AllTraces(), "0%% with no rules should drop everything even with ghost_spans enabled")
}

func TestStatisticalGhostRate(t *testing.T) {
	// Default 10%, rule at 50% for name=="special".
	// Non-matching spans: effective=10%, max=50%.
	// Expected: ~10% kept, ~40% ghost, ~50% dropped.
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		DefaultSamplingPercentage: 10,
		GhostSpans:                true,
		Rules: []RuleConfig{
			{SamplingPercentage: 50, Condition: `name == "special"`},
		},
	}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	const numTraces = 10000
	for i := 0; i < numTraces; i++ {
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		span.SetName("normal") // Does NOT match rule, uses default 10%.
		var traceID [16]byte
		traceID[0] = byte(i >> 24)
		traceID[1] = byte(i >> 16)
		traceID[2] = byte(i >> 8)
		traceID[3] = byte(i)
		span.SetTraceID(pcommon.TraceID(traceID))
		span.SetSpanID([8]byte{1})
		_ = proc.ConsumeTraces(context.Background(), td)
	}

	kept := 0
	ghosts := 0
	for _, tr := range sink.AllTraces() {
		for ri := 0; ri < tr.ResourceSpans().Len(); ri++ {
			for si := 0; si < tr.ResourceSpans().At(ri).ScopeSpans().Len(); si++ {
				spans := tr.ResourceSpans().At(ri).ScopeSpans().At(si).Spans()
				for spi := 0; spi < spans.Len(); spi++ {
					if spans.At(spi).Name() == "unsampled" {
						ghosts++
					} else {
						kept++
					}
				}
			}
		}
	}
	dropped := numTraces - kept - ghosts

	keepRatio := float64(kept) / float64(numTraces)
	ghostRatio := float64(ghosts) / float64(numTraces)
	dropRatio := float64(dropped) / float64(numTraces)

	assert.InDelta(t, 0.10, keepRatio, 0.05, "expected ~10%% kept, got %f", keepRatio)
	assert.InDelta(t, 0.40, ghostRatio, 0.05, "expected ~40%% ghost, got %f", ghostRatio)
	assert.InDelta(t, 0.50, dropRatio, 0.05, "expected ~50%% dropped, got %f", dropRatio)
}

func TestGhostSpansDisabledByDefault(t *testing.T) {
	// Default config (ghost_spans not set). Verify no ghosts even with rules.
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		DefaultSamplingPercentage: 0,
		// GhostSpans defaults to false.
		Rules: []RuleConfig{
			{SamplingPercentage: 100, Condition: `attributes["important"] == "yes"`},
		},
	}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := newTestTraces(10) // No span has attributes["important"]=="yes"
	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)

	// With ghost_spans=false, default 0%, no matching rules: all spans should be dropped.
	assert.Empty(t, sink.AllTraces(), "ghost_spans disabled should not produce ghost spans")
}

func TestStatisticalMultipleRates(t *testing.T) {
	// Verify that ~25% rate produces approximately 25% sampling.
	factory := NewFactory()
	cfg := &Config{DefaultSamplingPercentage: 25}
	sink := new(consumertest.TracesSink)
	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	const numTraces = 10000
	for i := 0; i < numTraces; i++ {
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		span.SetName("rate-test")
		var traceID [16]byte
		traceID[0] = byte(i >> 24)
		traceID[1] = byte(i >> 16)
		traceID[2] = byte(i >> 8)
		traceID[3] = byte(i)
		span.SetTraceID(pcommon.TraceID(traceID))
		span.SetSpanID([8]byte{1})
		_ = proc.ConsumeTraces(context.Background(), td)
	}

	sampled := countSpansInSink(sink)
	ratio := float64(sampled) / float64(numTraces)
	assert.InDelta(t, 0.25, ratio, 0.05, "expected ~25%% rate, got %f", ratio)

	// Sanity check: math.Abs to make sure delta check is reasonable
	assert.Less(t, math.Abs(ratio-0.25), 0.05)
}
