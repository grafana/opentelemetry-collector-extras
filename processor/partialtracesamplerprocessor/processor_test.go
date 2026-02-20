package partialtracesamplerprocessor

import (
	"context"
	"fmt"
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
		var traceID [16]byte
		traceID[0] = byte(i >> 8)
		traceID[1] = byte(i)
		span.SetTraceID(pcommon.TraceID(traceID))
		span.SetSpanID([8]byte{byte(i)})
	}
	return td
}

// traceIDWithRandomness creates a trace ID where bytes 9-15 encode the
// given randomness value (used by TraceIDToRandomness).
func traceIDWithRandomness(highByte byte) [16]byte {
	var tid [16]byte
	tid[9] = highByte
	return tid
}

// helper: count spans across all collected traces in a sink.
func countSpansInSink(sink *consumertest.TracesSink) int {
	total := 0
	for _, t := range sink.AllTraces() {
		total += t.SpanCount()
	}
	return total
}

// getOutputTraceState returns the tracestate of the first span in the sink.
func getOutputTraceState(t *testing.T, sink *consumertest.TracesSink) string {
	t.Helper()
	require.NotEmpty(t, sink.AllTraces())
	got := sink.AllTraces()[0]
	require.Greater(t, got.SpanCount(), 0)
	return got.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).TraceState().AsRaw()
}

// ========================================================================
// Hash seed mode tests (existing)
// ========================================================================

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
	traces := sink.AllTraces()
	if len(traces) > 0 {
		assert.Len(t, traces, 2, "deterministic: same trace ID should always be sampled the same way")
	}
}

func TestStatistical(t *testing.T) {
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{DefaultSamplingPercentage: 50}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	const numTraces = 10000
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
	sampled := len(sink.AllTraces())
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

	kept := ss.Spans().AppendEmpty()
	kept.SetName("kept")
	kept.SetTraceID([16]byte{1})
	kept.SetSpanID([8]byte{1})
	kept.Attributes().PutStr("keep", "yes")

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
	rs1 := td.ResourceSpans().AppendEmpty()
	rs1.Resource().Attributes().PutStr("service.name", "keep-me")
	span1 := rs1.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span1.SetName("match")
	span1.SetTraceID([16]byte{1})
	span1.SetSpanID([8]byte{1})

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
	assert.Equal(t, uint32(numHashBuckets), uint32(100*percentageScaleFactor))
	assert.InDelta(t, 163.84, percentageScaleFactor, 0.01)

	threshold100 := uint32(float64(100) * percentageScaleFactor)
	assert.True(t, threshold100 >= numHashBuckets)

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

func TestStatisticalMultipleRates(t *testing.T) {
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
	assert.Less(t, math.Abs(ratio-0.25), 0.05)
}

// ========================================================================
// Config validation tests
// ========================================================================

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
			name: "invalid sampler_mode",
			cfg: Config{
				SamplerMode: "invalid",
			},
			wantErr: "sampler_mode must be one of",
		},
		{
			name: "equalizing with precision 0",
			cfg: Config{
				SamplerMode:       Equalizing,
				SamplingPrecision: 0,
			},
			wantErr: "sampling_precision must be between 1 and 14",
		},
		{
			name: "proportional with precision 15",
			cfg: Config{
				SamplerMode:       Proportional,
				SamplingPrecision: 15,
			},
			wantErr: "sampling_precision must be between 1 and 14",
		},
		{
			name: "hash_seed ignores precision",
			cfg: Config{
				SamplerMode:       HashSeed,
				SamplingPrecision: 0,
			},
		},
		{
			name: "valid equalizing config",
			cfg: Config{
				SamplerMode:               Equalizing,
				SamplingPrecision:         4,
				DefaultSamplingPercentage: 50,
			},
		},
		{
			name: "valid proportional config",
			cfg: Config{
				SamplerMode:               Proportional,
				SamplingPrecision:         4,
				DefaultSamplingPercentage: 25,
			},
		},
		{
			name: "valid config with rules",
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

// ========================================================================
// Equalizing mode tests
// ========================================================================

func TestEqualizingKeepsAllAt100(t *testing.T) {
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		SamplerMode:               Equalizing,
		SamplingPrecision:         4,
		DefaultSamplingPercentage: 100,
	}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("eq100")
	span.SetTraceID(traceIDWithRandomness(0x10)) // any randomness
	span.SetSpanID([8]byte{1})

	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)
	require.Len(t, sink.AllTraces(), 1)

	// Tracestate should be set with th:0 (always sample).
	ts := getOutputTraceState(t, sink)
	assert.Contains(t, ts, "th:0")
}

func TestEqualizingDropsAllAt0(t *testing.T) {
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		SamplerMode:               Equalizing,
		SamplingPrecision:         4,
		DefaultSamplingPercentage: 0,
	}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("eq0")
	span.SetTraceID(traceIDWithRandomness(0xF0))
	span.SetSpanID([8]byte{1})

	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)
	assert.Empty(t, sink.AllTraces())
}

func TestEqualizingPreservesLowerProbability(t *testing.T) {
	// Incoming: 50% (th:8). Our config: 75%.
	// 75% threshold (0x40...) < 50% threshold (0x80...).
	// Equalizing keeps the higher threshold (50%), preserving the more aggressive sampling.
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		SamplerMode:               Equalizing,
		SamplingPrecision:         4,
		DefaultSamplingPercentage: 75,
	}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("preserve-lower")
	// Randomness 0xA0... passes 50% threshold (0x80...)
	span.SetTraceID(traceIDWithRandomness(0xA0))
	span.SetSpanID([8]byte{1})
	span.TraceState().FromRaw("ot=th:8") // 50% sampling

	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)
	require.Len(t, sink.AllTraces(), 1)

	// Threshold should still be 8 (50%), not raised to 75%.
	ts := getOutputTraceState(t, sink)
	assert.Contains(t, ts, "th:8")
}

func TestEqualizingReducesProbability(t *testing.T) {
	// Incoming: 50% (th:8). Our config: 25% (th:c).
	// 25% threshold (0xC0...) > 50% threshold (0x80...).
	// Equalizing applies our higher threshold (25%).
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		SamplerMode:               Equalizing,
		SamplingPrecision:         4,
		DefaultSamplingPercentage: 25,
	}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("reduce-prob")
	// Randomness 0xD0... passes 25% threshold (0xC0...)
	span.SetTraceID(traceIDWithRandomness(0xD0))
	span.SetSpanID([8]byte{1})
	span.TraceState().FromRaw("ot=th:8")

	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)
	require.Len(t, sink.AllTraces(), 1)

	// Threshold should be updated to c (25%).
	ts := getOutputTraceState(t, sink)
	assert.Contains(t, ts, "th:c")
}

func TestEqualizingRejectsReducedProbability(t *testing.T) {
	// Same as above but with randomness that doesn't pass the 25% threshold.
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		SamplerMode:               Equalizing,
		SamplingPrecision:         4,
		DefaultSamplingPercentage: 25,
	}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("reject-reduced")
	// Randomness 0xA0... passes 50% (0x80...) but NOT 25% (0xC0...)
	span.SetTraceID(traceIDWithRandomness(0xA0))
	span.SetSpanID([8]byte{1})
	span.TraceState().FromRaw("ot=th:8")

	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)
	assert.Empty(t, sink.AllTraces(), "span should be dropped at 25%%")
}

func TestEqualizingNoTracestate(t *testing.T) {
	// No existing tracestate: applies our threshold and sets tracestate.
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		SamplerMode:               Equalizing,
		SamplingPrecision:         4,
		DefaultSamplingPercentage: 50,
	}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("no-ts")
	// Randomness 0x90... passes 50% threshold (0x80...)
	span.SetTraceID(traceIDWithRandomness(0x90))
	span.SetSpanID([8]byte{1})

	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)
	require.Len(t, sink.AllTraces(), 1)

	ts := getOutputTraceState(t, sink)
	assert.Contains(t, ts, "th:8")
}

func TestEqualizingStatistical(t *testing.T) {
	factory := NewFactory()
	cfg := &Config{
		SamplerMode:               Equalizing,
		SamplingPrecision:         4,
		DefaultSamplingPercentage: 50,
	}
	sink := new(consumertest.TracesSink)
	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	const numTraces = 10000
	for i := 0; i < numTraces; i++ {
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		span.SetName("eq-stat")
		// The most significant randomness byte (byte 9) must span 0-255
		// for good distribution across the threshold boundary.
		var traceID [16]byte
		traceID[9] = byte(i % 256)
		traceID[10] = byte(i / 256)
		span.SetTraceID(pcommon.TraceID(traceID))
		span.SetSpanID([8]byte{1})
		_ = proc.ConsumeTraces(context.Background(), td)
	}

	sampled := countSpansInSink(sink)
	ratio := float64(sampled) / float64(numTraces)
	assert.InDelta(t, 0.50, ratio, 0.05, "equalizing: expected ~50%% rate, got %f", ratio)
}

func TestEqualizingWithOTTLRule(t *testing.T) {
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		SamplerMode:               Equalizing,
		SamplingPrecision:         4,
		DefaultSamplingPercentage: 0,
		Rules: []RuleConfig{
			{SamplingPercentage: 100, Condition: `attributes["important"] == "true"`},
		},
	}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()

	kept := ss.Spans().AppendEmpty()
	kept.SetName("kept")
	kept.SetTraceID(traceIDWithRandomness(0x10))
	kept.SetSpanID([8]byte{1})
	kept.Attributes().PutStr("important", "true")

	dropped := ss.Spans().AppendEmpty()
	dropped.SetName("dropped")
	dropped.SetTraceID(traceIDWithRandomness(0xF0))
	dropped.SetSpanID([8]byte{2})

	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)
	require.Len(t, sink.AllTraces(), 1)
	got := sink.AllTraces()[0]
	assert.Equal(t, 1, got.SpanCount())
	assert.Equal(t, "kept", got.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name())
}

func TestEqualizingWithExplicitRValue(t *testing.T) {
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		SamplerMode:               Equalizing,
		SamplingPrecision:         4,
		DefaultSamplingPercentage: 50,
	}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("explicit-rv")
	span.SetTraceID([16]byte{1}) // trace ID doesn't matter when R-value is explicit
	span.SetSpanID([8]byte{1})
	// Set explicit R-value (0x90... passes 50%)
	span.TraceState().FromRaw("ot=rv:90000000000000")

	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)
	require.Len(t, sink.AllTraces(), 1)

	ts := getOutputTraceState(t, sink)
	assert.Contains(t, ts, "rv:90000000000000", "R-value should be preserved")
	assert.Contains(t, ts, "th:8", "threshold should be set")
}

func TestEqualizingPreservesOtherVendors(t *testing.T) {
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		SamplerMode:               Equalizing,
		SamplingPrecision:         4,
		DefaultSamplingPercentage: 100,
	}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("vendor")
	span.SetTraceID(traceIDWithRandomness(0x90))
	span.SetSpanID([8]byte{1})
	span.TraceState().FromRaw("other=value,ot=th:8")

	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)
	require.Len(t, sink.AllTraces(), 1)

	ts := getOutputTraceState(t, sink)
	assert.Contains(t, ts, "other=value", "other vendor fields should be preserved")
}

// ========================================================================
// Proportional mode tests
// ========================================================================

func TestProportionalNoTracestate(t *testing.T) {
	// No tracestate: incoming probability is 1.0 (100%).
	// Proportional 50%: new probability = 1.0 * 0.5 = 0.5 (50%).
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		SamplerMode:               Proportional,
		SamplingPrecision:         4,
		DefaultSamplingPercentage: 50,
	}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("prop-no-ts")
	// Randomness 0x90... passes 50% threshold (0x80...)
	span.SetTraceID(traceIDWithRandomness(0x90))
	span.SetSpanID([8]byte{1})

	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)
	require.Len(t, sink.AllTraces(), 1)

	ts := getOutputTraceState(t, sink)
	assert.Contains(t, ts, "th:8")
}

func TestProportionalMultipliesProbability(t *testing.T) {
	// Incoming: 50% (th:8). Config: 50%.
	// New probability: 0.5 * 0.5 = 0.25 (25%), threshold ~0xC0...
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		SamplerMode:               Proportional,
		SamplingPrecision:         4,
		DefaultSamplingPercentage: 50,
	}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("prop-multiply")
	// Randomness 0xD0... passes 25% threshold (0xC0...)
	span.SetTraceID(traceIDWithRandomness(0xD0))
	span.SetSpanID([8]byte{1})
	span.TraceState().FromRaw("ot=th:8")

	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)
	require.Len(t, sink.AllTraces(), 1)

	ts := getOutputTraceState(t, sink)
	assert.Contains(t, ts, "th:c", "threshold should reflect 25%% (50%% * 50%%)")
}

func TestProportionalRejectsAfterMultiply(t *testing.T) {
	// Incoming: 50% (th:8). Config: 50%. Result: 25%.
	// Randomness 0xA0... passes 50% but NOT 25%.
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		SamplerMode:               Proportional,
		SamplingPrecision:         4,
		DefaultSamplingPercentage: 50,
	}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("prop-reject")
	// Randomness 0xA0... is between 0x80... and 0xC0..., so passes 50% but not 25%
	span.SetTraceID(traceIDWithRandomness(0xA0))
	span.SetSpanID([8]byte{1})
	span.TraceState().FromRaw("ot=th:8")

	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)
	assert.Empty(t, sink.AllTraces(), "should be dropped: randomness doesn't pass 25%% threshold")
}

func TestProportional100PreservesIncoming(t *testing.T) {
	// 100% proportional: new probability = incoming * 1.0 = incoming.
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		SamplerMode:               Proportional,
		SamplingPrecision:         4,
		DefaultSamplingPercentage: 100,
	}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("prop-100")
	span.SetTraceID(traceIDWithRandomness(0x90))
	span.SetSpanID([8]byte{1})
	span.TraceState().FromRaw("ot=th:8")

	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)
	require.Len(t, sink.AllTraces(), 1)

	ts := getOutputTraceState(t, sink)
	assert.Contains(t, ts, "th:8", "100%% proportional should preserve incoming threshold")
}

func TestProportionalStatistical(t *testing.T) {
	factory := NewFactory()
	cfg := &Config{
		SamplerMode:               Proportional,
		SamplingPrecision:         4,
		DefaultSamplingPercentage: 50,
	}
	sink := new(consumertest.TracesSink)
	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	const numTraces = 10000
	for i := 0; i < numTraces; i++ {
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		span.SetName("prop-stat")
		var traceID [16]byte
		traceID[9] = byte(i % 256)
		traceID[10] = byte(i / 256)
		span.SetTraceID(pcommon.TraceID(traceID))
		span.SetSpanID([8]byte{1})
		_ = proc.ConsumeTraces(context.Background(), td)
	}

	sampled := countSpansInSink(sink)
	ratio := float64(sampled) / float64(numTraces)
	assert.InDelta(t, 0.50, ratio, 0.05, "proportional: expected ~50%% rate, got %f", ratio)
}

func TestProportionalWithOTTLRule(t *testing.T) {
	// Rule matches: 100% proportional on incoming 50% → 50% effective.
	// Default: 0% → drops non-matching spans.
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		SamplerMode:               Proportional,
		SamplingPrecision:         4,
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

	kept := ss.Spans().AppendEmpty()
	kept.SetName("kept")
	kept.SetTraceID(traceIDWithRandomness(0x90))
	kept.SetSpanID([8]byte{1})
	kept.Attributes().PutStr("keep", "yes")
	kept.TraceState().FromRaw("ot=th:8") // incoming 50%

	dropped := ss.Spans().AppendEmpty()
	dropped.SetName("dropped")
	dropped.SetTraceID(traceIDWithRandomness(0x90))
	dropped.SetSpanID([8]byte{2})
	dropped.TraceState().FromRaw("ot=th:8")

	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)
	require.Len(t, sink.AllTraces(), 1)
	got := sink.AllTraces()[0]
	assert.Equal(t, 1, got.SpanCount())
	assert.Equal(t, "kept", got.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name())
}

// ========================================================================
// Fail-closed / fail-open tests
// ========================================================================

func TestFailClosedRejectsOnMissingRandomness(t *testing.T) {
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		SamplerMode:               Equalizing,
		SamplingPrecision:         4,
		DefaultSamplingPercentage: 100,
		FailClosed:                true,
	}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("no-randomness")
	// Empty trace ID and no R-value → missing randomness.
	span.SetSpanID([8]byte{1})

	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)
	assert.Empty(t, sink.AllTraces(), "fail_closed should reject when randomness is missing")
}

func TestFailOpenAcceptsOnMissingRandomness(t *testing.T) {
	sink := new(consumertest.TracesSink)
	factory := NewFactory()
	cfg := &Config{
		SamplerMode:               Equalizing,
		SamplingPrecision:         4,
		DefaultSamplingPercentage: 100,
		FailClosed:                false,
	}

	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("no-randomness")
	span.SetSpanID([8]byte{1})

	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)
	require.Len(t, sink.AllTraces(), 1, "fail_open should accept when randomness is missing")
}

// ========================================================================
// Mixed mode verification: ensure all three modes are usable
// ========================================================================

func TestAllModesCreateSuccessfully(t *testing.T) {
	factory := NewFactory()
	for _, mode := range []SamplerMode{"", HashSeed, Equalizing, Proportional} {
		t.Run(fmt.Sprintf("mode_%s", mode), func(t *testing.T) {
			sink := new(consumertest.TracesSink)
			cfg := &Config{
				SamplerMode:               mode,
				SamplingPrecision:         4,
				DefaultSamplingPercentage: 50,
			}
			proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(factory.Type()), cfg, sink)
			require.NoError(t, err)
			assert.True(t, proc.Capabilities().MutatesData)
		})
	}
}
