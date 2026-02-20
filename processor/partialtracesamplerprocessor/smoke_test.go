package partialtracesamplerprocessor

import (
	"context"
	"fmt"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/probabilisticsamplerprocessor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"
)

// generateTraceIDs returns n non-empty trace IDs with good entropy across
// both the FNV-hashed range (all 16 bytes) and the W3C randomness range
// (bytes 9-15). Index starts at 1 to avoid the all-zeros trace ID.
func generateTraceIDs(n int) [][16]byte {
	ids := make([][16]byte, n)
	for j := range ids {
		i := j + 1 // avoid all-zeros trace ID
		ids[j] = [16]byte{
			byte(i), byte(i >> 8), byte(i >> 16), byte(i * 7),
			byte(i * 13), byte(i * 31), byte(i * 53), byte(i * 97),
			byte(i * 127),
			byte(i % 256),  // byte 9: MSB of W3C randomness
			byte(i / 256),  // byte 10
			byte(i * 3),    // byte 11
			byte(i * 17),   // byte 12
			byte(i * 41),   // byte 13
			byte(i * 59),   // byte 14
			byte(i * 83),   // byte 15
		}
	}
	return ids
}


// buildSingleSpanTraces creates a ptrace.Traces with one span.
func buildSingleSpanTraces(tid [16]byte, tracestate string) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "smoke-test")
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("op")
	span.SetTraceID(pcommon.TraceID(tid))
	span.SetSpanID([8]byte{1})
	if tracestate != "" {
		span.TraceState().FromRaw(tracestate)
	}
	return td
}

// collectSampledTraceIDs extracts all trace IDs from a sink.
func collectSampledTraceIDs(sink *consumertest.TracesSink) map[[16]byte]bool {
	result := make(map[[16]byte]bool)
	for _, td := range sink.AllTraces() {
		for ri := 0; ri < td.ResourceSpans().Len(); ri++ {
			rs := td.ResourceSpans().At(ri)
			for si := 0; si < rs.ScopeSpans().Len(); si++ {
				ss := rs.ScopeSpans().At(si)
				for spi := 0; spi < ss.Spans().Len(); spi++ {
					result[ss.Spans().At(spi).TraceID()] = true
				}
			}
		}
	}
	return result
}

// runComparison feeds the same trace IDs through both processors and
// asserts identical sampling decisions.
func runComparison(
	t *testing.T,
	traceIDs [][16]byte,
	tracestate string,
	ourCfg *Config,
	probCfg *probabilisticsamplerprocessor.Config,
) {
	t.Helper()

	ourSink := new(consumertest.TracesSink)
	ourFactory := NewFactory()
	ourProc, err := ourFactory.CreateTraces(
		context.Background(),
		processortest.NewNopSettings(ourFactory.Type()),
		ourCfg,
		ourSink,
	)
	require.NoError(t, err)

	probSink := new(consumertest.TracesSink)
	probFactory := probabilisticsamplerprocessor.NewFactory()
	probProc, err := probFactory.CreateTraces(
		context.Background(),
		processortest.NewNopSettings(probFactory.Type()),
		probCfg,
		probSink,
	)
	require.NoError(t, err)

	for _, tid := range traceIDs {
		_ = ourProc.ConsumeTraces(context.Background(), buildSingleSpanTraces(tid, tracestate))
		_ = probProc.ConsumeTraces(context.Background(), buildSingleSpanTraces(tid, tracestate))
	}

	ourSampled := collectSampledTraceIDs(ourSink)
	probSampled := collectSampledTraceIDs(probSink)

	assert.Equal(t, len(probSampled), len(ourSampled),
		"sampled count mismatch: probabilistic=%d, ours=%d", len(probSampled), len(ourSampled))

	for tid := range probSampled {
		assert.True(t, ourSampled[tid],
			"trace ID %x sampled by probabilistic but not by ours", tid)
	}
	for tid := range ourSampled {
		assert.True(t, probSampled[tid],
			"trace ID %x sampled by ours but not by probabilistic", tid)
	}
}

func TestSmokeComparisonWithProbabilisticSampler(t *testing.T) {
	const numTraceIDs = 500
	traceIDs := generateTraceIDs(numTraceIDs)

	type modeSpec struct {
		name     string
		ourMode  SamplerMode
		probMode probabilisticsamplerprocessor.SamplerMode
	}

	modes := []modeSpec{
		{"hash_seed", HashSeed, probabilisticsamplerprocessor.HashSeed},
		{"equalizing", Equalizing, probabilisticsamplerprocessor.Equalizing},
		{"proportional", Proportional, probabilisticsamplerprocessor.Proportional},
	}

	rates := []float32{0, 1, 10, 25, 50, 75, 100}

	for _, mode := range modes {
		for _, rate := range rates {
			t.Run(fmt.Sprintf("%s/rate_%g", mode.name, rate), func(t *testing.T) {
				ourCfg := &Config{
					DefaultSamplingPercentage: rate,
					HashSeed:                  1234,
					SamplerMode:               mode.ourMode,
					SamplingPrecision:         4,
					FailClosed:                true,
				}
				probCfg := &probabilisticsamplerprocessor.Config{
					SamplingPercentage: rate,
					HashSeed:           1234,
					Mode:               mode.probMode,
					SamplingPrecision:  4,
					FailClosed:         true,
				}
				runComparison(t, traceIDs, "", ourCfg, probCfg)
			})
		}
	}
}

func TestSmokeComparisonWithTracestate(t *testing.T) {
	// Uses the regular trace ID set which includes both consistent and
	// inconsistent trace IDs relative to the incoming tracestate. Both
	// processors should handle inconsistent tracestate the same way
	// (reject with fail_closed: true).
	const numTraceIDs = 500
	traceIDs := generateTraceIDs(numTraceIDs)

	type modeSpec struct {
		name     string
		ourMode  SamplerMode
		probMode probabilisticsamplerprocessor.SamplerMode
	}

	modes := []modeSpec{
		{"equalizing", Equalizing, probabilisticsamplerprocessor.Equalizing},
		{"proportional", Proportional, probabilisticsamplerprocessor.Proportional},
	}

	rates := []float32{10, 25, 50, 75, 100}
	incomingTracestate := "ot=th:8"

	for _, mode := range modes {
		for _, rate := range rates {
			t.Run(fmt.Sprintf("%s/rate_%g/with_tracestate", mode.name, rate), func(t *testing.T) {
				ourCfg := &Config{
					DefaultSamplingPercentage: rate,
					SamplerMode:               mode.ourMode,
					SamplingPrecision:         4,
					FailClosed:                true,
				}
				probCfg := &probabilisticsamplerprocessor.Config{
					SamplingPercentage: rate,
					Mode:               mode.probMode,
					SamplingPrecision:  4,
					FailClosed:         true,
				}
				runComparison(t, traceIDs, incomingTracestate, ourCfg, probCfg)
			})
		}
	}
}
