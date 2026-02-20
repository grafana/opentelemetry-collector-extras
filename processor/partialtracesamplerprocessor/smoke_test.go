// Copyright Grafana Labs
// SPDX-License-Identifier: Apache-2.0

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
// the FNV-hashed range (all 16 bytes). Index starts at 1 to avoid the
// all-zeros trace ID.
func generateTraceIDs(n int) [][16]byte {
	ids := make([][16]byte, n)
	for j := range ids {
		i := j + 1 // avoid all-zeros trace ID
		ids[j] = [16]byte{
			byte(i), byte(i >> 8), byte(i >> 16), byte(i * 7),
			byte(i * 13), byte(i * 31), byte(i * 53), byte(i * 97),
			byte(i * 127),
			byte(i % 256),
			byte(i / 256),
			byte(i * 3),
			byte(i * 17),
			byte(i * 41),
			byte(i * 59),
			byte(i * 83),
		}
	}
	return ids
}

// buildSingleSpanTraces creates a ptrace.Traces with one span.
func buildSingleSpanTraces(tid [16]byte) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "smoke-test")
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("op")
	span.SetTraceID(pcommon.TraceID(tid))
	span.SetSpanID([8]byte{1})
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

func TestSmokeComparisonWithProbabilisticSampler(t *testing.T) {
	const numTraceIDs = 500
	traceIDs := generateTraceIDs(numTraceIDs)

	rates := []float32{0, 1, 10, 25, 50, 75, 100}

	for _, rate := range rates {
		t.Run(fmt.Sprintf("rate_%g", rate), func(t *testing.T) {
			ourSink := new(consumertest.TracesSink)
			ourFactory := NewFactory()
			ourCfg := &Config{
				DefaultSamplingPercentage: rate,
				HashSeed:                  1234,
			}
			ourProc, err := ourFactory.CreateTraces(
				context.Background(),
				processortest.NewNopSettings(ourFactory.Type()),
				ourCfg,
				ourSink,
			)
			require.NoError(t, err)

			probSink := new(consumertest.TracesSink)
			probFactory := probabilisticsamplerprocessor.NewFactory()
			probCfg := &probabilisticsamplerprocessor.Config{
				SamplingPercentage: rate,
				HashSeed:           1234,
			}
			probProc, err := probFactory.CreateTraces(
				context.Background(),
				processortest.NewNopSettings(probFactory.Type()),
				probCfg,
				probSink,
			)
			require.NoError(t, err)

			for _, tid := range traceIDs {
				_ = ourProc.ConsumeTraces(context.Background(), buildSingleSpanTraces(tid))
				_ = probProc.ConsumeTraces(context.Background(), buildSingleSpanTraces(tid))
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
		})
	}
}
