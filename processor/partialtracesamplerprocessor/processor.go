// Copyright The OpenTelemetry Authors
// Copyright Grafana Labs
// SPDX-License-Identifier: Apache-2.0

// Original source: github.com/open-telemetry/opentelemetry-collector-contrib/processor/probabilisticsamplerprocessor
// Modified to support partial trace sampling with per-span OTTL-based rules.

package partialtracesamplerprocessor

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/sampling"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.uber.org/zap"
)

const (
	numHashBuckets        = 0x4000 // 16384
	bitMaskHashBuckets    = numHashBuckets - 1
	percentageScaleFactor = numHashBuckets / 100.0
)

type sampleDecision int

const (
	decisionDrop sampleDecision = iota
	decisionGhost
	decisionKeep
)

const (
	ghostSpanName         = "unsampled"
	ghostSpanAttributeKey = "grafana.partial_trace.ghost"
)

type compiledRule struct {
	condition       ottl.ConditionSequence[*ottlspan.TransformContext]
	scaledThreshold uint32
}

type partialTraceSampler struct {
	logger                 *zap.Logger
	rules                  []compiledRule
	defaultScaledThreshold uint32
	hashSeed               uint32
	ghostSpans             bool
	maxScaledThreshold     uint32
}

func newPartialTraceSampler(
	ctx context.Context,
	set processor.Settings,
	cfg *Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	converters := ottlfuncs.StandardConverters[*ottlspan.TransformContext]()
	parser, err := ottlspan.NewParser(converters, set.TelemetrySettings)
	if err != nil {
		return nil, fmt.Errorf("creating OTTL parser: %w", err)
	}

	var rules []compiledRule
	for _, r := range cfg.Rules {
		conditions, err := parser.ParseConditions([]string{r.Condition})
		if err != nil {
			return nil, fmt.Errorf("parsing condition %q: %w", r.Condition, err)
		}
		condSeq := ottlspan.NewConditionSequence(conditions, set.TelemetrySettings, ottlspan.WithConditionSequenceErrorMode(ottl.IgnoreError))
		rules = append(rules, compiledRule{
			condition:       condSeq,
			scaledThreshold: uint32(float64(r.SamplingPercentage) * percentageScaleFactor),
		})
	}

	defaultScaledThreshold := uint32(float64(cfg.DefaultSamplingPercentage) * percentageScaleFactor)

	var maxScaledThreshold uint32
	if cfg.GhostSpans {
		maxScaledThreshold = defaultScaledThreshold
		for _, r := range rules {
			if r.scaledThreshold > maxScaledThreshold {
				maxScaledThreshold = r.scaledThreshold
			}
		}
	}

	sampler := &partialTraceSampler{
		logger:                 set.Logger,
		rules:                  rules,
		defaultScaledThreshold: defaultScaledThreshold,
		hashSeed:               cfg.HashSeed,
		ghostSpans:             cfg.GhostSpans,
		maxScaledThreshold:     maxScaledThreshold,
	}

	return processorhelper.NewTraces(ctx, set, cfg, nextConsumer, sampler.processTraces,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}))
}

func computeHash(b []byte, seed uint32) uint32 {
	h := fnv.New32a()
	if seed != 0 {
		seedBytes := [4]byte{}
		binary.LittleEndian.PutUint32(seedBytes[:], seed)
		h.Write(seedBytes[:])
	}
	h.Write(b)
	return h.Sum32()
}

func (p *partialTraceSampler) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
		rs.ScopeSpans().RemoveIf(func(ss ptrace.ScopeSpans) bool {
			ss.Spans().RemoveIf(func(span ptrace.Span) bool {
				decision, scaledThreshold := p.decideSampling(ctx, rs, ss, span)
				switch decision {
				case decisionKeep:
					setTraceStateThreshold(span, scaledThresholdToSamplingThreshold(scaledThreshold))
					return false
				case decisionGhost:
					convertToGhostSpan(span, scaledThresholdToSamplingThreshold(scaledThreshold))
					return false
				default:
					return true
				}
			})
			return ss.Spans().Len() == 0
		})
		return rs.ScopeSpans().Len() == 0
	})
	if td.ResourceSpans().Len() == 0 {
		return td, processorhelper.ErrSkipProcessingData
	}
	return td, nil
}

func (p *partialTraceSampler) decideSampling(ctx context.Context, rs ptrace.ResourceSpans, ss ptrace.ScopeSpans, span ptrace.Span) (sampleDecision, uint32) {
	effectiveThreshold := p.defaultScaledThreshold

	for i := range p.rules {
		tCtx := ottlspan.NewTransformContextPtr(rs, ss, span)
		matched, err := p.rules[i].condition.Eval(ctx, tCtx)
		tCtx.Close()
		if err != nil {
			p.logger.Debug("OTTL condition evaluation error", zap.Error(err))
			continue
		}
		if matched && p.rules[i].scaledThreshold > effectiveThreshold {
			effectiveThreshold = p.rules[i].scaledThreshold
		}
	}

	if effectiveThreshold >= numHashBuckets {
		return decisionKeep, numHashBuckets
	}

	traceID := span.TraceID()
	hash := computeHash(traceID[:], p.hashSeed) & bitMaskHashBuckets

	if hash < effectiveThreshold {
		return decisionKeep, effectiveThreshold
	}
	if p.ghostSpans && hash < p.maxScaledThreshold {
		return decisionGhost, p.maxScaledThreshold
	}
	return decisionDrop, 0
}

func convertToGhostSpan(span ptrace.Span, th sampling.Threshold) {
	// Preserve: TraceID, SpanID, ParentSpanID, Kind (already set on the span)
	// Preserve the original name for client, server, producer, and consumer spans
	// so they remain useful in trace visualizations. Internal spans get a generic name.
	switch span.Kind() {
	case ptrace.SpanKindClient, ptrace.SpanKindServer, ptrace.SpanKindProducer, ptrace.SpanKindConsumer:
		// keep original name
	default:
		span.SetName(ghostSpanName)
	}

	span.Attributes().Clear()
	span.Attributes().PutBool(ghostSpanAttributeKey, true)

	span.Events().RemoveIf(func(ptrace.SpanEvent) bool { return true })
	span.Links().RemoveIf(func(ptrace.SpanLink) bool { return true })
	span.Status().SetCode(ptrace.StatusCodeUnset)
	span.Status().SetMessage("")

	// Set trace state with sampling threshold (ghost spans don't preserve original tracestate).
	w3c, _ := sampling.NewW3CTraceState("")
	_ = w3c.OTelValue().UpdateTValueWithSampling(th)
	var buf strings.Builder
	_ = w3c.Serialize(&buf)
	span.TraceState().FromRaw(buf.String())
}

func setTraceStateThreshold(span ptrace.Span, th sampling.Threshold) {
	w3c, err := sampling.NewW3CTraceState(span.TraceState().AsRaw())
	if err != nil {
		// If tracestate is unparseable, start fresh.
		w3c, _ = sampling.NewW3CTraceState("")
	}
	_ = w3c.OTelValue().UpdateTValueWithSampling(th)
	var buf strings.Builder
	_ = w3c.Serialize(&buf)
	span.TraceState().FromRaw(buf.String())
}

func scaledThresholdToSamplingThreshold(scaled uint32) sampling.Threshold {
	if scaled >= numHashBuckets {
		return sampling.AlwaysSampleThreshold
	}
	prob := float64(scaled) / float64(numHashBuckets)
	th, err := sampling.ProbabilityToThreshold(prob)
	if err != nil {
		// prob was 0 or invalid, use NeverSample.
		return sampling.NeverSampleThreshold
	}
	return th
}
