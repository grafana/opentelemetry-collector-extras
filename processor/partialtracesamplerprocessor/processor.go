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

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
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

type compiledRule struct {
	condition       ottl.ConditionSequence[*ottlspan.TransformContext]
	scaledThreshold uint32
}

type partialTraceSampler struct {
	logger                 *zap.Logger
	rules                  []compiledRule
	defaultScaledThreshold uint32
	hashSeed               uint32
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

	sampler := &partialTraceSampler{
		logger:                 set.Logger,
		rules:                  rules,
		defaultScaledThreshold: uint32(float64(cfg.DefaultSamplingPercentage) * percentageScaleFactor),
		hashSeed:               cfg.HashSeed,
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
				return !p.shouldSample(ctx, rs, ss, span)
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

func (p *partialTraceSampler) shouldSample(ctx context.Context, rs ptrace.ResourceSpans, ss ptrace.ScopeSpans, span ptrace.Span) bool {
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
		return true
	}
	if effectiveThreshold == 0 {
		return false
	}

	traceID := span.TraceID()
	hash := computeHash(traceID[:], p.hashSeed) & bitMaskHashBuckets
	return hash < effectiveThreshold
}
