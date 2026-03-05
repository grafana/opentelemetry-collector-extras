// Copyright The OpenTelemetry Authors
// Copyright Grafana Labs
// SPDX-License-Identifier: Apache-2.0

// Original source: github.com/open-telemetry/opentelemetry-collector-contrib/processor/probabilisticsamplerprocessor
// Modified to support partial trace sampling with per-span OTTL-based rules.

package partialtracesamplerprocessor

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/sampling"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/grafana/opentelemetry-collector-extras/processor/partialtracesamplerprocessor/internal/metadata"
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
	ghostSpanName          = "unsampled"
	ghostSpanAttributeKey  = "grafana.partial_trace.ghost"
	collapsedSpanIDsKey    = "grafana.partial_trace.collapsed_span_ids"
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
	telemetryBuilder       *metadata.TelemetryBuilder
}

func newPartialTraceSampler(
	ctx context.Context,
	set processor.Settings,
	cfg *Config,
	nextConsumer consumer.Traces,
	telemetryBuilder *metadata.TelemetryBuilder,
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
		telemetryBuilder:       telemetryBuilder,
	}

	return processorhelper.NewTraces(ctx, set, cfg, nextConsumer, sampler.processTraces,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}),
		processorhelper.WithShutdown(sampler.shutdown))
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
	p.recordSize(ctx, td, p.telemetryBuilder.ProcessorPartialtracesamplerBytesReceived, p.telemetryBuilder.ProcessorPartialtracesamplerCompressedBytesReceived)

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

	if p.ghostSpans {
		collapseInternalGhosts(td)
		consolidateGhostScopes(td)
	}

	p.recordSize(ctx, td, p.telemetryBuilder.ProcessorPartialtracesamplerBytesEmitted, p.telemetryBuilder.ProcessorPartialtracesamplerCompressedBytesEmitted)

	if td.ResourceSpans().Len() == 0 {
		return td, processorhelper.ErrSkipProcessingData
	}
	return td, nil
}

func (p *partialTraceSampler) recordSize(ctx context.Context, td ptrace.Traces, uncompressed, compressed metric.Int64Counter) {
	var m ptrace.ProtoMarshaler
	data, err := m.MarshalTraces(td)
	if err != nil {
		return
	}
	uncompressed.Add(ctx, int64(len(data)))
	compressed.Add(ctx, int64(gzipLen(data)))
}

func gzipLen(data []byte) int {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, _ = w.Write(data)
	_ = w.Close()
	return buf.Len()
}

func (p *partialTraceSampler) shutdown(_ context.Context) error {
	p.telemetryBuilder.Shutdown()
	return nil
}

// consolidateGhostScopes moves all ghost spans within each ResourceSpans into
// a single ScopeSpans with an empty InstrumentationScope. This avoids
// duplicating scope metadata (name, version, attributes) for every ghost span.
func consolidateGhostScopes(td ptrace.Traces) {
	for ri := 0; ri < td.ResourceSpans().Len(); ri++ {
		rs := td.ResourceSpans().At(ri)
		originalLen := rs.ScopeSpans().Len()

		// Check whether any scope is entirely ghost spans.
		hasAllGhostScope := false
		for si := 0; si < originalLen; si++ {
			if allGhosts(rs.ScopeSpans().At(si)) {
				hasAllGhostScope = true
				break
			}
		}
		if !hasAllGhostScope {
			continue
		}

		ghostSS := rs.ScopeSpans().AppendEmpty()

		for si := 0; si < originalLen; si++ {
			ss := rs.ScopeSpans().At(si)
			if !allGhosts(ss) {
				continue
			}
			// Move all spans from this all-ghost scope into the consolidated scope.
			for k := 0; k < ss.Spans().Len(); k++ {
				ss.Spans().At(k).MoveTo(ghostSS.Spans().AppendEmpty())
			}
			// Mark as empty so it gets cleaned up below.
			ss.Spans().RemoveIf(func(ptrace.Span) bool { return true })
		}

		// Clean up original scopes that are now empty, and the ghost scope
		// if no all-ghost scopes were found.
		rs.ScopeSpans().RemoveIf(func(ss ptrace.ScopeSpans) bool {
			return ss.Spans().Len() == 0
		})
	}
}

func allGhosts(ss ptrace.ScopeSpans) bool {
	for k := 0; k < ss.Spans().Len(); k++ {
		if !isGhostSpan(ss.Spans().At(k)) {
			return false
		}
	}
	return ss.Spans().Len() > 0
}

// isInternalGhost returns true if the span is a ghost with internal or unspecified kind.
func isInternalGhost(span ptrace.Span) bool {
	if !isGhostSpan(span) {
		return false
	}
	switch span.Kind() {
	case ptrace.SpanKindClient, ptrace.SpanKindServer, ptrace.SpanKindProducer, ptrace.SpanKindConsumer:
		return false
	default:
		return true
	}
}

// collapseInternalGhosts removes internal/unspecified-kind ghost spans by
// reparenting their children to the nearest non-internal-ghost ancestor.
// Collapsed span IDs are stored on the ancestor so the ghost span processor
// can still reparent orphaned descendants after groupbytrace assembles the
// full trace.
func collapseInternalGhosts(td ptrace.Traces) {
	for ri := 0; ri < td.ResourceSpans().Len(); ri++ {
		rs := td.ResourceSpans().At(ri)

		// Build span map across all scopes in this resource.
		type spanInfo struct {
			span  ptrace.Span
			scope int // scope index
		}
		spanMap := make(map[pcommon.SpanID]spanInfo)
		for si := 0; si < rs.ScopeSpans().Len(); si++ {
			ss := rs.ScopeSpans().At(si)
			for k := 0; k < ss.Spans().Len(); k++ {
				span := ss.Spans().At(k)
				spanMap[span.SpanID()] = spanInfo{span: span, scope: si}
			}
		}

		// Identify internal ghosts and resolve their ancestors.
		// resolvedAncestor maps internal ghost span ID → nearest non-internal-ghost ancestor span ID.
		resolvedAncestor := make(map[pcommon.SpanID]pcommon.SpanID)
		toRemove := make(map[pcommon.SpanID]struct{})

		for id, info := range spanMap {
			if !isInternalGhost(info.span) {
				continue
			}

			// Walk up parent chain to find nearest non-internal-ghost ancestor in this batch.
			ancestor := pcommon.SpanID{}
			found := false
			current := info.span.ParentSpanID()
			visited := make(map[pcommon.SpanID]struct{})
			for {
				if current.IsEmpty() {
					break
				}
				if _, cycle := visited[current]; cycle {
					break
				}
				visited[current] = struct{}{}
				parentInfo, inBatch := spanMap[current]
				if !inBatch {
					// Parent not in this batch — can't safely collapse.
					break
				}
				if !isInternalGhost(parentInfo.span) {
					ancestor = current
					found = true
					break
				}
				current = parentInfo.span.ParentSpanID()
			}

			if found {
				resolvedAncestor[id] = ancestor
				toRemove[id] = struct{}{}
			}
		}

		if len(toRemove) == 0 {
			continue
		}

		// Collect collapsed span IDs per ancestor.
		ancestorCollapsed := make(map[pcommon.SpanID][]string)
		for ghostID, ancestorID := range resolvedAncestor {
			ancestorCollapsed[ancestorID] = append(ancestorCollapsed[ancestorID], hex.EncodeToString(ghostID[:]))
		}

		// Reparent remaining spans whose parent is a collapsed internal ghost.
		for _, info := range spanMap {
			if _, removed := toRemove[info.span.SpanID()]; removed {
				continue
			}
			parentID := info.span.ParentSpanID()
			if newParent, ok := resolvedAncestor[parentID]; ok {
				info.span.SetParentSpanID(newParent)
			}
		}

		// Store collapsed span IDs on ancestor spans.
		for ancestorID, collapsedIDs := range ancestorCollapsed {
			info := spanMap[ancestorID]
			arr := info.span.Attributes().PutEmptySlice(collapsedSpanIDsKey)
			for _, id := range collapsedIDs {
				arr.AppendEmpty().SetStr(id)
			}
		}

		// Remove collapsed spans and clean up empty scopes.
		for si := 0; si < rs.ScopeSpans().Len(); si++ {
			rs.ScopeSpans().At(si).Spans().RemoveIf(func(span ptrace.Span) bool {
				_, remove := toRemove[span.SpanID()]
				return remove
			})
		}
		rs.ScopeSpans().RemoveIf(func(ss ptrace.ScopeSpans) bool {
			return ss.Spans().Len() == 0
		})
	}
}

func isGhostSpan(span ptrace.Span) bool {
	v, ok := span.Attributes().Get(ghostSpanAttributeKey)
	return ok && v.Type() == pcommon.ValueTypeBool && v.Bool()
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
		// keep original name and timestamps for visualization
	default:
		span.SetName(ghostSpanName)
		span.SetStartTimestamp(0)
		span.SetEndTimestamp(0)
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
