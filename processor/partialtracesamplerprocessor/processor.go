package partialtracesamplerprocessor

import (
	"context"
	"encoding/binary"
	"errors"
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

type compiledRule struct {
	condition          ottl.ConditionSequence[*ottlspan.TransformContext]
	samplingPercentage float64
}

type partialTraceSampler struct {
	logger                    *zap.Logger
	rules                     []compiledRule
	defaultSamplingPercentage float64
	hashSeed                  uint32
	mode                      SamplerMode
	samplingPrecision         int
	failClosed                bool
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
			condition:          condSeq,
			samplingPercentage: float64(r.SamplingPercentage),
		})
	}

	mode := cfg.SamplerMode
	if mode == "" {
		mode = HashSeed
	}

	sampler := &partialTraceSampler{
		logger:                    set.Logger,
		rules:                     rules,
		defaultSamplingPercentage: float64(cfg.DefaultSamplingPercentage),
		hashSeed:                  cfg.HashSeed,
		mode:                      mode,
		samplingPrecision:         cfg.SamplingPrecision,
		failClosed:                cfg.FailClosed,
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

// effectivePercentage evaluates all OTTL rules and returns the highest
// matching sampling percentage (or the default if no rules match).
func (p *partialTraceSampler) effectivePercentage(ctx context.Context, rs ptrace.ResourceSpans, ss ptrace.ScopeSpans, span ptrace.Span) float64 {
	pct := p.defaultSamplingPercentage
	for i := range p.rules {
		tCtx := ottlspan.NewTransformContextPtr(rs, ss, span)
		matched, err := p.rules[i].condition.Eval(ctx, tCtx)
		tCtx.Close()
		if err != nil {
			p.logger.Debug("OTTL condition evaluation error", zap.Error(err))
			continue
		}
		if matched && p.rules[i].samplingPercentage > pct {
			pct = p.rules[i].samplingPercentage
		}
	}
	return pct
}

func (p *partialTraceSampler) shouldSample(ctx context.Context, rs ptrace.ResourceSpans, ss ptrace.ScopeSpans, span ptrace.Span) bool {
	pct := p.effectivePercentage(ctx, rs, ss, span)

	switch p.mode {
	case Equalizing:
		return p.shouldSampleEqualizing(span, pct)
	case Proportional:
		return p.shouldSampleProportional(span, pct)
	default:
		return p.shouldSampleHashSeed(span, pct)
	}
}

// shouldSampleHashSeed uses FNV hash of the trace ID (legacy mode).
func (p *partialTraceSampler) shouldSampleHashSeed(span ptrace.Span, pct float64) bool {
	if pct >= 100 {
		return true
	}
	if pct <= 0 {
		return false
	}
	scaledThreshold := uint32(pct * percentageScaleFactor)
	traceID := span.TraceID()
	hash := computeHash(traceID[:], p.hashSeed) & bitMaskHashBuckets
	return hash < scaledThreshold
}

// shouldSampleEqualizing applies an absolute threshold using W3C consistent
// probability sampling. If the span already has a higher threshold (lower
// probability) from upstream, that threshold is preserved.
func (p *partialTraceSampler) shouldSampleEqualizing(span ptrace.Span, pct float64) bool {
	rnd, w3c, err := p.extractRandomnessAndState(span)
	if err != nil {
		return !p.failClosed
	}

	// Compute our threshold for the effective percentage.
	myThreshold := p.percentageToThreshold(pct)

	// Equalizing: take the higher threshold (lower probability).
	// If the incoming threshold is already more aggressive, keep it.
	threshold := myThreshold
	if w3c != nil {
		if existingTh, has := w3c.OTelValue().TValueThreshold(); has {
			if err := p.consistencyCheck(rnd, existingTh); err != nil {
				p.logger.Debug("inconsistent tracestate", zap.Error(err))
				return !p.failClosed
			} else if sampling.ThresholdGreater(existingTh, myThreshold) {
				threshold = existingTh
			}
		}
	}

	sampled := threshold.ShouldSample(rnd)
	if sampled && w3c != nil {
		p.updateTraceState(span, w3c, threshold)
	}
	return sampled
}

// shouldSampleProportional multiplies the incoming sampling probability by the
// configured ratio using W3C consistent probability sampling.
func (p *partialTraceSampler) shouldSampleProportional(span ptrace.Span, pct float64) bool {
	rnd, w3c, err := p.extractRandomnessAndState(span)
	if err != nil {
		return !p.failClosed
	}

	// Get incoming probability (default 1.0 = 100% if no threshold present).
	incomingProb := 1.0
	if w3c != nil {
		if existingTh, has := w3c.OTelValue().TValueThreshold(); has {
			if err := p.consistencyCheck(rnd, existingTh); err != nil {
				p.logger.Debug("inconsistent tracestate", zap.Error(err))
				return !p.failClosed
			}
			incomingProb = existingTh.Probability()
		}
	}

	// Multiply incoming probability by our ratio.
	newProb := incomingProb * pct / 100.0
	threshold, err := sampling.ProbabilityToThresholdWithPrecision(newProb, p.samplingPrecision)
	if err != nil {
		if errors.Is(err, sampling.ErrProbabilityRange) {
			// Probability underflowed below minimum representable value.
			return false
		}
		p.logger.Debug("failed to compute threshold", zap.Error(err))
		return false
	}

	sampled := threshold.ShouldSample(rnd)
	if sampled && w3c != nil {
		p.updateTraceState(span, w3c, threshold)
	}
	return sampled
}

// percentageToThreshold converts a sampling percentage (0-100) to a
// sampling.Threshold. Handles boundary cases for 0% and 100%.
func (p *partialTraceSampler) percentageToThreshold(pct float64) sampling.Threshold {
	if pct >= 100 {
		return sampling.AlwaysSampleThreshold
	}
	if pct <= 0 {
		return sampling.NeverSampleThreshold
	}
	th, err := sampling.ProbabilityToThresholdWithPrecision(pct/100.0, p.samplingPrecision)
	if err != nil {
		return sampling.NeverSampleThreshold
	}
	return th
}

// extractRandomnessAndState parses the W3C tracestate and extracts randomness.
// Priority: explicit R-value > TraceID-derived randomness > error.
func (p *partialTraceSampler) extractRandomnessAndState(span ptrace.Span) (sampling.Randomness, *sampling.W3CTraceState, error) {
	w3c, parseErr := sampling.NewW3CTraceState(span.TraceState().AsRaw())

	if parseErr == nil {
		// Try explicit R-value from tracestate.
		if rv, has := w3c.OTelValue().RValueRandomness(); has {
			return rv, &w3c, nil
		}
	}

	// Fall back to TraceID-derived randomness.
	if !span.TraceID().IsEmpty() {
		rnd := sampling.TraceIDToRandomness(span.TraceID())
		if parseErr == nil {
			return rnd, &w3c, nil
		}
		return rnd, nil, nil
	}

	return sampling.Randomness{}, nil, fmt.Errorf("missing randomness: no R-value and empty trace ID")
}

// consistencyCheck verifies that the existing threshold is consistent with
// the randomness (i.e., the span should have been sampled given its threshold).
func (p *partialTraceSampler) consistencyCheck(rnd sampling.Randomness, th sampling.Threshold) error {
	if !th.ShouldSample(rnd) {
		return fmt.Errorf("inconsistent tracestate: threshold %s should not have sampled randomness", th.TValue())
	}
	return nil
}

// updateTraceState writes the new threshold into the span's W3C tracestate.
func (p *partialTraceSampler) updateTraceState(span ptrace.Span, w3c *sampling.W3CTraceState, threshold sampling.Threshold) {
	err := w3c.OTelValue().UpdateTValueWithSampling(threshold)
	if err != nil {
		p.logger.Debug("failed to update tracestate threshold", zap.Error(err))
		return
	}
	var w strings.Builder
	if err := w3c.Serialize(&w); err != nil {
		p.logger.Debug("failed to serialize tracestate", zap.Error(err))
		return
	}
	span.TraceState().FromRaw(w.String())
}
