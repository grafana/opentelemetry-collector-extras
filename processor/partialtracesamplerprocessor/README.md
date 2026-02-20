# Partial Trace Sampler Processor

| Status | |
|--------|-----------|
| Stability | [development]: traces |
| Distributions | |

[development]: https://github.com/open-telemetry/opentelemetry-collector/blob/main/docs/component-stability.md#development

The `partialtracesampler` processor makes per-span sampling decisions in the
OpenTelemetry Collector pipeline. Unlike tail-based sampling, it does not wait
for complete traces to arrive. Instead, it evaluates each span independently
using an FNV hash of the trace ID combined with optional
[OTTL](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/pkg/ottl)
condition rules. This makes it suitable for high-throughput pipelines where
buffering full traces is not practical.

## Relationship to the probabilistic sampler

This processor is derived from the
[`probabilisticsamplerprocessor`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/processor/probabilisticsamplerprocessor)
in opentelemetry-collector-contrib. It uses the same `hash_seed` sampling mode
and produces identical sampling decisions for the same trace IDs and hash seed,
which is verified by smoke tests that run both processors side by side.

The key difference is that the probabilistic sampler applies a single sampling
percentage to all spans, while this processor supports multiple OTTL-based rules
that can assign different sampling percentages based on span attributes,
resource attributes, or span name. When multiple rules match, the highest
sampling percentage wins, ensuring important spans are never dropped by a
lower-priority rule.

## Configuration

```yaml
processors:
  partialtracesampler:
    # Sampling percentage applied to spans that do not match any rule.
    # Must be between 0 and 100. Default: 0 (drop all unmatched spans).
    default_sampling_percentage: 10

    # Seed for the FNV hash function. Using the same seed across collector
    # instances ensures consistent sampling decisions for the same trace ID.
    # Default: 0.
    hash_seed: 42

    # When true, spans that fall between their effective sampling rate and the
    # highest configured rate are emitted as ghost spans instead of being
    # dropped. See the Ghost Spans section below. Default: false.
    ghost_spans: true

    # Optional list of rules evaluated in order. Each rule has an OTTL
    # condition and a sampling percentage. When multiple rules match a span,
    # the highest sampling_percentage wins.
    rules:
      # Keep all error spans.
      - sampling_percentage: 100
        condition: 'attributes["http.status_code"] >= 500'
      # Sample 50% of spans for a specific operation.
      - sampling_percentage: 50
        condition: 'name == "important-operation"'
```

### Configuration fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `default_sampling_percentage` | float32 | `0` | Sampling percentage for spans that do not match any rule (0-100). |
| `hash_seed` | uint32 | `0` | Seed for the FNV hash. Use the same value across instances for consistent sampling. |
| `ghost_spans` | bool | `false` | Enable ghost spans to preserve trace structure for dropped spans. |
| `rules` | list | `[]` | Ordered list of sampling rules. |
| `rules[].sampling_percentage` | float32 | | Sampling percentage if the condition matches (0-100). |
| `rules[].condition` | string | | [OTTL condition](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/pkg/ottl) evaluated against each span. Has access to span attributes, resource attributes, span name, and other span fields. |

## How it works

1. For each span, rules are evaluated in order. The highest
   `sampling_percentage` among all matching rules is selected. If no rules
   match, `default_sampling_percentage` is used.
2. The effective percentage is converted to a scaled threshold out of 16384
   hash buckets.
3. An FNV-32a hash of the trace ID (with optional seed) is computed and masked
   to the bucket range. If the hash falls below the threshold, the span is
   kept; otherwise it is dropped.
4. Empty resource spans and scope spans are removed after filtering. If all
   spans are dropped, the entire batch is skipped.

Because sampling decisions are based on the trace ID hash, all spans belonging
to the same trace will receive the same decision (assuming they match the same
rules), even across multiple collector instances using the same `hash_seed`.

## Ghost spans

When `ghost_spans` is enabled, spans that would normally be dropped can instead
be emitted as lightweight "ghost" spans that preserve trace topology. This
allows downstream systems to reconstruct the structure of a trace even when
most spans have been sampled away.

### When ghost spans are produced

Ghost spans are produced for spans whose trace ID hash falls between their
effective sampling threshold and the maximum configured threshold across all
rules. The sampling decision becomes three-valued:

| Hash range | Outcome |
|---|---|
| `hash < effectiveThreshold` | **Keep** — span passes through unchanged |
| `effectiveThreshold <= hash < maxConfiguredThreshold` | **Ghost** — span stripped to skeleton |
| `hash >= maxConfiguredThreshold` | **Drop** — span removed entirely |

When no rules are configured (or all rules have the same percentage as the
default), the effective and maximum thresholds are equal, so no ghost spans are
produced and behavior is identical to having `ghost_spans` disabled.

### What ghost spans contain

A ghost span retains only the minimum information needed to represent a node in
the trace tree:

- **Preserved:** Trace ID, Span ID, Parent Span ID, start and end timestamps, kind, flags
- **Set:** Name is `"unsampled"`, attribute `grafana.partial_trace.ghost = true`
- **Cleared:** All other attributes, events, links, status, dropped counts, trace state

### Example

With `default_sampling_percentage: 10` and a rule at `sampling_percentage: 50`
for error spans, non-error spans will be:

- **Kept** (~10%) — full span data preserved
- **Ghost** (~40%) — skeleton span with trace structure only
- **Dropped** (~50%) — removed entirely

Error spans matching the 50% rule will be kept at 50% and ghosted/dropped at
higher hash values according to the same logic.
