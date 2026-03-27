package ghostspanprocessor

import (
	"context"
	"encoding/hex"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

const (
	ghostSpanAttributeKey = "grafana.partial_trace.ghost"
	collapsedSpanIDsKey   = "grafana.partial_trace.collapsed_span_ids"
)

type ghostSpanProcessor struct{}

func newGhostSpanProcessor() *ghostSpanProcessor {
	return &ghostSpanProcessor{}
}

func (p *ghostSpanProcessor) processTraces(_ context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	if td.SpanCount() == 0 {
		return td, processorhelper.ErrSkipProcessingData
	}

	// Step 1: Build a map of ghost span ID -> parent span ID.
	ghostParents := make(map[pcommon.SpanID]pcommon.SpanID)
	serverGhosts := make(map[pcommon.SpanID]struct{})
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				span := ss.Spans().At(k)
				if isGhost(span) {
					ghostParents[span.SpanID()] = span.ParentSpanID()
					if span.Kind() == ptrace.SpanKindServer {
						serverGhosts[span.SpanID()] = struct{}{}
					}
				}
			}
		}
	}

	// Check if any span has collapsed span IDs from the partial trace sampler.
	hasCollapsed := false
	for i := 0; i < td.ResourceSpans().Len() && !hasCollapsed; i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len() && !hasCollapsed; j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				if _, ok := ss.Spans().At(k).Attributes().Get(collapsedSpanIDsKey); ok {
					hasCollapsed = true
					break
				}
			}
		}
	}

	if len(ghostParents) == 0 && !hasCollapsed {
		return td, nil
	}

	// Find server ghosts that are ancestors of at least one non-ghost span.
	// Walk up from each non-ghost span through ghost parents, marking any
	// server ghosts encountered along the way.
	neededServers := make(map[pcommon.SpanID]struct{})
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				span := ss.Spans().At(k)
				if isGhost(span) {
					continue
				}
				current := span.ParentSpanID()
				visited := make(map[pcommon.SpanID]struct{})
				for {
					if _, cycle := visited[current]; cycle {
						break
					}
					if _, ok := ghostParents[current]; !ok {
						break
					}
					if _, ok := serverGhosts[current]; ok {
						neededServers[current] = struct{}{}
					}
					visited[current] = struct{}{}
					current = ghostParents[current]
				}
			}
		}
	}

	// Keep only server ghosts that have non-ghost descendants.
	for id := range neededServers {
		delete(ghostParents, id)
	}

	// Step 2: Resolve chains — follow parent pointers until reaching a non-ghost.
	resolved := make(map[pcommon.SpanID]pcommon.SpanID, len(ghostParents))
	for ghostID := range ghostParents {
		resolveGhostChain(ghostID, ghostParents, resolved)
	}

	// Build a lookup from collapsed span IDs to the span that absorbed them.
	// This lets us reparent spans whose parent was collapsed during partial
	// trace sampling (before groupbytrace assembled the full trace).
	collapsedLookup := make(map[pcommon.SpanID]pcommon.SpanID)
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				span := ss.Spans().At(k)
				v, ok := span.Attributes().Get(collapsedSpanIDsKey)
				if !ok || v.Type() != pcommon.ValueTypeSlice {
					continue
				}
				sl := v.Slice()
				for idx := 0; idx < sl.Len(); idx++ {
					idStr := sl.At(idx).Str()
					b, err := hex.DecodeString(idStr)
					if err != nil || len(b) != 8 {
						continue
					}
					var collapsedID pcommon.SpanID
					copy(collapsedID[:], b)
					collapsedLookup[collapsedID] = span.SpanID()
				}
			}
		}
	}

	// Step 3: Reparent any span (including kept server ghosts) whose parent
	// is a ghost being removed, or was collapsed into another span.
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				span := ss.Spans().At(k)
				if _, beingRemoved := ghostParents[span.SpanID()]; !beingRemoved {
					parentID := span.ParentSpanID()
					if newParent, ok := resolved[parentID]; ok {
						span.SetParentSpanID(newParent)
					} else if newParent, ok := collapsedLookup[parentID]; ok {
						span.SetParentSpanID(newParent)
					}
				}
				// Clean up the collapsed span IDs attribute.
				span.Attributes().Remove(collapsedSpanIDsKey)
			}
		}
	}

	// Step 4: Remove ghost spans that were not kept, then clean up empty scopes/resources.
	td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
		rs.ScopeSpans().RemoveIf(func(ss ptrace.ScopeSpans) bool {
			ss.Spans().RemoveIf(func(span ptrace.Span) bool {
				_, remove := ghostParents[span.SpanID()]
				return remove
			})
			return ss.Spans().Len() == 0
		})
		return rs.ScopeSpans().Len() == 0
	})

	return td, nil
}

// isGhost checks if a span has the ghost attribute set to true.
func isGhost(span ptrace.Span) bool {
	v, ok := span.Attributes().Get(ghostSpanAttributeKey)
	return ok && v.Type() == pcommon.ValueTypeBool && v.Bool()
}

// resolveGhostChain follows parent pointers from a ghost span until it reaches
// a non-ghost ancestor. Uses cycle detection via a visited set.
func resolveGhostChain(id pcommon.SpanID, ghostParents map[pcommon.SpanID]pcommon.SpanID, resolved map[pcommon.SpanID]pcommon.SpanID) pcommon.SpanID {
	if r, ok := resolved[id]; ok {
		return r
	}

	// Walk up the ghost chain collecting all ghost span IDs visited.
	var chain []pcommon.SpanID
	visited := make(map[pcommon.SpanID]struct{})
	current := id
	for {
		if _, cycle := visited[current]; cycle {
			// Cycle detected — resolve to the parent of the current ghost.
			break
		}
		parent, isGhost := ghostParents[current]
		if !isGhost {
			// current is not a ghost — it's the resolved non-ghost ancestor.
			break
		}
		visited[current] = struct{}{}
		chain = append(chain, current)
		current = parent
	}

	// current is the first non-ghost ancestor. Cache for all ghosts in chain.
	for _, ghostID := range chain {
		resolved[ghostID] = current
	}
	return current
}
