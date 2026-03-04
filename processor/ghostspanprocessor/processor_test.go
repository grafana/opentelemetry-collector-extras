package ghostspanprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

func newSpanID(b byte) pcommon.SpanID {
	return pcommon.SpanID{0, 0, 0, 0, 0, 0, 0, b}
}

var emptySpanID = pcommon.SpanID{}

// addSpan adds a span to the traces with the given parameters.
// If ghost is true, the ghost attribute is set.
func addSpan(td ptrace.Traces, name string, spanID, parentSpanID pcommon.SpanID, ghost bool) ptrace.Span {
	rs := td.ResourceSpans()
	if rs.Len() == 0 {
		rs.AppendEmpty()
	}
	ss := rs.At(0).ScopeSpans()
	if ss.Len() == 0 {
		ss.AppendEmpty()
	}
	span := ss.At(0).Spans().AppendEmpty()
	span.SetName(name)
	span.SetSpanID(spanID)
	span.SetParentSpanID(parentSpanID)
	if ghost {
		span.Attributes().PutBool(ghostSpanAttributeKey, true)
	}
	return span
}

func TestReparentGhosts(t *testing.T) {
	// server(1) -> ghost(2) -> client(3)
	// Should become: server(1) -> client(3)
	td := ptrace.NewTraces()
	addSpan(td, "server", newSpanID(1), emptySpanID, false)
	addSpan(td, "ghost", newSpanID(2), newSpanID(1), true)
	addSpan(td, "client", newSpanID(3), newSpanID(2), false)

	p := newGhostSpanProcessor()
	result, err := p.processTraces(context.Background(), td)
	require.NoError(t, err)

	// Should have 2 spans (ghost removed).
	assert.Equal(t, 2, result.SpanCount())

	spans := collectSpans(result)
	// client's parent should now be server.
	client := spans["client"]
	require.NotNil(t, client)
	assert.Equal(t, newSpanID(1), client.ParentSpanID())

	// server should be unchanged.
	server := spans["server"]
	require.NotNil(t, server)
	assert.Equal(t, emptySpanID, server.ParentSpanID())

	// ghost should be gone.
	assert.Nil(t, spans["ghost"])
}

func TestReparentChainedGhosts(t *testing.T) {
	// server(1) -> ghost1(2) -> ghost2(3) -> client(4)
	// Should become: server(1) -> client(4)
	td := ptrace.NewTraces()
	addSpan(td, "server", newSpanID(1), emptySpanID, false)
	addSpan(td, "ghost1", newSpanID(2), newSpanID(1), true)
	addSpan(td, "ghost2", newSpanID(3), newSpanID(2), true)
	addSpan(td, "client", newSpanID(4), newSpanID(3), false)

	p := newGhostSpanProcessor()
	result, err := p.processTraces(context.Background(), td)
	require.NoError(t, err)

	assert.Equal(t, 2, result.SpanCount())

	spans := collectSpans(result)
	client := spans["client"]
	require.NotNil(t, client)
	assert.Equal(t, newSpanID(1), client.ParentSpanID())

	assert.Nil(t, spans["ghost1"])
	assert.Nil(t, spans["ghost2"])
}

func TestNonGhostSpansUnchanged(t *testing.T) {
	td := ptrace.NewTraces()
	addSpan(td, "server", newSpanID(1), emptySpanID, false)
	addSpan(td, "client", newSpanID(2), newSpanID(1), false)

	p := newGhostSpanProcessor()
	result, err := p.processTraces(context.Background(), td)
	require.NoError(t, err)

	assert.Equal(t, 2, result.SpanCount())

	spans := collectSpans(result)
	server := spans["server"]
	require.NotNil(t, server)
	assert.Equal(t, emptySpanID, server.ParentSpanID())

	client := spans["client"]
	require.NotNil(t, client)
	assert.Equal(t, newSpanID(1), client.ParentSpanID())
}

func TestEmptyTraces(t *testing.T) {
	td := ptrace.NewTraces()

	p := newGhostSpanProcessor()
	_, err := p.processTraces(context.Background(), td)
	assert.ErrorIs(t, err, processorhelper.ErrSkipProcessingData)
}

func TestRemovesScopes(t *testing.T) {
	// Create traces with a ghost span in its own resource/scope.
	td := ptrace.NewTraces()

	// Resource 1: only ghost spans (non-root) — should be removed entirely.
	rs1 := td.ResourceSpans().AppendEmpty()
	rs1.Resource().Attributes().PutStr("service.name", "ghost-service")
	ss1 := rs1.ScopeSpans().AppendEmpty()
	ghost := ss1.Spans().AppendEmpty()
	ghost.SetName("ghost")
	ghost.SetSpanID(newSpanID(1))
	ghost.SetParentSpanID(newSpanID(99))
	ghost.Attributes().PutBool(ghostSpanAttributeKey, true)

	// Resource 2: has a real span — should be kept.
	rs2 := td.ResourceSpans().AppendEmpty()
	rs2.Resource().Attributes().PutStr("service.name", "real-service")
	ss2 := rs2.ScopeSpans().AppendEmpty()
	real := ss2.Spans().AppendEmpty()
	real.SetName("real")
	real.SetSpanID(newSpanID(2))

	p := newGhostSpanProcessor()
	result, err := p.processTraces(context.Background(), td)
	require.NoError(t, err)

	assert.Equal(t, 1, result.ResourceSpans().Len())
	assert.Equal(t, 1, result.SpanCount())

	svc, ok := result.ResourceSpans().At(0).Resource().Attributes().Get("service.name")
	require.True(t, ok)
	assert.Equal(t, "real-service", svc.Str())
}

func TestKeepServerGhostWithNonGhostParent(t *testing.T) {
	// real(1) -> server_ghost(2) -> client(3)
	// Server ghost is kept because its parent is not a ghost.
	// client's parent stays as server_ghost.
	td := ptrace.NewTraces()
	addSpan(td, "real", newSpanID(1), emptySpanID, false)
	ghost := addSpan(td, "server_ghost", newSpanID(2), newSpanID(1), true)
	ghost.SetKind(ptrace.SpanKindServer)
	addSpan(td, "client", newSpanID(3), newSpanID(2), false)

	p := newGhostSpanProcessor()
	result, err := p.processTraces(context.Background(), td)
	require.NoError(t, err)

	assert.Equal(t, 3, result.SpanCount())

	spans := collectSpans(result)
	assert.NotNil(t, spans["server_ghost"])
	assert.Equal(t, newSpanID(1), spans["server_ghost"].ParentSpanID())

	// client's parent is the kept server ghost.
	assert.Equal(t, newSpanID(2), spans["client"].ParentSpanID())
}

func TestKeepServerGhostWithGhostParent(t *testing.T) {
	// real(1) -> ghost(2) -> server_ghost(3) -> client(4)
	// Server ghost is kept and reparented to real(1). ghost(2) removed.
	// Result: real(1) -> server_ghost(3) -> client(4)
	td := ptrace.NewTraces()
	addSpan(td, "real", newSpanID(1), emptySpanID, false)
	addSpan(td, "ghost", newSpanID(2), newSpanID(1), true)
	sGhost := addSpan(td, "server_ghost", newSpanID(3), newSpanID(2), true)
	sGhost.SetKind(ptrace.SpanKindServer)
	addSpan(td, "client", newSpanID(4), newSpanID(3), false)

	p := newGhostSpanProcessor()
	result, err := p.processTraces(context.Background(), td)
	require.NoError(t, err)

	assert.Equal(t, 3, result.SpanCount())

	spans := collectSpans(result)
	assert.Nil(t, spans["ghost"])
	assert.NotNil(t, spans["server_ghost"])
	// server_ghost reparented past the removed ghost to real.
	assert.Equal(t, newSpanID(1), spans["server_ghost"].ParentSpanID())
	assert.Equal(t, newSpanID(3), spans["client"].ParentSpanID())
}

func TestKeepServerGhostChainedWithNonGhostChildren(t *testing.T) {
	// real(1) -> server_ghost(2) -> ghost(3) -> client(4)
	// server_ghost kept (parent is real), ghost(3) removed, client reparented to server_ghost.
	td := ptrace.NewTraces()
	addSpan(td, "real", newSpanID(1), emptySpanID, false)
	sGhost := addSpan(td, "server_ghost", newSpanID(2), newSpanID(1), true)
	sGhost.SetKind(ptrace.SpanKindServer)
	addSpan(td, "ghost", newSpanID(3), newSpanID(2), true)
	addSpan(td, "client", newSpanID(4), newSpanID(3), false)

	p := newGhostSpanProcessor()
	result, err := p.processTraces(context.Background(), td)
	require.NoError(t, err)

	assert.Equal(t, 3, result.SpanCount())

	spans := collectSpans(result)
	assert.NotNil(t, spans["server_ghost"])
	assert.Nil(t, spans["ghost"])
	// client reparented to the kept server ghost.
	assert.Equal(t, newSpanID(2), spans["client"].ParentSpanID())
}

func TestRemoveRootGhostSpan(t *testing.T) {
	// root_ghost(1) -> ghost(2) -> client(3)
	// Root ghost is removed (not a server span), client reparented to empty.
	td := ptrace.NewTraces()
	addSpan(td, "root_ghost", newSpanID(1), emptySpanID, true)
	addSpan(td, "ghost", newSpanID(2), newSpanID(1), true)
	addSpan(td, "client", newSpanID(3), newSpanID(2), false)

	p := newGhostSpanProcessor()
	result, err := p.processTraces(context.Background(), td)
	require.NoError(t, err)

	assert.Equal(t, 1, result.SpanCount())

	spans := collectSpans(result)
	assert.Nil(t, spans["root_ghost"])
	assert.Nil(t, spans["ghost"])
	assert.Equal(t, emptySpanID, spans["client"].ParentSpanID())
}

func TestRemoveServerGhostWithNoRealDescendants(t *testing.T) {
	// real(1) -> server_ghost(2) -> ghost(3)
	// server_ghost has no non-ghost descendants, so it gets removed too.
	// Result: real(1)
	td := ptrace.NewTraces()
	addSpan(td, "real", newSpanID(1), emptySpanID, false)
	sGhost := addSpan(td, "server_ghost", newSpanID(2), newSpanID(1), true)
	sGhost.SetKind(ptrace.SpanKindServer)
	addSpan(td, "ghost", newSpanID(3), newSpanID(2), true)

	p := newGhostSpanProcessor()
	result, err := p.processTraces(context.Background(), td)
	require.NoError(t, err)

	assert.Equal(t, 1, result.SpanCount())

	spans := collectSpans(result)
	assert.NotNil(t, spans["real"])
	assert.Nil(t, spans["server_ghost"])
	assert.Nil(t, spans["ghost"])
}

func TestFullServiceCallChain(t *testing.T) {
	// real(1) -> client_ghost(2) -> server_ghost(3) -> ghost(4) -> real_child(5)
	// Collapses to: real(1) -> server_ghost(3) -> real_child(5)
	td := ptrace.NewTraces()
	addSpan(td, "real", newSpanID(1), emptySpanID, false)
	addSpan(td, "client_ghost", newSpanID(2), newSpanID(1), true)
	sGhost := addSpan(td, "server_ghost", newSpanID(3), newSpanID(2), true)
	sGhost.SetKind(ptrace.SpanKindServer)
	addSpan(td, "ghost", newSpanID(4), newSpanID(3), true)
	addSpan(td, "real_child", newSpanID(5), newSpanID(4), false)

	p := newGhostSpanProcessor()
	result, err := p.processTraces(context.Background(), td)
	require.NoError(t, err)

	assert.Equal(t, 3, result.SpanCount())

	spans := collectSpans(result)
	assert.Nil(t, spans["client_ghost"])
	assert.Nil(t, spans["ghost"])

	assert.NotNil(t, spans["real"])
	assert.Equal(t, emptySpanID, spans["real"].ParentSpanID())

	assert.NotNil(t, spans["server_ghost"])
	assert.Equal(t, newSpanID(1), spans["server_ghost"].ParentSpanID())

	assert.NotNil(t, spans["real_child"])
	assert.Equal(t, newSpanID(3), spans["real_child"].ParentSpanID())
}

func TestNestedServiceCalls(t *testing.T) {
	// real(1) -> client_ghost_A(2) -> server_ghost_A(3) -> client_ghost_B(4) -> server_ghost_B(5) -> real_leaf(6)
	// Collapses to: real(1) -> server_ghost_A(3) -> server_ghost_B(5) -> real_leaf(6)
	td := ptrace.NewTraces()
	addSpan(td, "real", newSpanID(1), emptySpanID, false)
	addSpan(td, "client_ghost_A", newSpanID(2), newSpanID(1), true)
	sgA := addSpan(td, "server_ghost_A", newSpanID(3), newSpanID(2), true)
	sgA.SetKind(ptrace.SpanKindServer)
	addSpan(td, "client_ghost_B", newSpanID(4), newSpanID(3), true)
	sgB := addSpan(td, "server_ghost_B", newSpanID(5), newSpanID(4), true)
	sgB.SetKind(ptrace.SpanKindServer)
	addSpan(td, "real_leaf", newSpanID(6), newSpanID(5), false)

	p := newGhostSpanProcessor()
	result, err := p.processTraces(context.Background(), td)
	require.NoError(t, err)

	assert.Equal(t, 4, result.SpanCount())

	spans := collectSpans(result)
	assert.Nil(t, spans["client_ghost_A"])
	assert.Nil(t, spans["client_ghost_B"])

	assert.Equal(t, emptySpanID, spans["real"].ParentSpanID())
	assert.Equal(t, newSpanID(1), spans["server_ghost_A"].ParentSpanID())
	assert.Equal(t, newSpanID(3), spans["server_ghost_B"].ParentSpanID())
	assert.Equal(t, newSpanID(5), spans["real_leaf"].ParentSpanID())
}

// collectSpans returns a map of span name -> span for easy assertions.
func collectSpans(td ptrace.Traces) map[string]*ptrace.Span {
	result := make(map[string]*ptrace.Span)
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				span := ss.Spans().At(k)
				result[span.Name()] = &span
			}
		}
	}
	return result
}
