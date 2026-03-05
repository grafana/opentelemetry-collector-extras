// Copyright Grafana Labs
// SPDX-License-Identifier: Apache-2.0

package partialtracesamplerprocessor

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/sampling"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// TestGhostSpanSize measures the protobuf size reduction from ghost span
// conversion on a realistic trace. This is not a correctness test — it serves
// as documentation that ghost spans provide meaningful size savings.
func TestGhostSpanSize(t *testing.T) {
	td := buildMultiScopeTrace(10)

	marshaler := ptrace.ProtoMarshaler{}
	original, err := marshaler.MarshalTraces(td)
	require.NoError(t, err)

	// Convert all spans to ghosts.
	convertAllToGhosts(td)

	ghost, err := marshaler.MarshalTraces(td)
	require.NoError(t, err)

	// Collapse internal ghosts.
	collapseInternalGhosts(td)

	collapsed, err := marshaler.MarshalTraces(td)
	require.NoError(t, err)

	// Consolidate ghost spans into a single empty scope per resource.
	consolidateGhostScopes(td)

	consolidated, err := marshaler.MarshalTraces(td)
	require.NoError(t, err)

	originalGz := gzipSize(t, original)
	ghostGz := gzipSize(t, ghost)
	collapsedGz := gzipSize(t, collapsed)
	consolidatedGz := gzipSize(t, consolidated)

	t.Logf("Protobuf:")
	t.Logf("  Original:     %d bytes", len(original))
	t.Logf("  Ghost:        %d bytes (%.1f%%)", len(ghost), pct(len(ghost), len(original)))
	t.Logf("  Collapsed:    %d bytes (%.1f%%)", len(collapsed), pct(len(collapsed), len(original)))
	t.Logf("  Consolidated: %d bytes (%.1f%%)", len(consolidated), pct(len(consolidated), len(original)))
	t.Logf("Gzipped protobuf:")
	t.Logf("  Original:     %d bytes", originalGz)
	t.Logf("  Ghost:        %d bytes (%.1f%%)", ghostGz, pct(ghostGz, originalGz))
	t.Logf("  Collapsed:    %d bytes (%.1f%%)", collapsedGz, pct(collapsedGz, originalGz))
	t.Logf("  Consolidated: %d bytes (%.1f%%)", consolidatedGz, pct(consolidatedGz, originalGz))
}

func pct(part, whole int) float64 { return float64(part) / float64(whole) * 100 }

func convertAllToGhosts(td ptrace.Traces) {
	for ri := 0; ri < td.ResourceSpans().Len(); ri++ {
		rs := td.ResourceSpans().At(ri)
		for si := 0; si < rs.ScopeSpans().Len(); si++ {
			spans := rs.ScopeSpans().At(si).Spans()
			for spi := 0; spi < spans.Len(); spi++ {
				convertToGhostSpan(spans.At(spi), sampling.AlwaysSampleThreshold)
			}
		}
	}
}

func gzipSize(t *testing.T, data []byte) int {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, err := w.Write(data)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Len()
}

// buildMultiScopeTrace creates a trace spread across multiple instrumentation
// scopes within a single resource, simulating a real service that uses several
// instrumented libraries (HTTP, gRPC, DB).
func buildMultiScopeTrace(spansPerScope int) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	res := rs.Resource().Attributes()
	res.PutStr("service.name", "api-gateway")
	res.PutStr("service.version", "1.4.2")
	res.PutStr("deployment.environment", "production")
	res.PutStr("host.name", "ip-10-0-1-42.ec2.internal")

	traceID := [16]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
		0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10}

	scopes := []struct {
		name    string
		version string
	}{
		{"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp", "0.49.0"},
		{"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc", "0.49.0"},
		{"go.opentelemetry.io/contrib/instrumentation/database/sql/otelsql", "0.49.0"},
	}

	spanIdx := 0
	for _, scope := range scopes {
		ss := rs.ScopeSpans().AppendEmpty()
		ss.Scope().SetName(scope.name)
		ss.Scope().SetVersion(scope.version)

		for i := range spansPerScope {
			span := ss.Spans().AppendEmpty()
			span.SetTraceID(pcommon.TraceID(traceID))
			span.SetSpanID([8]byte{byte(spanIdx + 1), 0, 0, 0, 0, 0, 0, byte(spanIdx)})
			if spanIdx > 0 {
				span.SetParentSpanID([8]byte{byte(spanIdx), 0, 0, 0, 0, 0, 0, byte(spanIdx - 1)})
			}
			// Realistic mix: ~1/3 server, ~1/3 client, ~1/3 internal.
			switch spanIdx % 3 {
			case 0:
				span.SetName(fmt.Sprintf("GET /api/users/%d", i))
				span.SetKind(ptrace.SpanKindServer)
			case 1:
				span.SetName(fmt.Sprintf("GET /api/users/%d", i))
				span.SetKind(ptrace.SpanKindClient)
			default:
				span.SetName(fmt.Sprintf("middleware.auth.%d", i))
				span.SetKind(ptrace.SpanKindInternal)
			}
			span.SetStartTimestamp(pcommon.Timestamp(1700000000000000000 + int64(spanIdx)*1000000))
			span.SetEndTimestamp(pcommon.Timestamp(1700000000050000000 + int64(spanIdx)*1000000))
			span.Status().SetCode(ptrace.StatusCodeOk)

			attrs := span.Attributes()
			attrs.PutStr("http.method", "GET")
			attrs.PutInt("http.status_code", 200)
			attrs.PutStr("http.url", fmt.Sprintf("https://api.example.com/api/users/%d", i))
			attrs.PutStr("net.peer.ip", "10.0.1.100")
			attrs.PutInt("net.peer.port", 443)
			attrs.PutStr("http.user_agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")

			evt1 := span.Events().AppendEmpty()
			evt1.SetName("request.received")
			evt1.SetTimestamp(pcommon.Timestamp(1700000000000000000 + int64(spanIdx)*1000000))
			evt1.Attributes().PutStr("request.id", fmt.Sprintf("req-%d", spanIdx))
			evt1.Attributes().PutInt("request.size", 1024)

			evt2 := span.Events().AppendEmpty()
			evt2.SetName("response.sent")
			evt2.SetTimestamp(pcommon.Timestamp(1700000000050000000 + int64(spanIdx)*1000000))
			evt2.Attributes().PutInt("response.size", 4096)

			link := span.Links().AppendEmpty()
			link.SetTraceID([16]byte{0xff, 0xee, 0xdd, 0xcc, byte(spanIdx)})
			link.Attributes().PutStr("link.type", "parent_from_other_trace")

			spanIdx++
		}
	}

	return td
}
