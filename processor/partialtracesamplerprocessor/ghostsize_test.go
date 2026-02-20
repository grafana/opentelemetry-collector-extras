// Copyright Grafana Labs
// SPDX-License-Identifier: Apache-2.0

package partialtracesamplerprocessor

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// TestGhostSpanSize measures the protobuf size reduction from ghost span
// conversion on a realistic trace. This is not a correctness test — it serves
// as documentation that ghost spans provide meaningful size savings.
func TestGhostSpanSize(t *testing.T) {
	// Uncomment to run this and get an idea of ghost span size.
	td := buildRealisticTrace(10)

	marshaler := ptrace.ProtoMarshaler{}
	original, err := marshaler.MarshalTraces(td)
	require.NoError(t, err)

	// Convert all spans to ghosts.
	for ri := 0; ri < td.ResourceSpans().Len(); ri++ {
		rs := td.ResourceSpans().At(ri)
		for si := 0; si < rs.ScopeSpans().Len(); si++ {
			spans := rs.ScopeSpans().At(si).Spans()
			for spi := 0; spi < spans.Len(); spi++ {
				convertToGhostSpan(spans.At(spi))
			}
		}
	}

	ghost, err := marshaler.MarshalTraces(td)
	require.NoError(t, err)

	originalGz := gzipSize(t, original)
	ghostGz := gzipSize(t, ghost)

	ratio := float64(len(ghost)) / float64(len(original)) * 100
	ratioGz := float64(ghostGz) / float64(originalGz) * 100

	t.Logf("Protobuf:")
	t.Logf("  Original: %d bytes", len(original))
	t.Logf("  Ghost:    %d bytes", len(ghost))
	t.Logf("  Ratio:    %.1f%% (%.1fx smaller)", ratio, float64(len(original))/float64(len(ghost)))
	t.Logf("Gzipped protobuf:")
	t.Logf("  Original: %d bytes", originalGz)
	t.Logf("  Ghost:    %d bytes", ghostGz)
	t.Logf("  Ratio:    %.1f%% (%.1fx smaller)", ratioGz, float64(originalGz)/float64(ghostGz))
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

// buildRealisticTrace creates a trace with the given number of spans, each
// populated with attributes, events, and links typical of an HTTP service.
func buildRealisticTrace(numSpans int) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	res := rs.Resource().Attributes()
	res.PutStr("service.name", "api-gateway")
	res.PutStr("service.version", "1.4.2")
	res.PutStr("deployment.environment", "production")
	res.PutStr("host.name", "ip-10-0-1-42.ec2.internal")

	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("go.opentelemetry.io/contrib/instrumentation/net/http")
	ss.Scope().SetVersion("0.49.0")

	traceID := [16]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
		0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10}

	for i := range numSpans {
		span := ss.Spans().AppendEmpty()
		span.SetTraceID(pcommon.TraceID(traceID))
		span.SetSpanID([8]byte{byte(i + 1), 0, 0, 0, 0, 0, 0, byte(i)})
		if i > 0 {
			span.SetParentSpanID([8]byte{byte(i), 0, 0, 0, 0, 0, 0, byte(i - 1)})
		}
		span.SetName(fmt.Sprintf("GET /api/users/%d", i))
		span.SetKind(ptrace.SpanKindServer)
		span.SetStartTimestamp(pcommon.Timestamp(1700000000000000000 + int64(i)*1000000))
		span.SetEndTimestamp(pcommon.Timestamp(1700000000050000000 + int64(i)*1000000))
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
		evt1.SetTimestamp(pcommon.Timestamp(1700000000000000000 + int64(i)*1000000))
		evt1.Attributes().PutStr("request.id", fmt.Sprintf("req-%d", i))
		evt1.Attributes().PutInt("request.size", 1024)

		evt2 := span.Events().AppendEmpty()
		evt2.SetName("response.sent")
		evt2.SetTimestamp(pcommon.Timestamp(1700000000050000000 + int64(i)*1000000))
		evt2.Attributes().PutInt("response.size", 4096)

		link := span.Links().AppendEmpty()
		link.SetTraceID([16]byte{0xff, 0xee, 0xdd, 0xcc, byte(i)})
		link.Attributes().PutStr("link.type", "parent_from_other_trace")
	}

	return td
}
