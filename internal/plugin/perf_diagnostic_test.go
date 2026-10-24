package plugin

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/mars160/cpa-distill/internal/config"
)

// Diagnostic: a request already over budget must not retain further bytes.
// Measure how much synchronous hook work still happens after capture is dropped:
// the envelope is parsed, but its body is never base64-decoded.
func BenchmarkDiagnosticDroppedHook(b *testing.B) {
	for _, size := range []int{1024, 64 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("body_%d", size), func(b *testing.B) {
			cfg, _ := config.Parse("")
			p := New()
			p.current = &backend{cfg: cfg}
			p.pending["bench"] = &pending{dropped: true}
			raw, _ := json.Marshal(map[string]any{"RequestID": "bench", "ChunkIndex": 0, "Body": bytes.Repeat([]byte("x"), size)})
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p.Handle("response.intercept_stream_chunk", raw)
			}
		})
	}
}
