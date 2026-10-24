package plugin

import (
	"fmt"
	"strings"
	"testing"
)

func TestUsageKeyCacheIsBoundedAndCleared(t *testing.T) {
	sink := &fakeSink{}
	seq := &sinkSeq{sinks: []*fakeSink{sink}}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML(""))
	t.Cleanup(func() { _ = p.Close() })
	send(t, p, "usage.handle", map[string]any{"APIKey": "sk-failed", "Failed": true})
	send(t, p, "usage.handle", map[string]any{"APIKey": strings.Repeat("x", maxClientKeyBytes+1)})
	p.mu.Lock()
	empty := len(p.keys) == 0 && p.keyBytes == 0
	p.mu.Unlock()
	if !empty {
		t.Fatal("failed/oversized usage key was cached")
	}
	for i := 0; i < 1100; i++ {
		send(t, p, "usage.handle", map[string]any{"APIKey": fmt.Sprintf("sk-client-%04d", i)})
	}
	p.mu.Lock()
	bounded := len(p.keys) <= 1024 && p.keyBytes <= 4<<20 && p.keyBytes <= p.current.cfg.MaxBufferBytes/16 && p.bytes+p.keyBytes <= p.current.cfg.MaxBufferBytes
	p.mu.Unlock()
	if !bounded {
		t.Fatal("client key cache exceeds capture budget")
	}
	closePlugin(t, p)
	if len(sink.snapshot()) != 0 {
		t.Fatal("usage-only observation must not create captures")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.keys) != 0 || p.keyBytes != 0 || p.bytes != 0 {
		t.Fatal("key cache not released")
	}
}
