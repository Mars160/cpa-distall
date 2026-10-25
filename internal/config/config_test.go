package config

import (
	"strconv"
	"testing"
)

func TestParse(t *testing.T) {
	c, err := Parse("enabled: true\npriority: 1\n")
	if err != nil || c.Driver != "sqlite" || c.DSN != "cpa-distill-data" || c.WriterCount != 1 {
		t.Fatalf("defaults: %+v %v", c, err)
	}
	for _, raw := range []string{"driver: mysql", "driver: postgres", "write_timeout: nope", "write_timeout: 0s", "write_timeout: 2m", "dsn: [", "queue_size: 0", "request_ttl: -1s", "max_request_bytes: 0", "drain_timeout: 0s", "drain_timeout: 2m", "writer_count: 0", "writer_count: -1", "writer_count: 65"} {
		if _, err := Parse(raw); err == nil {
			t.Errorf("accepted %q", raw)
		}
	}
	c, err = Parse("driver: postgres\ndsn: postgres://localhost/test\nwrite_timeout: 2s")
	if err != nil || c.Timeout().Seconds() != 2 {
		t.Fatalf("postgres: %+v %v", c, err)
	}
}

// TestParseWriterCount pins the writer_count contract: 1 is the default, the whole
// range up to the documented maximum is accepted, and anything outside it is
// refused instead of being clamped silently.
func TestParseWriterCount(t *testing.T) {
	for _, count := range []int{1, 2, 8, MaxWriterCount} {
		c, err := Parse("writer_count: " + strconv.Itoa(count))
		if err != nil {
			t.Fatalf("Parse(writer_count %d): %v", count, err)
		}
		if c.WriterCount != count {
			t.Fatalf("WriterCount = %d, want %d", c.WriterCount, count)
		}
	}
	for _, raw := range []string{"writer_count: -5", "writer_count: 0", "writer_count: 65", "writer_count: 1000"} {
		if _, err := Parse(raw); err == nil {
			t.Errorf("accepted %q", raw)
		}
	}
	// An unset writer_count keeps the documented default.
	if c, err := Parse("driver: sqlite\ndsn: /tmp/data"); err != nil || c.WriterCount != 1 {
		t.Fatalf("unset writer_count: %+v %v", c, err)
	}
}
