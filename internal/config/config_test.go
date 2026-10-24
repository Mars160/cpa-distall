package config

import "testing"

func TestParse(t *testing.T) {
	c, err := Parse("enabled: true\npriority: 1\n")
	if err != nil || c.Driver != "sqlite" || c.DSN != "cpa-distill.db" {
		t.Fatalf("defaults: %+v %v", c, err)
	}
	for _, raw := range []string{"driver: mysql", "driver: postgres", "write_timeout: nope", "write_timeout: 0s", "write_timeout: 2m", "dsn: [", "queue_size: 0", "request_ttl: -1s", "max_request_bytes: 0", "drain_timeout: 0s", "drain_timeout: 2m"} {
		if _, err := Parse(raw); err == nil {
			t.Errorf("accepted %q", raw)
		}
	}
	c, err = Parse("driver: postgres\ndsn: postgres://localhost/test\nwrite_timeout: 2s")
	if err != nil || c.Timeout().Seconds() != 2 {
		t.Fatalf("postgres: %+v %v", c, err)
	}
}
