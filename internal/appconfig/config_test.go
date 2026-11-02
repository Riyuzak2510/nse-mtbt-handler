package appconfig

import (
	"testing"
)

func TestValidateBindAddress(t *testing.T) {
	tests := []struct {
		name             string
		addr             string
		allowNonLoopback bool
		expectError      bool
		errorSubstring   string
	}{
		// Valid loopback cases
		{"loopback 127.0.0.1", "127.0.0.1:8080", false, false, ""},
		{"loopback 127.0.0.2", "127.0.0.2:9000", false, false, ""},
		{"loopback 127.255.255.255", "127.255.255.255:19002", false, false, ""},
		{"IPv6 loopback", "[::1]:8080", false, false, ""},
		{"IPv6 loopback port 0", "[::1]:0", false, false, ""},

		// Valid wildcard with permission
		{"wildcard with permission", "0.0.0.0:8080", true, false, ""},
		{"specific IP with permission", "192.168.1.1:8080", true, false, ""},

		// Invalid: non-loopback without permission
		{"0.0.0.0 without permission", "0.0.0.0:8080", false, true, "non-loopback bind requires explicit -allow-non-loopback flag"},
		{"192.168.1.1 without permission", "192.168.1.1:8080", false, true, "non-loopback bind requires explicit -allow-non-loopback flag"},
		{"10.0.0.1 without permission", "10.0.0.1:8080", false, true, "non-loopback bind requires explicit -allow-non-loopback flag"},

		// Invalid: DNS names not allowed for bind
		{"DNS name", "localhost:8080", false, true, "address must be numeric IP, not DNS or hostname"},
		{"DNS name with permission", "localhost:8080", true, true, "address must be numeric IP, not DNS or hostname"},

		// Invalid: malformed addresses
		{"missing port", "127.0.0.1", false, true, "invalid address format"},
		{"missing host", ":8080", false, true, "empty host or port"},
		{"invalid host", "256.256.256.256:8080", false, true, ""},
		{"invalid port", "127.0.0.1:99999", false, true, "invalid port"},
		{"non-numeric port", "127.0.0.1:abc", false, true, "invalid port"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateBindAddress(tc.addr, tc.allowNonLoopback)
			if tc.expectError && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tc.expectError && err != nil {
				t.Errorf("expected no error, got: %v", err)
			}
			if tc.expectError && tc.errorSubstring != "" && (err == nil || !contains(err.Error(), tc.errorSubstring)) {
				t.Errorf("expected error containing %q, got: %v", tc.errorSubstring, err)
			}
		})
	}
}

func TestValidateDestAddress(t *testing.T) {
	tests := []struct {
		name             string
		addr             string
		allowNonLoopback bool
		expectError      bool
		errorSubstring   string
	}{
		// Valid loopback numeric cases
		{"loopback 127.0.0.1", "127.0.0.1:19001", false, false, ""},
		{"loopback 127.0.0.2", "127.0.0.2:19001", false, false, ""},
		{"loopback 127.255.255.255", "127.255.255.255:19001", false, false, ""},
		{"IPv6 loopback", "[::1]:19001", false, false, ""},

		// Valid non-loopback numeric with permission
		{"0.0.0.0 with permission", "0.0.0.0:19001", true, false, ""},
		{"192.168.1.1 with permission", "192.168.1.1:19001", true, false, ""},
		{"specific IP with permission", "10.0.0.1:19001", true, false, ""},

		// Invalid: non-loopback numeric without permission
		{"0.0.0.0 without permission", "0.0.0.0:19001", false, true, "non-loopback destination requires explicit -allow-non-loopback flag"},
		{"192.168.1.1 without permission", "192.168.1.1:19001", false, true, "non-loopback destination requires explicit -allow-non-loopback flag"},

		// Valid: DNS names only with permission
		{"DNS name with permission", "publisher:19001", true, false, ""},
		{"FQDN with permission", "publisher.example.com:19001", true, false, ""},
		{"localhost without permission resolves to loopback", "localhost:19001", false, false, ""},

		// Invalid: DNS names without permission (localhost resolves to loopback so it's allowed)
		{"DNS name without permission", "publisher:19001", false, true, "DNS destination requires explicit -allow-non-loopback flag"},

		// Invalid: malformed addresses
		{"missing port", "127.0.0.1", false, true, "invalid address format"},
		{"missing host", ":19001", false, true, "empty host or port"},
		{"invalid host", "256.256.256.256:19001", false, true, ""},
		{"port 0", "127.0.0.1:0", false, true, "invalid port"},
		{"invalid port", "127.0.0.1:99999", false, true, "invalid port"},
		{"non-numeric port", "127.0.0.1:abc", false, true, "invalid port"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateDestAddress(tc.addr, tc.allowNonLoopback)
			if tc.expectError && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tc.expectError && err != nil {
				t.Errorf("expected no error, got: %v", err)
			}
			if tc.expectError && tc.errorSubstring != "" && (err == nil || !contains(err.Error(), tc.errorSubstring)) {
				t.Errorf("expected error containing %q, got: %v", tc.errorSubstring, err)
			}
		})
	}
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
