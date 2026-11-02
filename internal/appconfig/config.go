package appconfig

import (
	"fmt"
	"net"
	"os"
	"strconv"
)

// These defaults are for isolated loopback development ONLY; never production secrets.
func Env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

const DefaultDB = "postgres://mtbt:local-dev-only@127.0.0.1:15432/mtbt?sslmode=disable"
const DefaultNATS = "nats://127.0.0.1:14222"

// ValidateBindAddress checks that addr is a numeric IPv4/IPv6 loopback or allows non-loopback if explicitly permitted.
// Returns error if the address is invalid, not numeric, or non-loopback without permission.
func ValidateBindAddress(addr string, allowNonLoopback bool) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid address format: %w", err)
	}

	if host == "" || port == "" {
		return fmt.Errorf("invalid address format: empty host or port")
	}

	// Validate port is numeric and in valid range.
	portNum, err := strconv.Atoi(port)
	if err != nil || portNum < 0 || portNum > 65535 {
		return fmt.Errorf("invalid port: must be numeric 0..65535")
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("address must be numeric IP, not DNS or hostname: %s", host)
	}

	// Allow IPv6 loopback or any IPv4/IPv6 if permission granted
	if ip.IsLoopback() {
		return nil
	}

	if !allowNonLoopback {
		return fmt.Errorf("non-loopback bind requires explicit -allow-non-loopback flag; got %s", host)
	}

	return nil
}

// ValidateDestAddress checks destination address (for simulator/client connections).
// Allows DNS names and numeric IPs; IPv6 loopback acceptable; DNS requires opt-in unless resolved to loopback.
func ValidateDestAddress(addr string, allowNonLoopback bool) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid address format: %w", err)
	}

	if host == "" || port == "" {
		return fmt.Errorf("invalid address format: empty host or port")
	}

	// Validate port is numeric and in valid range (>0 for destination).
	portNum, err := strconv.Atoi(port)
	if err != nil || portNum <= 0 || portNum > 65535 {
		return fmt.Errorf("invalid port: must be numeric 1..65535 for destination")
	}

	ip := net.ParseIP(host)
	if ip == nil {
		// DNS name provided - allow only if opted-in or resolves to loopback
		if allowNonLoopback {
			return nil
		}
		// Try to resolve to check if it's localhost
		resolved, err := net.ResolveUDPAddr("udp4", host+":"+port)
		if err == nil && resolved.IP.IsLoopback() {
			return nil
		}
		return fmt.Errorf("DNS destination requires explicit -allow-non-loopback flag; got %s", host)
	}

	// Numeric IP provided - allow if loopback or opted-in
	if ip.IsLoopback() {
		return nil
	}

	if !allowNonLoopback {
		return fmt.Errorf("non-loopback destination requires explicit -allow-non-loopback flag; got %s", host)
	}

	return nil
}
