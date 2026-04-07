package e164

import (
	"fmt"
	"strings"

	"github.com/nyaruka/phonenumbers"
)

// Normalize converts a dialed number to E.164 digits-only format (no +).
func Normalize(raw, defaultRegion string) (string, error) {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "+")
	raw = strings.TrimLeft(raw, "0")

	num, err := phonenumbers.Parse("+"+raw, defaultRegion)
	if err != nil {
		// Fallback: if all digits, use as-is for prefix matching
		if isDigits(raw) && len(raw) >= 3 {
			return raw, nil
		}
		return "", fmt.Errorf("invalid number: %s", raw)
	}
	if !phonenumbers.IsValidNumber(num) && !isDigits(raw) {
		return "", fmt.Errorf("invalid number: %s", raw)
	}
	formatted := phonenumbers.Format(num, phonenumbers.E164)
	return strings.TrimPrefix(formatted, "+"), nil
}

// NormalizePrefix standardizes a rate prefix (country/destination code).
func NormalizePrefix(prefix string) (string, error) {
	prefix = strings.TrimSpace(prefix)
	prefix = strings.TrimPrefix(prefix, "+")
	prefix = strings.TrimLeft(prefix, "0")
	if !isDigits(prefix) || len(prefix) < 1 {
		return "", fmt.Errorf("invalid prefix: %s", prefix)
	}
	return prefix, nil
}

func isDigits(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(s) > 0
}
