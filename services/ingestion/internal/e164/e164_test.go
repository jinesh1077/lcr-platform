package e164_test

import (
	"testing"

	"github.com/carrier-opt/ingestion/internal/e164"
)

func TestNormalizeUK(t *testing.T) {
	got, err := e164.Normalize("447700900123", "GB")
	if err != nil {
		t.Fatal(err)
	}
	if got != "447700900123" {
		t.Errorf("got %s, want 447700900123", got)
	}
}

func TestNormalizePrefix(t *testing.T) {
	got, err := e164.NormalizePrefix("+44")
	if err != nil {
		t.Fatal(err)
	}
	if got != "44" {
		t.Errorf("got %s, want 44", got)
	}
}

func TestNormalizeWithPlus(t *testing.T) {
	got, err := e164.Normalize("+33123456789", "FR")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) < 2 {
		t.Errorf("expected valid prefix, got %s", got)
	}
}
