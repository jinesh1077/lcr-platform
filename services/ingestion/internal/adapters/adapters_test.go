package adapters_test

import (
	"strings"
	"testing"
	"time"

	"github.com/carrier-opt/ingestion/internal/adapters"
)

func TestDefaultCSV(t *testing.T) {
	csv := `prefix,carrier_id,cost_per_min
44,nexatel,0.012
447,clearpath,0.011`
	a := adapters.Get("default")
	sheet, err := a.Parse(strings.NewReader(csv), "vendor-default", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(sheet.Rates) != 2 {
		t.Fatalf("got %d rates", len(sheet.Rates))
	}
}

func TestVendorA(t *testing.T) {
	csv := `Destination,Carrier,Rate
+44,nexatel,0.0125`
	a := adapters.Get("vendor_a")
	sheet, err := a.Parse(strings.NewReader(csv), "vendor-a", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if sheet.Rates[0].Prefix != "44" {
		t.Errorf("prefix = %s", sheet.Rates[0].Prefix)
	}
}
