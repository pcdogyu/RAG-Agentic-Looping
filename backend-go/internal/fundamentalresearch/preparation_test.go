package fundamentalresearch

import (
	"testing"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/fundamentals"
)

func TestDeriveFactualInputsKeepsMissingValuesNull(t *testing.T) {
	income := fundamentals.Snapshot{ID: "income", Currency: "USD", Unit: "reported", Metrics: map[string]any{"revenue": 100.0}}
	cashFlow := fundamentals.Snapshot{ID: "cash", Currency: "USD", Unit: "reported", Metrics: map[string]any{}}
	inputs, lineage, missing := deriveFactualInputs(income, cashFlow)
	if inputs.Revenue == nil || *inputs.Revenue != 100 {
		t.Fatalf("revenue=%v", inputs.Revenue)
	}
	if inputs.OperatingMargin != nil || inputs.TaxRate != nil || inputs.Depreciation != nil || inputs.Capex != nil || inputs.ChangeNWC != nil || inputs.DilutedShares != nil {
		t.Fatalf("missing facts were converted into values: %#v", inputs)
	}
	if len(lineage) != 1 || len(missing) != 6 {
		t.Fatalf("lineage=%v missing=%v", lineage, missing)
	}
}

func TestSameFinancialUnitsRejectsMixedStatements(t *testing.T) {
	if sameFinancialUnits(
		fundamentals.Snapshot{Currency: "USD", Unit: "millions"},
		fundamentals.Snapshot{Currency: "USD", Unit: "reported"},
		fundamentals.Snapshot{Currency: "USD", Unit: "millions"},
	) {
		t.Fatal("mixed financial units must not be combined")
	}
}
