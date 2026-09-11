package modelprompt

import (
	"strings"
	"testing"
)

func TestComposeKeepsDefaultExactAndAppendsSafetyToOverride(t *testing.T) {
	definition := Definition{DefaultPrompt: "default", RequiredSuffix: "never fabricate"}
	if got := Compose(nil, definition); got != "default" {
		t.Fatalf("default prompt changed: %q", got)
	}
	override := "custom instructions"
	got := Compose(&override, definition)
	if !strings.HasPrefix(got, override) || !strings.Contains(got, "never fabricate") || !strings.Contains(got, "系统固定安全边界") {
		t.Fatalf("override did not preserve editable and mandatory sections: %q", got)
	}
}
