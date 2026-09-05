package sourcefilter

import "testing"

func TestEvaluateRoutesAllNonBlockedNews(t *testing.T) {
	cfg := Config{Enabled: true, Whitelist: []string{"NVDA", "美联储"}, Blacklist: []string{"天气"}}
	if decision := Evaluate("普通公司发布新品", cfg); decision.Profile != "fast" || decision.Blocked {
		t.Fatalf("ordinary title = %#v", decision)
	}
	if decision := Evaluate("NVDA launches a new chip", cfg); decision.Profile != "deep" {
		t.Fatalf("whitelist title = %#v", decision)
	}
	if decision := Evaluate("美联储讨论利率与天气", cfg); !decision.Blocked || decision.Profile != "blocked" {
		t.Fatalf("blacklist must win = %#v", decision)
	}
}

func TestShortTickerRequiresASCIIBoundary(t *testing.T) {
	for _, title := range []string{"Alibaba reports earnings", "Basic materials rally", "Closing bell"} {
		if decision := Evaluate(title, Config{Enabled: true, Whitelist: []string{"B", "LI", "C"}}); decision.Profile != "fast" {
			t.Fatalf("%q unexpectedly matched: %#v", title, decision)
		}
	}
	for _, title := range []string{"C falls after earnings", "NYSE:LI gains", "BRK.A reaches a record"} {
		if decision := Evaluate(title, Config{Enabled: true, Whitelist: []string{"C", "LI", "BRK.A"}}); decision.Profile != "deep" {
			t.Fatalf("%q did not match: %#v", title, decision)
		}
	}
}

func TestNormalizeListsDeduplicatesAndRejectsConflicts(t *testing.T) {
	white, black, issues, warnings := NormalizeLists([]string{" NVDA ", "ｎｖｄａ", "美联储"}, []string{"天气", " 美联储 "})
	if len(white) != 2 || len(black) != 2 || len(warnings) != 1 {
		t.Fatalf("normalization = %v %v issues=%v warnings=%v", white, black, issues, warnings)
	}
	if len(issues) != 2 || issues[0].Code != "cross_list_conflict" || issues[1].Field != "blacklist_keywords" {
		t.Fatalf("conflict issues = %#v", issues)
	}
}

func TestNormalizeListsRejectsEmbeddedSeparatorsAndLimits(t *testing.T) {
	values := make([]string, 201)
	for index := range values {
		values[index] = string(rune(0x4e00 + index))
	}
	_, _, issues, _ := NormalizeLists(append(values, "bad,keyword", string([]byte{1})), nil)
	codes := map[string]bool{}
	for _, issue := range issues {
		codes[issue.Code] = true
	}
	if !codes["too_many"] || !codes["embedded_separator"] || !codes["invalid_character"] {
		t.Fatalf("missing validation errors: %#v", issues)
	}
}
