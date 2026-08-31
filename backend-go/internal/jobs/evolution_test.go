package jobs

import (
	"strings"
	"testing"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
)

func TestEvolutionHandlersCoverMigrationManifest(t *testing.T) {
	handlers := NewEvolutionHandlers(config.Config{}, nil, nil)
	lane, err := ValidateBatchFourActivation("evolution", []string{"extract", "mapping", "research"})
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateLaneHandlers(lane, handlers); err != nil {
		t.Fatalf("evolution handlers are incomplete: %v", err)
	}
}

func TestEvolutionRejectsSecretsAndProtectedFiles(t *testing.T) {
	if err := assertEvolutionNoSecret("+ token = \"should-not-appear\""); err == nil {
		t.Fatal("expected secret detection")
	}
	patch := "diff --git a/evals/baseline.json b/evals/baseline.json\n--- a/evals/baseline.json\n+++ b/evals/baseline.json\n"
	if _, err := evolutionCandidatePaths(patch); err == nil || !strings.Contains(err.Error(), "protected") {
		t.Fatalf("expected protected path rejection, got %v", err)
	}
}

func TestEvolutionCandidateScopeAcceptsMinimalPatchAndRejectsDelete(t *testing.T) {
	patch := "diff --git a/backend/app/example.py b/backend/app/example.py\n--- a/backend/app/example.py\n+++ b/backend/app/example.py\n"
	paths, err := evolutionCandidatePaths(patch)
	if err != nil || !paths["backend/app/example.py"] {
		t.Fatalf("valid patch was rejected: paths=%v err=%v", paths, err)
	}
	if _, err := evolutionCandidatePaths(patch + "deleted file mode 100644\n"); err == nil {
		t.Fatal("expected file deletion rejection")
	}
}

func TestEvolutionSlugIsStableAndBounded(t *testing.T) {
	value := evolutionSlug("Expected Calibration Error with spaces and symbols !!!")
	if value != "expected-calibration-error-with-spaces-a" || len(value) > 40 {
		t.Fatalf("unexpected slug %q", value)
	}
}
