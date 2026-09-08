package jobs

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
)

func TestEvolutionHandlersCoverMigrationManifest(t *testing.T) {
	handlers := NewEvolutionHandlers(config.Config{}, nil, nil)
	lane, err := RequireWorkerLane("evolution")
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
	patch := "diff --git a/backend-go/internal/jobs/evaluation.go b/backend-go/internal/jobs/evaluation.go\n--- a/backend-go/internal/jobs/evaluation.go\n+++ b/backend-go/internal/jobs/evaluation.go\n"
	if _, err := evolutionCandidatePaths(patch); err == nil || !strings.Contains(err.Error(), "protected") {
		t.Fatalf("expected protected path rejection, got %v", err)
	}
}

func TestEvolutionProtectsRatingAndCalibrationPolicy(t *testing.T) {
	for _, path := range []string{
		"backend-go/internal/rating/store.go",
		"backend-go/internal/calibration/calibration.go",
		"backend-go/internal/governance/governance.go",
	} {
		patch := "diff --git a/" + path + " b/" + path + "\n--- a/" + path + "\n+++ b/" + path + "\n"
		if _, err := evolutionCandidatePaths(patch); err == nil || !strings.Contains(err.Error(), "protected") {
			t.Fatalf("expected protected path rejection for %s, got %v", path, err)
		}
	}
}

func TestEvolutionExecutionRequiresMatchingApprovedIdentity(t *testing.T) {
	candidate := map[string]any{"status": "approved", "approved_by": "operator@example.com"}
	if validEvolutionApproval(candidate, "") || validEvolutionApproval(candidate, "other@example.com") {
		t.Fatal("missing or mismatched approval was accepted")
	}
	if !validEvolutionApproval(candidate, " operator@example.com ") {
		t.Fatal("matching approval was rejected")
	}
	candidate["status"] = "proposed"
	if validEvolutionApproval(candidate, "operator@example.com") {
		t.Fatal("unapproved candidate status was accepted")
	}
}

func TestEvolutionCandidateScopeAcceptsMinimalPatchAndRejectsDelete(t *testing.T) {
	patch := "diff --git a/backend-go/internal/jobs/example.go b/backend-go/internal/jobs/example.go\n--- a/backend-go/internal/jobs/example.go\n+++ b/backend-go/internal/jobs/example.go\n"
	paths, err := evolutionCandidatePaths(patch)
	if err != nil || !paths["backend-go/internal/jobs/example.go"] {
		t.Fatalf("valid patch was rejected: paths=%v err=%v", paths, err)
	}
	if _, err := evolutionCandidatePaths(patch + "deleted file mode 100644\n"); err == nil {
		t.Fatal("expected file deletion rejection")
	}
	pythonPatch := "diff --git a/backend/app/example.py b/backend/app/example.py\n--- a/backend/app/example.py\n+++ b/backend/app/example.py\n"
	if _, err := evolutionCandidatePaths(pythonPatch); err == nil || !strings.Contains(err.Error(), "backend-go") {
		t.Fatalf("expected Python path rejection, got %v", err)
	}
}

func TestEvolutionCandidateChecksUseOnlyGoTooling(t *testing.T) {
	for _, spec := range candidateCommandSpecs(filepath.Clean("/repo")) {
		command := strings.ToLower(strings.Join(spec.args, " "))
		for _, forbidden := range []string{"python", "pytest", "ruff", "pip"} {
			if strings.Contains(command, forbidden) {
				t.Fatalf("check %s still invokes %s: %v", spec.name, forbidden, spec.args)
			}
		}
	}
}

func TestEvolutionSlugIsStableAndBounded(t *testing.T) {
	value := evolutionSlug("Expected Calibration Error with spaces and symbols !!!")
	if value != "expected-calibration-error-with-spaces-a" || len(value) > 40 {
		t.Fatalf("unexpected slug %q", value)
	}
}
