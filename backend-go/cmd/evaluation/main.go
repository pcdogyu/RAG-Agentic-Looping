package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/jobs"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: evaluation <fixed-evidence|walk-forward|probability-calibration|research-quality|compare-models> [flags]")
		os.Exit(2)
	}
	suite := os.Args[1]
	flags := flag.NewFlagSet("evaluation "+suite, flag.ContinueOnError)
	root := flags.String("root", ".", "repository root")
	baseline := flags.String("baseline", "evals/baseline.json", "baseline metrics path")
	candidate := flags.String("candidate", "evals/candidate.json", "candidate metrics path")
	if err := flags.Parse(os.Args[2:]); err != nil {
		os.Exit(2)
	}
	resolvedRoot, err := filepath.Abs(*root)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	result, err := jobs.RunOfflineEvaluation(resolvedRoot, suite, *baseline, *candidate)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if suite == "fixed-evidence" {
		if err := jobs.WriteEvaluationResult(filepath.Join(resolvedRoot, "evals", "candidate.json"), result); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	body, _ := json.Marshal(result)
	fmt.Println(string(body))
	if !jobs.EvaluationPassed(result) {
		os.Exit(1)
	}
}
