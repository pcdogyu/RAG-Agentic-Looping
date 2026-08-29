package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

func main() {
	source := "http://localhost:8000/openapi.json"
	if len(os.Args) > 1 {
		source = os.Args[1]
	}
	client := &http.Client{Timeout: 15 * time.Second}
	response, err := client.Get(source)
	if err != nil {
		fail(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		fail(fmt.Errorf("OpenAPI returned %s", response.Status))
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		fail(err)
	}
	var document any
	if err := json.Unmarshal(body, &document); err != nil {
		fail(err)
	}
	formatted, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		fail(err)
	}
	path := filepath.Join("contracts", "openapi.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		fail(err)
	}
	formatted = append(formatted, '\n')
	if err := os.WriteFile(path, formatted, 0o644); err != nil {
		fail(err)
	}
	fmt.Printf("frozen %d bytes to %s\n", len(formatted), path)
}

func fail(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
