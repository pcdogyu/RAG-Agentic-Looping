package httpapi

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"sort"
	"testing"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
)

func TestNativeOperationsMatchFrozenContractAndCompleteBatchTwo(t *testing.T) {
	body, err := os.ReadFile("../../contracts/openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	var spec struct {
		Paths map[string]map[string]struct {
			OperationID string `json:"operationId"`
		} `json:"paths"`
	}
	if err := json.Unmarshal(body, &spec); err != nil {
		t.Fatal(err)
	}
	contract := map[string]string{}
	total := 0
	for path, methods := range spec.Paths {
		for method, details := range methods {
			switch method {
			case "get", "post", "put", "patch", "delete":
				total++
				contract[details.OperationID] = method + " " + path
			}
		}
	}
	if total != totalContractOperations {
		t.Fatalf("frozen contract has %d operations, server expects %d", total, totalContractOperations)
	}

	operations := (&Server{}).operations()
	if got, want := len(operations), 52; got != want {
		t.Fatalf("batch two must own %d native operations, got %d", want, got)
	}
	seenIDs := map[string]bool{}
	seenRoutes := map[string]bool{}
	for _, item := range operations {
		if seenIDs[item.ID] {
			t.Errorf("duplicate native operation id %s", item.ID)
		}
		seenIDs[item.ID] = true
		key := item.Method + " " + normalizeChiPath(item.Path)
		if seenRoutes[key] {
			t.Errorf("duplicate native route %s", key)
		}
		seenRoutes[key] = true
		contractRoute, ok := contract[item.ID]
		if !ok {
			t.Errorf("native operation %s is absent from frozen contract", item.ID)
			continue
		}
		if key != upperMethod(contractRoute) {
			t.Errorf("operation %s owns %s, frozen contract has %s", item.ID, key, contractRoute)
		}
	}
}

func TestMigrationStatusIsDerivedFromRegisteredRoutes(t *testing.T) {
	server := &Server{}
	server.nativeOperations = server.operations()
	request := httptest.NewRequest("GET", "/go/migration-status", nil)
	response := httptest.NewRecorder()
	server.migrationStatus(response, request)
	if response.Code != 200 {
		t.Fatalf("got status %d", response.Code)
	}
	var payload struct {
		Total       int      `json:"total_operations"`
		Native      int      `json:"native_operations"`
		Remaining   int      `json:"remaining_operations"`
		Ready       bool     `json:"cutover_ready"`
		OperationID []string `json:"native_operation_ids"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Total != 78 || payload.Native != 52 || payload.Remaining != 26 || payload.Ready {
		t.Fatalf("unexpected migration status: %+v", payload)
	}
	if len(payload.OperationID) != payload.Native || !sort.StringsAreSorted(payload.OperationID) {
		t.Fatalf("operation ids are incomplete or unstable")
	}
}

func TestFernetCredentialCompatibility(t *testing.T) {
	server := &Server{cfg: config.Config{MCPSecretKey: "hRf__9cJDARAmRQQRfkdC6YpRUwq9dkoaOYEMDO5sJU="}}
	const pythonToken = "gAAAAABqk7w2qdnOwfrObY4H66N0-BIZ7bm_b-6S-17yR98cs5LyhuIlGLLuLGCC1p3ObTajkq8woLtsmzJxH8GUmFKwfU6xUg=="
	plain, err := server.decryptSecret(pythonToken)
	if err != nil {
		t.Fatal(err)
	}
	if plain != "super-secret" {
		t.Fatalf("got %q", plain)
	}
	encoded, err := server.encryptSecret("super-secret")
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := server.decryptSecret(encoded)
	if err != nil || decoded != "super-secret" {
		t.Fatalf("round trip got %q, %v", decoded, err)
	}
}

func normalizeChiPath(value string) string {
	result := ""
	inParam := false
	for _, char := range value {
		switch {
		case char == '{':
			inParam = true
			result += "{param}"
		case char == '}':
			inParam = false
		case !inParam:
			result += string(char)
		}
	}
	return result
}

func upperMethod(value string) string {
	for index, char := range value {
		if char == ' ' {
			return stringUpper(value[:index]) + " " + normalizeChiPath(value[index+1:])
		}
	}
	return value
}

func stringUpper(value string) string {
	bytes := []byte(value)
	for index, char := range bytes {
		if char >= 'a' && char <= 'z' {
			bytes[index] = char - 32
		}
	}
	return string(bytes)
}
