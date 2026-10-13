package httpapi

import (
	"encoding/json"
	"testing"
)

func TestAdapterItemsUnwrapsNestedDataItems(t *testing.T) {
	var payload any
	raw := `{"data":{"items":[{"title":"BTC surge","link":"https://example.com/a","content":"body","time":"2026-10-13T08:00:00Z"}],"next_cursor":"c1","has_more":true}}`
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatal(err)
	}
	items := adapterItems(payload, "news_items_v1")
	if len(items) != 1 || items[0]["title"] != "BTC surge" {
		t.Fatalf("expected one nested news item, got %#v", items)
	}
}

func TestAdapterItemsKeepsFlatAndSingleRecordShapes(t *testing.T) {
	flat := map[string]any{"items": []any{map[string]any{"title": "a"}}}
	if items := adapterItems(flat, "news_items_v1"); len(items) != 1 || items[0]["title"] != "a" {
		t.Fatalf("flat items shape broken: %#v", items)
	}
	record := map[string]any{"data": map[string]any{"id": 1, "title": "solo"}}
	if items := adapterItems(record, "raw_records_v1"); len(items) != 1 || items[0]["title"] != "solo" {
		t.Fatalf("single record shape broken: %#v", items)
	}
	jin10 := map[string]any{"data": map[string]any{"items": []any{map[string]any{"title": "flash"}}}}
	if items := adapterItems(jin10, "jin10_flash_v1"); len(items) != 1 || items[0]["title"] != "flash" {
		t.Fatalf("jin10 shape broken: %#v", items)
	}
}
