package httpapi

import "testing"

func TestWeakETagIsStable(t *testing.T) {
	first := weakETag([]byte("payload"))
	if first != weakETag([]byte("payload")) || first == weakETag([]byte("other")) {
		t.Fatal("etag must be stable and content-sensitive")
	}
}
