package httpapi

import (
	"crypto/sha256"
	"encoding/hex"
)

func weakETag(body []byte) string {
	sum := sha256.Sum256(body)
	return `W/"` + hex.EncodeToString(sum[:8]) + `"`
}
