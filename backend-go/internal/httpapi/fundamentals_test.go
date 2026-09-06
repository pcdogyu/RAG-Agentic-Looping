package httpapi

import "testing"

func TestFundamentalAssetIDUnescapesCanonicalID(t *testing.T) {
	value, err := fundamentalAssetID("equity%3ANYSE%3AVRT")
	if err != nil || value != "equity:NYSE:VRT" {
		t.Fatalf("asset id=%q err=%v", value, err)
	}
}
