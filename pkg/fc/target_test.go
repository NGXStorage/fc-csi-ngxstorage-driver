package fc

import (
	"encoding/json"
	"testing"

	ngxstorage "github.com/ngxstorage/ngxstorage-sdk-go/pkg/ngxstorage"
)

func TestLUNNumberRejectsMalformedAndNegativeValues(t *testing.T) {
	for _, tt := range []struct {
		name string
		raw  string
		want int
		fail bool
	}{
		{name: "valid", raw: "12", want: 12},
		{name: "suffix rejected", raw: "12x", fail: true},
		{name: "negative rejected", raw: "-1", fail: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var target ngxstorage.FCTarget
			payload, _ := json.Marshal(map[string]any{
				"id": "target-1", "luns": []map[string]string{{"id": "lun-1", "number": tt.raw}},
			})
			if err := json.Unmarshal(payload, &target); err != nil {
				t.Fatal(err)
			}
			got, err := LUNNumber(&target, "lun-1")
			if tt.fail {
				if err == nil {
					t.Fatalf("LUNNumber(%q) = %d, want error", tt.raw, got)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("LUNNumber(%q) = %d, %v; want %d", tt.raw, got, err, tt.want)
			}
		})
	}
}
