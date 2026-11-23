package fc

import (
	"fmt"
	"strconv"
	"strings"

	ngxstorage "github.com/ngxstorage/ngxstorage-sdk-go/pkg/ngxstorage"
)

// StripNAA removes the backend's naa. prefix from an FC WWPN.
func StripNAA(wwpn string) string { return strings.TrimPrefix(wwpn, "naa.") }

// TargetWWPNs returns deduplicated host-facing target WWPNs.
func TargetWWPNs(target *ngxstorage.FCTarget) []string {
	seen := make(map[string]bool)
	var wwpns []string
	for _, port := range target.Ports {
		for _, w := range port.Wwpns {
			clean := StripNAA(w.WWPN)
			if clean != "" && !seen[clean] {
				seen[clean] = true
				wwpns = append(wwpns, clean)
			}
		}
	}
	return wwpns
}

// LUNNumber parses the non-negative backend LUN number for a target mapping.
func LUNNumber(target *ngxstorage.FCTarget, lunID string) (int, error) {
	for _, lun := range target.Luns {
		if lun.ID == lunID {
			n, err := strconv.Atoi(lun.Number)
			if err != nil {
				return -1, fmt.Errorf("parse LUN number %q: %w", lun.Number, err)
			}
			if n < 0 {
				return -1, fmt.Errorf("parse LUN number %q: must be non-negative", lun.Number)
			}
			return n, nil
		}
	}
	return -1, fmt.Errorf("LUN %s not mapped to target %s", lunID, target.ID)
}
