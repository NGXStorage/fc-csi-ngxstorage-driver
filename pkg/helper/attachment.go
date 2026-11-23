package helper

import (
	"context"
	"errors"
	"strings"

	kubeutils "github.com/NGXStorage/csi-ngxstorage-utils/kubernetes"

	zap "go.uber.org/zap"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

type ErrAmbiguousVolumePV struct{ Count int }

func (e ErrAmbiguousVolumePV) Error() string {
	return "multiple persistent volumes match the CSI driver and volume handle"
}
func IsAmbiguousPV(err error) bool {
	var ambiguous kubeutils.ErrAmbiguousVolumePV
	if errors.As(err, &ambiguous) {
		return true
	}
	var local ErrAmbiguousVolumePV
	return errors.As(err, &local)
}

// volumeID builds a CSI volume ID from a backend LUN ID (SHA1).
func VolumeID(prefix, id string) string { return prefix + id }

// LUNIDFromVolumeIDChecked extracts the backend LUN ID from a CSI volume ID.
// A bare backend LUN ID (no driver prefix) is NOT a volume of this driver:
// the error is NotFound so CSI error semantics hold (Publish/Validate/Modify/
// Expand report a non-existent volume; idempotent DeleteVolume/Unpublish turn
// NotFound into OK; ListSnapshots turns it into an empty list). The bare ID
// never reaches a backend mutation. An empty ID is InvalidArgument.
func LUNIDFromVolumeIDChecked(prefix, id string) (string, error) {
	lunID := strings.TrimPrefix(id, prefix)
	if id == "" {
		return "", status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if lunID == "" || lunID == id {
		return "", status.Error(codes.NotFound, "volume not found: ID does not carry this driver's prefix")
	}
	return lunID, nil
}

// assertNodeExists fails with NotFound when the target Kubernetes node is
// absent, so ControllerPublish/Unpublish never mutate the backend for an
// unknown node. An unreadable cluster is Unavailable: never NotFound.
func AssertNodeExists(ctx context.Context, attachments kubeutils.AttachmentLookup, nodeID string) error {
	lookup, ok := attachments.(kubeutils.NodeLookup)
	if !ok || lookup == nil {
		// Attachment lookup is unavailable outside a cluster (unit tests).
		return nil
	}
	exists, err := lookup.NodeExists(ctx, nodeID)
	if err != nil {
		return status.Errorf(codes.Unavailable, "lookup node %s: %v", nodeID, err)
	}
	if !exists {
		return status.Errorf(codes.NotFound, "node %s not found", nodeID)
	}
	return nil
}

// remainderNodeIDs resolves which other nodes still hold a VolumeAttachment
// for the PV owning this CSI volume. Fail-closed: API errors (including a
// missing lookup dependency) return Unavailable so the shared mapping is
// never removed on an uncertain decision.
func RemainderNodeIDs(ctx context.Context, attachments kubeutils.AttachmentLookup, driverName, volumeID, excludingNodeID string, log *zap.Logger) ([]string, error) {
	if attachments == nil {
		return nil, status.Error(codes.Unavailable, "controller attachment lookup unavailable")
	}
	nodes, err := attachments.RemainingNodes(ctx, driverName, volumeID, excludingNodeID)
	if err != nil {
		if IsAmbiguousPV(err) {
			return nil, status.Errorf(codes.FailedPrecondition, "ambiguous volume: %v", err)
		}
		log.Error("remainderNodeIDs(): VolumeAttachment lookup failed; retaining mapping",
			zap.String("volume_id", volumeID), zap.String("node_id", excludingNodeID), zap.Error(err))
		return nil, status.Errorf(codes.Unavailable, "resolve remaining attachments: %v", err)
	}
	return nodes, nil
}
