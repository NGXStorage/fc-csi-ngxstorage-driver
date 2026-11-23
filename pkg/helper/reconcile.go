package helper

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	kubeutils "github.com/NGXStorage/csi-ngxstorage-utils/kubernetes"
	utils "github.com/ngxstorage/fc-csi-ngxstorage-driver/pkg/utils"
	ngxstorage "github.com/ngxstorage/ngxstorage-sdk-go/pkg/ngxstorage"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	zap "go.uber.org/zap"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

// MinVolumeBytes is the default minimum volume size (1 GiB).
const MinVolumeBytes int64 = 1 << 30

const (
	fcUnmapVerifyAttempts = 30
	fcUnmapVerifyInterval = time.Second
)

const (
	FCDeleteVerifyTimeout  = 60 * time.Second
	FCDeleteVerifyInterval = 2 * time.Second
)

// Reconciler owns FC backend target/LUN orchestration: target selection,
// unmap wait, locked delete and restored-LUN readback. The distributed-lock
// operation order and fail-closed decisions stay driver policy.
type Reconciler struct {
	Client           *ngxstorage.Client
	PoolOwner        string
	VolumeIDPrefix   string
	FCTargetNames    []string
	AttachmentLookup kubeutils.AttachmentLookup
	DriverName       string
	// DeleteVerifyTimeout/DeleteVerifyInterval bound the DeleteVolume
	// read-after-delete verification; zero values use the package defaults.
	// Overridable in tests for deterministic budgets.
	DeleteVerifyTimeout  time.Duration
	DeleteVerifyInterval time.Duration
	Log                  *zap.Logger
}

func (r *Reconciler) log() *zap.Logger {
	if r.Log != nil {
		return r.Log
	}
	return zap.NewNop()
}

func (r *Reconciler) SelectTargetByName(ctx context.Context, name string) (string, error) {
	targets, err := r.Client.FCTargets().List(ctx)
	if err != nil {
		return "", err
	}
	for _, target := range targets {
		if target.Name == name {
			return target.ID, nil
		}
	}
	return "", fmt.Errorf("FC target %q not found", name)
}
func (r *Reconciler) SelectTargetByOwner(ctx context.Context, names []string, owner string) (string, error) {
	targets, err := r.Client.FCTargets().List(ctx)
	if err != nil {
		return "", err
	}
	for _, target := range targets {
		for _, name := range names {
			if target.Name == name && target.Owner == owner {
				return target.ID, nil
			}
		}
	}
	return "", fmt.Errorf("no FC target among %v on controller owner %q", names, owner)
}
func (r *Reconciler) FindMappedFCTarget(ctx context.Context, lunID string) (string, bool, error) {
	targets, err := r.Client.FCTargets().List(ctx)
	if err != nil {
		return "", false, err
	}
	configured := make(map[string]struct{}, len(r.FCTargetNames))
	for _, name := range r.FCTargetNames {
		configured[name] = struct{}{}
	}
	for _, listed := range targets {
		if _, ok := configured[listed.Name]; !ok {
			continue
		}
		target, err := r.Client.FCTargets().Get(ctx, listed.ID)
		if err != nil {
			return "", false, err
		}
		for _, mappedLUN := range target.Luns {
			if mappedLUN.ID == lunID {
				return listed.ID, true, nil
			}
		}
	}
	return "", false, nil
}
func (r *Reconciler) WaitForFCUnmap(ctx context.Context, targetID, lunID string) error {
	for attempt := 1; attempt <= fcUnmapVerifyAttempts; attempt++ {
		target, err := r.Client.FCTargets().Get(ctx, targetID)
		if err != nil {
			return fmt.Errorf("get FC target %s: %w", targetID, err)
		}
		targetMapped := false
		for _, mappedLUN := range target.Luns {
			if mappedLUN.ID == lunID {
				targetMapped = true
				break
			}
		}

		lun, err := r.Client.LUNs().Get(ctx, lunID)
		if err != nil {
			if ngxstorage.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("get LUN %s: %w", lunID, err)
		}
		lunMapped := lun.Protocols["fc"] != ""
		if !targetMapped && !lunMapped {
			return nil
		}

		if attempt == fcUnmapVerifyAttempts {
			break
		}
		timer := time.NewTimer(fcUnmapVerifyInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return fmt.Errorf("LUN %s is still mapped to FC target %s", lunID, targetID)
}

// VerifyLUNDeleted polls the backend until the LUN is authoritatively
// NotFound, bounded by the request context and FCDeleteVerifyTimeout.
//   - present LUN: keep polling within budget; budget exhausted -> error
//     (CSI success must not be claimed while the backend object exists).
//   - ambiguous GET error (403/timeout/5xx/transport): retry within budget;
//     NEVER treated as absence; budget exhausted -> error.
//   - authoritative NotFound: success (idempotent).
//
// Lock/ownership rules are enforced by the caller; this method only reads.
func (r *Reconciler) VerifyLUNDeleted(ctx context.Context, lunID string) error {
	timeout := r.DeleteVerifyTimeout
	if timeout <= 0 {
		timeout = FCDeleteVerifyTimeout
	}
	interval := r.DeleteVerifyInterval
	if interval <= 0 {
		interval = FCDeleteVerifyInterval
	}
	vctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var lastErr error
	for {
		_, err := r.Client.LUNs().Get(vctx, lunID)
		switch {
		case err == nil:
			lastErr = status.Errorf(codes.Internal,
				"delete verification: LUN %s still present after a successful delete response (backend acknowledged but did not delete)", lunID)
		case objectAbsenceProven(err):
			return nil
		default:
			lastErr = status.Errorf(codes.Internal,
				"delete verification: LUN %s absence GET ambiguous (not absence proof): %v", lunID, err)
		}
		select {
		case <-vctx.Done():
			if ctx.Err() != nil {
				return status.FromContextError(ctx.Err()).Err()
			}
			return status.Errorf(codes.DeadlineExceeded,
				"delete verification budget exhausted for LUN %s; absence NOT proven: %v", lunID, lastErr)
		case <-time.After(interval):
		}
	}
}

// VerifySnapshotDeleted polls the backend until the snapshot is authoritatively
// NotFound, bounded by the request context and FCDeleteVerifyTimeout.
//   - present snapshot: keep polling within budget; budget exhausted -> error
//     (CSI success must not be claimed while the backend object exists).
//   - ambiguous GET error (403/timeout/5xx/transport): retry within budget;
//     NEVER treated as absence; budget exhausted -> error.
//   - authoritative NotFound: success (idempotent).
//
// Lock/ownership rules are enforced by the caller; this method only reads.
func (r *Reconciler) VerifySnapshotDeleted(ctx context.Context, snapshotID string) error {
	timeout := r.DeleteVerifyTimeout
	if timeout <= 0 {
		timeout = FCDeleteVerifyTimeout
	}
	interval := r.DeleteVerifyInterval
	if interval <= 0 {
		interval = FCDeleteVerifyInterval
	}
	vctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var lastErr error
	for {
		_, err := r.Client.Snapshots().Get(vctx, snapshotID)
		switch {
		case err == nil:
			lastErr = status.Errorf(codes.Internal,
				"delete verification: snapshot %s still present after a successful delete response (backend acknowledged but did not delete)", snapshotID)
		case objectAbsenceProven(err):
			return nil
		default:
			lastErr = status.Errorf(codes.Internal,
				"delete verification: snapshot %s absence GET ambiguous (not absence proof): %v", snapshotID, err)
		}
		select {
		case <-vctx.Done():
			if ctx.Err() != nil {
				return status.FromContextError(ctx.Err()).Err()
			}
			return status.Errorf(codes.DeadlineExceeded,
				"delete verification budget exhausted for snapshot %s; absence NOT proven: %v", snapshotID, lastErr)
		case <-time.After(interval):
		}
	}
}

// objectAbsenceProven reports whether err is an AUTHORITATIVE backend absence
// signal for a single-object GET (LUN or snapshot): the canonical NotFound
// code (5011) with HTTP 404, or a code-less HTTP 404 from the object endpoint.
// It is deliberately stricter than the general ClassifyError mapping (which
// treats any statusCode==404 as NotFound for CO error reporting): a bare
// HTTP 404 carrying a DIFFERENT explicit API code is not absence proof — a
// gateway/proxy 404 or an unrelated error surfaced with 404 must never be
// read as a successful delete. No other HTTP error status (403/5xx) or
// transport failure proves absence; those stay ambiguous and fail closed.
func objectAbsenceProven(err error) bool {
	var apiErr *ngxstorage.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	if apiErr.StatusCode != 404 {
		return false
	}
	return apiErr.Code == ngxstorage.CodeNotFound || apiErr.Code == 0
}

// deleteVolumeLocked enforces the destructive preconditions that must remain
// stable under volume coordination. When targetID is non-empty, the caller also
// owns that FC target's lock and this helper removes/verifies the mapping before
// deleting the LUN. Success is reported only after the backend confirms the
// LUN is absent.
func (r *Reconciler) DeleteVolumeLocked(
	ctx context.Context,
	volumeID string,
	lun *ngxstorage.LUN,
	targetID string,
	assertOwned func(context.Context) error,
) error {
	remaining, err := RemainderNodeIDs(ctx, r.AttachmentLookup, r.DriverName, volumeID, "", r.log())
	if err != nil {
		return err
	}
	if len(remaining) > 0 {
		return status.Errorf(codes.FailedPrecondition,
			"volume still has active attachments: %s", strings.Join(remaining, ","))
	}

	snaps, err := r.Client.Snapshots().ListDetail(ctx, lun.ID)
	if err != nil {
		return status.Errorf(codes.Unavailable, "list snapshots before delete: %v", err)
	}
	if len(snaps) > 0 {
		return status.Errorf(codes.FailedPrecondition,
			"volume still has snapshot %s", snaps[0].ID)
	}

	if targetID == "" {
		if strings.TrimSpace(lun.Protocols["fc"]) != "" {
			return status.Errorf(codes.FailedPrecondition,
				"volume reports an FC mapping that cannot be resolved to a configured target: %q", lun.Protocols["fc"])
		}
	} else {
		if err := assertOwned(ctx); err != nil {
			return utils.CoordinationError("verify delete mapping locks before FC cleanup", err)
		}
		if err := r.Client.FCTargets().RemoveLUN(ctx, targetID, lun.ID, r.PoolOwner); err != nil {
			return status.Errorf(codes.Internal, "unmap LUN from FC target before delete: %v", err)
		}
		if err := r.WaitForFCUnmap(ctx, targetID, lun.ID); err != nil {
			return status.Errorf(codes.Internal, "verify FC LUN unmap before delete: %v", err)
		}
	}

	if err := assertOwned(ctx); err != nil {
		return utils.CoordinationError("verify delete volume lock before backend mutation", err)
	}
	if err := r.Client.LUNs().Delete(ctx, lun.ID); err != nil {
		return utils.BackendStatusError("delete LUN", err)
	}
	// Read-after-delete: a 200-success delete response is not proof of
	// backend absence. Verify authoritative NotFound before claiming CSI
	// success; a still-present LUN must surface as an error so the caller
	// retries instead of attesting a deletion that never happened.
	if err := r.VerifyLUNDeleted(ctx, lun.ID); err != nil {
		return err
	}
	r.log().Info("DeleteVolume(): volume deleted (backend absence verified)", zap.String("lun_id", lun.ID))
	return nil
}

// lunByNameStrict returns the canonical LUN with the given name. Backend list
// errors are propagated so a failed idempotency probe never becomes a create.
func (r *Reconciler) LUNByNameStrict(ctx context.Context, name string) (*ngxstorage.LUN, error) {
	luns, err := r.Client.LUNs().List(ctx)
	if err != nil {
		return nil, err
	}
	for i := range luns {
		if luns[i].Name == name {
			if luns[i].ID == "" {
				return nil, fmt.Errorf("same-name LUN %q has an empty backend id", name)
			}
			detail, err := r.Client.LUNs().Get(ctx, luns[i].ID)
			if err != nil {
				return nil, fmt.Errorf("get same-name LUN %s: %w", luns[i].ID, err)
			}
			if detail == nil || detail.ID != luns[i].ID || detail.Name != name {
				return nil, fmt.Errorf("same-name LUN detail identity mismatch")
			}
			return detail, nil
		}
	}
	return nil, nil
}

// resizeRestoredLUN completes an interrupted snapshot restore before publication.
func (r *Reconciler) ResizeRestoredLUN(ctx context.Context, lun *ngxstorage.LUN, req *csi.CreateVolumeRequest, snapshot *ngxstorage.Snapshot, assertOwned func() error) (*ngxstorage.LUN, error) {
	if snapshot == nil {
		return lun, nil
	}
	identityRequest := &csi.CreateVolumeRequest{
		Parameters:    req.GetParameters(),
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1, LimitBytes: req.GetCapacityRange().GetLimitBytes()},
	}
	if err := ValidateExistingCreateVolume(lun, identityRequest, snapshot); err != nil {
		return nil, err
	}
	required := req.GetCapacityRange().GetRequiredBytes()
	if required <= 0 {
		required = MinVolumeBytes
	}
	current := int64(lun.Size)
	if current >= required {
		return lun, nil
	}
	for _, target := range lun.Protocols {
		if strings.TrimSpace(target) != "" {
			return nil, status.Error(codes.FailedPrecondition, "undersized restore is already mapped; refusing create-time resize")
		}
	}
	const gib = int64(1 << 30)
	targetGB := (required-1)/gib + 1
	if targetGB > (1<<63-1)/gib || current%gib != 0 {
		return nil, status.Error(codes.OutOfRange, "restore capacity cannot be represented in whole GiB")
	}
	if limit := req.GetCapacityRange().GetLimitBytes(); limit > 0 && targetGB*gib > limit {
		return nil, status.Error(codes.OutOfRange, "restored volume would exceed capacity limit")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if err := assertOwned(); err != nil {
		return nil, utils.CoordinationError("verify restore resize lock", err)
	}
	incrementGB := targetGB - current/gib
	if err := r.Client.LUNs().Expand(ctx, lun.ID, incrementGB); err != nil {
		return nil, utils.BackendStatusError("expand restored LUN", err)
	}
	updated, err := r.Client.LUNs().Get(ctx, lun.ID)
	if err != nil {
		return nil, utils.BackendStatusError("read expanded restore", err)
	}
	if updated == nil || updated.ID != lun.ID || updated.Name != lun.Name || int64(updated.Size) != targetGB*gib {
		return nil, status.Error(codes.Internal, "restored LUN resize readback mismatch")
	}
	if err := ValidateExistingCreateVolume(updated, req, snapshot); err != nil {
		return nil, err
	}
	r.log().Info("CreateVolume(): restored LUN expanded", zap.String("lun_id", lun.ID),
		zap.Int64("before_bytes", current), zap.Int64("after_bytes", int64(updated.Size)), zap.Int64("increment_gib", incrementGB))
	return updated, nil
}

// GetSnapshot returns the canonical backend snapshot for the requested ID.
// The RPC is advertised by the driver, so it must never fall back to the
// embedded UnimplementedControllerServer implementation.
// SnapshotByID looks up one snapshot by canonical ID and prepares its CSI
// proto. A nil snapshot with nil error means absence; the RPC owner makes
// the NotFound decision. Backend errors return unwrapped for classification.
func (r *Reconciler) SnapshotByID(ctx context.Context, snapshotID string) (*csi.Snapshot, error) {
	snap, err := r.Client.Snapshots().Get(ctx, snapshotID)
	if err != nil {
		return nil, err
	}
	if snap == nil || snap.ID == "" {
		return nil, nil
	}
	if snap.ID != snapshotID {
		return nil, status.Error(codes.Internal, "get snapshot: backend response has an invalid id")
	}
	return SnapshotProto(r.VolumeIDPrefix, snap), nil
}
func ValidateExistingCreateVolume(lun *ngxstorage.LUN, req *csi.CreateVolumeRequest, sourceSnapshot *ngxstorage.Snapshot) error {
	if lun == nil || lun.ID == "" {
		return status.Error(codes.AlreadyExists, "same-name LUN has invalid backend identity")
	}
	actualBytes := int64(lun.Size)
	requiredBytes := MinVolumeBytes
	limitBytes := int64(0)
	if req.GetCapacityRange() != nil {
		if req.GetCapacityRange().GetRequiredBytes() > 0 {
			requiredBytes = req.GetCapacityRange().GetRequiredBytes()
		}
		limitBytes = req.GetCapacityRange().GetLimitBytes()
	}
	if actualBytes <= 0 || actualBytes < requiredBytes || (limitBytes > 0 && actualBytes > limitBytes) {
		return status.Errorf(codes.AlreadyExists,
			"same-name LUN has incompatible capacity: actual=%d required=%d limit=%d",
			actualBytes, requiredBytes, limitBytes)
	}

	cloneMarker := strings.TrimSpace(lun.Clone)
	if sourceSnapshot == nil {
		if cloneMarker != "" {
			return status.Error(codes.AlreadyExists, "same-name LUN was created from a different content source")
		}
	} else {
		expectedMarkers := map[string]struct{}{
			sourceSnapshot.ID:   {},
			sourceSnapshot.Name: {},
		}
		if sourceSnapshot.VolumeName != "" && sourceSnapshot.Name != "" {
			expectedMarkers[sourceSnapshot.VolumeName+"@"+sourceSnapshot.Name] = struct{}{}
		}
		if _, ok := expectedMarkers[cloneMarker]; !ok {
			return status.Errorf(codes.AlreadyExists,
				"same-name LUN content source is incompatible with snapshot %q", sourceSnapshot.ID)
		}
	}

	opts, err := LUNCreateOptionsFromParams(req.GetParameters())
	if err != nil {
		return err
	}
	if opts.Blocksize != "" && !CompatibleBlockSize(lun.BlockSize, opts.Blocksize) {
		return status.Errorf(codes.AlreadyExists, "same-name LUN blocksize=%q, requested=%q", lun.BlockSize, opts.Blocksize)
	}
	return nil
}
func CompatibleBlockSize(actual, requested string) bool {
	actual = strings.ToLower(strings.TrimSpace(actual))
	requested = strings.ToLower(strings.TrimSpace(requested))
	if requested == "" || requested == "auto" || actual == requested {
		return true
	}
	if strings.HasSuffix(requested, "k") {
		kib, err := strconv.ParseInt(strings.TrimSuffix(requested, "k"), 10, 64)
		if err == nil {
			return actual == strconv.FormatInt(kib*1024, 10)
		}
	}
	return false
}
func VerifyLUNMutableReadback(lun *ngxstorage.LUN, expected map[string]interface{}) error {
	for key, want := range expected {
		var got string
		switch key {
		case "deduplication":
			got = string(lun.Deduplication)
		case "compression":
			got = string(lun.Compression)
		case "thin_provision":
			got = string(lun.ThinProvision)
		case "flash_cache":
			got = string(lun.FlashCache)
		case "dram_cache":
			got = string(lun.DramCache)
		case "io_type":
			got = lun.IoType
		case "qos_priority":
			got = strconv.FormatInt(int64(lun.QosPriority), 10)
		default:
			return status.Errorf(codes.Internal, "modified LUN readback has unsupported field %q", key)
		}
		if got != fmt.Sprint(want) {
			return status.Errorf(codes.Internal,
				"modified LUN readback mismatch for %s: got=%q want=%q", key, got, fmt.Sprint(want))
		}
	}
	return nil
}
func CreateVolumeResponse(prefix, lunID string, capacityBytes int64, name, fsType, owner string, req *csi.CreateVolumeRequest) *csi.CreateVolumeResponse {
	return &csi.CreateVolumeResponse{Volume: &csi.Volume{
		VolumeId:      VolumeID(prefix, lunID),
		CapacityBytes: capacityBytes,
		ContentSource: req.GetVolumeContentSource(),
		VolumeContext: map[string]string{
			"lun_id":            lunID,
			"name":              name,
			"fstype":            fsType,
			"owner":             owner,
			"mount_permissions": req.GetParameters()["mount_permissions"],
		},
	}}
}
func CreateSnapshotResponse(prefix string, snap *ngxstorage.Snapshot, sourceVolumeID string, fallbackSize int64) *csi.CreateSnapshotResponse {
	proto := SnapshotProto(prefix, snap)
	if proto.SourceVolumeId == "" {
		proto.SourceVolumeId = sourceVolumeID
	}
	if proto.SizeBytes == 0 {
		proto.SizeBytes = fallbackSize
	}
	return &csi.CreateSnapshotResponse{Snapshot: proto}
}
func SnapshotProto(prefix string, snap *ngxstorage.Snapshot) *csi.Snapshot {
	result := &csi.Snapshot{SnapshotId: snap.ID, ReadyToUse: snap.Ready}
	if snap.VolumeID != "" {
		result.SourceVolumeId = VolumeID(prefix, snap.VolumeID)
	}
	if created, err := strconv.ParseInt(strings.TrimSpace(snap.Created), 10, 64); err == nil && created >= 0 {
		result.CreationTime = timestamppb.New(time.Unix(created, 0))
	}
	if size, err := strconv.ParseInt(strings.TrimSpace(snap.Size), 10, 64); err == nil && size >= 0 {
		result.SizeBytes = size
	}
	return result
}

type ErrFCTargetChanged struct{ Expected, Actual string }

func (e ErrFCTargetChanged) Error() string {
	return fmt.Sprintf("FC target mapping changed while acquiring locks: expected=%s actual=%s", e.Expected, e.Actual)
}
