package driver

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	coord "github.com/NGXStorage/csi-ngxstorage-utils/coord"
	ngxnames "github.com/NGXStorage/csi-ngxstorage-utils/names"
	"github.com/ngxstorage/fc-csi-ngxstorage-driver/pkg/fc"
	helper "github.com/ngxstorage/fc-csi-ngxstorage-driver/pkg/helper"
	utils "github.com/ngxstorage/fc-csi-ngxstorage-driver/pkg/utils"
	ngxstorage "github.com/ngxstorage/ngxstorage-sdk-go/pkg/ngxstorage"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	zap "go.uber.org/zap"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// volumeIDPrefix prefixes CSI volume IDs so they are unique per driver.
const volumeIDPrefix = "fc.csi.ngxstorage.com."

const ()

// CreateVolume provisions a new LUN on the configured pool.
func (d *Driver) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	d.log().Info("CreateVolume(): init")
	d.log().Debug("CreateVolume(): request",
		zap.String("name", req.GetName()),
		zap.Reflect("capacity_range", req.GetCapacityRange()),
		zap.Reflect("volume_capabilities", req.GetVolumeCapabilities()),
		zap.Reflect("parameters", req.GetParameters()),
		zap.Bool("has_content_source", req.GetVolumeContentSource() != nil),
		zap.Bool("has_secrets", len(req.GetSecrets()) != 0),
	)

	if req.GetName() == "" {
		d.log().Error("CreateVolume(): volume name is required")
		return nil, status.Error(codes.InvalidArgument, "volume name is required")
	}

	// Mutable parameters are an optional VolumeAttributesClass surface: they
	// must be a supported, well-typed set, or the request is InvalidArgument.
	// They are applied after creation via ControllerModifyVolume, so validate
	// them here without passing them to the create payload.
	if len(req.GetMutableParameters()) > 0 {
		if _, err := helper.NormalizeLUNMutableParameters(req.GetMutableParameters()); err != nil {
			d.log().Error("CreateVolume(): mutable parameters rejected", zap.Error(err))
			return nil, err
		}
	}

	if req.GetCapacityRange().GetRequiredBytes() < 0 || req.GetCapacityRange().GetLimitBytes() < 0 {
		return nil, status.Error(codes.InvalidArgument, "capacity values must not be negative")
	}
	// Resolve the requested size in bytes; default to 1 GiB.
	capacityBytes := helper.MinVolumeBytes
	if req.GetCapacityRange() != nil && req.GetCapacityRange().GetRequiredBytes() > 0 {
		capacityBytes = req.GetCapacityRange().GetRequiredBytes()
	}
	const gib = int64(1 << 30)
	sizeGB := (capacityBytes-1)/gib + 1
	if sizeGB > (1<<63-1)/gib {
		return nil, status.Error(codes.OutOfRange, "capacity exceeds whole-GiB range")
	}
	if sizeGB < 1 {
		sizeGB = 1
	}
	if limit := req.GetCapacityRange().GetLimitBytes(); limit > 0 && sizeGB*gib > limit {
		return nil, status.Errorf(codes.OutOfRange,
			"requested capacity cannot be satisfied within limit: provisioned=%d limit=%d", sizeGB*gib, limit)
	}

	// Validate every requested capability against the advertised access modes.
	if len(req.GetVolumeCapabilities()) == 0 {
		d.log().Error("CreateVolume(): volume capabilities are required")
		return nil, status.Error(codes.InvalidArgument, "volume capabilities are required")
	}
	var fsType string
	for _, capability := range req.GetVolumeCapabilities() {
		if err := utils.ValidateVolumeCapability(capability, d.AccessModes); err != nil {
			d.log().Error("CreateVolume(): volume capability is invalid",
				zap.Reflect("volume_capability", capability),
				zap.Error(err),
			)
			return nil, err
		}
		if mount := capability.GetMount(); mount != nil {
			if mount.GetFsType() != "" {
				fsType = mount.GetFsType()
			}
		}
	}
	if fsType == "" {
		fsType = "ext4"
	}

	// Snapshot clone path: VolumeContentSource.Snapshot.
	var sourceSnapshotID string
	if src := req.GetVolumeContentSource(); src != nil {
		if snap := src.GetSnapshot(); snap != nil {
			sourceSnapshotID = snap.GetSnapshotId()
		} else if src.GetVolume() != nil {
			d.log().Warn("CreateVolume(): direct volume cloning is unsupported")
			return nil, status.Error(codes.Unimplemented, "NGX Storage does not support direct volume cloning")
		}
	}

	name := ngxnames.ModifyVolName(req.GetName())
	if d.coordination == nil {
		return nil, status.Error(codes.Unavailable, "create volume coordination is unavailable")
	}

	lockKeys := []string{"create-volume:" + name}
	if sourceSnapshotID != "" {
		lockKeys = append(lockKeys, "snapshot:"+sourceSnapshotID)
	}

	var lock coord.Lock
	var lockSet coord.LockSet
	var err error
	if sourceSnapshotID == "" {
		lock, err = d.coordination.Acquire(ctx, lockKeys[0], coord.AcquireOptions{
			Safety: coord.SafetyOrdinary, Operation: coord.OperationCreate,
		})
	} else {
		lockSet, err = d.coordination.AcquireMany(ctx, lockKeys, coord.AcquireOptions{
			Safety: coord.SafetyOrdinary, Operation: coord.OperationCreate,
		})
	}
	if err != nil {
		return nil, utils.CoordinationError("acquire create volume lock", err)
	}

	var response *csi.CreateVolumeResponse
	criticalErr := func() error {
		assertOwned := func() error {
			if lockSet != nil {
				return lockSet.AssertOwned(ctx)
			}
			return lock.AssertOwned(ctx)
		}
		if err := assertOwned(); err != nil {
			return utils.CoordinationError("verify create volume lock ownership", err)
		}

		var sourceSnapshot *ngxstorage.Snapshot
		if sourceSnapshotID != "" {
			sourceSnapshot, err = d.client.Snapshots().Get(ctx, sourceSnapshotID)
			if err != nil {
				if ngxstorage.IsNotFound(err) {
					return status.Errorf(codes.NotFound, "source snapshot %s not found", sourceSnapshotID)
				}
				return utils.BackendStatusError("get source snapshot", err)
			}
			if sourceSnapshot == nil || sourceSnapshot.ID != sourceSnapshotID {
				return status.Error(codes.NotFound, "source snapshot not found")
			}
		}

		// Authoritative idempotency re-read happens only after the pre-ID name
		// lock is owned. A list failure is not equivalent to "does not exist".
		existing, err := d.reconciler().LUNByNameStrict(ctx, name)
		if err != nil {
			return status.Errorf(codes.Unavailable, "list LUNs for create idempotency: %v", err)
		}
		if existing != nil {
			existing, err = d.reconciler().ResizeRestoredLUN(ctx, existing, req, sourceSnapshot, assertOwned)
			if err != nil {
				return err
			}
			if err := helper.ValidateExistingCreateVolume(existing, req, sourceSnapshot); err != nil {
				return err
			}
			d.log().Info("CreateVolume(): volume already exists (idempotent)",
				zap.String("name", name), zap.String("lun_id", existing.ID))
			response = helper.CreateVolumeResponse(volumeIDPrefix, existing.ID, int64(existing.Size), name, fsType, d.poolOwner, req)
			return nil
		}

		if err := assertOwned(); err != nil {
			return utils.CoordinationError("verify create volume lock before backend mutation", err)
		}
		availableBytes, _, capacityErr := d.client.Pools().GetConfiguredCapacity(ctx)
		if capacityErr != nil {
			return status.Errorf(codes.Unavailable,
				"configured pool %q is not currently usable for provisioning: capacity lookup failed: %v",
				d.client.PoolName(), capacityErr)
		}
		requiredBytes := sizeGB * gib
		if availableBytes < requiredBytes {
			return status.Errorf(codes.ResourceExhausted,
				"configured pool %q has insufficient usable capacity: available=%d required=%d",
				d.client.PoolName(), availableBytes, requiredBytes)
		}
		var lunID string
		if sourceSnapshotID != "" {
			lunID, err = d.client.Snapshots().Clone(ctx, sourceSnapshotID, name)
			if err != nil {
				return utils.BackendStatusError("clone snapshot", err)
			}
		} else {
			opts, optsErr := helper.LUNCreateOptionsFromParams(req.GetParameters())
			if optsErr != nil {
				return optsErr
			}
			lun, createErr := d.client.LUNs().CreateWithOptions(ctx, name, sizeGB, opts)
			if createErr != nil {
				return utils.BackendStatusError("create LUN", createErr)
			}
			if lun == nil || lun.ID == "" {
				return status.Error(codes.Internal, "create LUN: backend returned an invalid LUN")
			}
			lunID = lun.ID
		}

		// Verify the newly created identity before releasing either the name lock
		// or, for clones, the source snapshot lock.
		created, err := d.client.LUNs().Get(ctx, lunID)
		if err != nil {
			return utils.BackendStatusError("read back created LUN", err)
		}
		if created == nil || created.ID != lunID || created.Name != name {
			return status.Errorf(codes.Internal,
				"created LUN readback mismatch: id=%q name=%q expected_id=%q expected_name=%q",
				created.ID, created.Name, lunID, name)
		}
		created, err = d.reconciler().ResizeRestoredLUN(ctx, created, req, sourceSnapshot, assertOwned)
		if err != nil {
			return err
		}
		if err := helper.ValidateExistingCreateVolume(created, req, sourceSnapshot); err != nil {
			return status.Errorf(codes.Internal, "created LUN is incompatible with request: %v", err)
		}
		response = helper.CreateVolumeResponse(volumeIDPrefix, created.ID, int64(created.Size), name, fsType, d.poolOwner, req)
		d.log().Info("CreateVolume(): volume created", zap.String("lun_id", created.ID))
		return nil
	}()

	var releaseErr error
	if lockSet != nil {
		releaseErr = utils.ReleaseOperationLockSet(lockSet)
	} else {
		releaseErr = utils.ReleaseOperationLock(lock)
	}
	if criticalErr != nil {
		if releaseErr != nil {
			return nil, errors.Join(criticalErr, utils.CoordinationError("release create volume lock", releaseErr))
		}
		return nil, criticalErr
	}
	if releaseErr != nil {
		return nil, utils.CoordinationError("release create volume lock", releaseErr)
	}
	return response, nil
}

// DeleteVolume deletes a LUN after ensuring it has no snapshots and is not
// mapped to any FC target. Success is reported only after the backend
// confirms the LUN is absent.
func (d *Driver) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	d.log().Info("DeleteVolume(): init")
	d.log().Debug("DeleteVolume(): request",
		zap.String("volume_id", req.GetVolumeId()),
		zap.Bool("has_secrets", len(req.GetSecrets()) != 0),
	)

	if req.GetVolumeId() == "" {
		d.log().Error("DeleteVolume(): volume ID is required")
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	lunID, err := helper.LUNIDFromVolumeIDChecked(volumeIDPrefix, req.GetVolumeId())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			// CSI idempotency: a volume that is not this driver's (or does not
			// exist) is already deleted; never touch the backend for it.
			d.log().Info("DeleteVolume(): volume not owned by this driver; idempotent OK", zap.String("volume_id", req.GetVolumeId()))
			return &csi.DeleteVolumeResponse{}, nil
		}
		d.log().Error("DeleteVolume(): invalid volume ID", zap.String("volume_id", req.GetVolumeId()), zap.Error(err))
		return nil, err
	}
	if d.coordination == nil {
		return nil, status.Error(codes.Unavailable, "delete volume coordination is unavailable")
	}
	volumeLockKey := "volume:" + req.GetVolumeId()
	for {
		// Phase 1 intentionally acquires only the volume lock. This preserves
		// missing-LUN idempotency and lets us discover the current target without
		// ever nesting a target lock under a held volume lock (which would invert
		// AcquireMany's deterministic order and could deadlock with Publish).
		lock, err := d.coordination.Acquire(ctx, volumeLockKey, coord.AcquireOptions{
			Safety: coord.SafetyDestructive, Operation: coord.OperationDelete,
		})
		if err != nil {
			return nil, utils.CoordinationError("acquire delete volume lock", err)
		}

		var targetID string
		needsTargetLock := false
		criticalErr := func() error {
			if err := lock.AssertOwned(ctx); err != nil {
				return utils.CoordinationError("verify delete volume lock ownership", err)
			}
			lun, err := d.client.LUNs().Get(ctx, lunID)
			if err != nil {
				if ngxstorage.IsNotFound(err) {
					return nil
				}
				return utils.BackendStatusError("get LUN before delete", err)
			}
			if lun == nil || lun.ID != lunID {
				return status.Error(codes.Internal, "get LUN before delete: backend response has an invalid id")
			}

			var mapped bool
			targetID, mapped, err = d.reconciler().FindMappedFCTarget(ctx, lunID)
			if err != nil {
				return status.Errorf(codes.Unavailable, "resolve FC mapping before delete: %v", err)
			}
			if mapped {
				needsTargetLock = true
				return nil
			}
			return d.reconciler().DeleteVolumeLocked(ctx, req.GetVolumeId(), lun, "", lock.AssertOwned)
		}()
		releaseErr := utils.ReleaseOperationLock(lock)
		if criticalErr != nil {
			if releaseErr != nil {
				return nil, errors.Join(criticalErr, utils.CoordinationError("release delete volume lock", releaseErr))
			}
			return nil, criticalErr
		}
		if releaseErr != nil {
			return nil, utils.CoordinationError("release delete volume lock", releaseErr)
		}
		if !needsTargetLock {
			return &csi.DeleteVolumeResponse{}, nil
		}

		// Phase 2 reacquires the complete semantic lock set in canonical order.
		// Everything is re-read because mapping/attachment state may have changed
		// while phase 1 released the volume lock.
		lockSet, err := d.coordination.AcquireMany(ctx, []string{
			volumeLockKey,
			"fc-target:" + targetID,
		}, coord.AcquireOptions{Safety: coord.SafetyDestructive, Operation: coord.OperationDelete})
		if err != nil {
			return nil, utils.CoordinationError("acquire mapped delete volume locks", err)
		}
		criticalErr = func() error {
			if err := lockSet.AssertOwned(ctx); err != nil {
				return utils.CoordinationError("verify mapped delete volume lock ownership", err)
			}
			lun, err := d.client.LUNs().Get(ctx, lunID)
			if err != nil {
				if ngxstorage.IsNotFound(err) {
					return nil
				}
				return utils.BackendStatusError("get LUN before mapped delete", err)
			}
			if lun == nil || lun.ID != lunID {
				return status.Error(codes.Internal, "get LUN before mapped delete: backend response has an invalid id")
			}
			currentTargetID, mapped, err := d.reconciler().FindMappedFCTarget(ctx, lunID)
			if err != nil {
				return status.Errorf(codes.Unavailable, "recheck FC mapping before delete: %v", err)
			}
			if !mapped || currentTargetID != targetID {
				return helper.ErrFCTargetChanged{Expected: targetID, Actual: currentTargetID}
			}
			return d.reconciler().DeleteVolumeLocked(ctx, req.GetVolumeId(), lun, targetID, lockSet.AssertOwned)
		}()
		releaseErr = utils.ReleaseOperationLockSet(lockSet)
		var changed helper.ErrFCTargetChanged
		if errors.As(criticalErr, &changed) {
			if releaseErr != nil {
				return nil, utils.CoordinationError("release mapped delete locks after target change", releaseErr)
			}
			continue
		}
		if criticalErr != nil {
			if releaseErr != nil {
				return nil, errors.Join(criticalErr, utils.CoordinationError("release mapped delete volume locks", releaseErr))
			}
			return nil, criticalErr
		}
		if releaseErr != nil {
			return nil, utils.CoordinationError("release mapped delete volume locks", releaseErr)
		}
		return &csi.DeleteVolumeResponse{}, nil
	}
}

// ControllerPublishVolume maps the LUN to a pre-created FC target and returns
// the WWPNs, SCSI LUN number, and SCSI ID in the publish context.
func (d *Driver) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	d.log().Info("ControllerPublishVolume(): init")
	d.log().Debug("ControllerPublishVolume(): request",
		zap.String("volume_id", req.GetVolumeId()),
		zap.String("node_id", req.GetNodeId()),
		zap.Bool("readonly", req.GetReadonly()),
		zap.Bool("has_secrets", len(req.GetSecrets()) != 0),
	)

	if req.GetVolumeId() == "" || req.GetNodeId() == "" {
		d.log().Error("ControllerPublishVolume(): volume ID and node ID are required")
		return nil, status.Error(codes.InvalidArgument, "volume ID and node ID are required")
	}
	if req.GetVolumeCapability() == nil {
		d.log().Error("ControllerPublishVolume(): volume capability is required")
		return nil, status.Error(codes.InvalidArgument, "volume capability is required")
	}
	lunID, err := helper.LUNIDFromVolumeIDChecked(volumeIDPrefix, req.GetVolumeId())
	if err != nil {
		d.log().Error("ControllerPublishVolume(): invalid volume ID", zap.String("volume_id", req.GetVolumeId()), zap.Error(err))
		return nil, err
	}

	// The referenced volume and node must exist before any backend mutation;
	// CSI requires NotFound for either missing side. Resolving them first also
	// keeps a bogus ID from surfacing as a backend payload error.
	lun, err := d.client.LUNs().Get(ctx, lunID)
	if err != nil {
		return nil, utils.BackendStatusError("lookup volume", err)
	}
	if lun == nil || lun.ID == "" || lun.ID != lunID {
		d.log().Error("ControllerPublishVolume(): volume not found", zap.String("volume_id", req.GetVolumeId()))
		return nil, status.Error(codes.NotFound, "volume not found")
	}
	if err := helper.AssertNodeExists(ctx, d.attachmentLookup, req.GetNodeId()); err != nil {
		return nil, err
	}

	// The backend keeps every LUN mapped to a target on the SAME controller
	// ("all volumes in the scsi target must belong to the same controller").
	// LUN placement is pool-scoped, so the pool owner (resolved at startup)
	// is the owner for the LUN map and drives target selection. The volume
	// context may carry an explicit owner from create.
	owner := req.GetVolumeContext()["owner"]
	if owner == "" {
		owner = d.poolOwner
	}
	if owner == "" {
		d.log().Error("ControllerPublishVolume(): cannot resolve pool owner")
		return nil, status.Errorf(codes.Internal, "resolve pool owner")
	}

	// Resolve the FC target that serves this LUN's controller.
	targetID, err := d.reconciler().SelectTargetByOwner(ctx, d.fcTargetNames, owner)
	if err != nil {
		d.log().Error("ControllerPublishVolume(): FC target resolution failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "resolve FC target: %v", err)
	}
	if d.coordination == nil {
		return nil, status.Error(codes.Unavailable, "FC publish coordination is unavailable")
	}
	lockSet, err := d.coordination.AcquireMany(ctx, []string{
		"volume:" + req.GetVolumeId(),
		"fc-target:" + targetID,
	}, coord.AcquireOptions{Safety: coord.SafetyOrdinary, Operation: coord.OperationPublish})
	if err != nil {
		return nil, utils.CoordinationError("acquire FC publish locks", err)
	}
	var response *csi.ControllerPublishVolumeResponse
	criticalErr := func() error {
		if err := lockSet.AssertOwned(ctx); err != nil {
			return utils.CoordinationError("verify FC publish lock ownership", err)
		}
		// Re-read the target under the distributed target lock. If its owner no
		// longer matches the LUN placement, fail instead of mutating stale state.
		before, err := d.client.FCTargets().Get(ctx, targetID)
		if err != nil {
			return status.Errorf(codes.Internal, "get FC target before map: %v", err)
		}
		if before.Owner != "" && before.Owner != owner {
			return status.Errorf(codes.FailedPrecondition,
				"FC target owner changed: target=%s owner=%s expected=%s", targetID, before.Owner, owner)
		}
		if err := lockSet.AssertOwned(ctx); err != nil {
			return utils.CoordinationError("verify FC publish locks before backend mutation", err)
		}
		// Map the LUN to the target (idempotent).
		if err := d.client.FCTargets().AddLUN(ctx, targetID, lunID, owner); err != nil {
			return status.Errorf(codes.Internal, "map LUN to FC target: %v", err)
		}

		// Read back the target and require the exact mapping before returning a
		// publish context to kubelet.
		target, err := d.client.FCTargets().Get(ctx, targetID)
		if err != nil {
			return status.Errorf(codes.Internal, "get FC target after map: %v", err)
		}
		wwpns := fc.TargetWWPNs(target)
		lunNum, err := fc.LUNNumber(target, lunID)
		if err != nil {
			return status.Errorf(codes.Internal, "get LUN number after map: %v", err)
		}
		lun, err := d.client.LUNs().Get(ctx, lunID)
		if err != nil {
			return status.Errorf(codes.Internal, "get LUN detail after map: %v", err)
		}
		response = &csi.ControllerPublishVolumeResponse{PublishContext: map[string]string{
			"target_wwns":       strings.Join(wwpns, ","),
			"lun_number":        fmt.Sprintf("%d", lunNum),
			"scsi_id":           lun.ScsiID,
			"fstype":            req.GetVolumeContext()["fstype"],
			"mount_permissions": req.GetVolumeContext()["mount_permissions"],
		}}
		d.log().Info("ControllerPublishVolume(): volume published",
			zap.String("volume_id", req.GetVolumeId()), zap.String("target_id", targetID),
			zap.Int("lun_number", lunNum), zap.Int("wwpn_count", len(wwpns)))
		return nil
	}()
	releaseErr := utils.ReleaseOperationLockSet(lockSet)
	if criticalErr != nil {
		if releaseErr != nil {
			return nil, errors.Join(criticalErr, utils.CoordinationError("release FC publish locks", releaseErr))
		}
		return nil, criticalErr
	}
	if releaseErr != nil {
		return nil, utils.CoordinationError("release FC publish locks", releaseErr)
	}
	return response, nil
}

// ControllerUnpublishVolume unmaps the LUN from its FC target.
func (d *Driver) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	d.log().Info("ControllerUnpublishVolume(): init")
	d.log().Debug("ControllerUnpublishVolume(): request",
		zap.String("volume_id", req.GetVolumeId()),
		zap.String("node_id", req.GetNodeId()),
		zap.Bool("has_secrets", len(req.GetSecrets()) != 0),
	)

	if req.GetVolumeId() == "" {
		d.log().Error("ControllerUnpublishVolume(): volume ID is required")
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	lunID, err := helper.LUNIDFromVolumeIDChecked(volumeIDPrefix, req.GetVolumeId())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			// CSI idempotency: nothing of this driver's to unpublish.
			d.log().Info("ControllerUnpublishVolume(): volume not owned by this driver; idempotent OK", zap.String("volume_id", req.GetVolumeId()))
			return &csi.ControllerUnpublishVolumeResponse{}, nil
		}
		d.log().Error("ControllerUnpublishVolume(): invalid volume ID", zap.String("volume_id", req.GetVolumeId()), zap.Error(err))
		return nil, err
	}
	if d.coordination == nil {
		return nil, status.Error(codes.Unavailable, "FC unpublish coordination is unavailable")
	}
	// Validate that Kubernetes can authoritatively resolve attachment state
	// before touching backend target metadata. This early read is a fail-closed
	// dependency check only; the destructive decision is re-read under the
	// distributed volume/target lock below.
	if nodeID := req.GetNodeId(); nodeID != "" {
		if _, err := helper.RemainderNodeIDs(ctx, d.attachmentLookup, d.Name, req.GetVolumeId(), nodeID, d.log()); err != nil {
			return nil, err
		}
	}

	for {
		targetID, found, err := d.reconciler().FindMappedFCTarget(ctx, lunID)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "find FC target mapping: %v", err)
		}
		if !found {
			// Serialize against a concurrent publish of this volume, then recheck.
			volumeLock, err := d.coordination.Acquire(ctx, "volume:"+req.GetVolumeId(), coord.AcquireOptions{
				Safety: coord.SafetyOrdinary, Operation: coord.OperationPublish,
			})
			if err != nil {
				return nil, utils.CoordinationError("acquire FC unpublish volume lock", err)
			}
			_, appeared, checkErr := d.reconciler().FindMappedFCTarget(ctx, lunID)
			releaseErr := utils.ReleaseOperationLock(volumeLock)
			if checkErr != nil {
				return nil, status.Errorf(codes.Internal, "recheck FC target mapping: %v", checkErr)
			}
			if releaseErr != nil {
				return nil, utils.CoordinationError("release FC unpublish volume lock", releaseErr)
			}
			if appeared {
				continue
			}
			d.log().Info("ControllerUnpublishVolume(): volume already unmapped", zap.String("lun_id", lunID))
			return &csi.ControllerUnpublishVolumeResponse{}, nil
		}

		lockSet, err := d.coordination.AcquireMany(ctx, []string{
			"volume:" + req.GetVolumeId(),
			"fc-target:" + targetID,
		}, coord.AcquireOptions{Safety: coord.SafetyOrdinary, Operation: coord.OperationPublish})
		if err != nil {
			return nil, utils.CoordinationError("acquire FC unpublish locks", err)
		}
		criticalErr := func() error {
			if err := lockSet.AssertOwned(ctx); err != nil {
				return utils.CoordinationError("verify FC unpublish lock ownership", err)
			}
			currentTargetID, stillMapped, err := d.reconciler().FindMappedFCTarget(ctx, lunID)
			if err != nil {
				return status.Errorf(codes.Internal, "recheck FC target mapping under lock: %v", err)
			}
			if !stillMapped {
				return nil
			}
			if currentTargetID != targetID {
				return helper.ErrFCTargetChanged{Expected: targetID, Actual: currentTargetID}
			}

			// Kubernetes VolumeAttachment is authoritative. This recheck is inside
			// the same volume/target critical section as the destructive unmap.
			nodeID := req.GetNodeId()
			if nodeID != "" {
				remaining, err := helper.RemainderNodeIDs(ctx, d.attachmentLookup, d.Name, req.GetVolumeId(), nodeID, d.log())
				if err != nil {
					return err
				}
				if len(remaining) > 0 {
					d.log().Info("ControllerUnpublishVolume(): retaining shared mapping for remaining nodes",
						zap.String("lun_id", lunID), zap.String("node_id", nodeID),
						zap.Strings("remaining_nodes", remaining))
					return nil
				}
			}
			if err := lockSet.AssertOwned(ctx); err != nil {
				return utils.CoordinationError("verify FC unpublish lock before backend mutation", err)
			}
			if err := d.client.FCTargets().RemoveLUN(ctx, targetID, lunID, d.poolOwner); err != nil {
				return status.Errorf(codes.Internal, "unmap LUN from FC target: %v", err)
			}
			if err := d.reconciler().WaitForFCUnmap(ctx, targetID, lunID); err != nil {
				return status.Errorf(codes.Internal, "verify FC LUN unmap: %v", err)
			}
			d.log().Info("ControllerUnpublishVolume(): volume unpublished",
				zap.String("lun_id", lunID), zap.String("target_id", targetID))
			return nil
		}()
		releaseErr := utils.ReleaseOperationLockSet(lockSet)
		var changed helper.ErrFCTargetChanged
		if errors.As(criticalErr, &changed) {
			if releaseErr != nil {
				return nil, utils.CoordinationError("release FC unpublish locks after target change", releaseErr)
			}
			continue
		}
		if criticalErr != nil {
			if releaseErr != nil {
				return nil, errors.Join(criticalErr, utils.CoordinationError("release FC unpublish locks", releaseErr))
			}
			return nil, criticalErr
		}
		if releaseErr != nil {
			return nil, utils.CoordinationError("release FC unpublish locks", releaseErr)
		}
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}
}

// CreateSnapshot provisions a snapshot of a LUN.
// GetSnapshot returns one snapshot by canonical ID. Context/input checks and
// gRPC result decisions live here; backend lookup/proto preparation is owned
// by the reconcile helper.
func (d *Driver) GetSnapshot(ctx context.Context, req *csi.GetSnapshotRequest) (*csi.GetSnapshotResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if req == nil || req.GetSnapshotId() == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot ID is required")
	}
	snap, err := d.reconciler().SnapshotByID(ctx, req.GetSnapshotId())
	if err != nil {
		if ngxstorage.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "snapshot %s not found", req.GetSnapshotId())
		}
		return nil, utils.BackendStatusError("get snapshot", err)
	}
	if snap == nil {
		return nil, status.Errorf(codes.NotFound, "snapshot %s not found", req.GetSnapshotId())
	}
	return &csi.GetSnapshotResponse{Snapshot: snap}, nil
}

func (d *Driver) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	d.log().Info("CreateSnapshot(): init")
	d.log().Debug("CreateSnapshot(): request",
		zap.String("name", req.GetName()),
		zap.String("source_volume_id", req.GetSourceVolumeId()),
		zap.Bool("has_secrets", len(req.GetSecrets()) != 0),
	)

	if req.GetSourceVolumeId() == "" || req.GetName() == "" {
		d.log().Error("CreateSnapshot(): source volume ID and name are required")
		return nil, status.Error(codes.InvalidArgument, "source volume ID and name are required")
	}
	lunID, err := helper.LUNIDFromVolumeIDChecked(volumeIDPrefix, req.GetSourceVolumeId())
	if err != nil {
		d.log().Error("CreateSnapshot(): invalid source volume ID", zap.String("source_volume_id", req.GetSourceVolumeId()), zap.Error(err))
		return nil, err
	}
	name := ngxnames.ModifyVolName(req.GetName())
	if d.coordination == nil {
		return nil, status.Error(codes.Unavailable, "create snapshot coordination is unavailable")
	}
	lockSet, err := d.coordination.AcquireMany(ctx, []string{
		"snapshot-name:" + name,
		"volume:" + req.GetSourceVolumeId(),
	}, coord.AcquireOptions{Safety: coord.SafetyOrdinary, Operation: coord.OperationSnapshot})
	if err != nil {
		return nil, utils.CoordinationError("acquire create snapshot locks", err)
	}

	var response *csi.CreateSnapshotResponse
	criticalErr := func() error {
		if err := lockSet.AssertOwned(ctx); err != nil {
			return utils.CoordinationError("verify create snapshot lock ownership", err)
		}
		lun, err := d.client.LUNs().Get(ctx, lunID)
		if err != nil {
			return utils.BackendStatusError("get source LUN", err)
		}
		if lun == nil || lun.ID != lunID {
			return status.Error(codes.NotFound, "source volume not found")
		}

		sourceSnapshots, err := d.client.Snapshots().ListDetail(ctx, lunID)
		if err != nil {
			return status.Errorf(codes.Unavailable, "list source-volume snapshots for create idempotency: %v", err)
		}
		existingSnapshotID := ""
		for i := range sourceSnapshots {
			if sourceSnapshots[i].Name == name {
				if sourceSnapshots[i].ID == "" {
					return status.Errorf(codes.Unavailable, "same-name snapshot %q has an empty backend id", name)
				}
				existingSnapshotID = sourceSnapshots[i].ID
				break
			}
		}

		// The source-volume-filtered list is the common idempotent retry path.
		// Only a miss needs the global list to preserve cross-volume name
		// uniqueness; this keeps concurrent same-source retries from repeatedly
		// fetching every snapshot while holding the name lock.
		if existingSnapshotID == "" {
			snaps, err := d.client.Snapshots().ListDetail(ctx, "")
			if err != nil {
				return status.Errorf(codes.Unavailable, "list snapshots for create name check: %v", err)
			}
			for i := range snaps {
				if snaps[i].Name != name {
					continue
				}
				if snaps[i].VolumeID != lunID {
					return status.Errorf(codes.AlreadyExists,
						"snapshot name %q already exists for source volume %q", name, snaps[i].VolumeID)
				}
				if snaps[i].ID == "" {
					return status.Errorf(codes.Unavailable, "same-name snapshot %q has an empty backend id", name)
				}
				existingSnapshotID = snaps[i].ID
				break
			}
		}
		if existingSnapshotID != "" {
			readback, err := d.client.Snapshots().Get(ctx, existingSnapshotID)
			if err != nil {
				return utils.BackendStatusError("read back existing snapshot", err)
			}
			if readback == nil || readback.ID != existingSnapshotID || readback.Name != name || readback.VolumeID != lunID {
				return status.Error(codes.Unavailable, "same-name snapshot readback identity mismatch")
			}
			response = helper.CreateSnapshotResponse(volumeIDPrefix, readback, req.GetSourceVolumeId(), int64(lun.Size))
			d.log().Info("CreateSnapshot(): snapshot already exists (idempotent)",
				zap.String("snapshot_id", readback.ID), zap.String("name", name))
			return nil
		}

		if err := lockSet.AssertOwned(ctx); err != nil {
			return utils.CoordinationError("verify create snapshot locks before backend mutation", err)
		}
		created, err := d.client.Snapshots().Create(ctx, lunID, name)
		if err != nil {
			return utils.BackendStatusError("create snapshot", err)
		}
		if created == nil || created.ID == "" {
			return status.Error(codes.Internal, "create snapshot: backend returned an invalid snapshot")
		}
		readback, err := d.client.Snapshots().Get(ctx, created.ID)
		if err != nil {
			return utils.BackendStatusError("read back created snapshot", err)
		}
		if readback == nil || readback.ID != created.ID || readback.Name != name || readback.VolumeID != lunID {
			return status.Errorf(codes.Internal,
				"created snapshot readback mismatch: id=%q name=%q source=%q expected_id=%q expected_name=%q expected_source=%q",
				readback.ID, readback.Name, readback.VolumeID, created.ID, name, lunID)
		}
		response = helper.CreateSnapshotResponse(volumeIDPrefix, readback, req.GetSourceVolumeId(), int64(lun.Size))
		d.log().Info("CreateSnapshot(): snapshot created", zap.String("snapshot_id", readback.ID))
		return nil
	}()
	releaseErr := utils.ReleaseOperationLockSet(lockSet)
	if criticalErr != nil {
		if releaseErr != nil {
			return nil, errors.Join(criticalErr, utils.CoordinationError("release create snapshot locks", releaseErr))
		}
		return nil, criticalErr
	}
	if releaseErr != nil {
		return nil, utils.CoordinationError("release create snapshot locks", releaseErr)
	}
	return response, nil
}

// DeleteSnapshot deletes a snapshot. Idempotent. Success is reported only
// after the backend confirms the snapshot is absent.
func (d *Driver) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	d.log().Info("DeleteSnapshot(): init")
	d.log().Debug("DeleteSnapshot(): request",
		zap.String("snapshot_id", req.GetSnapshotId()),
		zap.Bool("has_secrets", len(req.GetSecrets()) != 0),
	)

	if req.GetSnapshotId() == "" {
		d.log().Error("DeleteSnapshot(): snapshot ID is required")
		return nil, status.Error(codes.InvalidArgument, "snapshot ID is required")
	}
	if d.coordination == nil {
		return nil, status.Error(codes.Unavailable, "delete snapshot coordination is unavailable")
	}
	lock, err := d.coordination.Acquire(ctx, "snapshot:"+req.GetSnapshotId(), coord.AcquireOptions{
		Safety: coord.SafetyDestructive, Operation: coord.OperationSnapshot,
	})
	if err != nil {
		return nil, utils.CoordinationError("acquire delete snapshot lock", err)
	}
	criticalErr := func() error {
		if err := lock.AssertOwned(ctx); err != nil {
			return utils.CoordinationError("verify delete snapshot lock ownership", err)
		}
		if _, err := d.client.Snapshots().Get(ctx, req.GetSnapshotId()); err != nil {
			if ngxstorage.IsNotFound(err) {
				return nil
			}
			return utils.BackendStatusError("get snapshot before delete", err)
		}
		if err := lock.AssertOwned(ctx); err != nil {
			return utils.CoordinationError("verify delete snapshot lock before backend mutation", err)
		}
		if err := d.client.Snapshots().Delete(ctx, req.GetSnapshotId()); err != nil {
			return utils.BackendStatusError("delete snapshot", err)
		}
		// Read-after-delete: a 200-success delete response is not proof of
		// backend absence. Verify authoritative NotFound before claiming CSI
		// success; a still-present snapshot must surface as an error so the
		// caller retries instead of attesting a deletion that never happened.
		if err := d.reconciler().VerifySnapshotDeleted(ctx, req.GetSnapshotId()); err != nil {
			return err
		}
		d.log().Info("DeleteSnapshot(): snapshot deleted (backend absence verified)", zap.String("snapshot_id", req.GetSnapshotId()))
		return nil
	}()
	releaseErr := utils.ReleaseOperationLock(lock)
	if criticalErr != nil {
		if releaseErr != nil {
			return nil, errors.Join(criticalErr, utils.CoordinationError("release delete snapshot lock", releaseErr))
		}
		return nil, criticalErr
	}
	if releaseErr != nil {
		return nil, utils.CoordinationError("release delete snapshot lock", releaseErr)
	}
	return &csi.DeleteSnapshotResponse{}, nil
}

// ListSnapshots lists snapshots. A nil response lists all snapshots; a
// snapshot-ID response lists a single snapshot.
func (d *Driver) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	d.log().Info("ListSnapshots(): init")
	d.log().Debug("ListSnapshots(): request",
		zap.String("snapshot_id", req.GetSnapshotId()),
		zap.String("source_volume_id", req.GetSourceVolumeId()),
		zap.Int32("max_entries", req.GetMaxEntries()),
	)

	if req.GetMaxEntries() < 0 {
		return nil, status.Error(codes.InvalidArgument, "max_entries must not be negative")
	}
	if req.GetSnapshotId() != "" {
		snap, err := d.client.Snapshots().Get(ctx, req.GetSnapshotId())
		if err != nil {
			if ngxstorage.IsNotFound(err) {
				// CSI: listing a specific missing snapshot id returns an empty
				// list, not an error.
				return &csi.ListSnapshotsResponse{}, nil
			}
			d.log().Error("ListSnapshots(): snapshot get failed", zap.Error(err))
			return nil, utils.BackendStatusError("get snapshot", err)
		}
		if snap == nil || snap.ID == "" {
			// Absent snapshot resolved to an empty record: also an empty list.
			return &csi.ListSnapshotsResponse{}, nil
		}
		return &csi.ListSnapshotsResponse{
			Entries: []*csi.ListSnapshotsResponse_Entry{{
				Snapshot: helper.SnapshotProto(volumeIDPrefix, snap),
			}},
		}, nil
	}
	// The detail endpoint carries created/reserved; the summary endpoint omits
	// them, which would leave every snapshot with an empty creation time.
	snaps, err := d.client.Snapshots().ListDetail(ctx, "")
	if err != nil {
		d.log().Error("ListSnapshots(): snapshot list failed", zap.Error(err))
		return nil, utils.BackendStatusError("list snapshots", err)
	}
	// CSI requires source filtering and opaque pagination.
	if sourceID := req.GetSourceVolumeId(); sourceID != "" {
		backendID, err := helper.LUNIDFromVolumeIDChecked(volumeIDPrefix, sourceID)
		if err != nil {
			if status.Code(err) == codes.NotFound {
				// CSI: a source volume that is not this driver's has no snapshots.
				return &csi.ListSnapshotsResponse{}, nil
			}
			return nil, err
		}
		filtered := snaps[:0]
		for _, snap := range snaps {
			if snap.VolumeID == backendID {
				filtered = append(filtered, snap)
			}
		}
		snaps = filtered
	}
	entries := make([]*csi.ListSnapshotsResponse_Entry, 0, len(snaps))
	for i := range snaps {
		if snaps[i].ID == "" {
			d.log().Error("ListSnapshots(): backend record is missing id, skipping")
			continue
		}
		entries = append(entries, &csi.ListSnapshotsResponse_Entry{Snapshot: helper.SnapshotProto(volumeIDPrefix, &snaps[i])})
	}

	// Sort by stable snapshot ID and paginate with an opaque canonical-ID
	// cursor (s1:<snapshotId>). An index token would shift under concurrent
	// create/delete and duplicate or omit entries; a cursor resolves by ID,
	// so a snapshot deleted between pages simply advances the position.
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].GetSnapshot().GetSnapshotId() < entries[j].GetSnapshot().GetSnapshotId()
	})
	start := 0
	const tokenPrefix = "s1:"
	if token := req.GetStartingToken(); token != "" {
		if !strings.HasPrefix(token, tokenPrefix) || len(token) == len(tokenPrefix) {
			// CSI: an invalid starting_token is Aborted, not InvalidArgument.
			return nil, status.Error(codes.Aborted, "invalid starting token")
		}
		cursor := strings.TrimPrefix(token, tokenPrefix)
		for start < len(entries) && entries[start].GetSnapshot().GetSnapshotId() <= cursor {
			start++
		}
	}
	end := len(entries)
	if max := req.GetMaxEntries(); max > 0 && int64(start)+int64(max) < int64(end) {
		end = start + int(max)
	}
	response := &csi.ListSnapshotsResponse{Entries: entries[start:end]}
	if end < len(entries) {
		response.NextToken = tokenPrefix + entries[end-1].GetSnapshot().GetSnapshotId()
	}
	d.log().Info("ListSnapshots(): snapshots listed", zap.Int("count", len(response.Entries)))
	return response, nil
}

// ControllerGetVolume returns the current canonical LUN identity and size.
func (d *Driver) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if req == nil || req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	backendID, err := helper.LUNIDFromVolumeIDChecked(volumeIDPrefix, req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	lun, err := d.client.LUNs().Get(ctx, backendID)
	if err != nil {
		return nil, utils.BackendStatusError("get volume", err)
	}
	if lun == nil || lun.ID == "" || lun.ID != backendID {
		return nil, status.Error(codes.Internal, "get volume: backend response has an invalid id")
	}
	return &csi.ControllerGetVolumeResponse{
		Volume: &csi.Volume{VolumeId: helper.VolumeID(volumeIDPrefix, lun.ID), CapacityBytes: int64(lun.Size)},
		Status: &csi.ControllerGetVolumeResponse_VolumeStatus{},
	}, nil
}

// ControllerExpandVolume grows a LUN. The node performs the filesystem resize.
func (d *Driver) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	d.log().Info("ControllerExpandVolume(): init")
	d.log().Debug("ControllerExpandVolume(): request",
		zap.String("volume_id", req.GetVolumeId()),
		zap.Reflect("capacity_range", req.GetCapacityRange()),
		zap.Bool("has_secrets", len(req.GetSecrets()) != 0),
	)

	if req.GetVolumeId() == "" || req.GetCapacityRange() == nil {
		d.log().Error("ControllerExpandVolume(): volume ID and capacity range are required")
		return nil, status.Error(codes.InvalidArgument, "volume ID and capacity range are required")
	}
	lunID, err := helper.LUNIDFromVolumeIDChecked(volumeIDPrefix, req.GetVolumeId())
	if err != nil {
		d.log().Error("ControllerExpandVolume(): invalid volume ID", zap.String("volume_id", req.GetVolumeId()), zap.Error(err))
		return nil, err
	}
	newBytes := req.GetCapacityRange().GetRequiredBytes()
	if newBytes <= 0 {
		d.log().Error("ControllerExpandVolume(): required capacity must be positive")
		return nil, status.Error(codes.InvalidArgument, "required capacity must be positive")
	}
	if d.coordination == nil {
		return nil, status.Error(codes.Unavailable, "controller expand coordination is unavailable")
	}
	lock, err := d.coordination.Acquire(ctx, "volume:"+req.GetVolumeId(), coord.AcquireOptions{
		Safety:    coord.SafetyOrdinary,
		Operation: coord.OperationExpand,
	})
	if err != nil {
		return nil, utils.CoordinationError("acquire controller expand lock", err)
	}
	var response *csi.ControllerExpandVolumeResponse
	criticalErr := func() error {
		if err := lock.AssertOwned(ctx); err != nil {
			return utils.CoordinationError("verify controller expand lock ownership", err)
		}
		lun, err := d.client.LUNs().Get(ctx, lunID)
		if err != nil {
			return utils.BackendStatusError("get LUN before expand", err)
		}
		if lun == nil || lun.ID != lunID {
			return status.Error(codes.NotFound, "volume not found")
		}
		if lun.Size <= 0 {
			return status.Error(codes.Internal, "get LUN before expand: backend response has invalid size")
		}
		const gib = int64(1 << 30)
		targetGB := (newBytes + gib - 1) / gib
		currentBytes := int64(lun.Size)
		// Shrink is rejected at byte granularity: the whole-GiB idempotency
		// check below would otherwise accept a smaller request that rounds
		// to the same GiB as a no-op success.
		if newBytes < currentBytes {
			d.log().Warn("ControllerExpandVolume(): volume shrink is not supported",
				zap.String("lun_id", lunID),
				zap.Int64("current_bytes", currentBytes),
				zap.Int64("requested_bytes", newBytes),
			)
			return status.Error(codes.InvalidArgument, "requested capacity must not shrink the volume")
		}
		currentGB := (currentBytes + gib - 1) / gib
		extendGB := targetGB - currentGB
		if extendGB <= 0 {
			d.log().Info("ControllerExpandVolume(): LUN already satisfies requested capacity",
				zap.String("lun_id", lunID),
				zap.Int64("current_bytes", currentBytes),
				zap.Int64("requested_bytes", newBytes),
			)
			response = &csi.ControllerExpandVolumeResponse{
				CapacityBytes:         currentBytes,
				NodeExpansionRequired: true,
			}
			return nil
		}
		if err := lock.AssertOwned(ctx); err != nil {
			return utils.CoordinationError("verify controller expand lock before backend mutation", err)
		}
		if err := d.client.LUNs().Expand(ctx, lunID, extendGB); err != nil {
			return status.Errorf(codes.Internal, "expand LUN: %v", err)
		}
		finalLUN, err := d.client.LUNs().Get(ctx, lunID)
		if err != nil {
			return utils.BackendStatusError("get LUN after expand", err)
		}
		finalBytes := int64(finalLUN.Size)
		finalGB := (finalBytes + gib - 1) / gib
		if finalBytes < newBytes || finalGB != targetGB {
			return status.Errorf(codes.Internal,
				"expand LUN readback mismatch: requested=%d target_gb=%d final_bytes=%d final_gb=%d",
				newBytes, targetGB, finalBytes, finalGB)
		}
		d.log().Info("ControllerExpandVolume(): LUN expanded",
			zap.String("lun_id", lunID),
			zap.Int64("current_gb", currentGB),
			zap.Int64("target_gb", targetGB),
			zap.Int64("extend_gb", extendGB),
			zap.Int64("final_bytes", finalBytes),
		)
		response = &csi.ControllerExpandVolumeResponse{
			CapacityBytes:         finalBytes,
			NodeExpansionRequired: true,
		}
		return nil
	}()
	releaseErr := utils.ReleaseOperationLock(lock)
	if criticalErr != nil {
		if releaseErr != nil {
			return nil, errors.Join(criticalErr, utils.CoordinationError("release controller expand lock", releaseErr))
		}
		return nil, criticalErr
	}
	if releaseErr != nil {
		return nil, utils.CoordinationError("release controller expand lock", releaseErr)
	}
	return response, nil
}

// ListVolumes lists all provisioned LUNs.
func (d *Driver) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	d.log().Info("ListVolumes(): init")
	if req.GetMaxEntries() < 0 {
		return nil, status.Error(codes.InvalidArgument, "max_entries must not be negative")
	}
	luns, err := d.client.LUNs().List(ctx)
	if err != nil {
		d.log().Error("ListVolumes(): LUN list failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "list LUNs: %v", err)
	}
	entries := make([]*csi.ListVolumesResponse_Entry, 0, len(luns))
	for i := range luns {
		if luns[i].ID == "" {
			d.log().Error("ListVolumes(): backend record is missing id, skipping")
			continue
		}
		entries = append(entries, &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{VolumeId: helper.VolumeID(volumeIDPrefix, luns[i].ID), CapacityBytes: int64(luns[i].Size)},
		})
	}

	// Sort by stable volume ID and paginate with an opaque canonical-ID
	// cursor (v1:<volumeId>). An index token would shift under concurrent
	// create/delete and duplicate or omit entries; a cursor resolves by ID,
	// so a volume deleted between pages simply advances the position.
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].GetVolume().GetVolumeId() < entries[j].GetVolume().GetVolumeId()
	})
	start := 0
	const tokenPrefix = "v1:"
	if token := req.GetStartingToken(); token != "" {
		if !strings.HasPrefix(token, tokenPrefix) || len(token) == len(tokenPrefix) {
			// CSI: an invalid starting_token is Aborted, not InvalidArgument.
			return nil, status.Error(codes.Aborted, "invalid starting token")
		}
		cursor := strings.TrimPrefix(token, tokenPrefix)
		for start < len(entries) && entries[start].GetVolume().GetVolumeId() <= cursor {
			start++
		}
	}
	end := len(entries)
	if max := req.GetMaxEntries(); max > 0 && int64(start)+int64(max) < int64(end) {
		end = start + int(max)
	}
	response := &csi.ListVolumesResponse{Entries: entries[start:end]}
	if end < len(entries) {
		response.NextToken = tokenPrefix + entries[end-1].GetVolume().GetVolumeId()
	}
	d.log().Info("ListVolumes(): volumes listed", zap.Int("count", len(response.Entries)))
	return response, nil
}

// GetCapacity returns the configured pool's available capacity in bytes.
func (d *Driver) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	d.log().Info("GetCapacity(): init")
	avail, _, err := d.client.Pools().GetConfiguredCapacity(ctx)
	if err != nil {
		d.log().Error("GetCapacity(): capacity lookup failed", zap.Error(err))
		return nil, status.Errorf(codes.Unavailable,
			"configured pool %q capacity lookup failed: %v", d.client.PoolName(), err)
	}
	d.log().Info("GetCapacity(): capacity returned", zap.Int64("available_bytes", avail))
	return &csi.GetCapacityResponse{AvailableCapacity: avail}, nil
}

// ValidateVolumeCapabilities validates a requested capability against the
// driver's advertised access modes and mount/block types.
//
// CSI requires the RPC to resolve the referenced volume and fail with
// NotFound when it does not exist. An empty capability list is not a
// confirmation: at least one capability must be present and valid.
func (d *Driver) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	d.log().Info("ValidateVolumeCapabilities(): init")
	if err := ctx.Err(); err != nil {
		d.log().Error("ValidateVolumeCapabilities(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	if req.GetVolumeId() == "" {
		d.log().Error("ValidateVolumeCapabilities(): volume ID is required")
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if len(req.GetVolumeCapabilities()) == 0 {
		d.log().Error("ValidateVolumeCapabilities(): at least one capability is required")
		return nil, status.Error(codes.InvalidArgument, "volume capabilities are required")
	}

	// The volume must exist: an empty Confirmed response alone would confirm
	// capabilities for a volume the backend does not have.
	lunID, err := helper.LUNIDFromVolumeIDChecked(volumeIDPrefix, req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	lun, err := d.client.LUNs().Get(ctx, lunID)
	if err != nil {
		return nil, utils.BackendStatusError("lookup volume", err)
	}
	if lun == nil || lun.ID == "" {
		return nil, status.Error(codes.NotFound, "volume not found")
	}

	for _, capability := range req.GetVolumeCapabilities() {
		if err := utils.ValidateVolumeCapability(capability, d.AccessModes); err != nil {
			d.log().Info("ValidateVolumeCapabilities(): capability not confirmed", zap.Error(err))
			return &csi.ValidateVolumeCapabilitiesResponse{}, nil
		}
	}
	d.log().Info("ValidateVolumeCapabilities(): capabilities confirmed",
		zap.String("volume_id", req.GetVolumeId()))
	response := &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.GetVolumeCapabilities(),
		},
	}
	if lun.ID != "" {
		response.Confirmed.VolumeContext = map[string]string{"owner": d.poolOwner}
	}
	return response, nil
}

// ControllerModifyVolume applies one explicit set of supported mutable NGX
// LUN attributes. It is the CSI VolumeAttributesClass update path; omitted
// fields are never defaulted or changed. Immutable create parameters such as
// blocksize are rejected.
//
// Backend:
//   - Reads GET /api/v2/lun/{id} to resolve the canonical LUN.
//   - Writes PATCH /api/v2/lun/{id} with only the validated mutable fields.
//   - Accepts deduplication, compression, thin_provision, flash_cache,
//     dram_cache, io_type, and qos_priority.
func (d *Driver) ControllerModifyVolume(ctx context.Context, req *csi.ControllerModifyVolumeRequest) (*csi.ControllerModifyVolumeResponse, error) {
	d.log().Info("ControllerModifyVolume(): init")
	if err := ctx.Err(); err != nil {
		d.log().Error("ControllerModifyVolume(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	if req == nil || req.GetVolumeId() == "" {
		d.log().Error("ControllerModifyVolume(): volume ID is required")
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	mutable, err := helper.NormalizeLUNMutableParameters(req.GetMutableParameters())
	if err != nil {
		d.log().Error("ControllerModifyVolume(): mutable parameters rejected",
			zap.String("volume_id", req.GetVolumeId()),
			zap.Error(err),
		)
		return nil, err
	}

	lunID, err := helper.LUNIDFromVolumeIDChecked(volumeIDPrefix, req.GetVolumeId())
	if err != nil {
		d.log().Error("ControllerModifyVolume(): invalid volume ID", zap.String("volume_id", req.GetVolumeId()), zap.Error(err))
		return nil, err
	}
	if d.coordination == nil {
		return nil, status.Error(codes.Unavailable, "controller modify coordination is unavailable")
	}
	lock, err := d.coordination.Acquire(ctx, "volume:"+req.GetVolumeId(), coord.AcquireOptions{
		Safety: coord.SafetyOrdinary, Operation: coord.OperationModify,
	})
	if err != nil {
		return nil, utils.CoordinationError("acquire controller modify lock", err)
	}
	criticalErr := func() error {
		if err := lock.AssertOwned(ctx); err != nil {
			return utils.CoordinationError("verify controller modify lock ownership", err)
		}
		lun, err := d.client.LUNs().Get(ctx, lunID)
		if err != nil {
			return utils.BackendStatusError("lookup volume", err)
		}
		if lun == nil || lun.ID == "" || lun.ID != lunID {
			return status.Error(codes.NotFound, "volume not found")
		}
		if err := lock.AssertOwned(ctx); err != nil {
			return utils.CoordinationError("verify controller modify lock before backend mutation", err)
		}
		if err := d.client.LUNs().Modify(ctx, lunID, mutable); err != nil {
			return utils.BackendStatusError("modify LUN", err)
		}
		readback, err := d.client.LUNs().Get(ctx, lunID)
		if err != nil {
			return utils.BackendStatusError("read back modified LUN", err)
		}
		if readback == nil || readback.ID != lunID {
			return status.Error(codes.Internal, "modified LUN readback has an invalid id")
		}
		if err := helper.VerifyLUNMutableReadback(readback, mutable); err != nil {
			return err
		}
		d.log().Info("ControllerModifyVolume(): volume modified", zap.String("lun_id", lunID))
		return nil
	}()
	releaseErr := utils.ReleaseOperationLock(lock)
	if criticalErr != nil {
		if releaseErr != nil {
			return nil, errors.Join(criticalErr, utils.CoordinationError("release controller modify lock", releaseErr))
		}
		return nil, criticalErr
	}
	if releaseErr != nil {
		return nil, utils.CoordinationError("release controller modify lock", releaseErr)
	}
	return &csi.ControllerModifyVolumeResponse{}, nil
}

// ControllerGetCapabilities returns the controller service capabilities
// configured during NewDriver. Sidecar containers (provisioner, attacher,
// resizer) call this on startup to discover the driver's feature set.
func (d *Driver) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	d.log().Info("ControllerGetCapabilities(): init")
	d.log().Debug("ControllerGetCapabilities(): request",
		zap.Bool("request_present", req != nil),
	)
	if err := ctx.Err(); err != nil {
		d.log().Error("ControllerGetCapabilities(): context ended", zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	d.log().Info("ControllerGetCapabilities(): capabilities returned",
		zap.Reflect("capabilities", d.csCap),
	)
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: d.csCap}, nil
}
