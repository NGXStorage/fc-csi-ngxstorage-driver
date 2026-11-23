package driver

import (
	"context"
	"errors"
	"fmt"
	utils "github.com/ngxstorage/fc-csi-ngxstorage-driver/pkg/utils"
	"os"
	"strconv"
	"strings"
	"time"

	blockmount "github.com/NGXStorage/csi-ngxstorage-utils/blockmount"
	"github.com/ngxstorage/fc-csi-ngxstorage-driver/pkg/fc"
	helper "github.com/ngxstorage/fc-csi-ngxstorage-driver/pkg/helper"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	zap "go.uber.org/zap"
	"golang.org/x/sys/unix"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	mount "k8s.io/mount-utils"
)

// newFCConnector builds the host FC device connector. Tests override it to
// inject discovery failures without host sysfs access.
var newFCConnector = fc.NewConnector

// deviceDiscoveryError maps an FC device-discovery failure to a CSI code by
// cause instead of reporting every failure as a wait timeout:
//   - ended request context: the context's own code (Canceled/DeadlineExceeded);
//   - permanent request error (unusable WWID/LUN number/target WWPNs):
//     InvalidArgument — retrying the same request cannot succeed;
//   - unreadable host FC subsystem: Unavailable — discovery may succeed once
//     the host FC state is readable again;
//   - anything else is a genuine convergence wait timeout: DeadlineExceeded.
//
// Cause sentinels win over a wrapping timeout error: a wait that exhausted
// its budget because the host subsystem was unreadable is reported by cause.
func deviceDiscoveryError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	switch {
	case errors.Is(err, fc.ErrInvalidDeviceRequest):
		return status.Errorf(codes.InvalidArgument, "wait for device: %v", err)
	case errors.Is(err, fc.ErrHostFCUnavailable):
		return status.Errorf(codes.Unavailable, "wait for device: %v", err)
	default:
		return status.Errorf(codes.DeadlineExceeded, "wait for device: %v", err)
	}
}

// NodeGetInfo returns the node ID. FC is fabric-zoned; the driver does not
// advertise topology.
func (d *Driver) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	d.log().Info("NodeGetInfo(): init")
	return &csi.NodeGetInfoResponse{NodeId: d.NodeId}, nil
}

// NodeGetCapabilities returns the advertised node capabilities.
func (d *Driver) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	d.log().Info("NodeGetCapabilities(): init")
	return &csi.NodeGetCapabilitiesResponse{Capabilities: d.nsCap}, nil
}

// NodeStageVolume attaches the FC device, formats it (filesystem volumes),
// and mounts it to the staging path.
func (d *Driver) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	d.log().Info("NodeStageVolume(): init")
	d.log().Debug("NodeStageVolume(): request",
		zap.String("volume_id", req.GetVolumeId()),
		zap.String("staging_target_path", req.GetStagingTargetPath()),
		zap.Reflect("volume_capability", req.GetVolumeCapability()),
		zap.Reflect("publish_context", req.GetPublishContext()),
		zap.Bool("has_secrets", len(req.GetSecrets()) != 0),
	)

	if req.GetStagingTargetPath() == "" || req.GetVolumeId() == "" {
		d.log().Error("NodeStageVolume(): volume ID and staging path are required")
		return nil, status.Error(codes.InvalidArgument, "volume ID and staging path are required")
	}
	volumeLock, err := d.nodeState.LockVolume(ctx, req.GetVolumeId())
	if err != nil {
		d.log().Error("NodeStageVolume(): context ended while waiting for volume lock",
			zap.String("volume_id", req.GetVolumeId()), zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	defer volumeLock.Unlock()
	publishContext := req.GetPublishContext()
	scsiID := publishContext["scsi_id"]
	fstype := publishContext["fstype"]
	if scsiID == "" {
		d.log().Error("NodeStageVolume(): publish context missing scsi_id")
		return nil, status.Error(codes.InvalidArgument, "publish context missing scsi_id")
	}

	targetWWNsRaw := strings.TrimSpace(publishContext["target_wwns"])
	if targetWWNsRaw == "" {
		d.log().Error("NodeStageVolume(): publish context missing target_wwns")
		return nil, status.Error(codes.InvalidArgument, "publish context missing target_wwns")
	}
	lunNumber, err := strconv.Atoi(strings.TrimSpace(publishContext["lun_number"]))
	if err != nil || lunNumber < 0 {
		d.log().Error("NodeStageVolume(): invalid lun_number",
			zap.String("lun_number", publishContext["lun_number"]),
			zap.Error(err),
		)
		return nil, status.Error(codes.InvalidArgument, "publish context contains invalid lun_number")
	}
	targetWWNs := strings.Split(targetWWNsRaw, ",")

	// Discover the exact target/LUN paths and wait for the multipath map to
	// converge before allowing filesystem detection or formatting.
	connector := newFCConnector()
	devicePath, err := connector.WaitForMappedDevice(ctx, scsiID, targetWWNs, lunNumber)
	if err != nil {
		d.log().Error("NodeStageVolume(): device discovery failed",
			zap.String("scsi_id", scsiID),
			zap.Int("lun_number", lunNumber),
			zap.Strings("target_wwns", targetWWNs),
			zap.Error(err),
		)
		return nil, deviceDiscoveryError(err)
	}
	d.log().Info("NodeStageVolume(): FC device discovered", zap.String("device", devicePath))
	mapName, pathNames, err := connector.DescribeDevice(devicePath)
	if err != nil {
		d.log().Error("NodeStageVolume(): FC device description failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "describe FC device: %v", err)
	}
	if err := helper.SaveFCState(d.fcStateDir, req.GetVolumeId(), helper.FCVolumeState{
		SCSIID: scsiID, MapName: mapName, PathNames: pathNames,
	}); err != nil {
		d.log().Error("NodeStageVolume(): FC state persistence failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "persist FC volume state: %v", err)
	}

	// Determine block vs mount access.
	if utils.IsBlockVolume(req.GetVolumeCapability()) {
		if err := blockmount.PrepareBlockTarget(req.GetStagingTargetPath()); err != nil {
			d.log().Error("NodeStageVolume(): block staging target creation failed", zap.Error(err))
			return nil, status.Errorf(codes.Internal, "create block staging target: %v", err)
		}
		// CSI idempotency: a repeat stage of an already-staged block volume is
		// a success (the second bind mount would fail EBUSY). The existing
		// bind must expose the expected device node; an unverified or
		// mismatched target fails closed.
		if alreadyStaged, err := blockmount.BlockStagingMatchesDevice(blockmount.Mounter, req.GetStagingTargetPath(), devicePath); err != nil {
			d.log().Error("NodeStageVolume(): block staging mount verification failed", zap.Error(err))
			return nil, status.Errorf(codes.Internal, "verify block staging mount: %v", err)
		} else if alreadyStaged {
			d.log().Info("NodeStageVolume(): block volume already staged",
				zap.String("staging_path", req.GetStagingTargetPath()))
			return &csi.NodeStageVolumeResponse{}, nil
		}
		// Raw block: bind-mount the device to the staging path.
		if err := blockmount.MountVolume(devicePath, req.GetStagingTargetPath(), "", false, nil); err != nil {
			d.log().Error("NodeStageVolume(): block mount failed", zap.Error(err))
			return nil, status.Errorf(codes.Internal, "mount block device: %v", err)
		}
		d.log().Info("NodeStageVolume(): block volume staged", zap.String("device", devicePath))
		return &csi.NodeStageVolumeResponse{}, nil
	}

	// Filesystem: shared/destructive initialization is coordinated across nodes;
	// the final staging mount happens only after initialization is complete.
	if err := os.MkdirAll(req.GetStagingTargetPath(), 0o750); err != nil {
		d.log().Error("NodeStageVolume(): staging dir creation failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "create staging dir: %v", err)
	}
	if fstype == "" {
		fstype = "ext4"
	}
	revalidate := func(revalidateCtx context.Context) (string, error) {
		resolved, err := connector.WaitForMappedDevice(revalidateCtx, scsiID, targetWWNs, lunNumber)
		if err != nil {
			return "", err
		}
		mapName, pathNames, err := connector.DescribeDevice(resolved)
		if err != nil {
			return "", fmt.Errorf("describe revalidated FC device: %w", err)
		}
		if err := helper.SaveFCState(d.fcStateDir, req.GetVolumeId(), helper.FCVolumeState{
			SCSIID: scsiID, MapName: mapName, PathNames: pathNames,
		}); err != nil {
			return "", fmt.Errorf("persist revalidated FC volume state: %w", err)
		}
		return resolved, nil
	}
	if err := d.filesystemStager().Stage(ctx, req.GetVolumeId(), devicePath, req.GetStagingTargetPath(), fstype,
		req.GetVolumeCapability(), utils.RequestedMountPermissions(publishContext), revalidate); err != nil {
		d.log().Error("NodeStageVolume(): filesystem staging failed",
			zap.String("fstype", fstype), zap.Error(err))
		if status.Code(err) != codes.Unknown {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "stage filesystem volume: %v", err)
	}
	if !utils.ReaderOnlyCapability(req.GetVolumeCapability()) {
		if err := d.filesystemStager().ResizeCoordinated(ctx, req.GetVolumeId(), devicePath, req.GetStagingTargetPath()); err != nil {
			d.log().Error("NodeStageVolume(): filesystem growth failed", zap.String("volume_id", req.GetVolumeId()), zap.Error(err))
			if status.Code(err) != codes.Unknown {
				return nil, err
			}
			return nil, status.Errorf(codes.Internal, "grow staged filesystem: %v", err)
		}
	}
	d.log().Info("NodeStageVolume(): volume staged",
		zap.String("device", devicePath),
		zap.String("fstype", fstype),
	)
	return &csi.NodeStageVolumeResponse{}, nil
}

// NodeUnstageVolume unmounts the staging path and removes the FC device.
func (d *Driver) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	d.log().Info("NodeUnstageVolume(): init")
	d.log().Debug("NodeUnstageVolume(): request",
		zap.String("volume_id", req.GetVolumeId()),
		zap.String("staging_target_path", req.GetStagingTargetPath()),
	)

	if req.GetStagingTargetPath() == "" || req.GetVolumeId() == "" {
		d.log().Error("NodeUnstageVolume(): volume ID and staging path are required")
		return nil, status.Error(codes.InvalidArgument, "volume ID and staging path are required")
	}
	volumeLock, err := d.nodeState.LockVolume(ctx, req.GetVolumeId())
	if err != nil {
		d.log().Error("NodeUnstageVolume(): context ended while waiting for volume lock",
			zap.String("volume_id", req.GetVolumeId()), zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	defer volumeLock.Unlock()
	if err := blockmount.UnmountVolume(req.GetStagingTargetPath()); err != nil {
		d.log().Error("NodeUnstageVolume(): unmount failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "unmount staging path: %v", err)
	}
	state, err := helper.LoadFCState(d.fcStateDir, req.GetVolumeId())
	if err != nil && !os.IsNotExist(err) {
		d.log().Error("NodeUnstageVolume(): FC state lookup failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "load FC volume state: %v", err)
	}
	if err == nil {
		connector := newFCConnector()
		var disconnectErr error
		if state.MapName != "" && len(state.PathNames) != 0 {
			disconnectErr = connector.DisconnectDevice(state.MapName, state.PathNames)
		} else {
			disconnectErr = connector.Disconnect(state.SCSIID)
		}
		if disconnectErr != nil {
			d.log().Error("NodeUnstageVolume(): FC disconnect failed", zap.Error(disconnectErr))
			return nil, status.Errorf(codes.Internal, "disconnect FC device: %v", disconnectErr)
		}
		if err := helper.DeleteFCState(d.fcStateDir, req.GetVolumeId()); err != nil {
			d.log().Error("NodeUnstageVolume(): FC state removal failed", zap.Error(err))
			return nil, status.Errorf(codes.Internal, "remove FC volume state: %v", err)
		}
	}
	d.log().Info("NodeUnstageVolume(): volume unstaged", zap.String("staging_path", req.GetStagingTargetPath()))
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodePublishVolume bind-mounts the staged volume to the target path.
func (d *Driver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	d.log().Info("NodePublishVolume(): init")
	d.log().Debug("NodePublishVolume(): request",
		zap.String("volume_id", req.GetVolumeId()),
		zap.String("staging_target_path", req.GetStagingTargetPath()),
		zap.String("target_path", req.GetTargetPath()),
		zap.Bool("readonly", req.GetReadonly()),
	)

	if req.GetTargetPath() == "" || req.GetStagingTargetPath() == "" || req.GetVolumeId() == "" {
		d.log().Error("NodePublishVolume(): volume ID, staging, and target paths are required")
		return nil, status.Error(codes.InvalidArgument, "volume ID, staging, and target paths are required")
	}
	volumeLock, err := d.nodeState.LockVolume(ctx, req.GetVolumeId())
	if err != nil {
		d.log().Error("NodePublishVolume(): context ended while waiting for volume lock",
			zap.String("volume_id", req.GetVolumeId()), zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	defer volumeLock.Unlock()

	if utils.IsBlockVolume(req.GetVolumeCapability()) {
		if err := blockmount.PrepareBlockTarget(req.GetTargetPath()); err != nil {
			d.log().Error("NodePublishVolume(): block target creation failed", zap.Error(err))
			return nil, status.Errorf(codes.Internal, "create block target: %v", err)
		}
	} else if err := os.MkdirAll(req.GetTargetPath(), 0o750); err != nil {
		d.log().Error("NodePublishVolume(): target dir creation failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "create target dir: %v", err)
	}

	// Reader-only access modes are definitionally read-only and their staging
	// mount is ro: the CO readonly flag is not authoritative for them.
	publishReadOnly := req.GetReadonly() || utils.ReaderOnlyCapability(req.GetVolumeCapability())

	// CSI contract: NodePublishVolume is idempotent. The kubelet retries
	// publish on a target that is already bind-mounted; returning an error
	// would loop forever on a volume that is already usable. Idempotency
	// requires compatible state, not merely an existing mount: the target
	// must expose the staged volume with a compatible read-only state, and
	// a first publish must never bind an unmounted staging directory (an
	// empty-dir fake success after lost staging).
	if d.mounter != nil {
		matched, err := blockmount.PublishTargetMatchesStaging(d.mounter, req.GetTargetPath(), req.GetStagingTargetPath(),
			utils.IsBlockVolume(req.GetVolumeCapability()), publishReadOnly)
		if err != nil {
			d.log().Error("NodePublishVolume(): published target verification failed", zap.Error(err))
			return nil, status.Errorf(codes.Internal, "verify published target: %v", err)
		}
		if matched {
			d.log().Info("NodePublishVolume(): target already published (identity verified)",
				zap.String("target_path", req.GetTargetPath()))
			return &csi.NodePublishVolumeResponse{}, nil
		}
		if err := blockmount.StagingReadyForPublish(d.mounter, req.GetStagingTargetPath(), utils.IsBlockVolume(req.GetVolumeCapability())); err != nil {
			d.log().Error("NodePublishVolume(): staging not ready for publish", zap.Error(err))
			return nil, status.Errorf(codes.Internal, "staging not ready for publish: %v", err)
		}
	}

	// Bind-mount staging → target. A bind of an ro superblock is ro-effective
	// either way and chmod on it can never succeed, so reader-only publishes
	// are enforced ro with no permission rewrite (fresh filesystems received
	// permissions at format time).
	mountFlags := []string{"bind"}
	if publishReadOnly {
		mountFlags = append(mountFlags, "ro")
	}
	if err := d.bindMount(req.GetStagingTargetPath(), req.GetTargetPath(), "", mountFlags); err != nil {
		d.log().Error("NodePublishVolume(): bind mount failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "bind mount volume: %v", err)
	}
	// A readonly bind mount roots a read-only superblock; chmod on the mount
	// root fails with EROFS. Permissions were already applied to the staging
	// mount in NodeStageVolume, so skip them for readonly publishes.
	if !publishReadOnly {
		if err := d.chmodForTest(req.GetTargetPath(), utils.RequestedMountPermissions(req.GetPublishContext())); err != nil {
			return nil, status.Errorf(codes.Internal, "set published volume permissions: %v", err)
		}
	} else if !req.GetReadonly() {
		d.log().Info("NodePublishVolume(): enforced read-only publish for reader-only access mode",
			zap.String("target_path", req.GetTargetPath()))
	}
	d.log().Info("NodePublishVolume(): volume published", zap.String("target_path", req.GetTargetPath()))
	return &csi.NodePublishVolumeResponse{}, nil
}

func (d *Driver) chmodForTest(path string, mode os.FileMode) error {
	if d.nodeChmod != nil {
		return d.nodeChmod(path, mode)
	}
	return os.Chmod(path, mode)
}

// NodeUnpublishVolume unmounts the target path.
func (d *Driver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	d.log().Info("NodeUnpublishVolume(): init")
	d.log().Debug("NodeUnpublishVolume(): request",
		zap.String("volume_id", req.GetVolumeId()),
		zap.String("target_path", req.GetTargetPath()),
	)

	if req.GetTargetPath() == "" || req.GetVolumeId() == "" {
		d.log().Error("NodeUnpublishVolume(): volume ID and target path are required")
		return nil, status.Error(codes.InvalidArgument, "volume ID and target path are required")
	}
	volumeLock, err := d.nodeState.LockVolume(ctx, req.GetVolumeId())
	if err != nil {
		d.log().Error("NodeUnpublishVolume(): context ended while waiting for volume lock",
			zap.String("volume_id", req.GetVolumeId()), zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	defer volumeLock.Unlock()
	if err := blockmount.UnmountVolume(req.GetTargetPath()); err != nil {
		d.log().Error("NodeUnpublishVolume(): unmount failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "unmount target path: %v", err)
	}
	// CSI requires the SP to delete the file or directory it created at the
	// target path. The unmount above left an empty directory (or block file).
	if err := os.Remove(req.GetTargetPath()); err != nil && !os.IsNotExist(err) {
		d.log().Error("NodeUnpublishVolume(): target path removal failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "remove target path: %v", err)
	}
	d.log().Info("NodeUnpublishVolume(): volume unpublished", zap.String("target_path", req.GetTargetPath()))
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeExpandVolume resizes the filesystem after a controller-side LUN expand.
func (d *Driver) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	d.log().Info("NodeExpandVolume(): init")
	d.log().Debug("NodeExpandVolume(): request",
		zap.String("volume_id", req.GetVolumeId()),
		zap.String("volume_path", req.GetVolumePath()),
	)

	if req.GetVolumeId() == "" || req.GetVolumePath() == "" {
		d.log().Error("NodeExpandVolume(): volume ID and volume path are required")
		return nil, status.Error(codes.InvalidArgument, "volume ID and volume path are required")
	}
	volumeLock, err := d.nodeState.LockVolume(ctx, req.GetVolumeId())
	if err != nil {
		d.log().Error("NodeExpandVolume(): context ended while waiting for volume lock",
			zap.String("volume_id", req.GetVolumeId()), zap.Error(err))
		return nil, status.FromContextError(err).Err()
	}
	defer volumeLock.Unlock()
	// A volume that was never staged on this node does not exist here: report
	// NotFound before validating the resize request or touching the fabric.
	if _, err := helper.LoadFCState(d.fcStateDir, req.GetVolumeId()); err != nil {
		if os.IsNotExist(err) {
			d.log().Error("NodeExpandVolume(): volume was not found",
				zap.String("volume_id", req.GetVolumeId()))
			return nil, status.Errorf(codes.NotFound, "volume %s was not found", req.GetVolumeId())
		}
		d.log().Error("NodeExpandVolume(): FC state lookup failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "load FC volume state: %v", err)
	}
	if req.GetCapacityRange() == nil || req.GetCapacityRange().GetRequiredBytes() <= 0 {
		d.log().Error("NodeExpandVolume(): required capacity must be positive")
		return nil, status.Error(codes.InvalidArgument, "required capacity must be positive")
	}
	// A block-mode volume path is the empty regular file kubelet bind-mounts
	// to the device in the kubelet's mount namespace; the driver container
	// only sees the unbound file, so no device can be resolved from it. The
	// array propagates the new LUN capacity to existing fabric paths
	// asynchronously, so a single rescan refreshes the kernel paths and the
	// kubelet bind mount then exposes the new size automatically. The fs
	// path below verifies the size; for block there is nothing to verify
	// here so we return success after one rescan.
	if info, err := os.Stat(req.GetVolumePath()); err == nil && !info.IsDir() {
		if state, serr := helper.LoadFCState(d.fcStateDir, req.GetVolumeId()); serr == nil && state.SCSIID != "" {
			connector := newFCConnector()
			if devicePath, derr := connector.WaitForDevice(ctx, state.SCSIID); derr == nil {
				if rerr := connector.RescanDevicePaths(devicePath); rerr != nil {
					d.log().Warn("NodeExpandVolume(): FC path rescan failed", zap.Error(rerr))
				}
				if m, merr := connector.MultipathName(devicePath); merr == nil {
					if rerr := fc.NewMultipath().ReloadDevice(m); rerr != nil {
						d.log().Warn("NodeExpandVolume(): multipath reload failed", zap.Error(rerr))
					}
				}
			} else {
				d.log().Warn("NodeExpandVolume(): block device not resolved for rescan", zap.Error(derr))
			}
		} else {
			d.log().Warn("NodeExpandVolume(): no FC stage state for block volume",
				zap.String("volume_id", req.GetVolumeId()), zap.Error(serr))
		}
		d.log().Info("NodeExpandVolume(): block volume expanded",
			zap.String("volume_path", req.GetVolumePath()))
		return &csi.NodeExpandVolumeResponse{}, nil
	}

	devicePath, _, err := mount.GetDeviceNameFromMount(d.mounter, req.GetVolumePath())
	if err != nil {
		d.log().Error("NodeExpandVolume(): device resolution failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "resolve device: %v", err)
	}
	volumePath := devicePath
	connector := newFCConnector()
	if err := connector.RescanDevicePaths(volumePath); err != nil {
		d.log().Error("NodeExpandVolume(): FC path rescan failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "rescan FC device paths: %v", err)
	}
	// The path rescan grows the kernel devices, but the existing multipath
	// map keeps the old size until it is reloaded; the size wait below then
	// sees the stale size forever without this. ReloadDevice uses one-shot
	// `multipath -r` and works when multipathd is not running. The wait is
	// the authoritative gate, so a reload failure only demotes to a warning.
	if strings.HasPrefix(volumePath, "/dev/mapper/") {
		if err := fc.NewMultipath().ReloadDevice(volumePath); err != nil {
			d.log().Warn("NodeExpandVolume(): multipath map reload failed",
				zap.String("device_path", volumePath), zap.Error(err))
		}
	}
	if err := connector.WaitForDeviceSize(ctx, volumePath, req.GetCapacityRange().GetRequiredBytes()); err != nil {
		d.log().Error("NodeExpandVolume(): expanded device size not visible", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "wait for expanded device: %v", err)
	}
	if err := d.filesystemStager().ResizeCoordinated(ctx, req.GetVolumeId(), volumePath, req.GetVolumePath()); err != nil {
		d.log().Error("NodeExpandVolume(): filesystem resize failed", zap.Error(err))
		if status.Code(err) != codes.Unknown {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "resize filesystem: %v", err)
	}
	d.log().Info("NodeExpandVolume(): volume expanded",
		zap.String("device_path", volumePath),
		zap.String("volume_path", req.GetVolumePath()),
	)
	return &csi.NodeExpandVolumeResponse{}, nil
}

// NodeGetVolumeStats returns filesystem usage stats for a mounted volume.
func (d *Driver) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	d.log().Info("NodeGetVolumeStats(): init")
	d.log().Debug("NodeGetVolumeStats(): request",
		zap.String("volume_id", req.GetVolumeId()),
		zap.String("volume_path", req.GetVolumePath()),
	)

	if req.GetVolumeId() == "" || req.GetVolumePath() == "" {
		d.log().Error("NodeGetVolumeStats(): volume ID and path are required")
		return nil, status.Error(codes.InvalidArgument, "volume ID and path are required")
	}
	if d.mounter == nil {
		d.log().Error("NodeGetVolumeStats(): mounter unavailable")
		return nil, status.Error(codes.Internal, "mounter unavailable")
	}
	// Require an active mount so an ordinary host directory is never reported
	// as storage capacity for the requested CSI volume.
	notMountPoint, err := d.mounter.IsLikelyNotMountPoint(req.GetVolumePath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, unix.ENOTDIR) {
			d.log().Error("NodeGetVolumeStats(): volume path does not exist",
				zap.String("volume_path", req.GetVolumePath()), zap.Error(err))
			return nil, status.Errorf(codes.NotFound, "volume path is not mounted: %s", req.GetVolumePath())
		}
		d.log().Error("NodeGetVolumeStats(): mount point inspection failed",
			zap.String("volume_path", req.GetVolumePath()), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "checking mount point: %v", err)
	}
	if notMountPoint {
		d.log().Error("NodeGetVolumeStats(): volume path is not mounted",
			zap.String("volume_path", req.GetVolumePath()))
		return nil, status.Errorf(codes.NotFound, "volume path is not mounted: %s", req.GetVolumePath())
	}
	var stat unix.Statfs_t
	if err := unix.Statfs(req.GetVolumePath(), &stat); err != nil {
		if errors.Is(err, unix.ENOENT) {
			d.log().Error("NodeGetVolumeStats(): volume does not exist at path",
				zap.String("volume_path", req.GetVolumePath()))
			return nil, status.Errorf(codes.NotFound, "volume does not exist at path %q", req.GetVolumePath())
		}
		d.log().Error("NodeGetVolumeStats(): statfs failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "get volume stats: %v", err)
	}
	totalBytes := int64(stat.Blocks) * int64(stat.Bsize)
	availableBytes := int64(stat.Bavail) * int64(stat.Bsize)
	usedBytes := (int64(stat.Blocks) - int64(stat.Bfree)) * int64(stat.Bsize)
	d.log().Info("NodeGetVolumeStats(): stats returned",
		zap.Int64("available_bytes", availableBytes),
		zap.Int64("total_bytes", totalBytes),
		zap.Int64("used_bytes", usedBytes),
	)
	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{{
			Available: availableBytes,
			Total:     totalBytes,
			Used:      usedBytes,
			Unit:      csi.VolumeUsage_BYTES,
		}},
	}, nil
}

// Filesystem operations (relocated from filesystem.go).

const coordinationReleaseTimeout = 5 * time.Second

// Node-side resize (relocated from resize.go).

// Staging mount identity (relocated from staging_mount.go).
