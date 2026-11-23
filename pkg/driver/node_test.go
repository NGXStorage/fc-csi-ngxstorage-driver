package driver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	blockmount "github.com/NGXStorage/csi-ngxstorage-utils/blockmount"
	coord "github.com/NGXStorage/csi-ngxstorage-utils/coord"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	"k8s.io/mount-utils"
)

func TestNodeMutationRPCsSerializePerVolume(t *testing.T) {
	const volumeID = "fc.csi.ngxstorage.com.lock-test"
	d := &Driver{}
	held, err := d.nodeState.LockVolume(context.Background(), volumeID)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Unlock()

	tests := []struct {
		name string
		call func(context.Context) error
	}{
		{
			name: "stage",
			call: func(ctx context.Context) error {
				_, err := d.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
					VolumeId: volumeID, StagingTargetPath: "/unused/stage",
				})
				return err
			},
		},
		{
			name: "unstage",
			call: func(ctx context.Context) error {
				_, err := d.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
					VolumeId: volumeID, StagingTargetPath: "/unused/stage",
				})
				return err
			},
		},
		{
			name: "publish",
			call: func(ctx context.Context) error {
				_, err := d.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
					VolumeId: volumeID, StagingTargetPath: "/unused/stage", TargetPath: "/unused/target",
				})
				return err
			},
		},
		{
			name: "unpublish",
			call: func(ctx context.Context) error {
				_, err := d.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
					VolumeId: volumeID, TargetPath: "/unused/target",
				})
				return err
			},
		},
		{
			name: "expand",
			call: func(ctx context.Context) error {
				_, err := d.NodeExpandVolume(ctx, &csi.NodeExpandVolumeRequest{
					VolumeId: volumeID, VolumePath: "/unused/volume",
					CapacityRange: &csi.CapacityRange{RequiredBytes: 1},
				})
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			if err := test.call(ctx); status.Code(err) != codes.DeadlineExceeded {
				t.Fatalf("status = %s, want DeadlineExceeded; err=%v", status.Code(err), err)
			}
		})
	}
}

func TestNodeGetVolumeStatsValidatesRequiredFieldsBeforeStat(t *testing.T) {
	d := &Driver{}
	tests := []struct {
		name string
		req  *csi.NodeGetVolumeStatsRequest
	}{
		{
			name: "missing volume ID",
			req:  &csi.NodeGetVolumeStatsRequest{VolumePath: "/path/that/does/not/exist"},
		},
		{
			name: "missing volume path",
			req:  &csi.NodeGetVolumeStatsRequest{VolumeId: "volume-1"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := d.NodeGetVolumeStats(context.Background(), test.req)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("status code = %s, want %s; err=%v", status.Code(err), codes.InvalidArgument, err)
			}
		})
	}
}

func TestAdvertisedCapabilitiesHaveImplementedHealthSurface(t *testing.T) {
	for _, capability := range supportedNodeCapabilities {
		if capability == csi.NodeServiceCapability_RPC_GET_VOLUME_HEALTH || capability == csi.NodeServiceCapability_RPC_GET_STORAGE_HEALTH {
			t.Fatalf("FC advertises unimplemented node health RPC: %s", capability)
		}
	}
	for _, capability := range supportedControllerCapabilities {
		if capability == csi.ControllerServiceCapability_RPC_GET_VOLUME_HEALTH || capability == csi.ControllerServiceCapability_RPC_LIST_VOLUME_HEALTH {
			t.Fatalf("FC advertises unimplemented controller health RPC: %s", capability)
		}
	}
}

func TestNodePublishVolumeIdempotentWhenAlreadyBound(t *testing.T) {
	dir := t.TempDir()
	staging := filepath.Join(dir, "staging")
	target := filepath.Join(dir, "target")
	for _, path := range []string{staging, target} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// Fixture models the real staged+published state: staging is mounted
	// from the volume device and the publish bind reports that same device
	// (FakeMounter resolves bind sources to the original device).
	mounter := mount.NewFakeMounter([]mount.MountPoint{
		{Device: "/dev/dm-test", Path: staging},
		{Device: "/dev/dm-test", Path: target, Opts: []string{"bind"}},
	})
	d := &Driver{mounter: mounter}
	request := &csi.NodePublishVolumeRequest{
		VolumeId: "fc.csi.ngxstorage.com.0001", StagingTargetPath: staging,
		TargetPath: target, VolumeCapability: &csi.VolumeCapability{}, PublishContext: map[string]string{},
	}
	if _, err := d.NodePublishVolume(context.Background(), request); err != nil {
		t.Fatalf("repeat NodePublishVolume on a bound target: got %v, want idempotent success", err)
	}
}

func TestNodePublishVolumeReadonlyDoesNotFailOnEROFS(t *testing.T) {
	dir := t.TempDir()
	staging := filepath.Join(dir, "staging")
	target := filepath.Join(dir, "target")
	if err := os.MkdirAll(staging, 0o755); err != nil {
		t.Fatal(err)
	}
	// Fixture models production: the staging mount always exists before a
	// first publish bind (NodeStageVolume ran successfully).
	mounter := mount.NewFakeMounter([]mount.MountPoint{{Device: "/dev/dm-test", Path: staging}})
	d := &Driver{mounter: mounter, bindMount: mounter.Mount}
	chmod := os.Chmod
	d.nodeChmod = func(name string, mode os.FileMode) error {
		points, _ := mounter.List()
		for _, point := range points {
			if point.Path == name {
				return &os.PathError{Op: "chmod", Path: name, Err: syscall.EROFS}
			}
		}
		return chmod(name, mode)
	}
	request := &csi.NodePublishVolumeRequest{
		VolumeId: "fc.csi.ngxstorage.com.0002", StagingTargetPath: staging, TargetPath: target,
		Readonly: true, VolumeCapability: &csi.VolumeCapability{}, PublishContext: map[string]string{},
	}
	if _, err := d.NodePublishVolume(context.Background(), request); err != nil {
		t.Fatalf("readonly publish must survive EROFS on the mounted root: %v", err)
	}
	if len(mounter.GetLog()) < 1 {
		t.Fatal("no mount action recorded")
	}
}

// Regression: the CO may send
// readonly=false for a reader-only access mode, while the staging mount
// is ro. The publish must then enforce ro with no chmod attempt (chmod on an
// ro superblock fails EROFS and fails the whole kubelet SetUp).
func TestNodePublishVolumeReaderOnlyModeEnforcesReadOnly(t *testing.T) {
	dir := t.TempDir()
	staging := filepath.Join(dir, "staging")
	target := filepath.Join(dir, "target")
	if err := os.MkdirAll(staging, 0o755); err != nil {
		t.Fatal(err)
	}
	// Fixture models production: the staging mount always exists before a
	// first publish bind (NodeStageVolume ran successfully).
	mounter := mount.NewFakeMounter([]mount.MountPoint{{Device: "/dev/dm-test", Path: staging}})
	var gotFlags []string
	bind := func(source, target, fstype string, options []string) error {
		gotFlags = append([]string(nil), options...)
		return mounter.Mount(source, target, fstype, options)
	}
	d := &Driver{mounter: mounter, bindMount: bind}
	chmodCalls := 0
	d.nodeChmod = func(name string, mode os.FileMode) error {
		chmodCalls++
		return &os.PathError{Op: "chmod", Path: name, Err: syscall.EROFS}
	}
	rox := &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY},
	}
	request := &csi.NodePublishVolumeRequest{
		VolumeId: "fc.csi.ngxstorage.com.0003", StagingTargetPath: staging, TargetPath: target,
		Readonly: false, VolumeCapability: rox, PublishContext: map[string]string{},
	}
	if _, err := d.NodePublishVolume(context.Background(), request); err != nil {
		t.Fatalf("reader-only publish with CO readonly=false must succeed without chmod: %v", err)
	}
	if chmodCalls != 0 {
		t.Fatalf("chmod called %d times for reader-only publish, want 0", chmodCalls)
	}
	hasRO := false
	for _, flag := range gotFlags {
		if flag == "ro" {
			hasRO = true
		}
	}
	if !hasRO {
		t.Fatalf("reader-only bind flags %v miss ro", gotFlags)
	}
}

func TestNodeUnpublishVolumeRemovesTargetPath(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatal(err)
	}
	d := &Driver{}
	if _, err := d.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
		VolumeId: "fc.csi.ngxstorage.com.0004", TargetPath: target,
	}); err != nil {
		t.Fatalf("NodeUnpublishVolume: %v", err)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("target path %q was not removed (stat err=%v)", target, err)
	}
}

func TestNodeGetVolumeStatsMissingPathReturnsNotFound(t *testing.T) {
	// Fixture models production: the node driver always has a mounter; the
	// absent path is reported by the mount-point inspection, not by a nil
	// mounter (which is now an Internal misconfiguration).
	d := &Driver{mounter: mount.NewFakeMounter(nil)}
	_, err := d.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
		VolumeId:   "fc.csi.ngxstorage.com.0005",
		VolumePath: filepath.Join(t.TempDir(), "not-mounted"),
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("status code = %s, want %s; err=%v", status.Code(err), codes.NotFound, err)
	}
}

func TestNodeExpandVolumeUnknownVolumeReturnsNotFound(t *testing.T) {
	d := &Driver{fcStateDir: t.TempDir()}
	_, err := d.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
		VolumeId:   "fc.csi.ngxstorage.com.0006",
		VolumePath: "/some/path",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("status code = %s, want %s; err=%v", status.Code(err), codes.NotFound, err)
	}
}

// The controller keeps no per-node publish state, so a read-only flip cannot
// be told apart from an idempotent repeat. Advertising PUBLISH_READONLY would
// promise controller-side read-only enforcement the driver does not do
// (csi-sanity "already published but incompatible"); read-only is enforced by
// the node plugin's readonly bind mounts.
func TestPublishReadonlyCapabilityNotAdvertised(t *testing.T) {
	for _, capability := range supportedControllerCapabilities {
		if capability == csi.ControllerServiceCapability_RPC_PUBLISH_READONLY {
			t.Fatal("PUBLISH_READONLY must not be advertised without per-node publish state")
		}
	}
}

// Filesystem operation tests (relocated from filesystem_test.go).

type fakeFilesystemOps struct {
	mu          sync.Mutex
	state       blockmount.FilesystemProbe
	probeErr    error
	formatCount int
	repairCount int
	chmodCount  int
	mounts      []fakeFilesystemMount
	unmounts    int
}

type fakeFilesystemMount struct {
	readOnly bool
	target   string
}

func (f *fakeFilesystemOps) Probe(string) (blockmount.FilesystemProbe, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.state, f.probeErr
}

func (f *fakeFilesystemOps) Format(_ string, fsType string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.formatCount++
	f.state = blockmount.FilesystemProbe{State: blockmount.FilesystemExisting, FSType: fsType}
	return nil
}

func (f *fakeFilesystemOps) Verify(_ string, fsType string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state.State != blockmount.FilesystemExisting || f.state.FSType != fsType {
		return errors.New("filesystem verification failed")
	}
	return nil
}

func (f *fakeFilesystemOps) Repair(string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.repairCount++
	return nil
}

func (f *fakeFilesystemOps) Mount(_ string, target, _ string, readOnly bool, _ []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mounts = append(f.mounts, fakeFilesystemMount{readOnly: readOnly, target: target})
	return nil
}

func (f *fakeFilesystemOps) Unmount(string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unmounts++
	return nil
}

func (f *fakeFilesystemOps) Chmod(string, os.FileMode) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.chmodCount++
	return nil
}

type fakeCoordinator struct {
	gate         chan struct{}
	attempts     chan string
	acquireCount atomic.Int32
	owned        atomic.Bool
	preflightErr error
	recordMu     sync.Mutex
	acquireKeys  []string
	multiKeys    [][]string
}

func newFakeCoordinator() *fakeCoordinator {
	return &fakeCoordinator{gate: make(chan struct{}, 1), attempts: make(chan string, 64)}
}

func (f *fakeCoordinator) Acquire(ctx context.Context, key string, _ coord.AcquireOptions) (coord.Lock, error) {
	f.recordMu.Lock()
	f.acquireKeys = append(f.acquireKeys, key)
	f.recordMu.Unlock()
	select {
	case f.attempts <- key:
	default:
	}
	select {
	case f.gate <- struct{}{}:
		f.acquireCount.Add(1)
		f.owned.Store(true)
		return &fakeLock{coordinator: f, lost: make(chan struct{})}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *fakeCoordinator) AcquireMany(ctx context.Context, keys []string, _ coord.AcquireOptions) (coord.LockSet, error) {
	f.recordMu.Lock()
	f.multiKeys = append(f.multiKeys, append([]string(nil), keys...))
	f.recordMu.Unlock()
	return f.Acquire(ctx, "multi", coord.AcquireOptions{})
}

func (f *fakeCoordinator) Preflight(context.Context) error { return f.preflightErr }

type fakeLock struct {
	coordinator *fakeCoordinator
	lost        chan struct{}
	once        sync.Once
}

func (f *fakeLock) AssertOwned(context.Context) error {
	if !f.coordinator.owned.Load() {
		return errors.New("lock ownership lost")
	}
	return nil
}

func (f *fakeLock) OwnershipLost() <-chan struct{} { return f.lost }

func (f *fakeLock) Release(context.Context) error {
	f.once.Do(func() {
		f.coordinator.owned.Store(false)
		<-f.coordinator.gate
	})
	return nil
}

func readerOnlyCapabilityForTest() *csi.VolumeCapability {
	return &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
		Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
	}}
}

func writerCapabilityForTest() *csi.VolumeCapability {
	return &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
		Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
	}}
}

func TestStageFilesystemConcurrentReadersFormatExactlyOnce(t *testing.T) {
	ops := &fakeFilesystemOps{state: blockmount.FilesystemProbe{State: blockmount.FilesystemEmpty}}
	coord := newFakeCoordinator()
	d := &Driver{filesystemOps: ops, coordination: coord}

	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, target := range []string{"/stage-a", "/stage-b"} {
		target := target
		go func() {
			<-start
			errs <- d.filesystemStager().Stage(context.Background(), "vol-1", "/dev/dm-test", target, "ext4",
				readerOnlyCapabilityForTest(), 0o770,
				func(context.Context) (string, error) { return "/dev/dm-test", nil })
		}()
	}
	close(start)
	for i := 0; i < 2; i++ {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatalf("stageFilesystem contender %d: %v", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("concurrent reader staging deadlocked")
		}
	}

	ops.mu.Lock()
	defer ops.mu.Unlock()
	if ops.formatCount != 1 {
		t.Fatalf("format count = %d, want exactly 1", ops.formatCount)
	}
	if ops.chmodCount != 1 || ops.unmounts != 1 {
		t.Fatalf("fresh readonly init chmod/unmount = %d/%d, want 1/1", ops.chmodCount, ops.unmounts)
	}
	var readonlyMounts, readwriteMounts int
	for _, mount := range ops.mounts {
		if mount.readOnly {
			readonlyMounts++
		} else {
			readwriteMounts++
		}
	}
	if readonlyMounts != 2 || readwriteMounts != 1 {
		t.Fatalf("mount counts ro/rw = %d/%d, want 2/1", readonlyMounts, readwriteMounts)
	}
	if got := coord.acquireCount.Load(); got != 2 {
		t.Fatalf("coordination acquire count = %d, want both readers to enter critical section", got)
	}
}

func TestStageFilesystemProbeErrorNeverFormats(t *testing.T) {
	ops := &fakeFilesystemOps{
		state:    blockmount.FilesystemProbe{State: blockmount.FilesystemUnknown},
		probeErr: errors.New("blkid I/O error"),
	}
	coord := newFakeCoordinator()
	d := &Driver{filesystemOps: ops, coordination: coord}
	err := d.filesystemStager().Stage(context.Background(), "vol-1", "/dev/dm-test", "/stage", "ext4",
		writerCapabilityForTest(), 0o770, nil)
	if err == nil {
		t.Fatal("probe error should fail staging")
	}
	if ops.formatCount != 0 {
		t.Fatalf("probe error triggered %d formats", ops.formatCount)
	}
	if got := coord.acquireCount.Load(); got != 0 {
		t.Fatalf("probe error acquired coordination lock %d times", got)
	}
}

func TestStageFilesystemExistingReaderSkipsRepairAndChmod(t *testing.T) {
	ops := &fakeFilesystemOps{state: blockmount.FilesystemProbe{State: blockmount.FilesystemExisting, FSType: "ext4"}}
	coord := newFakeCoordinator()
	d := &Driver{filesystemOps: ops, coordination: coord}
	if err := d.filesystemStager().Stage(context.Background(), "vol-1", "/dev/dm-test", "/stage", "ext4",
		readerOnlyCapabilityForTest(), 0o770, nil); err != nil {
		t.Fatal(err)
	}
	ops.mu.Lock()
	defer ops.mu.Unlock()
	if ops.formatCount != 0 || ops.repairCount != 0 || ops.chmodCount != 0 {
		t.Fatalf("existing reader mutated filesystem: format/repair/chmod=%d/%d/%d",
			ops.formatCount, ops.repairCount, ops.chmodCount)
	}
	if len(ops.mounts) != 1 || !ops.mounts[0].readOnly {
		t.Fatalf("existing reader staging mounts = %+v, want one readonly mount", ops.mounts)
	}
	if got := coord.acquireCount.Load(); got != 1 {
		t.Fatalf("existing reader acquire count = %d, want 1", got)
	}
}

func TestStageFilesystemExistingWriterRepairsWithoutDistributedLock(t *testing.T) {
	ops := &fakeFilesystemOps{state: blockmount.FilesystemProbe{State: blockmount.FilesystemExisting, FSType: "ext4"}}
	coord := newFakeCoordinator()
	d := &Driver{filesystemOps: ops, coordination: coord}
	if err := d.filesystemStager().Stage(context.Background(), "vol-1", "/dev/dm-test", "/stage", "ext4",
		writerCapabilityForTest(), 0o770, nil); err != nil {
		t.Fatal(err)
	}
	ops.mu.Lock()
	defer ops.mu.Unlock()
	if ops.formatCount != 0 || ops.repairCount != 1 || ops.chmodCount != 1 {
		t.Fatalf("existing writer operations format/repair/chmod=%d/%d/%d, want 0/1/1",
			ops.formatCount, ops.repairCount, ops.chmodCount)
	}
	if len(ops.mounts) != 1 || ops.mounts[0].readOnly {
		t.Fatalf("existing writer staging mounts = %+v, want one rw mount", ops.mounts)
	}
	if got := coord.acquireCount.Load(); got != 0 {
		t.Fatalf("existing writer unnecessarily acquired distributed lock %d times", got)
	}
}

// A repeat NodeStageVolume for a volume already staged at that path must
// succeed without mounting again (the second mount fails EBUSY) — and only
// when the existing mount comes from the same resolved device.
func TestStageFilesystemIdempotentWhenAlreadyMounted(t *testing.T) {
	staging := t.TempDir()
	original := blockmount.Mounter
	blockmount.Mounter = mount.NewFakeMounter([]mount.MountPoint{{Device: "/dev/dm-test", Path: staging}})
	t.Cleanup(func() { blockmount.Mounter = original })

	ops := &fakeFilesystemOps{state: blockmount.FilesystemProbe{State: blockmount.FilesystemExisting, FSType: "ext4"}}
	d := &Driver{filesystemOps: ops}
	if err := d.filesystemStager().Stage(context.Background(), "vol-1", "/dev/dm-test", staging, "ext4", nil, 0o777, nil); err != nil {
		t.Fatalf("idempotent stage: %v", err)
	}
	if len(ops.mounts) != 0 {
		t.Fatalf("mount called %d times, want 0 for an already-staged path", len(ops.mounts))
	}
}

// An existing mount from a different device must fail closed: accepting it
// would report a stale/wrong mount as a successful stage.
func TestStageFilesystemWrongDeviceFailsClosed(t *testing.T) {
	staging := t.TempDir()
	original := blockmount.Mounter
	blockmount.Mounter = mount.NewFakeMounter([]mount.MountPoint{{Device: "/dev/dm-other", Path: staging}})
	t.Cleanup(func() { blockmount.Mounter = original })

	ops := &fakeFilesystemOps{state: blockmount.FilesystemProbe{State: blockmount.FilesystemExisting, FSType: "ext4"}}
	d := &Driver{filesystemOps: ops}
	err := d.filesystemStager().Stage(context.Background(), "vol-1", "/dev/dm-test", staging, "ext4", nil, 0o777, nil)
	if err == nil {
		t.Fatal("stageFilesystem succeeded on a mount from a different device")
	}
	if !strings.Contains(err.Error(), "expected /dev/dm-test") {
		t.Fatalf("err = %v, want the expected-device mismatch", err)
	}
	if len(ops.mounts) != 0 {
		t.Fatalf("mount called %d times, want 0 on a fail-closed verification", len(ops.mounts))
	}
}

// A mount-point inspection error is not ignored.
func TestStageFilesystemMountInspectionFailureFailsClosed(t *testing.T) {
	staging := t.TempDir()
	original := blockmount.Mounter
	blockmount.Mounter = inspectionErrorMounter{Interface: mount.NewFakeMounter(nil), err: errors.New("inspection unavailable")}
	t.Cleanup(func() { blockmount.Mounter = original })

	ops := &fakeFilesystemOps{state: blockmount.FilesystemProbe{State: blockmount.FilesystemExisting, FSType: "ext4"}}
	d := &Driver{filesystemOps: ops}
	if err := d.filesystemStager().Stage(context.Background(), "vol-1", "/dev/dm-test", staging, "ext4", nil, 0o777, nil); err == nil {
		t.Fatal("stageFilesystem succeeded although the staging mount could not be inspected")
	}
	if len(ops.mounts) != 0 {
		t.Fatalf("mount called %d times, want 0", len(ops.mounts))
	}
}

type inspectionErrorMounter struct {
	mount.Interface
	err error
}

func (m inspectionErrorMounter) IsLikelyNotMountPoint(string) (bool, error) {
	return false, m.err
}

// Raw-block staging binds the device node onto the staging file; Linux
// reports the containing filesystem (devtmpfs) as the mount source, so the
// verifier must compare device identity instead of source strings.
func TestBlockStagingNotMountedReturnsFalse(t *testing.T) {
	staging := filepath.Join(t.TempDir(), "block-staging")
	if err := os.WriteFile(staging, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	matches, err := blockmount.BlockStagingMatchesDevice(mount.NewFakeMounter(nil), staging, "/dev/dm-42")
	if err != nil {
		t.Fatalf("blockStagingMatchesDevice: %v", err)
	}
	if matches {
		t.Fatal("matches = true, want false for a not-mounted staging path")
	}
}

func TestBlockStagingSameRdevSucceeds(t *testing.T) {
	staging := filepath.Join(t.TempDir(), "block-staging")
	if err := os.WriteFile(staging, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	mounter := mount.NewFakeMounter([]mount.MountPoint{{Device: "devtmpfs", Path: staging}})
	swapStatFile(t, func(string) (os.FileInfo, error) {
		return fakeDeviceInfo{mode: blockDeviceMode, rdev: 42}, nil
	})

	matches, err := blockmount.BlockStagingMatchesDevice(mounter, staging, "/dev/dm-42")
	if err != nil {
		t.Fatalf("blockStagingMatchesDevice: %v", err)
	}
	if !matches {
		t.Fatal("matches = false, want true for the same rdev behind devtmpfs")
	}
}

func TestBlockStagingDifferentRdevFailsClosed(t *testing.T) {
	staging := filepath.Join(t.TempDir(), "block-staging")
	if err := os.WriteFile(staging, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	mounter := mount.NewFakeMounter([]mount.MountPoint{{Device: "devtmpfs", Path: staging}})
	swapStatFile(t, func(path string) (os.FileInfo, error) {
		if path == staging {
			return fakeDeviceInfo{mode: blockDeviceMode, rdev: 43}, nil
		}
		return fakeDeviceInfo{mode: blockDeviceMode, rdev: 42}, nil
	})

	if _, err := blockmount.BlockStagingMatchesDevice(mounter, staging, "/dev/dm-42"); err == nil {
		t.Fatal("no error for a staging device with a different rdev")
	}
}

func TestBlockStagingRegularFileFailsClosed(t *testing.T) {
	staging := filepath.Join(t.TempDir(), "block-staging")
	if err := os.WriteFile(staging, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	mounter := mount.NewFakeMounter([]mount.MountPoint{{Device: "devtmpfs", Path: staging}})
	swapStatFile(t, func(string) (os.FileInfo, error) {
		return fakeDeviceInfo{mode: regularFileMode}, nil
	})

	if _, err := blockmount.BlockStagingMatchesDevice(mounter, staging, "/dev/dm-42"); err == nil {
		t.Fatal("no error when the staging target is a regular file")
	}
}

func TestBlockStagingExpectedNotDeviceFailsClosed(t *testing.T) {
	staging := filepath.Join(t.TempDir(), "block-staging")
	if err := os.WriteFile(staging, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	mounter := mount.NewFakeMounter([]mount.MountPoint{{Device: "devtmpfs", Path: staging}})
	swapStatFile(t, func(path string) (os.FileInfo, error) {
		if path == staging {
			return fakeDeviceInfo{mode: blockDeviceMode, rdev: 42}, nil
		}
		return fakeDeviceInfo{mode: regularFileMode}, nil
	})

	if _, err := blockmount.BlockStagingMatchesDevice(mounter, staging, "/dev/dm-42"); err == nil {
		t.Fatal("no error when the expected path is not a device node")
	}
}

func TestBlockStagingInspectionErrorFailsClosed(t *testing.T) {
	staging := t.TempDir()
	mounter := inspectionErrorMounter{Interface: mount.NewFakeMounter(nil), err: errors.New("inspection unavailable")}
	if _, err := blockmount.BlockStagingMatchesDevice(mounter, staging, "/dev/dm-42"); err == nil {
		t.Fatal("no error on a mount-point inspection failure")
	}
}

func TestBlockStagingStatErrorFailsClosed(t *testing.T) {
	staging := filepath.Join(t.TempDir(), "block-staging")
	if err := os.WriteFile(staging, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	mounter := mount.NewFakeMounter([]mount.MountPoint{{Device: "devtmpfs", Path: staging}})
	swapStatFile(t, func(string) (os.FileInfo, error) {
		return nil, errors.New("stat unavailable")
	})

	if _, err := blockmount.BlockStagingMatchesDevice(mounter, staging, "/dev/dm-42"); err == nil {
		t.Fatal("no error on a stat failure")
	}
}

// Filesystem staging through a /dev/mapper alias of the same device is still
// recognised as the same mount.
func TestStageFilesystemIdempotentWithMapperAlias(t *testing.T) {
	dir := t.TempDir()
	device := filepath.Join(dir, "dm-42")
	if err := os.WriteFile(device, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(dir, "mapper-alias")
	if err := os.Symlink(device, alias); err != nil {
		t.Fatal(err)
	}
	staging := t.TempDir()
	original := blockmount.Mounter
	blockmount.Mounter = mount.NewFakeMounter([]mount.MountPoint{{Device: device, Path: staging}})
	t.Cleanup(func() { blockmount.Mounter = original })

	ops := &fakeFilesystemOps{state: blockmount.FilesystemProbe{State: blockmount.FilesystemExisting, FSType: "ext4"}}
	d := &Driver{filesystemOps: ops}
	if err := d.filesystemStager().Stage(context.Background(), "vol-1", alias, staging, "ext4", nil, 0o777, nil); err != nil {
		t.Fatalf("idempotent stage through a mapper alias: %v", err)
	}
	if len(ops.mounts) != 0 {
		t.Fatalf("mount called %d times, want 0 through an alias of the same device", len(ops.mounts))
	}
}

const (
	blockDeviceMode = os.ModeDevice | 0o660
	regularFileMode = os.FileMode(0o600)
)

// fakeDeviceInfo injects device metadata without real host device nodes.
type fakeDeviceInfo struct {
	mode os.FileMode
	rdev uint64
}

func (f fakeDeviceInfo) Name() string       { return "fake" }
func (f fakeDeviceInfo) Size() int64        { return 0 }
func (f fakeDeviceInfo) Mode() os.FileMode  { return f.mode }
func (f fakeDeviceInfo) ModTime() time.Time { return time.Time{} }
func (f fakeDeviceInfo) IsDir() bool        { return f.mode.IsDir() }
func (f fakeDeviceInfo) Sys() interface{}   { return &syscall.Stat_t{Rdev: f.rdev} }

func swapStatFile(t *testing.T, fn func(string) (os.FileInfo, error)) {
	t.Helper()
	original := blockmount.StatFile
	blockmount.StatFile = fn
	t.Cleanup(func() { blockmount.StatFile = original })
}

func TestSameBlockDeviceResolvesSymlinks(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "dm-0")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(dir, "mapper-alias")
	if err := os.Symlink(target, alias); err != nil {
		t.Fatal(err)
	}
	if !blockmount.SameBlockDevice(target, alias) {
		t.Fatal("sameBlockDevice = false, want true for a symlink alias")
	}
	if blockmount.SameBlockDevice(target, filepath.Join(dir, "dm-1")) {
		t.Fatal("sameBlockDevice = true for different devices")
	}
}

// Node-side resize tests (relocated from resize_test.go).

type fakeFilesystemResizer struct {
	mu          sync.Mutex
	need        bool
	resizeCount int
}

func (f *fakeFilesystemResizer) NeedResize(string, string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.need, nil
}

func (f *fakeFilesystemResizer) Resize(string, string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.need {
		return false, nil
	}
	f.resizeCount++
	f.need = false
	return true, nil
}

func TestResizeFilesystemCoordinatedConcurrentCallsResizeOnce(t *testing.T) {
	coord := newFakeCoordinator()
	resizer := &fakeFilesystemResizer{need: true}
	d := &Driver{coordination: coord, filesystemResizer: resizer}
	start := make(chan struct{})
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			errs <- d.filesystemStager().ResizeCoordinated(context.Background(), "vol-1", "/dev/dm-test", "/mnt/test")
		}()
	}
	close(start)
	for i := 0; i < 2; i++ {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("filesystem resize contenders deadlocked")
		}
	}
	resizer.mu.Lock()
	defer resizer.mu.Unlock()
	if resizer.resizeCount != 1 {
		t.Fatalf("resize count = %d, want exactly 1", resizer.resizeCount)
	}
}

// --- Regression: publish identity and stats mount guard ---

// An already-mounted target bound from a different volume is not an
// idempotent publish.
func TestNodePublishVolumeForeignTargetFailsClosed(t *testing.T) {
	dir := t.TempDir()
	staging := filepath.Join(dir, "staging")
	target := filepath.Join(dir, "target")
	for _, p := range []string{staging, target} {
		if err := os.MkdirAll(p, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	mounter := mount.NewFakeMounter([]mount.MountPoint{
		{Device: "/dev/dm-1", Path: staging},
		{Device: "/dev/dm-2", Path: target, Opts: []string{"bind"}},
	})
	d := &Driver{mounter: mounter}
	_, err := d.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId: "fc.csi.ngxstorage.com.pub-foreign", StagingTargetPath: staging, TargetPath: target,
		VolumeCapability: &csi.VolumeCapability{}, PublishContext: map[string]string{},
	})
	if err == nil {
		t.Fatal("publish succeeded on a target bound from a different volume")
	}
}

// A writable existing mount does not satisfy a read-only publish.
func TestNodePublishVolumeReadOnlyMismatchFailsClosed(t *testing.T) {
	dir := t.TempDir()
	staging := filepath.Join(dir, "staging")
	target := filepath.Join(dir, "target")
	for _, p := range []string{staging, target} {
		if err := os.MkdirAll(p, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	mounter := mount.NewFakeMounter([]mount.MountPoint{
		{Device: "/dev/dm-1", Path: staging, Opts: []string{"rw"}},
		{Device: "/dev/dm-1", Path: target, Opts: []string{"bind"}},
	})
	d := &Driver{mounter: mounter}
	_, err := d.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId: "fc.csi.ngxstorage.com.pub-ro", StagingTargetPath: staging, TargetPath: target,
		Readonly: true, VolumeCapability: &csi.VolumeCapability{}, PublishContext: map[string]string{},
	})
	if err == nil {
		t.Fatal("read-only publish succeeded on a writable existing mount")
	}
}

// A first publish must never bind an unmounted staging directory (an
// empty directory would be exposed as the volume).
func TestNodePublishVolumeStagingNotMountedFailsClosed(t *testing.T) {
	dir := t.TempDir()
	staging := filepath.Join(dir, "staging")
	target := filepath.Join(dir, "target")
	for _, p := range []string{staging, target} {
		if err := os.MkdirAll(p, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	binds := 0
	d := &Driver{mounter: mount.NewFakeMounter(nil)}
	d.bindMount = func(source, target, fstype string, options []string) error {
		binds++
		return nil
	}
	_, err := d.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId: "fc.csi.ngxstorage.com.pub-nostage", StagingTargetPath: staging, TargetPath: target,
		VolumeCapability: &csi.VolumeCapability{}, PublishContext: map[string]string{},
	})
	if err == nil {
		t.Fatal("publish succeeded although staging is not mounted")
	}
	if binds != 0 {
		t.Fatalf("bind called %d times on unmounted staging, want 0", binds)
	}
}

// A block publish target bound from a different device rdev fails closed.
func TestNodePublishVolumeBlockForeignDeviceFailsClosed(t *testing.T) {
	dir := t.TempDir()
	staging := filepath.Join(dir, "block-staging")
	target := filepath.Join(dir, "block-target")
	for _, p := range []string{staging, target} {
		if err := os.WriteFile(p, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	mounter := mount.NewFakeMounter([]mount.MountPoint{{Device: "devtmpfs", Path: target, Opts: []string{"bind"}}})
	swapStatFile(t, func(path string) (os.FileInfo, error) {
		if path == target {
			return fakeDeviceInfo{mode: blockDeviceMode, rdev: 43}, nil
		}
		return fakeDeviceInfo{mode: blockDeviceMode, rdev: 42}, nil
	})
	d := &Driver{mounter: mounter}
	_, err := d.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId: "fc.csi.ngxstorage.com.pub-block", StagingTargetPath: staging, TargetPath: target,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		},
		PublishContext: map[string]string{},
	})
	if err == nil {
		t.Fatal("block publish succeeded on a target bound from a different device")
	}
}

// Stats require an active mount; an ordinary host directory is never
// reported as the volume's capacity.
func TestNodeGetVolumeStatsRequiresMountedPath(t *testing.T) {
	volumePath := t.TempDir()
	d := &Driver{mounter: mount.NewFakeMounter([]mount.MountPoint{{Device: "/dev/dm-1", Path: volumePath}})}
	resp, err := d.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
		VolumeId: "fc.csi.ngxstorage.com.stats", VolumePath: volumePath,
	})
	if err != nil {
		t.Fatalf("mounted volume stats: %v", err)
	}
	if len(resp.GetUsage()) != 1 || resp.GetUsage()[0].GetUnit() != csi.VolumeUsage_BYTES {
		t.Fatalf("usage = %#v, want one BYTES entry", resp.GetUsage())
	}
	d.mounter = mount.NewFakeMounter(nil)
	_, err = d.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
		VolumeId: "fc.csi.ngxstorage.com.stats", VolumePath: volumePath,
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("unmounted path: code=%s err=%v, want NotFound", status.Code(err), err)
	}
}
