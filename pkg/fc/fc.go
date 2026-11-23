package fc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// ErrInvalidDeviceRequest marks a device-discovery request that can never
// succeed as supplied (empty WWID, negative LUN number, missing or unusable
// target WWPNs). It is a permanent request error, not a convergence timeout.
var ErrInvalidDeviceRequest = errors.New("invalid FC device request")

// ErrHostFCUnavailable marks a failure reading the host FC subsystem (sysfs
// remote-port enumeration). Path discovery cannot proceed until the host FC
// state is readable again; it is not a device-convergence timeout.
var ErrHostFCUnavailable = errors.New("host FC subsystem unavailable")

// Connector discovers and manages FC block devices on the host. All sysfs
// reads/writes use os.ReadFile/os.WriteFile (never shell redirection), and all
// polling is bounded by a context deadline. The node container must mount the
// host root at /host; FC sysfs is therefore always addressed as /host/sys.
type Connector struct {
	// SYSFSPath is the host sysfs mount point (default /host/sys). Tests override it.
	SYSFSPath string
	// DEVRoot is the device directory (default /dev). Tests override it.
	DEVRoot string
	// WaitInterval is the polling interval between device-discovery checks.
	WaitInterval time.Duration
	// WaitTimeout is the maximum total time to wait for a device to appear.
	WaitTimeout time.Duration
	// ReloadMultipath refreshes one dm-multipath map after all expected SCSI
	// paths for a target/LUN have appeared. Tests override it.
	ReloadMultipath func(string) error
}

// NewConnector builds a Connector with production defaults.
func NewConnector() *Connector {
	return &Connector{
		SYSFSPath:    "/host/sys",
		DEVRoot:      "/dev",
		WaitInterval: 1 * time.Second,
		WaitTimeout:  60 * time.Second,
		ReloadMultipath: func(device string) error {
			return NewMultipath().ReloadDevice(device)
		},
	}
}

// DiscoverInitiatorWWPNs reads every FC HBA's port_name and returns the
// initiator WWPNs with the "0x" prefix stripped.
func (c *Connector) DiscoverInitiatorWWPNs() ([]string, error) {
	hostDir := filepath.Join(c.SYSFSPath, "class", "fc_host")
	entries, err := os.ReadDir(hostDir)
	if err != nil {
		return nil, fmt.Errorf("list fc_host: %w", err)
	}
	var wwpns []string
	for _, entry := range entries {
		portName := filepath.Join(hostDir, entry.Name(), "port_name")
		raw, err := os.ReadFile(portName)
		if err != nil {
			continue // HBA may not be initialized; skip
		}
		wwpn := strings.TrimSpace(string(raw))
		wwpn = strings.TrimPrefix(wwpn, "0x")
		if wwpn != "" {
			wwpns = append(wwpns, wwpn)
		}
	}
	if len(wwpns) == 0 {
		return nil, fmt.Errorf("no FC initiator WWPNs discovered")
	}
	return wwpns, nil
}

// RescanFCHosts triggers a rescan on every FC host so newly mapped LUNs are
// visible to the SCSI midlayer.
func (c *Connector) RescanFCHosts() error {
	hostDir := filepath.Join(c.SYSFSPath, "class", "fc_host")
	entries, err := os.ReadDir(hostDir)
	if err != nil {
		return fmt.Errorf("list fc_host: %w", err)
	}
	rescanned := 0
	for _, entry := range entries {
		scanPath := filepath.Join(hostDir, entry.Name(), "device", "scsi_host", entry.Name(), "scan")
		// Trigger a scan; the file may not exist on all HBAs.
		if err := os.WriteFile(scanPath, []byte("- - -\n"), 0o200); err != nil {
			continue
		}
		rescanned++
	}
	if rescanned == 0 {
		return fmt.Errorf("no FC host scan files written")
	}
	return nil
}

// ResolveDevicePath resolves a block device path from a WWID (SCSI VPD page
// 83 identifier). The WWID is matched against /dev/disk/by-id/wwn-0x<wwid>.
func (c *Connector) ResolveDevicePath(wwid string) (string, error) {
	if wwid == "" {
		return "", fmt.Errorf("empty WWID")
	}
	clean := strings.ToLower(strings.TrimPrefix(wwid, "0x"))
	byID := filepath.Join(c.DEVRoot, "disk", "by-id", "wwn-0x"+clean)
	resolved, err := filepath.EvalSymlinks(byID)
	if err != nil {
		return "", fmt.Errorf("resolve device for WWID %s: %w", wwid, err)
	}
	return resolved, nil
}

// WaitForDevice polls until a dm-multipath device for the given WWID appears,
// or ctx ends. NodeStage should prefer WaitForMappedDevice because a dm node
// can appear before every zoned FC path for a newly presented target/LUN is
// ready.
func (c *Connector) WaitForDevice(ctx context.Context, wwid string) (string, error) {
	interval := c.WaitInterval
	if interval <= 0 {
		interval = time.Second
	}
	deadline := time.Now().Add(c.WaitTimeout)
	for {
		// FC target mappings may become visible after the first scan. Rescan on
		// every bounded poll so an asynchronously presented LUN is discovered.
		_ = c.RescanFCHosts()
		if path, err := c.ResolveDevicePath(wwid); err == nil {
			// udev initially points wwn-* at the first SCSI path. Wait until
			// multipath owns the symlink so formatting never targets one path.
			if strings.HasPrefix(filepath.Base(path), "dm-") {
				return path, nil
			}
		}
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("timed out waiting for device with WWID %s", wwid)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(interval):
		}
	}
}

type targetPath struct {
	host   int
	target int
}

// WaitForMappedDevice waits for every online/zoned FC remote port that
// belongs to targetWWNs to expose the requested LUN, verifies each SCSI path
// reports the expected WWID, and only then returns the dm-multipath device.
//
// This guards against target/LUN-number reuse on long-running nodes: Linux may
// retain an old H:C:T:L device while the array has already mapped a different
// LUN at the same target/LUN number. Formatting such a partially converged dm
// map can corrupt an existing filesystem during cross-node handoff.
func (c *Connector) WaitForMappedDevice(
	ctx context.Context, wwid string, targetWWNs []string, lunNumber int,
) (string, error) {
	if wwid == "" {
		return "", fmt.Errorf("%w: empty WWID", ErrInvalidDeviceRequest)
	}
	if lunNumber < 0 {
		return "", fmt.Errorf("%w: invalid FC LUN number %d", ErrInvalidDeviceRequest, lunNumber)
	}
	if len(targetWWNs) == 0 {
		return "", fmt.Errorf("%w: no FC target WWPNs supplied", ErrInvalidDeviceRequest)
	}

	interval := c.WaitInterval
	if interval <= 0 {
		interval = time.Second
	}
	timeout := c.WaitTimeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	deadline := time.Now().Add(timeout)

	var lastErr error
	for {
		paths, err := c.matchingTargetPaths(targetWWNs)
		if err != nil {
			lastErr = err
		} else if len(paths) == 0 {
			lastErr = fmt.Errorf("no online zoned FC remote ports match target WWPNs")
		} else {
			blockNames := make([]string, 0, len(paths))
			allReady := true
			for _, path := range paths {
				_ = c.scanTargetLUN(path.host, path.target, lunNumber)
				hctl, blockName, findErr := c.findSCSIBlockDevice(path.host, path.target, lunNumber)
				if findErr != nil {
					lastErr = findErr
					allReady = false
					break
				}

				actualWWID, readErr := c.blockWWID(blockName)
				if readErr != nil {
					lastErr = readErr
					allReady = false
					break
				}
				if normalizeWWN(actualWWID) != normalizeWWN(wwid) {
					// First ask the existing SCSI device to refresh its VPD. If the
					// H:C:T:L still identifies a different LUN, purge exactly that
					// stale path and rescan this target/LUN only.
					_ = c.rescanBlockDevice(blockName)
					if refreshed, refreshErr := c.blockWWID(blockName); refreshErr == nil &&
						normalizeWWN(refreshed) == normalizeWWN(wwid) {
						blockNames = append(blockNames, blockName)
						continue
					}
					_ = c.RemoveDevice(hctl)
					_ = c.scanTargetLUN(path.host, path.target, lunNumber)
					lastErr = fmt.Errorf(
						"stale FC path %s reports WWID %s, want %s", hctl, actualWWID, wwid,
					)
					allReady = false
					break
				}
				blockNames = append(blockNames, blockName)
			}

			if allReady {
				devicePath, resolveErr := c.ResolveDevicePath(wwid)
				if resolveErr != nil {
					lastErr = resolveErr
				} else if !strings.HasPrefix(filepath.Base(devicePath), "dm-") {
					lastErr = fmt.Errorf("WWID %s has not converged to a multipath device", wwid)
				} else {
					_, slaves, describeErr := c.DescribeDevice(devicePath)
					if describeErr != nil {
						lastErr = describeErr
					} else if !containsAll(slaves, blockNames) {
						lastErr = fmt.Errorf(
							"multipath device %s missing expected FC paths: have=%v want=%v",
							devicePath, slaves, blockNames,
						)
					} else {
						// Refresh path capacities and the dm table after target/LUN
						// reuse. This is intentionally done only after identity and
						// path-completeness checks have succeeded.
						_ = c.RescanDevicePaths(devicePath)
						if c.ReloadMultipath != nil {
							_ = c.ReloadMultipath(devicePath)
						}
						return devicePath, nil
					}
				}
			}
		}

		if err := ctx.Err(); err != nil {
			return "", err
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				return "", fmt.Errorf("timed out waiting for FC LUN %d / WWID %s: %w", lunNumber, wwid, lastErr)
			}
			return "", fmt.Errorf("timed out waiting for FC LUN %d / WWID %s", lunNumber, wwid)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(interval):
		}
	}
}

func normalizeWWN(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.TrimPrefix(value, "naa.")
	value = strings.TrimPrefix(value, "0x")
	// Linux may expose the dm-multipath WWID with the SCSI designator type
	// prefix (for example 3<naa-id>); the backend scsi_id omits it.
	if len(value) == 33 && strings.HasPrefix(value, "3") {
		value = value[1:]
	}
	return value
}

func (c *Connector) matchingTargetPaths(targetWWNs []string) ([]targetPath, error) {
	desired := make(map[string]struct{}, len(targetWWNs))
	for _, wwn := range targetWWNs {
		if normalized := normalizeWWN(wwn); normalized != "" {
			desired[normalized] = struct{}{}
		}
	}
	if len(desired) == 0 {
		return nil, fmt.Errorf("%w: no valid FC target WWPNs supplied", ErrInvalidDeviceRequest)
	}

	root := filepath.Join(c.SYSFSPath, "class", "fc_remote_ports")
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("%w: list FC remote ports: %w", ErrHostFCUnavailable, err)
	}

	seen := make(map[string]struct{})
	paths := make([]targetPath, 0)
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "rport-") {
			continue
		}
		portName, readErr := os.ReadFile(filepath.Join(root, name, "port_name"))
		if readErr != nil {
			continue
		}
		if _, ok := desired[normalizeWWN(string(portName))]; !ok {
			continue
		}
		state, readErr := os.ReadFile(filepath.Join(root, name, "port_state"))
		if readErr != nil || !strings.EqualFold(strings.TrimSpace(string(state)), "Online") {
			continue
		}
		targetRaw, readErr := os.ReadFile(filepath.Join(root, name, "scsi_target_id"))
		if readErr != nil {
			continue
		}
		targetID, parseErr := strconv.Atoi(strings.TrimSpace(string(targetRaw)))
		if parseErr != nil || targetID < 0 {
			continue
		}
		hostID, parseErr := rportHostNumber(name)
		if parseErr != nil {
			continue
		}
		key := fmt.Sprintf("%d:%d", hostID, targetID)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		paths = append(paths, targetPath{host: hostID, target: targetID})
	}
	return paths, nil
}

func rportHostNumber(name string) (int, error) {
	trimmed := strings.TrimPrefix(name, "rport-")
	parts := strings.Split(trimmed, ":")
	if len(parts) < 2 {
		return 0, fmt.Errorf("invalid FC rport name %q", name)
	}
	host, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, fmt.Errorf("parse FC rport host from %q: %w", name, err)
	}
	return host, nil
}

func (c *Connector) scanTargetLUN(host, target, lun int) error {
	scanPath := filepath.Join(c.SYSFSPath, "class", "scsi_host", fmt.Sprintf("host%d", host), "scan")
	if err := os.WriteFile(scanPath, []byte(fmt.Sprintf("- %d %d\n", target, lun)), 0o200); err != nil {
		return fmt.Errorf("scan FC target %d LUN %d on host%d: %w", target, lun, host, err)
	}
	return nil
}

func (c *Connector) findSCSIBlockDevice(host, target, lun int) (string, string, error) {
	pattern := filepath.Join(
		c.SYSFSPath, "class", "scsi_device",
		fmt.Sprintf("%d:*:%d:%d", host, target, lun), "device", "block", "*",
	)
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return "", "", fmt.Errorf("glob FC SCSI device: %w", err)
	}
	if len(matches) == 0 {
		return "", "", fmt.Errorf("FC path host%d target%d LUN%d has no block device", host, target, lun)
	}
	blockName := filepath.Base(matches[0])
	hctl := filepath.Base(filepath.Dir(filepath.Dir(filepath.Dir(matches[0]))))
	return hctl, blockName, nil
}

func (c *Connector) blockWWID(blockName string) (string, error) {
	raw, err := os.ReadFile(filepath.Join(c.SYSFSPath, "class", "block", blockName, "device", "wwid"))
	if err != nil {
		return "", fmt.Errorf("read WWID for %s: %w", blockName, err)
	}
	value := strings.TrimSpace(string(raw))
	if value == "" {
		return "", fmt.Errorf("empty WWID for %s", blockName)
	}
	return value, nil
}

func (c *Connector) rescanBlockDevice(blockName string) error {
	path := filepath.Join(c.SYSFSPath, "class", "block", blockName, "device", "rescan")
	if err := os.WriteFile(path, []byte("1\n"), 0o200); err != nil {
		return fmt.Errorf("rescan SCSI block device %s: %w", blockName, err)
	}
	return nil
}

func containsAll(have, want []string) bool {
	set := make(map[string]struct{}, len(have))
	for _, value := range have {
		set[value] = struct{}{}
	}
	for _, value := range want {
		if _, ok := set[value]; !ok {
			return false
		}
	}
	return true
}

// RescanDevicePaths refreshes the capacity of only the SCSI paths underneath
// devicePath. For a multipath device, each slave path is rescanned.
func (c *Connector) RescanDevicePaths(devicePath string) error {
	pathNames, err := c.devicePathNames(devicePath)
	if err != nil {
		return err
	}
	rescanned := 0
	for _, pathName := range pathNames {
		rescanPath := filepath.Join(c.SYSFSPath, "class", "block", pathName, "device", "rescan")
		if err := os.WriteFile(rescanPath, []byte("1\n"), 0o200); err != nil {
			continue
		}
		rescanned++
	}
	if rescanned == 0 {
		return fmt.Errorf("no SCSI paths rescanned for %s", devicePath)
	}
	return nil
}

// Disconnect flushes the exact multipath map for wwid and removes only its
// underlying SCSI paths. A missing device is already disconnected.
func (c *Connector) Disconnect(wwid string) error {
	devicePath, err := c.ResolveDevicePath(wwid)
	if err != nil {
		return nil
	}
	mapName, pathNames, err := c.DescribeDevice(devicePath)
	if err != nil {
		return err
	}
	return c.DisconnectDevice(mapName, pathNames)
}

// DescribeDevice captures the stable multipath name and slave block-device
// names needed to disconnect a volume even after its WWID symlink disappears.
func (c *Connector) DescribeDevice(devicePath string) (string, []string, error) {
	pathNames, err := c.devicePathNames(devicePath)
	if err != nil {
		return "", nil, err
	}
	mapName, err := c.MultipathName(devicePath)
	if err != nil {
		return "", nil, err
	}
	return mapName, pathNames, nil
}

// DisconnectDevice flushes one captured multipath map and deletes only its
// captured SCSI paths. Missing objects make the operation idempotent.
func (c *Connector) DisconnectDevice(mapName string, pathNames []string) error {
	if mapName == "" {
		return fmt.Errorf("multipath name is required")
	}
	mapPath := filepath.Join(c.DEVRoot, "mapper", mapName)
	if _, err := os.Stat(mapPath); err == nil {
		currentMapName, currentPathNames, describeErr := c.DescribeDevice(mapPath)
		if describeErr != nil {
			return fmt.Errorf("refresh multipath device %s before disconnect: %w", mapName, describeErr)
		}
		if currentMapName != mapName {
			return fmt.Errorf("multipath device changed from %s to %s", mapName, currentMapName)
		}
		// Paths can appear after NodeStage persisted its initial state. Merge
		// the map's current slaves before flushing so late FC paths are also
		// deleted after the device-mapper map disappears.
		pathNames = mergePathNames(pathNames, currentPathNames)
		if err := NewMultipath().FlushDevice(mapName); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect multipath device %s: %w", mapName, err)
	}
	for _, pathName := range pathNames {
		deletePath := filepath.Join(c.SYSFSPath, "class", "block", pathName, "device", "delete")
		if err := os.WriteFile(deletePath, []byte("1\n"), 0o200); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove SCSI path %s: %w", pathName, err)
		}
	}
	return nil
}

func mergePathNames(captured, current []string) []string {
	seen := make(map[string]struct{}, len(captured)+len(current))
	merged := make([]string, 0, len(captured)+len(current))
	for _, names := range [][]string{captured, current} {
		for _, name := range names {
			if name == "" {
				continue
			}
			if _, exists := seen[name]; exists {
				continue
			}
			seen[name] = struct{}{}
			merged = append(merged, name)
		}
	}
	return merged
}

// MultipathName resolves the device-mapper name (for example mpathc) used by
// multipathd for devicePath.
func (c *Connector) MultipathName(devicePath string) (string, error) {
	blockName, err := c.blockName(devicePath)
	if err != nil {
		return "", err
	}
	raw, err := os.ReadFile(filepath.Join(c.SYSFSPath, "class", "block", blockName, "dm", "name"))
	if err != nil {
		return "", fmt.Errorf("read multipath name for %s: %w", devicePath, err)
	}
	name := strings.TrimSpace(string(raw))
	if name == "" {
		return "", fmt.Errorf("empty multipath name for %s", devicePath)
	}
	return name, nil
}

// WaitForDeviceSize waits until the kernel reports at least requiredBytes for
// devicePath. Linux exposes block size in 512-byte sectors in sysfs.
func (c *Connector) WaitForDeviceSize(ctx context.Context, devicePath string, requiredBytes int64) error {
	if requiredBytes <= 0 {
		return fmt.Errorf("required device size must be positive")
	}
	blockName, err := c.blockName(devicePath)
	if err != nil {
		return err
	}
	interval := c.WaitInterval
	if interval <= 0 {
		interval = time.Second
	}
	timeout := c.WaitTimeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	deadline := time.Now().Add(timeout)
	sizePath := filepath.Join(c.SYSFSPath, "class", "block", blockName, "size")
	for {
		raw, readErr := os.ReadFile(sizePath)
		if readErr == nil {
			sectors, parseErr := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
			if parseErr == nil && sectors*512 >= requiredBytes {
				return nil
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %s to reach %d bytes", devicePath, requiredBytes)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

func (c *Connector) blockName(devicePath string) (string, error) {
	resolved, err := filepath.EvalSymlinks(devicePath)
	if err != nil {
		return "", fmt.Errorf("resolve block device %s: %w", devicePath, err)
	}
	name := filepath.Base(resolved)
	if name == "." || name == string(filepath.Separator) || name == "" {
		return "", fmt.Errorf("invalid block device path %s", devicePath)
	}
	return name, nil
}

func (c *Connector) devicePathNames(devicePath string) ([]string, error) {
	blockName, err := c.blockName(devicePath)
	if err != nil {
		return nil, err
	}
	slavesDir := filepath.Join(c.SYSFSPath, "class", "block", blockName, "slaves")
	entries, readErr := os.ReadDir(slavesDir)
	if readErr != nil && !os.IsNotExist(readErr) {
		return nil, fmt.Errorf("list slaves for %s: %w", devicePath, readErr)
	}
	pathNames := make([]string, 0, len(entries))
	for _, entry := range entries {
		pathNames = append(pathNames, entry.Name())
	}
	if len(pathNames) == 0 {
		pathNames = append(pathNames, blockName)
	}
	return pathNames, nil
}

// RemoveDevice detaches a SCSI device by writing to its sysfs delete node.
func (c *Connector) RemoveDevice(hctl string) error {
	deletePath := filepath.Join(c.SYSFSPath, "class", "scsi_device", hctl, "device", "delete")
	if err := os.WriteFile(deletePath, []byte("1\n"), 0o200); err != nil {
		return fmt.Errorf("remove SCSI device %s: %w", hctl, err)
	}
	return nil
}
