package fc

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewConnectorUsesHostSysfs(t *testing.T) {
	if got := NewConnector().SYSFSPath; got != "/host/sys" {
		t.Fatalf("SYSFSPath = %q, want /host/sys", got)
	}
}

// writeSysfsFile creates a fake sysfs file with the given content.
func writeSysfsFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestDiscoverInitiatorWWPNs(t *testing.T) {
	sys := t.TempDir()
	writeSysfsFile(t, filepath.Join(sys, "class", "fc_host", "host0", "port_name"), "0x10000090facbf816\n")
	writeSysfsFile(t, filepath.Join(sys, "class", "fc_host", "host1", "port_name"), "0x10000090facbf817\n")

	c := &Connector{SYSFSPath: sys}
	wwpns, err := c.DiscoverInitiatorWWPNs()
	if err != nil {
		t.Fatalf("DiscoverInitiatorWWPNs: %v", err)
	}
	if len(wwpns) != 2 {
		t.Fatalf("got %d wwpns, want 2", len(wwpns))
	}
	if wwpns[0] != "10000090facbf816" || wwpns[1] != "10000090facbf817" {
		t.Fatalf("wwpns = %v, want 0x-stripped values", wwpns)
	}
}

func TestDiscoverInitiatorWWPNsNone(t *testing.T) {
	sys := t.TempDir()
	c := &Connector{SYSFSPath: sys}
	if _, err := c.DiscoverInitiatorWWPNs(); err == nil {
		t.Fatal("expected error when no FC hosts present")
	}
}

func TestRescanFCHostSymlink(t *testing.T) {
	sys := t.TempDir()
	hostTarget := filepath.Join(sys, "devices", "pci0000", "host14")
	writeSysfsFile(t, filepath.Join(hostTarget, "device", "scsi_host", "host14", "scan"), "")
	classDir := filepath.Join(sys, "class", "fc_host")
	if err := os.MkdirAll(classDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(hostTarget, filepath.Join(classDir, "host14")); err != nil {
		t.Fatal(err)
	}

	c := &Connector{SYSFSPath: sys}
	if err := c.RescanFCHosts(); err != nil {
		t.Fatalf("RescanFCHosts: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(hostTarget, "device", "scsi_host", "host14", "scan"))
	if err != nil || string(raw) != "- - -\n" {
		t.Fatalf("scan file = %q, err = %v", raw, err)
	}
}

func TestResolveDevicePath(t *testing.T) {
	dev := t.TempDir()
	// Create a fake by-id symlink chain.
	byID := filepath.Join(dev, "disk", "by-id")
	if err := os.MkdirAll(byID, 0o755); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(byID, "wwn-0x3600")
	if err := os.Symlink("../../sda", symlink); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dev, "sda")
	if err := os.WriteFile(target, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	c := &Connector{DEVRoot: dev}
	got, err := c.ResolveDevicePath("0x3600")
	if err != nil {
		t.Fatalf("ResolveDevicePath: %v", err)
	}
	want := filepath.Join(dev, "sda")
	if got != want {
		t.Fatalf("resolved = %s, want %s", got, want)
	}
}

func TestWaitForDeviceTimeout(t *testing.T) {
	dev := t.TempDir()
	c := &Connector{DEVRoot: dev, WaitInterval: time.Millisecond, WaitTimeout: 10 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := c.WaitForDevice(ctx, "0xnonexistent"); err == nil {
		t.Fatal("expected timeout waiting for nonexistent device")
	}
}

func TestWaitForDeviceWaitsForMultipath(t *testing.T) {
	dev := t.TempDir()
	byID := filepath.Join(dev, "disk", "by-id")
	if err := os.MkdirAll(byID, 0o755); err != nil {
		t.Fatal(err)
	}
	writeSysfsFile(t, filepath.Join(dev, "sde"), "")
	writeSysfsFile(t, filepath.Join(dev, "dm-2"), "")
	link := filepath.Join(byID, "wwn-0x589c")
	if err := os.Symlink("../../sde", link); err != nil {
		t.Fatal(err)
	}

	go func() {
		time.Sleep(5 * time.Millisecond)
		_ = os.Remove(link)
		_ = os.Symlink("../../dm-2", link)
	}()
	c := &Connector{SYSFSPath: filepath.Join(dev, "sys"), DEVRoot: dev, WaitInterval: time.Millisecond, WaitTimeout: 50 * time.Millisecond}
	path, err := c.WaitForDevice(context.Background(), "589c")
	if err != nil {
		t.Fatalf("WaitForDevice: %v", err)
	}
	if filepath.Base(path) != "dm-2" {
		t.Fatalf("device = %s, want multipath dm-2", path)
	}
}

func TestWaitForMappedDeviceWaitsForEveryZonedPath(t *testing.T) {
	root := t.TempDir()
	dev := filepath.Join(root, "dev")
	sys := filepath.Join(root, "sys")
	byID := filepath.Join(dev, "disk", "by-id")
	if err := os.MkdirAll(byID, 0o755); err != nil {
		t.Fatal(err)
	}
	writeSysfsFile(t, filepath.Join(dev, "dm-4"), "")
	if err := os.Symlink("../../dm-4", filepath.Join(byID, "wwn-0x589c")); err != nil {
		t.Fatal(err)
	}

	// Two online/zoned target ports are visible before the LUN is scanned.
	for _, tc := range []struct {
		rport  string
		port   string
		target string
	}{
		{rport: "rport-14:0-3", port: "0x20020090faeae1a5", target: "0"},
		{rport: "rport-14:0-5", port: "0x211200109b223cc1", target: "3"},
	} {
		base := filepath.Join(sys, "class", "fc_remote_ports", tc.rport)
		writeSysfsFile(t, filepath.Join(base, "port_name"), tc.port+"\n")
		writeSysfsFile(t, filepath.Join(base, "port_state"), "Online\n")
		writeSysfsFile(t, filepath.Join(base, "scsi_target_id"), tc.target+"\n")
	}
	writeSysfsFile(t, filepath.Join(sys, "class", "scsi_host", "host14", "scan"), "")

	// Path target 0 is ready immediately; target 3 arrives later. Returning
	// while only the first path exists would reproduce the live cross-node bug.
	writeSysfsFile(t, filepath.Join(sys, "class", "scsi_device", "14:0:0:6", "device", "block", "sda", ".keep"), "")
	writeSysfsFile(t, filepath.Join(sys, "class", "block", "sda", "device", "wwid"), "naa.589c\n")
	writeSysfsFile(t, filepath.Join(sys, "class", "block", "sda", "device", "rescan"), "")
	if err := os.MkdirAll(filepath.Join(sys, "class", "block", "dm-4", "slaves", "sda"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeSysfsFile(t, filepath.Join(sys, "class", "block", "dm-4", "dm", "name"), "mpath-test\n")

	go func() {
		time.Sleep(5 * time.Millisecond)
		writeSysfsFile(t, filepath.Join(sys, "class", "scsi_device", "14:0:3:6", "device", "block", "sdb", ".keep"), "")
		writeSysfsFile(t, filepath.Join(sys, "class", "block", "sdb", "device", "wwid"), "naa.589c\n")
		writeSysfsFile(t, filepath.Join(sys, "class", "block", "sdb", "device", "rescan"), "")
		_ = os.MkdirAll(filepath.Join(sys, "class", "block", "dm-4", "slaves", "sdb"), 0o755)
	}()

	reloads := 0
	c := &Connector{
		SYSFSPath:    sys,
		DEVRoot:      dev,
		WaitInterval: time.Millisecond,
		WaitTimeout:  100 * time.Millisecond,
		ReloadMultipath: func(device string) error {
			reloads++
			return nil
		},
	}
	path, err := c.WaitForMappedDevice(
		context.Background(), "589c",
		[]string{"20020090faeae1a5", "211200109b223cc1"}, 6,
	)
	if err != nil {
		t.Fatalf("WaitForMappedDevice: %v", err)
	}
	if filepath.Base(path) != "dm-4" {
		t.Fatalf("device = %s, want dm-4", path)
	}
	if reloads != 1 {
		t.Fatalf("multipath reloads = %d, want 1", reloads)
	}
}

func TestNormalizeWWN(t *testing.T) {
	for input, want := range map[string]string{
		"naa.589C":                          "589c",
		"0x589c":                            "589c",
		"3589c005636424757595041987f90ac8f": "589c005636424757595041987f90ac8f",
	} {
		if got := normalizeWWN(input); got != want {
			t.Fatalf("normalizeWWN(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestRescanMultipathDevicePaths(t *testing.T) {
	root := t.TempDir()
	dev := filepath.Join(root, "dev")
	sys := filepath.Join(root, "sys")
	if err := os.MkdirAll(filepath.Join(dev, "mapper"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeSysfsFile(t, filepath.Join(dev, "dm-2"), "")
	if err := os.Symlink("../dm-2", filepath.Join(dev, "mapper", "mpathc")); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(sys, "class", "block", "dm-2", "slaves", "sde"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeSysfsFile(t, filepath.Join(sys, "class", "block", "sde", "device", "rescan"), "")
	writeSysfsFile(t, filepath.Join(sys, "class", "block", "dm-2", "dm", "name"), "mpathc\n")

	c := &Connector{SYSFSPath: sys}
	devicePath := filepath.Join(dev, "mapper", "mpathc")
	if err := c.RescanDevicePaths(devicePath); err != nil {
		t.Fatalf("RescanDevicePaths: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(sys, "class", "block", "sde", "device", "rescan"))
	if err != nil || string(raw) != "1\n" {
		t.Fatalf("rescan file = %q, err = %v", raw, err)
	}
	name, err := c.MultipathName(devicePath)
	if err != nil || name != "mpathc" {
		t.Fatalf("MultipathName = %q, err = %v", name, err)
	}
}

func TestWaitForDeviceSize(t *testing.T) {
	root := t.TempDir()
	dev := filepath.Join(root, "dev")
	sys := filepath.Join(root, "sys")
	writeSysfsFile(t, filepath.Join(dev, "dm-2"), "")
	writeSysfsFile(t, filepath.Join(sys, "class", "block", "dm-2", "size"), "4194304\n")
	c := &Connector{SYSFSPath: sys, WaitInterval: time.Millisecond, WaitTimeout: 10 * time.Millisecond}
	if err := c.WaitForDeviceSize(context.Background(), filepath.Join(dev, "dm-2"), 2<<30); err != nil {
		t.Fatalf("WaitForDeviceSize: %v", err)
	}
}

func TestMergePathNamesIncludesLatePaths(t *testing.T) {
	got := mergePathNames([]string{"sde"}, []string{"sde", "sdf"})
	want := []string{"sde", "sdf"}
	if len(got) != len(want) {
		t.Fatalf("mergePathNames = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("mergePathNames = %v, want %v", got, want)
		}
	}
}
