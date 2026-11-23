package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	helper "github.com/ngxstorage/fc-csi-ngxstorage-driver/pkg/helper"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	ngxnames "github.com/NGXStorage/csi-ngxstorage-utils/names"
	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/ngxstorage/fc-csi-ngxstorage-driver/pkg/fc"
	ngxstorage "github.com/ngxstorage/ngxstorage-sdk-go/pkg/ngxstorage"
	"google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// newTestDriver builds a Driver whose SDK client points at a mock NGX API.
// The mock routes cluster status and pool list (for RefreshController) plus
// the operation-specific responses supplied by handler.
func newTestDriver(t *testing.T, mockHandler http.HandlerFunc) *Driver {
	return newTestDriverWithCapacity(t, mockHandler, `[{"name":"pool1","logical":{"available":1099511627776,"reserved":0}}]`)
}

func newTestDriverWithCapacity(t *testing.T, mockHandler http.HandlerFunc, capacityResponse string) *Driver {
	t.Helper()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Default routing for controller initialization: cluster status,
		// pool list and pool detail (owner). The pool belongs to owner "o".
		// Operation-specific paths delegate to the caller handler.
		switch {
		case strings.Contains(r.URL.Path, "/status/cluster"):
			w.Write([]byte(`{"connected":1,"status":"Master","peers":[]}`))
		case strings.HasSuffix(r.URL.Path, "/api/v2/pool"):
			w.Write([]byte(`[{"id":"pool1","name":"pool1","owner":"o"}]`))
		case strings.HasSuffix(r.URL.Path, "/api/v2/pool/pool1"):
			w.Write([]byte(`{"id":"pool1","name":"pool1","owner":"o"}`))
		case strings.HasSuffix(r.URL.Path, "/api/v2/status/capacity"):
			w.Write([]byte(capacityResponse))
		default:
			mockHandler(w, r)
		}
	})
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on 127.0.0.1: %v", err)
	}
	server := &httptest.Server{Listener: listener, Config: &http.Server{Handler: handler}}
	server.Start()
	t.Cleanup(server.Close)

	transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		req.URL.Scheme = "http"
		req.URL.Host = server.Listener.Addr().String()
		return http.DefaultTransport.RoundTrip(req)
	})

	d, err := NewDriverWithHTTPClient(InputParameters{
		Ip:            "10.0.0.1",
		PoolName:      "pool1",
		ApiKey:        "test-key",
		Endpoint:      "http://127.0.0.1",
		FcTargetNames: "tgt-a",
		NodeId:        "node-1",
		Role:          "controller",
	}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatalf("NewDriverWithHTTPClient: %v", err)
	}
	d.coordination = newFakeCoordinator()
	d.attachmentLookup = &fakeAttachmentLookup{}
	return d
}

// fakeAttachmentLookup is the unit-test stand-in for the Kubernetes
// VolumeAttachment API.
type fakeAttachmentLookup struct {
	remaining []string
	err       error
	calls     int
}

func (f *fakeAttachmentLookup) RemainingNodes(
	ctx context.Context, driverName, volumeHandle, excludingNodeID string,
) ([]string, error) {
	f.calls++
	return f.remaining, f.err
}

// roundTripperFunc adapts a func to http.RoundTripper.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func createVolumeRequestForTest(name string, capacityBytes int64) *csi.CreateVolumeRequest {
	return &csi.CreateVolumeRequest{
		Name:          name,
		CapacityRange: &csi.CapacityRange{RequiredBytes: capacityBytes},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"}},
		}},
	}
}

func TestCreateVolume(t *testing.T) {
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/api/v2/lun/list"):
			// Idempotency check: no existing LUN with this name.
			w.Write([]byte(`[]`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/api/v2/lun/"):
			w.Write([]byte(`{"luns":[{"id":"lun-1","name":"pvcvol1"}]}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			w.Write([]byte(`{"id":"lun-1","name":"pvcvol1","size":1073741824}`))
		default:
			w.Write([]byte(`{"connected":1,"status":"Master","peers":[]}`))
		}
	})

	resp, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "vol1",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"}},
		}},
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	// The volume ID embeds the backend LUN ID (SHA1), not the name.
	if resp.Volume.VolumeId != helper.VolumeID(volumeIDPrefix, "lun-1") {
		t.Fatalf("volume ID = %s, want %s", resp.Volume.VolumeId, helper.VolumeID(volumeIDPrefix, "lun-1"))
	}
	if resp.Volume.VolumeContext["lun_id"] != "lun-1" {
		t.Fatalf("lun_id = %s, want lun-1", resp.Volume.VolumeContext["lun_id"])
	}
}

func TestCreateVolumeRejectsPoolWithoutUsableCapacityRecord(t *testing.T) {
	createCalls := 0
	d := newTestDriverWithCapacity(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/list"):
			w.Write([]byte(`[]`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/api/v2/lun/"):
			createCalls++
			w.Write([]byte(`{"luns":[{"id":"unexpected","name":"pvcvol1"}]}`))
		default:
			w.Write([]byte(`{"connected":1,"status":"Master","peers":[]}`))
		}
	}, `[]`)

	_, err := d.CreateVolume(context.Background(), createVolumeRequestForTest("vol1", 1<<30))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("CreateVolume error = %v, want Unavailable for missing pool capacity record", err)
	}
	if !strings.Contains(err.Error(), `configured pool "pool1" is not currently usable`) {
		t.Fatalf("CreateVolume error = %v, want explicit configured-pool usability diagnostic", err)
	}
	if createCalls != 0 {
		t.Fatalf("backend LUN create calls = %d, want 0", createCalls)
	}
}

func TestGetCapacityReportsUnavailablePoolCapacityAsRetryable(t *testing.T) {
	d := newTestDriverWithCapacity(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`[]`))
	}, `[]`)

	_, err := d.GetCapacity(context.Background(), &csi.GetCapacityRequest{})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("GetCapacity error = %v, want Unavailable for missing capacity record", err)
	}
	if !strings.Contains(err.Error(), `configured pool "pool1" capacity lookup failed`) {
		t.Fatalf("GetCapacity error = %v, want configured-pool diagnostic", err)
	}
}

func TestCreateVolumeExistingLUNRemainsIdempotentWithoutCapacityRecord(t *testing.T) {
	d := newTestDriverWithCapacity(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/list"):
			w.Write([]byte(`[{"id":"lun-1","name":"pvcvol1","size":1073741824}]`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			w.Write([]byte(`{"id":"lun-1","name":"pvcvol1","size":1073741824}`))
		default:
			w.Write([]byte(`{"connected":1,"status":"Master","peers":[]}`))
		}
	}, `[]`)

	resp, err := d.CreateVolume(context.Background(), createVolumeRequestForTest("vol1", 1<<30))
	if err != nil {
		t.Fatalf("idempotent CreateVolume with missing capacity record: %v", err)
	}
	if got, want := resp.GetVolume().GetVolumeId(), helper.VolumeID(volumeIDPrefix, "lun-1"); got != want {
		t.Fatalf("volume ID = %q, want existing ID %q", got, want)
	}
}

func TestCreateVolumeRejectsInsufficientUsablePoolCapacity(t *testing.T) {
	createCalls := 0
	d := newTestDriverWithCapacity(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/list"):
			w.Write([]byte(`[]`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/api/v2/lun/"):
			createCalls++
			w.Write([]byte(`{"luns":[{"id":"unexpected","name":"pvcvol1"}]}`))
		default:
			w.Write([]byte(`{"connected":1,"status":"Master","peers":[]}`))
		}
	}, `[{"name":"pool1","logical":{"available":536870912,"reserved":0}}]`)

	_, err := d.CreateVolume(context.Background(), createVolumeRequestForTest("vol1", 1<<30))
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("CreateVolume error = %v, want ResourceExhausted for insufficient pool capacity", err)
	}
	if createCalls != 0 {
		t.Fatalf("backend LUN create calls = %d, want 0", createCalls)
	}
}

func TestCreateVolumeConcurrentSameNameExactlyOneBackendCreate(t *testing.T) {
	var mu sync.Mutex
	exists := false
	createCalls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/list"):
			if exists {
				w.Write([]byte(`[{"id":"lun-1","name":"pvcvol1","size":1073741824}]`))
			} else {
				w.Write([]byte(`[]`))
			}
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/api/v2/lun/"):
			createCalls++
			exists = true
			w.Write([]byte(`{"luns":[{"id":"lun-1","name":"pvcvol1"}]}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			w.Write([]byte(`{"id":"lun-1","name":"pvcvol1","size":1073741824}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	start := make(chan struct{})
	responses := make(chan *csi.CreateVolumeResponse, 2)
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			resp, err := d.CreateVolume(context.Background(), createVolumeRequestForTest("vol1", 1<<30))
			responses <- resp
			errs <- err
		}()
	}
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent CreateVolume: %v", err)
		}
	}
	for i := 0; i < 2; i++ {
		if got := (<-responses).GetVolume().GetVolumeId(); got != helper.VolumeID(volumeIDPrefix, "lun-1") {
			t.Fatalf("volume ID = %q, want %q", got, helper.VolumeID(volumeIDPrefix, "lun-1"))
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if createCalls != 1 {
		t.Fatalf("backend create calls = %d, want exactly 1", createCalls)
	}
}

func TestCreateVolumeSameNameIncompatibleReturnsAlreadyExists(t *testing.T) {
	createCalls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/list"):
			w.Write([]byte(`[{"id":"lun-1","name":"pvcvol1","size":1073741824}]`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			w.Write([]byte(`{"id":"lun-1","name":"pvcvol1","size":1073741824}`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/api/v2/lun/"):
			createCalls++
			w.Write([]byte(`{"luns":[{"id":"lun-2","name":"pvcvol1"}]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	_, err := d.CreateVolume(context.Background(), createVolumeRequestForTest("vol1", 2<<30))
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("code = %v, want AlreadyExists (err=%v)", status.Code(err), err)
	}
	if createCalls != 0 {
		t.Fatalf("backend create calls = %d, want 0", createCalls)
	}
}

func TestCreateVolumeConcurrentIncompatibleSameNameOneAlreadyExists(t *testing.T) {
	var mu sync.Mutex
	exists := false
	createdBlocksize := ""
	createCalls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/list"):
			if !exists {
				w.Write([]byte(`[]`))
				return
			}
			blockBytes := "4096"
			if createdBlocksize == "8k" {
				blockBytes = "8192"
			}
			w.Write([]byte(fmt.Sprintf(`[{"id":"lun-1","name":"pvcvol1","size":1073741824,"blocksize":%q}]`, blockBytes)))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/api/v2/lun/"):
			var body map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode create request: %v", err)
				return
			}
			createdBlocksize, _ = body["blocksize"].(string)
			createCalls++
			exists = true
			w.Write([]byte(`{"luns":[{"id":"lun-1","name":"pvcvol1"}]}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			blockBytes := "4096"
			if createdBlocksize == "8k" {
				blockBytes = "8192"
			}
			w.Write([]byte(fmt.Sprintf(`{"id":"lun-1","name":"pvcvol1","size":1073741824,"blocksize":%q}`, blockBytes)))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	req4k := createVolumeRequestForTest("vol1", 1<<30)
	req4k.Parameters = map[string]string{"blocksize": "4k"}
	req8k := createVolumeRequestForTest("vol1", 1<<30)
	req8k.Parameters = map[string]string{"blocksize": "8k"}

	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, req := range []*csi.CreateVolumeRequest{req4k, req8k} {
		req := req
		go func() {
			<-start
			_, err := d.CreateVolume(context.Background(), req)
			errs <- err
		}()
	}
	close(start)

	successes := 0
	alreadyExists := 0
	for i := 0; i < 2; i++ {
		err := <-errs
		switch status.Code(err) {
		case codes.OK:
			successes++
		case codes.AlreadyExists:
			alreadyExists++
		default:
			t.Fatalf("unexpected concurrent create error: %v", err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if successes != 1 || alreadyExists != 1 {
		t.Fatalf("results: success=%d AlreadyExists=%d, want 1/1", successes, alreadyExists)
	}
	if createCalls != 1 {
		t.Fatalf("backend create calls = %d, want exactly 1", createCalls)
	}
}

func TestCreateVolumeRoundsRequiredCapacityUpToGiB(t *testing.T) {
	var requestedGB int64
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/list"):
			w.Write([]byte(`[]`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/api/v2/lun/"):
			var body map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode create request: %v", err)
			}
			requestedGB = int64(body["size"].(float64))
			w.Write([]byte(`{"luns":[{"id":"lun-1","name":"pvcvol1"}]}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			w.Write([]byte(`{"id":"lun-1","name":"pvcvol1","size":2147483648}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	resp, err := d.CreateVolume(context.Background(), createVolumeRequestForTest("vol1", (1<<30)+(512<<20)))
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if requestedGB != 2 {
		t.Fatalf("backend create size = %d GiB, want 2", requestedGB)
	}
	if got := resp.GetVolume().GetCapacityBytes(); got != 2<<30 {
		t.Fatalf("response capacity = %d, want 2GiB", got)
	}
}

func TestCreateVolumeRejectsUnsatisfiableCapacityLimitBeforeMutation(t *testing.T) {
	backendCalls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		backendCalls++
		w.WriteHeader(http.StatusInternalServerError)
	})
	req := createVolumeRequestForTest("vol1", (1<<30)+(512<<20))
	req.CapacityRange.LimitBytes = (1 << 30) + (768 << 20)

	_, err := d.CreateVolume(context.Background(), req)
	if status.Code(err) != codes.OutOfRange {
		t.Fatalf("code = %v, want OutOfRange (err=%v)", status.Code(err), err)
	}
	if backendCalls != 0 {
		t.Fatalf("backend calls = %d, want 0 for unsatisfiable limit", backendCalls)
	}
}

func TestCreateVolumeIdempotencyLookupFailureFailsClosed(t *testing.T) {
	createCalls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/list"):
			w.WriteHeader(http.StatusInternalServerError)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/api/v2/lun/"):
			createCalls++
			w.Write([]byte(`{"luns":[{"id":"lun-1","name":"pvcvol1"}]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	_, err := d.CreateVolume(context.Background(), createVolumeRequestForTest("vol1", 1<<30))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %v, want Unavailable (err=%v)", status.Code(err), err)
	}
	if createCalls != 0 {
		t.Fatalf("backend create calls = %d, want 0 when idempotency lookup fails", createCalls)
	}
}

func TestCreateVolumeFailsClosedWithoutCoordinator(t *testing.T) {
	backendCalls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		backendCalls++
		w.WriteHeader(http.StatusInternalServerError)
	})
	d.coordination = nil

	_, err := d.CreateVolume(context.Background(), createVolumeRequestForTest("vol1", 1<<30))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %v, want Unavailable", status.Code(err))
	}
	if backendCalls != 0 {
		t.Fatalf("backend calls = %d, want 0 without coordinator", backendCalls)
	}
}

func TestGetSnapshotAndControllerGetVolume(t *testing.T) {
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/snapshot/snap-1"):
			w.Write([]byte(`{"id":"snap-1","volume_id":"lun-1","created":"1785499878","size":"1073741824","ready":true}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			w.Write([]byte(`{"id":"lun-1","size":1073741824}`))
		default:
			http.NotFound(w, r)
		}
	})
	snapshot, err := d.GetSnapshot(context.Background(), &csi.GetSnapshotRequest{SnapshotId: "snap-1"})
	if err != nil || snapshot.GetSnapshot().GetSnapshotId() != "snap-1" {
		t.Fatalf("GetSnapshot() = %#v, err=%v", snapshot, err)
	}
	volume, err := d.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{VolumeId: helper.VolumeID(volumeIDPrefix, "lun-1")})
	if err != nil || volume.GetVolume().GetVolumeId() != helper.VolumeID(volumeIDPrefix, "lun-1") || volume.GetVolume().GetCapacityBytes() != 1<<30 {
		t.Fatalf("ControllerGetVolume() = %#v, err=%v", volume, err)
	}
}

func TestDeleteVolumeIdempotent(t *testing.T) {
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/api/v2/lun/") {
			w.Write([]byte(`{"code":5011,"error":"not found"}`))
			return
		}
		w.Write([]byte(`{"connected":1,"status":"Master"}`))
	})

	if _, err := d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: helper.VolumeID(volumeIDPrefix, "vol1"),
	}); err != nil {
		t.Fatalf("DeleteVolume should be idempotent, got: %v", err)
	}
}

func TestDeleteVolumeFailsClosedWithActiveAttachment(t *testing.T) {
	deleteCalls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			w.Write([]byte(`{"id":"lun-1","name":"pvcvol1","protocols":{}}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc"):
			w.Write([]byte(`[]`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v2/snapshot/list"):
			w.Write([]byte(`[]`))
		case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/api/v2/lun/"):
			deleteCalls++
			w.Write([]byte(`{"success":true}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	d.attachmentLookup = &fakeAttachmentLookup{remaining: []string{"node-2"}}

	_, err := d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: helper.VolumeID(volumeIDPrefix, "lun-1")})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	if deleteCalls != 0 {
		t.Fatalf("backend delete calls = %d, want 0 with active attachment", deleteCalls)
	}
}

func TestDeleteVolumeFailsClosedWhenAttachmentLookupUnavailable(t *testing.T) {
	deleteCalls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			w.Write([]byte(`{"id":"lun-1","name":"pvcvol1","protocols":{}}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc"):
			w.Write([]byte(`[]`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v2/snapshot/list"):
			w.Write([]byte(`[]`))
		case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/api/v2/lun/"):
			deleteCalls++
			w.Write([]byte(`{"success":true}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	d.attachmentLookup = nil

	_, err := d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: helper.VolumeID(volumeIDPrefix, "lun-1")})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %v, want Unavailable (err=%v)", status.Code(err), err)
	}
	if deleteCalls != 0 {
		t.Fatalf("backend delete calls = %d, want 0 when attachment lookup is unavailable", deleteCalls)
	}
}

func TestDeleteVolumeMissingLUNIsIdempotentWhenAttachmentLookupUnavailable(t *testing.T) {
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-missing") {
			w.Write([]byte(`{"code":5011,"error":"not found"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	d.attachmentLookup = nil

	if _, err := d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: helper.VolumeID(volumeIDPrefix, "lun-missing")}); err != nil {
		t.Fatalf("DeleteVolume missing LUN should remain idempotent: %v", err)
	}
}

func TestDeleteVolumeFailsClosedForUnresolvedFCMapping(t *testing.T) {
	deleteCalls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			w.Write([]byte(`{"id":"lun-1","name":"pvcvol1","protocols":{"fc":"t1"}}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc"):
			w.Write([]byte(`[]`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v2/snapshot/list"):
			w.Write([]byte(`[]`))
		case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/api/v2/lun/"):
			deleteCalls++
			w.Write([]byte(`{"success":true}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	_, err := d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: helper.VolumeID(volumeIDPrefix, "lun-1")})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	if deleteCalls != 0 {
		t.Fatalf("backend delete calls = %d, want 0 for unresolved mapped LUN", deleteCalls)
	}
}

func TestDeleteVolumeCleansFCMappingBeforeDelete(t *testing.T) {
	mapped := true
	deleted := false
	removeCalls := 0
	deleteCalls := 0
	events := make([]string, 0, 4)
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			if deleted {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"code":5011,"error":"not found"}`))
				return
			}
			if mapped {
				w.Write([]byte(`{"id":"lun-1","name":"pvcvol1","protocols":{"fc":"tgt-a"}}`))
			} else {
				events = append(events, "verify-lun-unmapped")
				w.Write([]byte(`{"id":"lun-1","name":"pvcvol1","protocols":{}}`))
			}
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc"):
			w.Write([]byte(`[{"id":"t1","name":"tgt-a","owner":"o"}]`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc/t1"):
			if mapped {
				w.Write([]byte(`{"id":"t1","name":"tgt-a","owner":"o","luns":[{"id":"lun-1","number":"4"}]}`))
			} else {
				events = append(events, "verify-target-unmapped")
				w.Write([]byte(`{"id":"t1","name":"tgt-a","owner":"o","luns":[]}`))
			}
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v2/snapshot/list"):
			w.Write([]byte(`[]`))
		case r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc/lun/t1"):
			removeCalls++
			events = append(events, "remove")
			mapped = false
			w.Write([]byte(`{"success":true}`))
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			deleteCalls++
			deleted = true
			events = append(events, "delete")
			w.Write([]byte(`{"success":true}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	if _, err := d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: helper.VolumeID(volumeIDPrefix, "lun-1")}); err != nil {
		t.Fatalf("DeleteVolume mapped LUN: %v", err)
	}
	if removeCalls != 1 || deleteCalls != 1 {
		t.Fatalf("backend calls: remove=%d delete=%d, want 1/1", removeCalls, deleteCalls)
	}
	removeAt, verifyTargetAt, verifyLUNAt, deleteAt := -1, -1, -1, -1
	for i, event := range events {
		switch event {
		case "remove":
			removeAt = i
		case "verify-target-unmapped":
			if verifyTargetAt == -1 {
				verifyTargetAt = i
			}
		case "verify-lun-unmapped":
			if verifyLUNAt == -1 {
				verifyLUNAt = i
			}
		case "delete":
			deleteAt = i
		}
	}
	if !(removeAt >= 0 && verifyTargetAt > removeAt && verifyLUNAt > removeAt && deleteAt > verifyTargetAt && deleteAt > verifyLUNAt) {
		t.Fatalf("mapping cleanup/verification order = %v, want remove -> verified target+LUN unmapped -> delete", events)
	}
	coord := d.coordination.(*fakeCoordinator)
	coord.recordMu.Lock()
	defer coord.recordMu.Unlock()
	foundSet := false
	for _, keys := range coord.multiKeys {
		got := map[string]bool{}
		for _, key := range keys {
			got[key] = true
		}
		if got["volume:"+helper.VolumeID(volumeIDPrefix, "lun-1")] && got["fc-target:t1"] {
			foundSet = true
			break
		}
	}
	if !foundSet {
		t.Fatalf("DeleteVolume did not acquire mapped volume/target lock set: %v", coord.multiKeys)
	}
}

func TestDeleteVolumeUsesVolumeLock(t *testing.T) {
	deleted := false
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			if deleted {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"code":5011,"error":"not found"}`))
				return
			}
			w.Write([]byte(`{"id":"lun-1","name":"pvcvol1","protocols":{}}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc"):
			w.Write([]byte(`[]`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v2/snapshot/list"):
			w.Write([]byte(`[]`))
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			deleted = true
			w.Write([]byte(`{"success":true}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	if _, err := d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: helper.VolumeID(volumeIDPrefix, "lun-1")}); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}
	coord := d.coordination.(*fakeCoordinator)
	coord.recordMu.Lock()
	defer coord.recordMu.Unlock()
	found := false
	for _, key := range coord.acquireKeys {
		if key == "volume:"+helper.VolumeID(volumeIDPrefix, "lun-1") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("DeleteVolume did not acquire volume lock, keys=%v", coord.acquireKeys)
	}
}

func TestCreateVolumeRequiresCapabilities(t *testing.T) {
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"connected":1,"status":"Master"}`))
	})
	_, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{Name: "vol1"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateVolume without capabilities: code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestControllerPublishVolume(t *testing.T) {
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/target/fc"):
			// FCTargetService.List() → GET /api/v2/target/fc (lightweight list).
			w.Write([]byte(`[{"id":"t1","name":"tgt-a","owner":"o"}]`))
		case r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/target/fc/lun/"):
			w.Write([]byte(`{"success":true}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/target/fc/t1"):
			w.Write([]byte(`{"id":"t1","name":"tgt-a","owner":"o","ports":[{"id":"p1","name":"fc0/1","wwpns":[{"wwpn":"naa.2002a","owner":"o","target_id":"t1"}],"initiators":[]}],"luns":[{"id":"lun-1","name":"pvcvol1","number":"6","pool_name":"pool1","scsi_id":"3600"}]}`))
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/api/v2/lun/lun-1"):
			w.Write([]byte(`{"id":"lun-1","name":"pvcvol1","scsi_id":"3600","owner":"o"}`))
		default:
			w.Write([]byte(`{"connected":1,"status":"Master"}`))
		}
	})

	resp, err := d.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId:         helper.VolumeID(volumeIDPrefix, "lun-1"),
		NodeId:           "node-1",
		VolumeContext:    map[string]string{"fstype": "ext4"},
		VolumeCapability: &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}},
	})
	if err != nil {
		t.Fatalf("ControllerPublishVolume: %v", err)
	}
	pc := resp.PublishContext
	if pc["target_wwns"] != "2002a" {
		t.Fatalf("target_wwns = %q, want 2002a (naa. stripped)", pc["target_wwns"])
	}
	if pc["lun_number"] != "6" {
		t.Fatalf("lun_number = %q, want 6", pc["lun_number"])
	}
	if pc["scsi_id"] != "3600" {
		t.Fatalf("scsi_id = %q, want 3600", pc["scsi_id"])
	}
	coord := d.coordination.(*fakeCoordinator)
	coord.recordMu.Lock()
	defer coord.recordMu.Unlock()
	if len(coord.multiKeys) != 1 {
		t.Fatalf("AcquireMany calls = %d, want 1", len(coord.multiKeys))
	}
	gotLocks := map[string]bool{}
	for _, key := range coord.multiKeys[0] {
		gotLocks[key] = true
	}
	for _, want := range []string{"volume:" + helper.VolumeID(volumeIDPrefix, "lun-1"), "fc-target:t1"} {
		if !gotLocks[want] {
			t.Fatalf("publish lock set = %v, missing %q", coord.multiKeys[0], want)
		}
	}
}

func TestControllerExpandVolumeSendsIncrement(t *testing.T) {
	var extendSize int64
	var sizeBytes int64 = 1 << 30
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/api/v2/lun/lun-1"):
			w.Write([]byte(fmt.Sprintf(`{"id":"lun-1","name":"pvcvol1","size":%d}`, sizeBytes)))
		case r.Method == http.MethodPatch && strings.Contains(r.URL.Path, "/api/v2/lun/lun-1"):
			var body map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode expand request: %v", err)
			}
			extendSize = int64(body["size"].(float64))
			sizeBytes += extendSize << 30
			w.Write([]byte(`{"success":true}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	resp, err := d.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      helper.VolumeID(volumeIDPrefix, "lun-1"),
		CapacityRange: &csi.CapacityRange{RequiredBytes: 2 << 30},
	})
	if err != nil {
		t.Fatalf("ControllerExpandVolume: %v", err)
	}
	if extendSize != 1 {
		t.Fatalf("backend extend size = %d GiB, want 1 GiB increment", extendSize)
	}
	if resp.GetCapacityBytes() != 2<<30 || !resp.GetNodeExpansionRequired() {
		t.Fatalf("response = %+v", resp)
	}
}

func TestControllerExpandVolumeConcurrentSameTargetDoesNotDoubleGrow(t *testing.T) {
	var mu sync.Mutex
	sizeBytes := int64(1 << 30)
	patchCalls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/api/v2/lun/lun-1"):
			mu.Lock()
			current := sizeBytes
			mu.Unlock()
			w.Write([]byte(fmt.Sprintf(`{"id":"lun-1","name":"pvcvol1","size":%d}`, current)))
		case r.Method == http.MethodPatch && strings.Contains(r.URL.Path, "/api/v2/lun/lun-1"):
			var body map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode expand request: %v", err)
				return
			}
			increment := int64(body["size"].(float64))
			mu.Lock()
			patchCalls++
			sizeBytes += increment << 30
			mu.Unlock()
			w.Write([]byte(`{"success":true}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	req := &csi.ControllerExpandVolumeRequest{
		VolumeId:      helper.VolumeID(volumeIDPrefix, "lun-1"),
		CapacityRange: &csi.CapacityRange{RequiredBytes: 2 << 30},
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			_, err := d.ControllerExpandVolume(context.Background(), req)
			errs <- err
		}()
	}
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if sizeBytes != 2<<30 {
		t.Fatalf("final size = %d, want exactly 2GiB", sizeBytes)
	}
	if patchCalls != 1 {
		t.Fatalf("expand PATCH calls = %d, want exactly 1", patchCalls)
	}
}

func TestControllerModifyVolumeUsesVolumeLockAndVerifiesReadback(t *testing.T) {
	dedup := "off"
	patchCalls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			w.Write([]byte(fmt.Sprintf(`{"id":"lun-1","deduplication":%q}`, dedup)))
		case r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			var body map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode modify request: %v", err)
			}
			if got := fmt.Sprint(body["deduplication"]); got != "on" {
				t.Fatalf("deduplication patch = %q, want on", got)
			}
			patchCalls++
			dedup = "on"
			w.Write([]byte(`{"success":true}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	if _, err := d.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
		VolumeId: helper.VolumeID(volumeIDPrefix, "lun-1"),
		MutableParameters: map[string]string{
			"deduplication": "true",
		},
	}); err != nil {
		t.Fatalf("ControllerModifyVolume: %v", err)
	}
	if patchCalls != 1 {
		t.Fatalf("modify PATCH calls = %d, want 1", patchCalls)
	}
	coord := d.coordination.(*fakeCoordinator)
	coord.recordMu.Lock()
	defer coord.recordMu.Unlock()
	found := false
	for _, key := range coord.acquireKeys {
		if key == "volume:"+helper.VolumeID(volumeIDPrefix, "lun-1") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("ControllerModifyVolume did not acquire volume lock, keys=%v", coord.acquireKeys)
	}
}

func TestControllerModifyVolumeFailsClosedWithoutCoordinator(t *testing.T) {
	backendCalls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		backendCalls++
		w.WriteHeader(http.StatusInternalServerError)
	})
	d.coordination = nil

	_, err := d.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
		VolumeId:          helper.VolumeID(volumeIDPrefix, "lun-1"),
		MutableParameters: map[string]string{"deduplication": "true"},
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %v, want Unavailable (err=%v)", status.Code(err), err)
	}
	if backendCalls != 0 {
		t.Fatalf("backend calls = %d, want 0 without coordinator", backendCalls)
	}
}

func TestControllerModifyAndDeleteSerializeOnVolumeLock(t *testing.T) {
	modifyPatchEntered := make(chan struct{})
	allowModify := make(chan struct{})
	postPatchLUNGet := make(chan struct{}, 4)
	var stateMu sync.Mutex
	dedup := "off"
	patchStarted := false
	lunDeleted := false
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			stateMu.Lock()
			started := patchStarted
			currentDedup := dedup
			gone := lunDeleted
			stateMu.Unlock()
			if gone {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"code":5011,"error":"not found"}`))
				return
			}
			if started {
				select {
				case postPatchLUNGet <- struct{}{}:
				default:
				}
			}
			w.Write([]byte(fmt.Sprintf(`{"id":"lun-1","name":"pvcvol1","deduplication":%q,"protocols":{}}`, currentDedup)))
		case r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			stateMu.Lock()
			patchStarted = true
			stateMu.Unlock()
			close(modifyPatchEntered)
			<-allowModify
			stateMu.Lock()
			dedup = "on"
			stateMu.Unlock()
			w.Write([]byte(`{"success":true}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc"):
			w.Write([]byte(`[]`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v2/snapshot/list"):
			w.Write([]byte(`[]`))
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			stateMu.Lock()
			lunDeleted = true
			stateMu.Unlock()
			w.Write([]byte(`{"success":true}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	coord := d.coordination.(*fakeCoordinator)

	modifyErr := make(chan error, 1)
	go func() {
		_, err := d.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
			VolumeId:          helper.VolumeID(volumeIDPrefix, "lun-1"),
			MutableParameters: map[string]string{"deduplication": "true"},
		})
		modifyErr <- err
	}()
	select {
	case <-modifyPatchEntered:
	case <-time.After(time.Second):
		t.Fatal("ControllerModifyVolume did not reach PATCH")
	}

	deleteErr := make(chan error, 1)
	go func() {
		_, err := d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: helper.VolumeID(volumeIDPrefix, "lun-1")})
		deleteErr <- err
	}()

	volumeKey := "volume:" + helper.VolumeID(volumeIDPrefix, "lun-1")
	attempts := 0
	for attempts < 2 {
		select {
		case key := <-coord.attempts:
			if key == volumeKey {
				attempts++
			}
		case <-time.After(time.Second):
			t.Fatal("DeleteVolume did not attempt shared volume lock")
		}
	}
	select {
	case <-postPatchLUNGet:
		t.Fatal("DeleteVolume reached LUN GET while ControllerModifyVolume held volume lock")
	default:
	}

	close(allowModify)
	if err := <-modifyErr; err != nil {
		t.Fatalf("ControllerModifyVolume: %v", err)
	}
	if err := <-deleteErr; err != nil {
		t.Fatalf("DeleteVolume after modify: %v", err)
	}
}

func TestControllerUnpublishRemovesOnlyMappedTarget(t *testing.T) {
	removeCalls := 0
	mapped := true
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc"):
			w.Write([]byte(`[{"id":"t1","name":"tgt-a"},{"id":"t2","name":"tgt-b"}]`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc/t1"):
			if mapped {
				w.Write([]byte(`{"id":"t1","name":"tgt-a","ports":[],"luns":[{"id":"lun-1","number":4}]}`))
			} else {
				w.Write([]byte(`{"id":"t1","name":"tgt-a","ports":[],"luns":[]}`))
			}
		case r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc/lun/t1"):
			removeCalls++
			mapped = false
			w.Write([]byte(`{"success":true}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			w.Write([]byte(`{"id":"lun-1","protocols":{}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	d.fcTargetNames = []string{"tgt-a", "tgt-b"}
	d.attachmentLookup = &fakeAttachmentLookup{remaining: nil}

	if _, err := d.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: helper.VolumeID(volumeIDPrefix, "lun-1"),
		NodeId:   "node-1",
	}); err != nil {
		t.Fatalf("ControllerUnpublishVolume: %v", err)
	}
	if removeCalls != 1 {
		t.Fatalf("RemoveLUN calls = %d, want exactly 1", removeCalls)
	}
}

func TestControllerUnpublishRetainsMappingForRemainingNodes(t *testing.T) {
	removeCalls := 0
	mapped := true
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc"):
			w.Write([]byte(`[{"id":"t1","name":"tgt-a"}]`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc/t1"):
			if mapped {
				w.Write([]byte(`{"id":"t1","name":"tgt-a","ports":[],"luns":[{"id":"lun-1","number":4}]}`))
			} else {
				w.Write([]byte(`{"id":"t1","name":"tgt-a","ports":[],"luns":[]}`))
			}
		case r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc/lun/t1"):
			removeCalls++
			mapped = false
			w.Write([]byte(`{"success":true}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			w.Write([]byte(`{"id":"lun-1","protocols":{}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	d.fcTargetNames = []string{"tgt-a"}

	// A second node still holds a VolumeAttachment for the PV (MULTI_NODE_
	// READER_ONLY); the shared FC target mapping must outlive node-1.
	d.attachmentLookup = &fakeAttachmentLookup{remaining: []string{"node-2"}}

	if _, err := d.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: helper.VolumeID(volumeIDPrefix, "lun-1"),
		NodeId:   "node-1",
	}); err != nil {
		t.Fatalf("ControllerUnpublishVolume node-1: %v", err)
	}
	if removeCalls != 0 {
		t.Fatalf("RemoveLUN calls = %d, want 0 while node-2 still holds the mapping", removeCalls)
	}

	// Last node unpublishes: no remaining attachment, mapping removed.
	d.attachmentLookup = &fakeAttachmentLookup{remaining: nil}
	if _, err := d.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: helper.VolumeID(volumeIDPrefix, "lun-1"),
		NodeId:   "node-2",
	}); err != nil {
		t.Fatalf("ControllerUnpublishVolume node-2: %v", err)
	}
	if removeCalls != 1 {
		t.Fatalf("RemoveLUN calls = %d, want 1 after the last node unpublishes", removeCalls)
	}
}

func TestControllerPublishVolumeContextOwnerTakesPrecedence(t *testing.T) {
	var addLUNOwner string
	var addLUNPath string
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/target/fc"):
			// tgt-a belongs to controller B and comes first in the config;
			// the LUN lives on controller A, so tgt-b must be selected.
			w.Write([]byte(`[{"id":"t1","name":"tgt-a","owner":"SGZ1RVC2_B"},{"id":"t2","name":"tgt-b","owner":"SGZ1RVC2_A"}]`))
		case r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/target/fc/lun/"):
			var body map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode AddLUN: %v", err)
			}
			addLUNOwner, _ = body["owner"].(string)
			addLUNPath = r.URL.Path
			w.Write([]byte(`{"success":true}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			w.Write([]byte(`{"id":"lun-1","name":"pvcvol1","scsi_id":"3600","owner":"SGZ1RVC2_A"}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/target/fc/t2"):
			w.Write([]byte(`{"id":"t2","name":"tgt-b","owner":"SGZ1RVC2_A","ports":[{"id":"p1","name":"fc0/1","wwpns":[{"wwpn":"naa.2002b","owner":"SGZ1RVC2_A","target_id":"t2"}],"initiators":[]}],"luns":[{"id":"lun-1","name":"pvcvol1","number":"6","pool_name":"test2","scsi_id":"3600"}]}`))
		default:
			w.Write([]byte(`{"connected":1,"status":"Master"}`))
		}
	})
	d.fcTargetNames = []string{"tgt-a", "tgt-b"}

	_, err := d.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId:         helper.VolumeID(volumeIDPrefix, "lun-1"),
		NodeId:           "node-1",
		VolumeContext:    map[string]string{"owner": "SGZ1RVC2_A", "fstype": "ext4"},
		VolumeCapability: &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}},
	})
	if err != nil {
		t.Fatalf("ControllerPublishVolume: %v", err)
	}
	if !strings.HasSuffix(addLUNPath, "/target/fc/lun/t2") {
		t.Fatalf("AddLUN target = %s, want t2 (owner SGZ1RVC2_A)", addLUNPath)
	}
	if addLUNOwner != "SGZ1RVC2_A" {
		t.Fatalf("AddLUN owner = %q, want SGZ1RVC2_A", addLUNOwner)
	}
}

func TestControllerUnpublishVolumeUsesPoolOwner(t *testing.T) {
	var removeOwner string
	mapped := true
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc"):
			w.Write([]byte(`[{"id":"t1","name":"tgt-a"},{"id":"t2","name":"tgt-b"}]`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc/t1"):
			if mapped {
				w.Write([]byte(`{"id":"t1","name":"tgt-a","ports":[],"luns":[{"id":"lun-1","number":4}]}`))
			} else {
				w.Write([]byte(`{"id":"t1","name":"tgt-a","ports":[],"luns":[]}`))
			}
		case r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc/lun/t1"):
			var body map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode RemoveLUN: %v", err)
			}
			removeOwner, _ = body["owner"].(string)
			mapped = false
			w.Write([]byte(`{"success":true}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			w.Write([]byte(`{"id":"lun-1","protocols":{}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	d.fcTargetNames = []string{"tgt-a", "tgt-b"}
	d.attachmentLookup = &fakeAttachmentLookup{remaining: nil}

	if _, err := d.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: helper.VolumeID(volumeIDPrefix, "lun-1"),
		NodeId:   "node-1",
	}); err != nil {
		t.Fatalf("ControllerUnpublishVolume: %v", err)
	}
	if removeOwner != "o" {
		t.Fatalf("RemoveLUN owner = %q, want %q (pool owner)", removeOwner, "o")
	}
}

func TestCreateVolumeFromSnapshotReturnsContentSource(t *testing.T) {
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/snapshot/snap-1"):
			w.Write([]byte(`{"id":"snap-1","name":"snap1","volume_id":"lun-src","volume_name":"src"}`))
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/api/v2/lun/list"):
			w.Write([]byte(`[]`))
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/api/v2/snapshot/clone/snap-1"):
			w.Write([]byte(`{"volume_id":"lun-clone"}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-clone"):
			w.Write([]byte(`{"id":"lun-clone","name":"pvcrestore","size":1073741824,"clone":"src@snap1"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	source := &csi.VolumeContentSource{
		Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "snap-1"},
		},
	}
	resp, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:                "restore",
		CapacityRange:       &csi.CapacityRange{RequiredBytes: 1 << 30},
		VolumeContentSource: source,
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"}},
		}},
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if resp.GetVolume().GetContentSource().GetSnapshot().GetSnapshotId() != "snap-1" {
		t.Fatalf("content source = %+v, want snapshot snap-1", resp.GetVolume().GetContentSource())
	}
	coord := d.coordination.(*fakeCoordinator)
	coord.recordMu.Lock()
	defer coord.recordMu.Unlock()
	if len(coord.multiKeys) != 1 {
		t.Fatalf("AcquireMany calls = %d, want 1", len(coord.multiKeys))
	}
	got := map[string]bool{}
	for _, key := range coord.multiKeys[0] {
		got[key] = true
	}
	for _, want := range []string{"create-volume:pvcrestore", "snapshot:snap-1"} {
		if !got[want] {
			t.Fatalf("clone lock set = %v, missing %q", coord.multiKeys[0], want)
		}
	}
}

func TestCreateSnapshotReturnsMetadata(t *testing.T) {
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/api/v2/lun/lun-1"):
			w.Write([]byte(`{"id":"lun-1","size":"1073741824"}`))
		case r.Method == http.MethodGet && (r.URL.Path == "/api/v2/snapshot" || r.URL.Path == "/api/v2/snapshot/list"):
			w.Write([]byte(`[]`))
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/api/v2/snapshot"):
			w.Write([]byte(`{"id":"snap-1","name":"pvcsnapshot1","volume_id":"lun-1","created":"1700000000"}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/snapshot/snap-1"):
			w.Write([]byte(`{"id":"snap-1","name":"pvcsnapshot1","volume_id":"lun-1","reserved":"1073741824","created":"1700000000","ready":true}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	resp, err := d.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name:           "snapshot-1",
		SourceVolumeId: helper.VolumeID(volumeIDPrefix, "lun-1"),
	})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	snap := resp.GetSnapshot()
	if snap.GetSizeBytes() != 1<<30 || snap.GetCreationTime().GetSeconds() != 1700000000 {
		t.Fatalf("snapshot metadata = %+v", snap)
	}
	coord := d.coordination.(*fakeCoordinator)
	coord.recordMu.Lock()
	defer coord.recordMu.Unlock()
	if len(coord.multiKeys) != 1 {
		t.Fatalf("AcquireMany calls = %d, want 1", len(coord.multiKeys))
	}
	gotLocks := map[string]bool{}
	for _, key := range coord.multiKeys[0] {
		gotLocks[key] = true
	}
	for _, want := range []string{"snapshot-name:pvcsnapshot1", "volume:" + helper.VolumeID(volumeIDPrefix, "lun-1")} {
		if !gotLocks[want] {
			t.Fatalf("snapshot lock set = %v, missing %q", coord.multiKeys[0], want)
		}
	}
}

func TestCreateSnapshotConcurrentSameNameSourceExactlyOneBackendCreate(t *testing.T) {
	var mu sync.Mutex
	exists := false
	createCalls := 0
	sourceListCalls := 0
	globalListCalls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			w.Write([]byte(`{"id":"lun-1","size":1073741824}`))
		case r.Method == http.MethodGet && (r.URL.Path == "/api/v2/snapshot" || r.URL.Path == "/api/v2/snapshot/list"):
			if r.URL.Query().Get("volume_id") == "lun-1" {
				sourceListCalls++
			} else {
				globalListCalls++
			}
			if exists {
				w.Write([]byte(`[{"id":"snap-1","name":"pvcsnapshot1","volume_id":"lun-1","reserved":"1073741824","created":"1700000000","ready":true}]`))
			} else {
				w.Write([]byte(`[]`))
			}
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/snapshot":
			createCalls++
			exists = true
			w.Write([]byte(`{"id":"snap-1","name":"pvcsnapshot1","volume_id":"lun-1"}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/snapshot/snap-1"):
			w.Write([]byte(`{"id":"snap-1","name":"pvcsnapshot1","volume_id":"lun-1","reserved":"1073741824","created":"1700000000","ready":true}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	req := &csi.CreateSnapshotRequest{Name: "snapshot-1", SourceVolumeId: helper.VolumeID(volumeIDPrefix, "lun-1")}
	start := make(chan struct{})
	responses := make(chan *csi.CreateSnapshotResponse, 2)
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			resp, err := d.CreateSnapshot(context.Background(), req)
			responses <- resp
			errs <- err
		}()
	}
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent CreateSnapshot: %v", err)
		}
	}
	for i := 0; i < 2; i++ {
		if got := (<-responses).GetSnapshot().GetSnapshotId(); got != "snap-1" {
			t.Fatalf("snapshot ID = %q, want snap-1", got)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if createCalls != 1 {
		t.Fatalf("backend snapshot create calls = %d, want exactly 1", createCalls)
	}
	if sourceListCalls != 2 || globalListCalls != 1 {
		t.Fatalf("snapshot list calls: source=%d global=%d, want source=2 global=1", sourceListCalls, globalListCalls)
	}
}

func TestCreateSnapshotSameNameDifferentSourceReturnsAlreadyExists(t *testing.T) {
	createCalls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-2"):
			w.Write([]byte(`{"id":"lun-2","size":1073741824}`))
		case r.Method == http.MethodGet && (r.URL.Path == "/api/v2/snapshot" || r.URL.Path == "/api/v2/snapshot/list"):
			if r.URL.Query().Get("volume_id") == "lun-2" {
				w.Write([]byte(`[]`))
			} else {
				w.Write([]byte(`[{"id":"snap-1","name":"pvcsnapshot1","volume_id":"lun-1"}]`))
			}
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/snapshot":
			createCalls++
			w.Write([]byte(`{"id":"snap-2"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	_, err := d.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name: "snapshot-1", SourceVolumeId: helper.VolumeID(volumeIDPrefix, "lun-2"),
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("code = %v, want AlreadyExists (err=%v)", status.Code(err), err)
	}
	if createCalls != 0 {
		t.Fatalf("backend snapshot create calls = %d, want 0", createCalls)
	}
}

func TestDeleteSnapshotIdempotentAndLocked(t *testing.T) {
	deleteCalls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/snapshot/snap-missing"):
			w.Write([]byte(`{"code":5011,"error":"not found"}`))
		case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/api/v2/snapshot/"):
			deleteCalls++
			w.Write([]byte(`{"success":true}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	if _, err := d.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{SnapshotId: "snap-missing"}); err != nil {
		t.Fatalf("DeleteSnapshot idempotent missing: %v", err)
	}
	if deleteCalls != 0 {
		t.Fatalf("backend delete calls = %d, want 0 for missing snapshot", deleteCalls)
	}
	coord := d.coordination.(*fakeCoordinator)
	coord.recordMu.Lock()
	defer coord.recordMu.Unlock()
	found := false
	for _, key := range coord.acquireKeys {
		if key == "snapshot:snap-missing" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("DeleteSnapshot did not acquire snapshot:snap-missing, keys=%v", coord.acquireKeys)
	}
}

func TestCloneHoldsSnapshotLockAgainstDeleteSnapshot(t *testing.T) {
	cloneEntered := make(chan struct{})
	allowClone := make(chan struct{})
	deleteBackendEntered := make(chan struct{}, 1)
	var snapDeleted atomic.Bool
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/snapshot/snap-1"):
			if snapDeleted.Load() {
				// Honest backend: after a successful delete the snapshot is
				// absent (authoritative NotFound), so the driver's bounded
				// absence verification converges instead of polling forever.
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"code":5011,"error":"not found"}`))
				return
			}
			select {
			case deleteBackendEntered <- struct{}{}:
			default:
			}
			w.Write([]byte(`{"id":"snap-1","name":"snap1","volume_id":"lun-src","volume_name":"src"}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/list"):
			w.Write([]byte(`[]`))
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/api/v2/snapshot/clone/snap-1"):
			close(cloneEntered)
			<-allowClone
			w.Write([]byte(`{"volume_id":"lun-clone"}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-clone"):
			w.Write([]byte(`{"id":"lun-clone","name":"pvcrestore","size":1073741824,"clone":"src@snap1"}`))
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/api/v2/snapshot/snap-1"):
			select {
			case deleteBackendEntered <- struct{}{}:
			default:
			}
			snapDeleted.Store(true)
			w.Write([]byte(`{"success":true}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	coord := d.coordination.(*fakeCoordinator)

	source := &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
		Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "snap-1"},
	}}
	cloneReq := createVolumeRequestForTest("restore", 1<<30)
	cloneReq.VolumeContentSource = source
	cloneErr := make(chan error, 1)
	go func() {
		_, err := d.CreateVolume(context.Background(), cloneReq)
		cloneErr <- err
	}()

	select {
	case <-cloneEntered:
	case <-time.After(time.Second):
		t.Fatal("clone did not reach backend")
	}
	// The source-snapshot GET performed by the clone happens before the clone
	// mutation and is expected. Drain its observation so only delete-side
	// backend entry is considered below.
	select {
	case <-deleteBackendEntered:
	default:
	}

	deleteErr := make(chan error, 1)
	go func() {
		_, err := d.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{SnapshotId: "snap-1"})
		deleteErr <- err
	}()

	// Wait until DeleteSnapshot has attempted the same semantic snapshot lock.
	for {
		select {
		case key := <-coord.attempts:
			if key == "snapshot:snap-1" {
				goto deleteWaiting
			}
		case <-time.After(time.Second):
			t.Fatal("DeleteSnapshot did not attempt snapshot:snap-1 lock")
		}
	}

deleteWaiting:
	select {
	case <-deleteBackendEntered:
		t.Fatal("DeleteSnapshot reached backend while clone still held snapshot lock")
	default:
	}
	close(allowClone)
	if err := <-cloneErr; err != nil {
		t.Fatalf("CreateVolume clone: %v", err)
	}
	if err := <-deleteErr; err != nil {
		t.Fatalf("DeleteSnapshot after clone: %v", err)
	}
}

func TestListSnapshotsFiltersSourceAndPaginates(t *testing.T) {
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/api/v2/snapshot") {
			w.Write([]byte(`[{"id":"s1","volume_id":"lun-1","reserved":"1073741824","created":"1700000000","ready":true},{"id":"s2","volume_id":"lun-2"},{"id":"s3","volume_id":"lun-1"}]`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	resp, err := d.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
		SourceVolumeId: helper.VolumeID(volumeIDPrefix, "lun-1"), MaxEntries: 1,
	})
	if err != nil || len(resp.GetEntries()) != 1 || resp.GetEntries()[0].GetSnapshot().GetSnapshotId() != "s1" || resp.GetEntries()[0].GetSnapshot().GetSourceVolumeId() != helper.VolumeID(volumeIDPrefix, "lun-1") || resp.GetEntries()[0].GetSnapshot().GetSizeBytes() != 1<<30 || resp.GetNextToken() != "s1:s1" {
		t.Fatalf("first page = %+v, err=%v", resp, err)
	}
	resp, err = d.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
		SourceVolumeId: helper.VolumeID(volumeIDPrefix, "lun-1"), StartingToken: "s1:s1", MaxEntries: 1,
	})
	if err != nil || len(resp.GetEntries()) != 1 || resp.GetEntries()[0].GetSnapshot().GetSnapshotId() != "s3" || resp.GetNextToken() != "" {
		t.Fatalf("second page = %+v, err=%v", resp, err)
	}
}

func TestListSnapshotsRejectsNegativeMaxEntries(t *testing.T) {
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNotFound) })
	_, err := d.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{MaxEntries: -1})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code=%v, want InvalidArgument", status.Code(err))
	}
}

func TestListSnapshotsRejectsPastEndToken(t *testing.T) {
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`[{"id":"s1","volume_id":"lun-1"}]`))
	})
	_, err := d.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{StartingToken: "2"})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("code=%v, want Aborted", status.Code(err))
	}
}

func TestListVolumesPaginates(t *testing.T) {
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/list") {
			w.Write([]byte(`[{"id":"l1","size":2147483648},{"id":"l2","size":4294967296}]`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	resp, err := d.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: 1})
	if err != nil || len(resp.GetEntries()) != 1 || resp.GetNextToken() != "v1:"+helper.VolumeID(volumeIDPrefix, "l1") {
		t.Fatalf("page=%+v err=%v", resp, err)
	}
	if got := resp.GetEntries()[0].GetVolume().GetCapacityBytes(); got != 2147483648 {
		t.Fatalf("capacity=%d, want 2147483648", got)
	}
}

// TestListVolumesStableIDCursorWalk pins the token round-trip: a full
// paginated walk over an unsorted backend list returns every volume exactly
// once in stable ID order, and max_entries=0 returns the complete list
// without a token.
func TestListVolumesStableIDCursorWalk(t *testing.T) {
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/list") {
			w.Write([]byte(`[{"id":"l3","size":1073741824},{"id":"l1","size":1073741824},{"id":"l2","size":1073741824}]`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})

	var walked []string
	token := ""
	for i := 0; ; i++ {
		if i > 10 {
			t.Fatalf("pagination walk did not terminate; walked=%v", walked)
		}
		resp, err := d.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: 1, StartingToken: token})
		if err != nil {
			t.Fatalf("walk page %d: %v", i, err)
		}
		if len(resp.GetEntries()) != 1 {
			t.Fatalf("walk page %d entries = %d, want 1", i, len(resp.GetEntries()))
		}
		walked = append(walked, resp.GetEntries()[0].GetVolume().GetVolumeId())
		if resp.GetNextToken() == "" {
			break
		}
		token = resp.GetNextToken()
	}
	want := []string{
		helper.VolumeID(volumeIDPrefix, "l1"),
		helper.VolumeID(volumeIDPrefix, "l2"),
		helper.VolumeID(volumeIDPrefix, "l3"),
	}
	if len(walked) != len(want) {
		t.Fatalf("walked = %v, want %v", walked, want)
	}
	for i := range want {
		if walked[i] != want[i] {
			t.Fatalf("walked = %v, want stable ID order %v", walked, want)
		}
	}

	resp, err := d.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil || len(resp.GetEntries()) != 3 || resp.GetNextToken() != "" {
		t.Fatalf("unbounded list = %+v err=%v, want all 3 entries and no token", resp, err)
	}
}

// TestListVolumesInvalidStartingTokenAborted pins CSI token semantics: a
// starting_token that is not a v1:<volumeId> cursor (including legacy index
// tokens) is Aborted.
func TestListVolumesInvalidStartingTokenAborted(t *testing.T) {
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/list") {
			w.Write([]byte(`[{"id":"l1","size":1073741824}]`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	for _, token := range []string{"2", "-1", "v1:", "v1", "s1:l1", "index:2"} {
		t.Run(token, func(t *testing.T) {
			_, err := d.ListVolumes(context.Background(), &csi.ListVolumesRequest{StartingToken: token})
			if status.Code(err) != codes.Aborted {
				t.Fatalf("token %q: code=%v, want Aborted (err=%v)", token, status.Code(err), err)
			}
		})
	}
}

// TestListVolumesCursorIDDeletedBetweenPages pins cursor resolution when the
// cursor volume itself is deleted between pages: the walk resumes after the
// cursor's sort position instead of failing or repeating entries.
func TestListVolumesCursorIDDeletedBetweenPages(t *testing.T) {
	listCalls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/list") {
			listCalls++
			if listCalls == 1 {
				w.Write([]byte(`[{"id":"l1","size":1073741824},{"id":"l2","size":1073741824},{"id":"l3","size":1073741824}]`))
			} else {
				// The cursor volume l2 was deleted between pages.
				w.Write([]byte(`[{"id":"l1","size":1073741824},{"id":"l3","size":1073741824}]`))
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	page1, err := d.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: 2})
	if err != nil || page1.GetNextToken() != "v1:"+helper.VolumeID(volumeIDPrefix, "l2") {
		t.Fatalf("page1 = %+v err=%v, want a v1: cursor on l2", page1, err)
	}
	page2, err := d.ListVolumes(context.Background(), &csi.ListVolumesRequest{StartingToken: page1.GetNextToken()})
	if err != nil {
		t.Fatalf("page2 with a deleted cursor ID: %v", err)
	}
	if len(page2.GetEntries()) != 1 || page2.GetEntries()[0].GetVolume().GetVolumeId() != helper.VolumeID(volumeIDPrefix, "l3") {
		t.Fatalf("page2 = %+v, want [l3]", page2)
	}
}

// TestListSnapshotsStableIDCursorWalk is the ListSnapshots counterpart of the
// volume token round-trip: stable ID order, no duplicates, max_entries=0
// returns everything without a token.
func TestListSnapshotsStableIDCursorWalk(t *testing.T) {
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v2/snapshot/list") {
			w.Write([]byte(`[{"id":"s3","volume_id":"lun-1"},{"id":"s1","volume_id":"lun-1"},{"id":"s2","volume_id":"lun-1"}]`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})

	var walked []string
	token := ""
	for i := 0; ; i++ {
		if i > 10 {
			t.Fatalf("pagination walk did not terminate; walked=%v", walked)
		}
		resp, err := d.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{MaxEntries: 1, StartingToken: token})
		if err != nil {
			t.Fatalf("walk page %d: %v", i, err)
		}
		if len(resp.GetEntries()) != 1 {
			t.Fatalf("walk page %d entries = %d, want 1", i, len(resp.GetEntries()))
		}
		walked = append(walked, resp.GetEntries()[0].GetSnapshot().GetSnapshotId())
		if resp.GetNextToken() == "" {
			break
		}
		token = resp.GetNextToken()
	}
	want := []string{"s1", "s2", "s3"}
	if len(walked) != len(want) {
		t.Fatalf("walked = %v, want %v", walked, want)
	}
	for i := range want {
		if walked[i] != want[i] {
			t.Fatalf("walked = %v, want stable ID order %v", walked, want)
		}
	}

	resp, err := d.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{})
	if err != nil || len(resp.GetEntries()) != 3 || resp.GetNextToken() != "" {
		t.Fatalf("unbounded list = %+v err=%v, want all 3 entries and no token", resp, err)
	}
}

// TestListSnapshotsInvalidStartingTokenAborted pins CSI token semantics for
// snapshots: a starting_token that is not an s1:<snapshotId> cursor
// (including legacy index tokens) is Aborted.
func TestListSnapshotsInvalidStartingTokenAborted(t *testing.T) {
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v2/snapshot/list") {
			w.Write([]byte(`[{"id":"s1","volume_id":"lun-1"}]`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	for _, token := range []string{"2", "-1", "s1:", "v1:s1", "index:2"} {
		t.Run(token, func(t *testing.T) {
			_, err := d.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{StartingToken: token})
			if status.Code(err) != codes.Aborted {
				t.Fatalf("token %q: code=%v, want Aborted (err=%v)", token, status.Code(err), err)
			}
		})
	}
}

// TestListSnapshotsCursorIDDeletedBetweenPages pins cursor resolution when
// the cursor snapshot itself is deleted between pages.
func TestListSnapshotsCursorIDDeletedBetweenPages(t *testing.T) {
	listCalls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v2/snapshot/list") {
			listCalls++
			if listCalls == 1 {
				w.Write([]byte(`[{"id":"s1","volume_id":"lun-1"},{"id":"s2","volume_id":"lun-1"},{"id":"s3","volume_id":"lun-1"}]`))
			} else {
				// The cursor snapshot s2 was deleted between pages.
				w.Write([]byte(`[{"id":"s1","volume_id":"lun-1"},{"id":"s3","volume_id":"lun-1"}]`))
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	page1, err := d.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{MaxEntries: 2})
	if err != nil || page1.GetNextToken() != "s1:s2" {
		t.Fatalf("page1 = %+v err=%v, want an s1: cursor on s2", page1, err)
	}
	page2, err := d.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{StartingToken: page1.GetNextToken()})
	if err != nil {
		t.Fatalf("page2 with a deleted cursor ID: %v", err)
	}
	if len(page2.GetEntries()) != 1 || page2.GetEntries()[0].GetSnapshot().GetSnapshotId() != "s3" {
		t.Fatalf("page2 = %+v, want [s3]", page2)
	}
}

func TestStripNAA(t *testing.T) {
	if fc.StripNAA("naa.2002a") != "2002a" {
		t.Fatal("stripNAA should remove naa. prefix")
	}
	if fc.StripNAA("2002a") != "2002a" {
		t.Fatal("stripNAA without prefix should be unchanged")
	}
}

func TestControllerUnpublishFailClosedOnLookupError(t *testing.T) {
	removeCalls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc"):
			w.Write([]byte(`[{"id":"t1","name":"tgt-a"}]`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc/t1"):
			w.Write([]byte(`{"id":"t1","name":"tgt-a","ports":[],"luns":[{"id":"lun-1","number":4}]}`))
		case r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc/lun/t1"):
			removeCalls++
			w.Write([]byte(`{"success":true}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			w.Write([]byte(`{"id":"lun-1","protocols":{}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	d.fcTargetNames = []string{"tgt-a"}
	d.attachmentLookup = &fakeAttachmentLookup{err: errors.New("api timeout")}

	_, err := d.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: helper.VolumeID(volumeIDPrefix, "lun-1"),
		NodeId:   "node-1",
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("lookup error must fail closed with Unavailable, got %v", err)
	}
	if removeCalls != 0 {
		t.Fatalf("RemoveLUN calls = %d, want 0 on lookup failure", removeCalls)
	}
}

func TestControllerUnpublishAmbiguousPVIsFailedPrecondition(t *testing.T) {
	removeCalls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc"):
			w.Write([]byte(`[{"id":"t1","name":"tgt-a"}]`))
		case r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc/lun/t1"):
			removeCalls++
			w.Write([]byte(`{"success":true}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	d.fcTargetNames = []string{"tgt-a"}
	d.attachmentLookup = &fakeAttachmentLookup{err: helper.ErrAmbiguousVolumePV{Count: 2}}

	_, err := d.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: helper.VolumeID(volumeIDPrefix, "lun-1"),
		NodeId:   "node-1",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ambiguous PV must return FailedPrecondition, got %v", err)
	}
	if removeCalls != 0 {
		t.Fatalf("RemoveLUN calls = %d, want 0 on ambiguity", removeCalls)
	}
}

func TestLocalSanityLookupNodeExists(t *testing.T) {
	lookup := localSanityLookup{nodeID: "local-sanity"}
	for _, nodeID := range []string{"local-sanity", "unknown-node", ""} {
		t.Run(nodeID, func(t *testing.T) {
			exists, err := lookup.NodeExists(context.Background(), nodeID)
			want := nodeID == "local-sanity"
			if err != nil || exists != want {
				t.Fatalf("NodeExists(%q) = %v, %v; want %v, nil", nodeID, exists, err, want)
			}
		})
	}
}

type localSanityLookup struct{ nodeID string }

func (l localSanityLookup) NodeExists(_ context.Context, nodeID string) (bool, error) {
	return l.nodeID != "" && nodeID == l.nodeID, nil
}
func (localSanityLookup) RemainingNodes(context.Context, string, string, string) ([]string, error) {
	return nil, nil
}

// TestLocalSanityBackendPreflight verifies the live backend is ready before
// the shell runner asks for root privileges to access FC devices.
func TestLocalSanityBackendPreflight(t *testing.T) {
	if os.Getenv("FC_LIVE_MOCK_PREFLIGHT") != "1" {
		t.Skip("explicit backend preflight opt-in required")
	}
	_, err := NewDriver(InputParameters{
		Endpoint: "unix:///sanity-preflight.sock", Role: "all", NodeId: "local-sanity",
		Ip: os.Getenv("STORAGE_IPS"), PoolName: os.Getenv("STORAGE_POOL_NAME"),
		ApiKey: os.Getenv("STORAGE_API_KEY"), FcTargetNames: os.Getenv("STORAGE_FC_TARGET_NAMES"),
	})
	if err != nil {
		t.Fatalf("live FC backend preflight: %v", err)
	}
}

// TestLocalSanityWithMockCoordination runs csi-sanity against the live FC
// backend with test-process-only coordination. It does not validate Lease
// behavior or protect against another driver process using the same backend.
func TestLocalSanityWithMockCoordination(t *testing.T) {
	if os.Getenv("FC_LIVE_MOCK_SANITY") != "1" {
		t.Skip("explicit live mock sanity opt-in required")
	}
	focus := os.Getenv("SANITY_FOCUS_FILE")
	fullSuite := os.Getenv("SANITY_FULL") == "1"
	if focus == "" && !fullSuite {
		t.Fatal("SANITY_FOCUS_FILE must select one sanity spec")
	}
	if focus != "" && fullSuite {
		t.Fatal("set either SANITY_FOCUS_FILE or SANITY_FULL=1, not both")
	}
	sanityBin := os.Getenv("FC_SANITY_BIN")
	configDir := os.Getenv("FC_SANITY_CONFIG_DIR")
	if sanityBin == "" || configDir == "" {
		t.Fatal("FC_SANITY_BIN and FC_SANITY_CONFIG_DIR are required")
	}
	dir, err := os.MkdirTemp(os.Getenv("TMPDIR"), "fc-sanity-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "csi.sock")
	d, err := NewDriver(InputParameters{
		Endpoint: "unix://" + socket, Role: "all", NodeId: "local-sanity",
		Ip: os.Getenv("STORAGE_IPS"), PoolName: os.Getenv("STORAGE_POOL_NAME"),
		ApiKey: os.Getenv("STORAGE_API_KEY"), FcTargetNames: os.Getenv("STORAGE_FC_TARGET_NAMES"),
	})
	if err != nil {
		t.Fatalf("initialize live test driver: %v", err)
	}
	d.coordination = newFakeCoordinator()
	d.attachmentLookup = localSanityLookup{nodeID: d.NodeId}
	d.fcStateDir = filepath.Join(dir, "fc-state")
	if err := d.prepareCoordination(context.Background()); err != nil {
		t.Fatalf("test coordination preflight: %v", err)
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("test CSI socket: %v", err)
	}
	server := grpc.NewServer()
	d.Server = server
	csi.RegisterIdentityServer(server, d)
	csi.RegisterControllerServer(server, d)
	csi.RegisterNodeServer(server, d)
	d.Ready = true
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.Serve(listener) }()
	defer server.Stop()
	waitForLocalSanitySocket(t, socket, serverErr)

	timeout := 60 * time.Second
	if fullSuite {
		timeout = 60 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	skip := "GroupController"
	if extra := os.Getenv("SANITY_SKIP_TESTS"); extra != "" {
		skip += "|" + extra
	}
	args := []string{
		"--ginkgo.no-color", "--ginkgo.v", "--ginkgo.fail-on-empty",
		"--ginkgo.skip=" + skip,
		"--csi.endpoint=unix://" + socket, "--csi.controllerendpoint=unix://" + socket,
		"--csi.testvolumesize=1073741824", "--csi.testvolumeexpandsize=2147483648",
		"--csi.testvolumeaccesstype=block", "--csi.mountdir=" + filepath.Join(dir, "mnt"),
		"--csi.stagingdir=" + filepath.Join(dir, "staging"),
		"--csi.secrets=" + filepath.Join(configDir, "secrets.yaml"),
		"--csi.testvolumeparameters=" + filepath.Join(configDir, "params.yaml"),
		"--csi.testvolumemutableparameters=" + filepath.Join(configDir, "mutableparams.yaml"),
	}
	if focus != "" {
		args = append(args, "--ginkgo.focus-file="+focus)
	}
	// Opt-in shared-array inventory checks; unset => dedicated mode.
	if os.Getenv("SANITY_SHARED_ARRAY") != "" {
		args = append(args, "--shared-array")
	}
	cmd := exec.CommandContext(ctx, sanityBin, args...)
	cmd.Dir = dir
	var output bytes.Buffer
	combined := io.MultiWriter(os.Stdout, &output)
	cmd.Stdout, cmd.Stderr = combined, combined
	if err := cmd.Run(); err != nil {
		t.Fatalf("live sanity run failed: %v", err)
	}
	result := output.String()
	if fullSuite && (!strings.Contains(result, "Will run ") || !strings.Contains(result, "SUCCESS! -- ") || !strings.Contains(result, " | 0 Failed | ")) {
		t.Fatal("full live sanity suite did not complete with zero failures")
	}
	if !fullSuite && (!strings.Contains(result, "Will run 1 of ") || !strings.Contains(result, "1 Passed | 0 Failed")) {
		t.Fatalf("live sanity focus %s must select exactly one passing spec and have zero failures", focus)
	}
}

func waitForLocalSanitySocket(t *testing.T, socket string, serverErr <-chan error) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		conn, err := net.DialTimeout("unix", socket, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		select {
		case serveErr := <-serverErr:
			t.Fatalf("test driver startup failed: %v", serveErr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("test driver socket did not become ready")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestCreateVolumeRestoresRequestedCapacity(t *testing.T) {
	for _, tc := range []struct {
		name                                         string
		existing, wrongSource, mapped, refuse, stale bool
		initial                                      int64
		want                                         codes.Code
		patches                                      int
	}{
		{name: "new larger restore", initial: 1 << 30, want: codes.OK, patches: 1},
		{name: "resume partial restore", existing: true, initial: 1 << 30, want: codes.OK, patches: 1},
		{name: "already expanded", existing: true, initial: 2 << 30, want: codes.OK},
		{name: "different source untouched", existing: true, wrongSource: true, initial: 1 << 30, want: codes.AlreadyExists},
		{name: "mapped restore untouched", existing: true, mapped: true, initial: 1 << 30, want: codes.FailedPrecondition},
		{name: "resize rejection", existing: true, refuse: true, initial: 1 << 30, want: codes.InvalidArgument, patches: 1},
		{name: "resize readback mismatch", existing: true, stale: true, initial: 1 << 30, want: codes.Internal, patches: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			size, exists, patches := tc.initial, tc.existing, 0
			clone := "src@snap1"
			if tc.wrongSource {
				clone = "other@snapshot"
			}
			lun := func() map[string]interface{} {
				protocols := map[string]string{}
				if tc.mapped {
					protocols["fc"] = "target"
				}
				return map[string]interface{}{"id": "lun-clone", "name": "pvcrestore", "size": size, "clone": clone, "protocols": protocols}
			}
			d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/snapshot/snap-1"):
					w.Write([]byte(`{"id":"snap-1","name":"snap1","volume_id":"lun-src","volume_name":"src"}`))
				case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/api/v2/lun/list"):
					if exists {
						json.NewEncoder(w).Encode([]interface{}{lun()})
					} else {
						w.Write([]byte(`[]`))
					}
				case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/api/v2/snapshot/clone/snap-1"):
					exists = true
					w.Write([]byte(`{"volume_id":"lun-clone"}`))
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-clone"):
					json.NewEncoder(w).Encode(lun())
				case r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-clone"):
					patches++
					var body map[string]interface{}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					if body["size"] != float64(1) || body["sing"] != "G" {
						t.Errorf("resize must send a 1 GiB increment: %v", body)
					}
					if tc.refuse {
						w.WriteHeader(400)
						w.Write([]byte(`{"code":999,"error":"resize rejected"}`))
						return
					}
					if !tc.stale {
						size += 1 << 30
					}
					w.Write([]byte(`{"success":true}`))
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			})
			req := createVolumeRequestForTest("restore", 2<<30)
			req.VolumeContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "snap-1"}}}
			response, err := d.CreateVolume(context.Background(), req)
			if status.Code(err) != tc.want {
				t.Fatalf("code=%v want=%v err=%v", status.Code(err), tc.want, err)
			}
			if patches != tc.patches {
				t.Fatalf("resize requests=%d want=%d", patches, tc.patches)
			}
			if tc.want == codes.OK {
				if response.GetVolume().GetCapacityBytes() != 2<<30 {
					t.Fatalf("capacity=%d", response.GetVolume().GetCapacityBytes())
				}
				if _, err := d.CreateVolume(context.Background(), req); err != nil {
					t.Fatalf("repeat create: %v", err)
				}
				if patches != tc.patches {
					t.Fatal("repeat create expanded again")
				}
			}
		})
	}
}

func TestLocalSnapshotRestoreLargerFilesystem(t *testing.T) {
	if os.Getenv("FC_LIVE_MOCK_SANITY") != "1" {
		t.Skip("explicit live backend opt-in required")
	}
	d, err := NewDriver(InputParameters{Endpoint: "unix:///restore-probe.sock", Role: "all", NodeId: "local-sanity", Ip: os.Getenv("STORAGE_IPS"), PoolName: os.Getenv("STORAGE_POOL_NAME"), ApiKey: os.Getenv("STORAGE_API_KEY"), FcTargetNames: os.Getenv("STORAGE_FC_TARGET_NAMES")})
	if err != nil {
		t.Fatal(err)
	}
	d.coordination = newFakeCoordinator()
	d.attachmentLookup = localSanityLookup{nodeID: d.NodeId}
	dir, err := os.MkdirTemp(os.Getenv("TMPDIR"), "restore-capacity-")
	if err != nil {
		t.Fatal(err)
	}
	d.fcStateDir = filepath.Join(dir, "state")
	token := fmt.Sprintf("%x", time.Now().UnixNano())
	sourceName, restoredName := "s"+token, "d"+token
	var sourceID, restoredID, snapshotID string
	staged, published := map[string]bool{}, map[string]bool{}
	paths := map[string]string{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Minute)
		defer stop()
		if restoredID == "" && snapshotID != "" {
			lun, lookupErr := d.reconciler().LUNByNameStrict(cleanup, ngxnames.ModifyVolName(restoredName))
			if lookupErr != nil {
				t.Errorf("cleanup restore lookup: %v", lookupErr)
			}
			if lun != nil {
				snap, snapErr := d.client.Snapshots().Get(cleanup, snapshotID)
				check := createVolumeRequestForTest(restoredName, 1)
				if snapErr == nil && helper.ValidateExistingCreateVolume(lun, check, snap) == nil {
					restoredID = helper.VolumeID(volumeIDPrefix, lun.ID)
				} else {
					t.Errorf("cleanup cannot confirm orphan identity: %v", snapErr)
				}
			}
		}
		for _, id := range []string{restoredID, sourceID} {
			if id == "" {
				continue
			}
			if staged[id] {
				if _, e := d.NodeUnstageVolume(cleanup, &csi.NodeUnstageVolumeRequest{VolumeId: id, StagingTargetPath: paths[id]}); e != nil {
					t.Errorf("cleanup unstage: %v", e)
				}
			}
			if published[id] {
				if _, e := d.ControllerUnpublishVolume(cleanup, &csi.ControllerUnpublishVolumeRequest{VolumeId: id, NodeId: d.NodeId}); e != nil {
					t.Errorf("cleanup unpublish: %v", e)
				}
			}
		}
		if restoredID != "" {
			if _, e := d.DeleteVolume(cleanup, &csi.DeleteVolumeRequest{VolumeId: restoredID}); e != nil {
				t.Errorf("cleanup restore: %v", e)
			}
		}
		if snapshotID != "" {
			if _, e := d.DeleteSnapshot(cleanup, &csi.DeleteSnapshotRequest{SnapshotId: snapshotID}); e != nil {
				t.Errorf("cleanup snapshot: %v", e)
			}
		}
		if sourceID != "" {
			if _, e := d.DeleteVolume(cleanup, &csi.DeleteVolumeRequest{VolumeId: sourceID}); e != nil {
				t.Errorf("cleanup source: %v", e)
			}
		}
		if !t.Failed() {
			os.RemoveAll(dir)
		}
	}()
	capability := createVolumeRequestForTest("", 1<<30).GetVolumeCapabilities()[0]
	stage := func(volume *csi.Volume) string {
		id := volume.GetVolumeId()
		published[id] = true
		pub, e := d.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{VolumeId: id, NodeId: d.NodeId, VolumeCapability: capability})
		if e != nil {
			t.Fatalf("publish %s: %v", id, e)
		}
		path := filepath.Join(dir, fmt.Sprintf("stage-%d", len(paths)))
		paths[id], staged[id] = path, true
		_, e = d.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{VolumeId: id, StagingTargetPath: path, PublishContext: pub.GetPublishContext(), VolumeContext: volume.GetVolumeContext(), VolumeCapability: capability})
		if e != nil {
			t.Fatalf("stage %s: %v", id, e)
		}
		return path
	}
	filesystemBytes := func(path string) uint64 {
		var stat syscall.Statfs_t
		if err := syscall.Statfs(path, &stat); err != nil {
			t.Fatal(err)
		}
		return stat.Blocks * uint64(stat.Bsize)
	}
	source, err := d.CreateVolume(ctx, createVolumeRequestForTest(sourceName, 1<<30))
	if err != nil {
		t.Fatal(err)
	}
	sourceID = source.GetVolume().GetVolumeId()
	sourcePath := stage(source.GetVolume())
	before := filesystemBytes(sourcePath)
	marker := []byte("restore-marker-" + token)
	if err := os.WriteFile(filepath.Join(sourcePath, "marker"), marker, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := d.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{VolumeId: sourceID, StagingTargetPath: sourcePath}); err != nil {
		t.Fatal(err)
	}
	staged[sourceID] = false
	if _, err := d.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: sourceID, NodeId: d.NodeId}); err != nil {
		t.Fatal(err)
	}
	published[sourceID] = false
	snapshot, err := d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{Name: "p" + token, SourceVolumeId: sourceID})
	if err != nil {
		t.Fatal(err)
	}
	snapshotID = snapshot.GetSnapshot().GetSnapshotId()
	req := createVolumeRequestForTest(restoredName, 2<<30)
	req.VolumeContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapshotID}}}
	restored, err := d.CreateVolume(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	restoredID = restored.GetVolume().GetVolumeId()
	if restored.GetVolume().GetCapacityBytes() < 2<<30 {
		t.Fatal("restore response is smaller than requested")
	}
	if _, err := d.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: snapshotID}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("snapshot with a dependent restore must return FailedPrecondition: %v", err)
	}
	restoredPath := stage(restored.GetVolume())
	after := filesystemBytes(restoredPath)
	if after <= before {
		t.Fatalf("restored filesystem did not grow: %d -> %d bytes", before, after)
	}
	data, err := os.ReadFile(filepath.Join(restoredPath, "marker"))
	if err != nil || !bytes.Equal(data, marker) {
		t.Fatalf("restored data mismatch: %q, %v", data, err)
	}
	t.Logf("restore PASS: 1 GiB -> 2 GiB, filesystem %d -> %d bytes, marker preserved", before, after)
}

func TestGetSnapshotRPCContract(t *testing.T) {
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/snapshot/snap-1") {
			w.Write([]byte(`{"id":"snap-1","volume_id":"lun-1","created":"1785499878","size":"1073741824","ready":true}`))
			return
		}
		http.NotFound(w, r)
	})
	// The typed CSI surface must reach the real implementation, never the
	// embedded Unimplemented stub.
	var srv csi.ControllerServer = d
	if _, err := srv.GetSnapshot(context.Background(), &csi.GetSnapshotRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty id code = %s, want InvalidArgument", status.Code(err))
	}
	if _, err := srv.GetSnapshot(context.Background(), &csi.GetSnapshotRequest{SnapshotId: "missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing id code = %s, want NotFound", status.Code(err))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := srv.GetSnapshot(ctx, &csi.GetSnapshotRequest{SnapshotId: "snap-1"}); status.Code(err) != codes.Canceled {
		t.Fatalf("canceled ctx code = %s, want Canceled", status.Code(err))
	}
	resp, err := srv.GetSnapshot(context.Background(), &csi.GetSnapshotRequest{SnapshotId: "snap-1"})
	if err != nil || resp.GetSnapshot().GetSnapshotId() != "snap-1" {
		t.Fatalf("canonical lookup = %#v, err=%v", resp, err)
	}
}

// --- Regression: expand semantics and ID prefix ---

// B4: a smaller request is a shrink and must be rejected, not silently
// accepted by the whole-GiB idempotency rounding.
func TestControllerExpandVolumeShrinkRejected(t *testing.T) {
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/api/v2/lun/lun-1") {
			w.Write([]byte(`{"id":"lun-1","name":"pvcvol1","size":2147483648}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	_, err := d.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      helper.VolumeID(volumeIDPrefix, "lun-1"),
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("shrink: code=%s err=%v, want InvalidArgument", status.Code(err), err)
	}
}

// B4: a missing LUN is NotFound, not Internal.
func TestControllerExpandVolumeNotFoundMapped(t *testing.T) {
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	_, err := d.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      helper.VolumeID(volumeIDPrefix, "lun-1"),
		CapacityRange: &csi.CapacityRange{RequiredBytes: 2 << 30},
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("absent LUN: code=%s err=%v, want NotFound", status.Code(err), err)
	}
}

// B4: an empty/mismatched/zero-size backend record is never expanded against.
func TestControllerExpandVolumeInvalidRecordFailsClosed(t *testing.T) {
	for _, body := range []string{`{}`, `{"id":"lun-other","size":1073741824}`, `{"id":"lun-1","size":0}`} {
		d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/api/v2/lun/lun-1") {
				w.Write([]byte(body))
				return
			}
			w.WriteHeader(http.StatusNotFound)
		})
		_, err := d.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
			VolumeId:      helper.VolumeID(volumeIDPrefix, "lun-1"),
			CapacityRange: &csi.CapacityRange{RequiredBytes: 2 << 30},
		})
		if err == nil {
			t.Fatalf("record %s: expand succeeded against an invalid LUN record", body)
		}
	}
}

// B4: an equal-capacity request stays an idempotent success with no mutation.
func TestControllerExpandVolumeEqualCapacityIdempotent(t *testing.T) {
	patches := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/api/v2/lun/lun-1"):
			w.Write([]byte(`{"id":"lun-1","name":"pvcvol1","size":1073741824}`))
		case r.Method == http.MethodPatch:
			patches++
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	resp, err := d.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      helper.VolumeID(volumeIDPrefix, "lun-1"),
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30},
	})
	if err != nil {
		t.Fatalf("equal capacity: %v", err)
	}
	if resp.GetCapacityBytes() != 1<<30 || patches != 0 {
		t.Fatalf("resp=%+v patches=%d, want idempotent success without mutation", resp, patches)
	}
}

// B7 (CSI-semantics corrected after csi-sanity FC run 490): a bare backend ID
// is not this driver's volume. DeleteVolume is idempotent -> OK, and the bare
// ID must NEVER reach the backend.
func TestDeleteVolumeBareBackendIDIdempotentOK(t *testing.T) {
	calls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusInternalServerError)
	})
	resp, err := d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "lun-1"})
	if err != nil {
		t.Fatalf("bare ID delete: err=%v, want idempotent OK", err)
	}
	if resp == nil || calls != 0 {
		t.Fatalf("resp=%v backend calls=%d, want OK without touching the backend", resp, calls)
	}
}

func TestControllerExpandVolumeBareBackendIDNotFound(t *testing.T) {
	calls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusInternalServerError)
	})
	_, err := d.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      "lun-1",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 2 << 30},
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("bare ID expand: code=%s err=%v, want NotFound", status.Code(err), err)
	}
	if calls != 0 {
		t.Fatalf("backend touched %d times for a bare volume ID, want 0", calls)
	}
}

func TestControllerUnpublishVolumeBareBackendIDIdempotentOK(t *testing.T) {
	calls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusInternalServerError)
	})
	resp, err := d.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: "lun-1", NodeId: "worker0",
	})
	if err != nil || resp == nil {
		t.Fatalf("bare ID unpublish: resp=%v err=%v, want idempotent OK", resp, err)
	}
	if calls != 0 {
		t.Fatalf("backend touched %d times for a bare volume ID, want 0", calls)
	}
}

func TestControllerPublishVolumeBareBackendIDNotFound(t *testing.T) {
	calls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusInternalServerError)
	})
	_, err := d.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: "lun-1", NodeId: "worker0",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		},
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("bare ID publish: code=%s err=%v, want NotFound", status.Code(err), err)
	}
	if calls != 0 {
		t.Fatalf("backend touched %d times for a bare volume ID, want 0", calls)
	}
}

func TestListSnapshotsBareSourceReturnsEmpty(t *testing.T) {
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && (strings.HasSuffix(r.URL.Path, "/api/v2/snapshot") || strings.HasSuffix(r.URL.Path, "/api/v2/snapshot/list")) {
			w.Write([]byte(`[]`))
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	})
	resp, err := d.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{SourceVolumeId: "lun-1"})
	if err != nil {
		t.Fatalf("bare source ListSnapshots: err=%v, want empty OK", err)
	}
	if len(resp.GetEntries()) != 0 {
		t.Fatalf("entries=%d, want 0 for a non-driver source", len(resp.GetEntries()))
	}
}

// Adverse-backend regression tests. These pin fail-closed behavior when the
// backend acks a mutation without applying it, or when pagination races a
// concurrent create/delete. Tests whose names still carry "Characterization"
// were converted in place from documenting the old gap to asserting the
// implemented fix; their names are kept as stable test identities.

// charEventRecorder records backend call order race-safely.
type charEventRecorder struct {
	mu     sync.Mutex
	events []string
}

func (c *charEventRecorder) record(event string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, event)
}

func (c *charEventRecorder) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.events...)
}

func charCount(events []string, event string) int {
	n := 0
	for _, e := range events {
		if e == event {
			n++
		}
	}
	return n
}

func charLastIndex(events []string, event string) int {
	last := -1
	for i, e := range events {
		if e == event {
			last = i
		}
	}
	return last
}

// TestCharacterizationR1DeleteSnapshotTrusts200NoAbsenceReadback was converted
// from a characterization of the unverified DELETE ack to a regression of the
// fix: DeleteSnapshot must re-read the snapshot after a 200 delete ack and
// fail closed while the backend keeps the object. Adverse condition: the
// backend acks but keeps the snapshot (a silent no-op would require a backend
// contract violation). The second subtest pins the mitigation for the known
// real trigger (clone dependency -> SDK ErrSnapshotInUse -> FailedPrecondition).
func TestCharacterizationR1DeleteSnapshotTrusts200NoAbsenceReadback(t *testing.T) {
	t.Run("200 ack without absence proof fails closed", func(t *testing.T) {
		rec := &charEventRecorder{}
		d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/snapshot/snap-1"):
				rec.record("get")
				// The snapshot stays present even after the delete ack.
				w.Write([]byte(`{"id":"snap-1","name":"snap1","volume_id":"lun-src","ready":true}`))
			case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/api/v2/snapshot/snap-1"):
				rec.record("delete")
				w.Write([]byte(`{"success":true}`))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		})

		// The short context bounds the verification poll loop for unit-test
		// runtime; the production budget is FCDeleteVerifyTimeout.
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		resp, err := d.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: "snap-1"})
		if err == nil || resp != nil {
			t.Fatalf("DeleteSnapshot succeeded while the backend snapshot still exists: resp=%v err=%v", resp, err)
		}
		if code := status.Code(err); code != codes.DeadlineExceeded && code != codes.Internal {
			t.Fatalf("code = %v, want DeadlineExceeded/Internal (err=%v)", code, err)
		}
		events := rec.snapshot()
		if charCount(events, "delete") != 1 {
			t.Fatalf("backend call pattern = %v, want exactly one DELETE", events)
		}
		if charCount(events, "get") < 2 || charLastIndex(events, "get") <= charLastIndex(events, "delete") {
			t.Fatalf("expected absence readback GETs after the delete ack: %v", events)
		}
		// The object is still there; the driver must not have attested success.
		still, err := d.client.Snapshots().Get(context.Background(), "snap-1")
		if err != nil || still == nil || still.ID != "snap-1" {
			t.Fatalf("snapshot should still be present in the adverse backend: err=%v snap=%+v", err, still)
		}
	})

	t.Run("clone dependency mitigation maps to FailedPrecondition", func(t *testing.T) {
		d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/snapshot/snap-1"):
				w.Write([]byte(`{"id":"snap-1","name":"snap1","volume_id":"lun-src","ready":true}`))
			case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/api/v2/snapshot/snap-1"):
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(`{"code":999,"error":"Snapshot could not be deleted. Because there are clone volumes created from this snapshot."}`))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		})
		_, err := d.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{SnapshotId: "snap-1"})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("code = %v, want FailedPrecondition via SDK ErrSnapshotInUse (err=%v)", status.Code(err), err)
		}
	})
}

// TestDeleteSnapshotAbsenceVerifiedAfterDelete pins the read-after-delete
// contract for snapshots: a 200-success DELETE response is not proof of
// backend absence. DeleteSnapshot must observe an authoritative NotFound
// before reporting CSI success; a still-present snapshot or an ambiguous
// absence GET fails closed so the CO never completes a deletion that did not
// happen (a false success would orphan the snapshot and let its source LUN
// delete proceed while backend clone dependencies may still exist).
func TestDeleteSnapshotAbsenceVerifiedAfterDelete(t *testing.T) {
	t.Run("authoritative absence after delete succeeds", func(t *testing.T) {
		rec := &charEventRecorder{}
		deleted := false
		d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/snapshot/snap-1"):
				rec.record("get")
				if deleted {
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte(`{"code":5011,"error":"not found"}`))
					return
				}
				w.Write([]byte(`{"id":"snap-1","name":"snap1","volume_id":"lun-src","ready":true}`))
			case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/api/v2/snapshot/snap-1"):
				rec.record("delete")
				deleted = true
				w.Write([]byte(`{"success":true}`))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		})

		resp, err := d.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{SnapshotId: "snap-1"})
		if err != nil || resp == nil {
			t.Fatalf("DeleteSnapshot with authoritative absence: resp=%v err=%v", resp, err)
		}
		events := rec.snapshot()
		if charCount(events, "delete") != 1 {
			t.Fatalf("backend call pattern = %v, want exactly one DELETE", events)
		}
		if charLastIndex(events, "get") <= charLastIndex(events, "delete") {
			t.Fatalf("expected an absence readback GET after the delete ack: %v", events)
		}
	})

	t.Run("code-less 404 after delete succeeds", func(t *testing.T) {
		deleted := false
		d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/snapshot/snap-1"):
				if deleted {
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte(`{"error":"not found"}`))
					return
				}
				w.Write([]byte(`{"id":"snap-1","name":"snap1","volume_id":"lun-src","ready":true}`))
			case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/api/v2/snapshot/snap-1"):
				deleted = true
				w.Write([]byte(`{"success":true}`))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		})
		if _, err := d.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{SnapshotId: "snap-1"}); err != nil {
			t.Fatalf("DeleteSnapshot with code-less 404 absence: %v", err)
		}
	})

	t.Run("404 with conflicting code fails closed", func(t *testing.T) {
		deleted := false
		deleteCalls := 0
		d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/snapshot/snap-1"):
				if deleted {
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte(`{"code":710,"error":"exists"}`))
					return
				}
				w.Write([]byte(`{"id":"snap-1","name":"snap1","volume_id":"lun-src","ready":true}`))
			case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/api/v2/snapshot/snap-1"):
				deleteCalls++
				deleted = true
				w.Write([]byte(`{"success":true}`))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		})
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		resp, err := d.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: "snap-1"})
		if err == nil || resp != nil {
			t.Fatalf("DeleteSnapshot treated a 404 with a conflicting code as absence: resp=%v err=%v", resp, err)
		}
		if deleteCalls != 1 {
			t.Fatalf("delete calls = %d, want 1", deleteCalls)
		}
	})

	t.Run("persistent server error fails closed", func(t *testing.T) {
		var stateMu sync.Mutex
		deleted := false
		deleteCalls := 0
		verifyGets := 0
		d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/snapshot/snap-1"):
				stateMu.Lock()
				gone := deleted
				if gone {
					verifyGets++
				}
				stateMu.Unlock()
				if gone {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte(`{"code":5000,"error":"backend unavailable"}`))
					return
				}
				w.Write([]byte(`{"id":"snap-1","name":"snap1","volume_id":"lun-src","ready":true}`))
			case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/api/v2/snapshot/snap-1"):
				stateMu.Lock()
				deleteCalls++
				deleted = true
				stateMu.Unlock()
				w.Write([]byte(`{"success":true}`))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		})
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		resp, err := d.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: "snap-1"})
		if err == nil || resp != nil {
			t.Fatalf("DeleteSnapshot treated a 5xx absence GET as success: resp=%v err=%v", resp, err)
		}
		stateMu.Lock()
		gotDeletes, gotGets := deleteCalls, verifyGets
		stateMu.Unlock()
		if gotDeletes != 1 || gotGets == 0 {
			t.Fatalf("delete calls = %d, verification GETs = %d, want 1 delete and at least one readback", gotDeletes, gotGets)
		}
	})

	t.Run("transient transport error then absence succeeds", func(t *testing.T) {
		var stateMu sync.Mutex
		deleted := false
		// Fail the first post-delete GET(s) at the transport layer; the SDK
		// may replay a safe GET once after a controller refresh, so allow
		// two wire failures before the authoritative 404.
		transportFailures := 2
		d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/snapshot/snap-1"):
				stateMu.Lock()
				gone := deleted
				fail := false
				if gone && transportFailures > 0 {
					transportFailures--
					fail = true
				}
				stateMu.Unlock()
				if gone {
					if fail {
						hj, ok := w.(http.Hijacker)
						if !ok {
							t.Error("response writer does not support hijack")
							return
						}
						conn, _, err := hj.Hijack()
						if err != nil {
							t.Errorf("hijack: %v", err)
							return
						}
						conn.Close()
						return
					}
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte(`{"code":5011,"error":"not found"}`))
					return
				}
				w.Write([]byte(`{"id":"snap-1","name":"snap1","volume_id":"lun-src","ready":true}`))
			case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/api/v2/snapshot/snap-1"):
				stateMu.Lock()
				deleted = true
				stateMu.Unlock()
				w.Write([]byte(`{"success":true}`))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		})
		resp, err := d.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{SnapshotId: "snap-1"})
		if err != nil || resp == nil {
			t.Fatalf("DeleteSnapshot should retry an ambiguous GET and converge on absence: resp=%v err=%v", resp, err)
		}
	})
}

// TestDeleteVolumeAbsenceVerifiedAfterDelete pins the read-after-delete
// contract: a 200-success LUN DELETE response is not proof of backend
// absence. DeleteVolume must observe an authoritative NotFound before
// reporting CSI success. A still-present LUN or an ambiguous absence GET
// fails closed, so the CO never completes a deletion that did not happen
// (a false success would let a later same-name create adopt the stale LUN
// with the previous tenant's data as a "new" volume).
func TestDeleteVolumeAbsenceVerifiedAfterDelete(t *testing.T) {
	t.Run("delete ack while LUN persists fails closed", func(t *testing.T) {
		rec := &charEventRecorder{}
		d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
				rec.record("lun-get")
				// All preconditions pass; the LUN survives the delete ack.
				w.Write([]byte(`{"id":"lun-1","name":"pvcvol1","size":1073741824,"protocols":{}}`))
			case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc"):
				w.Write([]byte(`[]`))
			case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v2/snapshot/list"):
				w.Write([]byte(`[]`))
			case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
				rec.record("lun-delete")
				w.Write([]byte(`{"success":true}`))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		})

		// The short context bounds the verification poll loop for unit-test
		// runtime; the production budget is FCDeleteVerifyTimeout.
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		resp, err := d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{
			VolumeId: helper.VolumeID(volumeIDPrefix, "lun-1"),
		})
		if err == nil || resp != nil {
			t.Fatalf("DeleteVolume succeeded while the backend LUN still exists: resp=%v err=%v", resp, err)
		}
		if code := status.Code(err); code != codes.DeadlineExceeded && code != codes.Internal {
			t.Fatalf("code = %v, want DeadlineExceeded/Internal (err=%v)", code, err)
		}
		events := rec.snapshot()
		if charCount(events, "lun-delete") != 1 {
			t.Fatalf("backend call pattern = %v, want exactly one LUN DELETE", events)
		}
		if charLastIndex(events, "lun-get") <= charLastIndex(events, "lun-delete") {
			t.Fatalf("expected an absence readback GET after the delete ack: %v", events)
		}
		still, err := d.client.LUNs().Get(context.Background(), "lun-1")
		if err != nil || still == nil || still.ID != "lun-1" {
			t.Fatalf("LUN should still be present in the adverse backend: err=%v lun=%+v", err, still)
		}
	})

	t.Run("authoritative absence after delete succeeds", func(t *testing.T) {
		rec := &charEventRecorder{}
		deleted := false
		d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
				rec.record("lun-get")
				if deleted {
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte(`{"code":5011,"error":"not found"}`))
					return
				}
				w.Write([]byte(`{"id":"lun-1","name":"pvcvol1","size":1073741824,"protocols":{}}`))
			case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc"):
				w.Write([]byte(`[]`))
			case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v2/snapshot/list"):
				w.Write([]byte(`[]`))
			case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
				rec.record("lun-delete")
				deleted = true
				w.Write([]byte(`{"success":true}`))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		})

		resp, err := d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
			VolumeId: helper.VolumeID(volumeIDPrefix, "lun-1"),
		})
		if err != nil || resp == nil {
			t.Fatalf("DeleteVolume with authoritative absence: resp=%v err=%v", resp, err)
		}
		events := rec.snapshot()
		if charCount(events, "lun-delete") != 1 {
			t.Fatalf("backend call pattern = %v, want exactly one LUN DELETE", events)
		}
		if charLastIndex(events, "lun-get") <= charLastIndex(events, "lun-delete") {
			t.Fatalf("expected an absence readback GET after the delete ack: %v", events)
		}
		if _, err := d.client.LUNs().Get(context.Background(), "lun-1"); !ngxstorage.IsNotFound(err) {
			t.Fatalf("LUN should be absent after the honest delete: %v", err)
		}
	})

	t.Run("transient transport error then absence succeeds", func(t *testing.T) {
		var stateMu sync.Mutex
		deleted := false
		// Fail the first post-delete GET(s) at the transport layer; the SDK
		// may replay a safe GET once after a controller refresh, so allow
		// two wire failures before the authoritative 404.
		transportFailures := 2
		d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
				stateMu.Lock()
				gone := deleted
				fail := false
				if gone && transportFailures > 0 {
					transportFailures--
					fail = true
				}
				stateMu.Unlock()
				if gone {
					if fail {
						hj, ok := w.(http.Hijacker)
						if !ok {
							t.Error("response writer does not support hijack")
							return
						}
						conn, _, err := hj.Hijack()
						if err != nil {
							t.Errorf("hijack: %v", err)
							return
						}
						conn.Close()
						return
					}
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte(`{"code":5011,"error":"not found"}`))
					return
				}
				w.Write([]byte(`{"id":"lun-1","name":"pvcvol1","size":1073741824,"protocols":{}}`))
			case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc"):
				w.Write([]byte(`[]`))
			case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v2/snapshot/list"):
				w.Write([]byte(`[]`))
			case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
				stateMu.Lock()
				deleted = true
				stateMu.Unlock()
				w.Write([]byte(`{"success":true}`))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		})

		resp, err := d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
			VolumeId: helper.VolumeID(volumeIDPrefix, "lun-1"),
		})
		if err != nil || resp == nil {
			t.Fatalf("DeleteVolume should retry an ambiguous GET and converge on absence: resp=%v err=%v", resp, err)
		}
	})

	t.Run("persistent transport error fails closed", func(t *testing.T) {
		var stateMu sync.Mutex
		deleted := false
		deleteCalls := 0
		verifyGets := 0
		d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
				stateMu.Lock()
				gone := deleted
				if gone {
					verifyGets++
				}
				stateMu.Unlock()
				if gone {
					hj, ok := w.(http.Hijacker)
					if !ok {
						t.Error("response writer does not support hijack")
						return
					}
					conn, _, err := hj.Hijack()
					if err != nil {
						t.Errorf("hijack: %v", err)
						return
					}
					conn.Close()
					return
				}
				w.Write([]byte(`{"id":"lun-1","name":"pvcvol1","size":1073741824,"protocols":{}}`))
			case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc"):
				w.Write([]byte(`[]`))
			case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v2/snapshot/list"):
				w.Write([]byte(`[]`))
			case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
				stateMu.Lock()
				deleteCalls++
				deleted = true
				stateMu.Unlock()
				w.Write([]byte(`{"success":true}`))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		})

		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		resp, err := d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{
			VolumeId: helper.VolumeID(volumeIDPrefix, "lun-1"),
		})
		if err == nil || resp != nil {
			t.Fatalf("DeleteVolume treated an uncertain absence GET as success: resp=%v err=%v", resp, err)
		}
		stateMu.Lock()
		gotDeletes, gotGets := deleteCalls, verifyGets
		stateMu.Unlock()
		if gotDeletes != 1 || gotGets == 0 {
			t.Fatalf("delete calls = %d, verification GETs = %d, want 1 delete and at least one readback", gotDeletes, gotGets)
		}
	})

	t.Run("persistent server error fails closed", func(t *testing.T) {
		var stateMu sync.Mutex
		deleted := false
		deleteCalls := 0
		verifyGets := 0
		d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
				stateMu.Lock()
				gone := deleted
				if gone {
					verifyGets++
				}
				stateMu.Unlock()
				if gone {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte(`{"code":5000,"error":"backend unavailable"}`))
					return
				}
				w.Write([]byte(`{"id":"lun-1","name":"pvcvol1","size":1073741824,"protocols":{}}`))
			case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc"):
				w.Write([]byte(`[]`))
			case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v2/snapshot/list"):
				w.Write([]byte(`[]`))
			case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
				stateMu.Lock()
				deleteCalls++
				deleted = true
				stateMu.Unlock()
				w.Write([]byte(`{"success":true}`))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		})

		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		resp, err := d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{
			VolumeId: helper.VolumeID(volumeIDPrefix, "lun-1"),
		})
		if err == nil || resp != nil {
			t.Fatalf("DeleteVolume treated a 5xx absence GET as success: resp=%v err=%v", resp, err)
		}
		stateMu.Lock()
		gotDeletes, gotGets := deleteCalls, verifyGets
		stateMu.Unlock()
		if gotDeletes != 1 || gotGets == 0 {
			t.Fatalf("delete calls = %d, verification GETs = %d, want 1 delete and at least one readback", gotDeletes, gotGets)
		}
	})
}

// TestSameNameCreateAfterFailedDeleteRecoversExistingLUN pins the downstream
// half of the delete-verification contract: when the backend acknowledges a
// delete but keeps the LUN, DeleteVolume fails, so no deletion attestation
// reaches the CO and the PV is not removed. A same-name CreateVolume while
// the LUN persists is therefore idempotent recovery of the SAME volume (same
// backend ID, no new create) — never adoption of an orphaned stale LUN as a
// "new" volume with the previous tenant's data.
func TestSameNameCreateAfterFailedDeleteRecoversExistingLUN(t *testing.T) {
	deleteCalls := 0
	createCalls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/list"):
			w.Write([]byte(`[{"id":"lun-1","name":"pvcvol1","size":1073741824}]`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			// Adverse backend: the LUN survives every delete ack.
			w.Write([]byte(`{"id":"lun-1","name":"pvcvol1","size":1073741824,"protocols":{}}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc"):
			w.Write([]byte(`[]`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v2/snapshot/list"):
			w.Write([]byte(`[]`))
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			deleteCalls++
			w.Write([]byte(`{"success":true}`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/api/v2/lun/"):
			createCalls++
			w.Write([]byte(`{"luns":[{"id":"unexpected","name":"pvcvol1"}]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{
		VolumeId: helper.VolumeID(volumeIDPrefix, "lun-1"),
	}); err == nil {
		t.Fatal("DeleteVolume must fail while the backend keeps the LUN; a false success would attest a deletion that never happened")
	}
	if deleteCalls != 1 {
		t.Fatalf("delete calls = %d, want 1", deleteCalls)
	}

	resp, err := d.CreateVolume(context.Background(), createVolumeRequestForTest("vol1", 1<<30))
	if err != nil {
		t.Fatalf("same-name CreateVolume after failed delete: %v", err)
	}
	if got, want := resp.GetVolume().GetVolumeId(), helper.VolumeID(volumeIDPrefix, "lun-1"); got != want {
		t.Fatalf("volume ID = %q, want the existing LUN ID %q (idempotent recovery, not a new volume)", got, want)
	}
	if createCalls != 0 {
		t.Fatalf("backend LUN create calls = %d, want 0 while the same-name LUN exists", createCalls)
	}
}

// TestCharacterizationR3FCUnmapNoOpFailsClosed pins the unmap convergence
// guard: when the unmap PATCH acks 200 but the mapping never converges,
// WaitForFCUnmap prevents a success report and the LUN delete is never
// reached. The short context bounds the 30x1s verify loop for unit-test
// runtime.
func TestCharacterizationR3FCUnmapNoOpFailsClosed(t *testing.T) {
	deleteCalls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			// The LUN never loses its FC protocol mapping.
			w.Write([]byte(`{"id":"lun-1","name":"pvcvol1","protocols":{"fc":"tgt-a"}}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc"):
			w.Write([]byte(`[{"id":"t1","name":"tgt-a","owner":"o"}]`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc/t1"):
			// Silent no-op unmap: the target keeps the LUN mapped.
			w.Write([]byte(`{"id":"t1","name":"tgt-a","owner":"o","luns":[{"id":"lun-1","number":"4"}]}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v2/snapshot/list"):
			w.Write([]byte(`[]`))
		case r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc/lun/t1"):
			w.Write([]byte(`{"success":true}`))
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			deleteCalls++
			w.Write([]byte(`{"success":true}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{
		VolumeId: helper.VolumeID(volumeIDPrefix, "lun-1"),
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("code = %v, want Internal from unmap convergence failure (err=%v)", status.Code(err), err)
	}
	if deleteCalls != 0 {
		t.Fatalf("LUN delete calls = %d, want 0 while the mapping never converged", deleteCalls)
	}
}

// TestCharacterizationR4ListVolumesIndexTokenDuplicatesAfterInsertion was
// converted from a characterization of index-token duplication to a
// regression of the stable-ID cursor fix: a concurrent create that inserts
// at the head between pages must not repeat an already-returned volume.
// The v1:<volumeId> cursor resolves by ID, so the shifted list still yields
// exactly the entries after the cursor.
func TestCharacterizationR4ListVolumesIndexTokenDuplicatesAfterInsertion(t *testing.T) {
	listCalls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/list") {
			listCalls++
			if listCalls == 1 {
				w.Write([]byte(`[{"id":"l1","size":1073741824},{"id":"l2","size":1073741824},{"id":"l3","size":1073741824}]`))
			} else {
				// Concurrent create inserted l0 at the head between pages.
				w.Write([]byte(`[{"id":"l0","size":1073741824},{"id":"l1","size":1073741824},{"id":"l2","size":1073741824},{"id":"l3","size":1073741824}]`))
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})

	page1, err := d.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: 2})
	if err != nil || len(page1.GetEntries()) != 2 || page1.GetNextToken() != "v1:"+helper.VolumeID(volumeIDPrefix, "l2") {
		t.Fatalf("page1 = %+v err=%v, want [l1 l2] with a v1: cursor on l2", page1, err)
	}
	page2, err := d.ListVolumes(context.Background(), &csi.ListVolumesRequest{
		StartingToken: page1.GetNextToken(), MaxEntries: 2,
	})
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	seen := map[string]int{}
	for _, entry := range append(page1.GetEntries(), page2.GetEntries()...) {
		seen[entry.GetVolume().GetVolumeId()]++
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("volume %s returned %d times across pages, want exactly 1; seen=%v", id, count, seen)
		}
	}
	// The walk covers every volume that existed at page 1; l3 must not be
	// lost to the head insertion. l0 sorts before the cursor and is not part
	// of this walk.
	if len(page2.GetEntries()) != 1 || page2.GetEntries()[0].GetVolume().GetVolumeId() != helper.VolumeID(volumeIDPrefix, "l3") {
		t.Fatalf("page2 = %+v, want [l3] after the l2 cursor", page2)
	}
	if page2.GetNextToken() != "" {
		t.Fatalf("page2 next token = %q, want empty final page", page2.GetNextToken())
	}
}

// TestCharacterizationR4ListVolumesIndexTokenOmitsAfterDeletion was converted
// from a characterization of index-token omission to a regression of the
// stable-ID cursor fix: a concurrent delete between pages shrinks the list,
// but the v1:<volumeId> cursor still resolves by ID and the trailing volume
// is returned to the CO instead of being silently skipped.
func TestCharacterizationR4ListVolumesIndexTokenOmitsAfterDeletion(t *testing.T) {
	listCalls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/list") {
			listCalls++
			if listCalls == 1 {
				w.Write([]byte(`[{"id":"l1","size":1073741824},{"id":"l2","size":1073741824},{"id":"l3","size":1073741824}]`))
			} else {
				// Concurrent delete removed l1 between pages.
				w.Write([]byte(`[{"id":"l2","size":1073741824},{"id":"l3","size":1073741824}]`))
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})

	page1, err := d.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: 2})
	if err != nil || len(page1.GetEntries()) != 2 || page1.GetNextToken() != "v1:"+helper.VolumeID(volumeIDPrefix, "l2") {
		t.Fatalf("page1 = %+v err=%v, want [l1 l2] with a v1: cursor on l2", page1, err)
	}
	page2, err := d.ListVolumes(context.Background(), &csi.ListVolumesRequest{
		StartingToken: page1.GetNextToken(), MaxEntries: 2,
	})
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	if len(page2.GetEntries()) != 1 || page2.GetEntries()[0].GetVolume().GetVolumeId() != helper.VolumeID(volumeIDPrefix, "l3") {
		t.Fatalf("page2 = %+v, want [l3]; the trailing volume must not be omitted", page2)
	}
	if page2.GetNextToken() != "" {
		t.Fatalf("page2 next token = %q, want empty final page", page2.GetNextToken())
	}
	seen := map[string]int{}
	for _, entry := range append(page1.GetEntries(), page2.GetEntries()...) {
		seen[entry.GetVolume().GetVolumeId()]++
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("volume %s returned %d times across pages, want exactly 1; seen=%v", id, count, seen)
		}
	}
}

// TestCharacterizationR4ListSnapshotsIndexTokenDuplicatesAfterInsertion was
// converted from a characterization of index-token duplication to a
// regression of the stable-ID cursor fix on the ListSnapshots path: a
// concurrent snapshot create between pages must not repeat an
// already-returned snapshot; the s1:<snapshotId> cursor resolves by ID.
func TestCharacterizationR4ListSnapshotsIndexTokenDuplicatesAfterInsertion(t *testing.T) {
	listCalls := 0
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v2/snapshot/list") {
			listCalls++
			if listCalls == 1 {
				w.Write([]byte(`[{"id":"s1","volume_id":"lun-1"},{"id":"s2","volume_id":"lun-1"},{"id":"s3","volume_id":"lun-1"}]`))
			} else {
				// Concurrent snapshot create inserted s0 at the head.
				w.Write([]byte(`[{"id":"s0","volume_id":"lun-1"},{"id":"s1","volume_id":"lun-1"},{"id":"s2","volume_id":"lun-1"},{"id":"s3","volume_id":"lun-1"}]`))
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})

	page1, err := d.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{MaxEntries: 2})
	if err != nil || len(page1.GetEntries()) != 2 || page1.GetNextToken() != "s1:s2" {
		t.Fatalf("page1 = %+v err=%v, want [s1 s2] with an s1: cursor on s2", page1, err)
	}
	page2, err := d.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
		StartingToken: page1.GetNextToken(), MaxEntries: 2,
	})
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	seen := map[string]int{}
	for _, entry := range append(page1.GetEntries(), page2.GetEntries()...) {
		seen[entry.GetSnapshot().GetSnapshotId()]++
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("snapshot %s returned %d times across pages, want exactly 1; seen=%v", id, count, seen)
		}
	}
	if len(page2.GetEntries()) != 1 || page2.GetEntries()[0].GetSnapshot().GetSnapshotId() != "s3" {
		t.Fatalf("page2 = %+v, want [s3] after the s2 cursor", page2)
	}
	if page2.GetNextToken() != "" {
		t.Fatalf("page2 next token = %q, want empty final page", page2.GetNextToken())
	}
}

// TestDeleteVolumeContradictoryStatusNotAbsence binds the contradictory
// status/code guard to the real DeleteVolume call path: a delete ack followed
// by an absence readback returning HTTP 500 carrying the NotFound code (5011)
// must NOT be treated as authoritative absence — DeleteVolume fails closed.
func TestDeleteVolumeContradictoryStatusNotAbsence(t *testing.T) {
	rec := &charEventRecorder{}
	deleted := false
	d := newTestDriver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			rec.record("lun-get")
			if deleted {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(`{"code":5011,"error":"internal"}`))
				return
			}
			w.Write([]byte(`{"id":"lun-1","name":"pvcvol1","size":1073741824,"protocols":{}}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v2/target/fc"):
			w.Write([]byte(`[]`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v2/snapshot/list"):
			w.Write([]byte(`[]`))
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/api/v2/lun/lun-1"):
			rec.record("lun-delete")
			deleted = true
			w.Write([]byte(`{"success":true}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	resp, err := d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{
		VolumeId: helper.VolumeID(volumeIDPrefix, "lun-1"),
	})
	if err == nil || resp != nil {
		t.Fatalf("DeleteVolume succeeded on contradictory 500+NotFound readback: resp=%v err=%v", resp, err)
	}
	if code := status.Code(err); code != codes.DeadlineExceeded && code != codes.Internal {
		t.Fatalf("code = %v, want DeadlineExceeded/Internal (err=%v)", code, err)
	}
	events := rec.snapshot()
	if charCount(events, "lun-delete") != 1 {
		t.Fatalf("backend call pattern = %v, want exactly one LUN DELETE", events)
	}
	if charLastIndex(events, "lun-get") <= charLastIndex(events, "lun-delete") {
		t.Fatalf("expected an absence readback GET after the delete ack: %v", events)
	}
}
