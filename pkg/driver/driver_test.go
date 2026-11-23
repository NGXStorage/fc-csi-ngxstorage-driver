package driver

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

func TestNewDriverRejectsInvalidConfiguration(t *testing.T) {
	valid := InputParameters{
		Ip:       "10.0.0.1",
		PoolName: "pool1",
		ApiKey:   "test-key",
		Endpoint: "unix:///csi/csi.sock",
		NodeId:   "node-1",
		Role:     "controller",
	}
	tests := []struct {
		name   string
		mutate func(*InputParameters)
		want   string
	}{
		{"empty IP", func(p *InputParameters) { p.Ip = "" }, "IP is empty"},
		{"default IP", func(p *InputParameters) { p.Ip = "defaultIPValue" }, "IP is empty"},
		{"empty pool", func(p *InputParameters) { p.PoolName = "" }, "PoolName is empty"},
		{"default pool", func(p *InputParameters) { p.PoolName = "defaultPoolNameValue" }, "PoolName is empty"},
		{"empty API key", func(p *InputParameters) { p.ApiKey = "" }, "ApiKey is empty"},
		{"default API key", func(p *InputParameters) { p.ApiKey = "defaultApiKeyValue" }, "ApiKey is empty"},
		{"empty endpoint", func(p *InputParameters) { p.Endpoint = "" }, "Endpoint is empty"},
		{"default endpoint", func(p *InputParameters) { p.Endpoint = "defaultEndpointValue" }, "Endpoint is empty"},
		{"invalid role", func(p *InputParameters) { p.Role = "sideways" }, "Role must be node, controller, or all"},
		{"node role without node ID", func(p *InputParameters) { p.Role = "node"; p.NodeId = "" }, "NodeID is empty"},
		{"all role without node ID", func(p *InputParameters) { p.Role = "all"; p.NodeId = "defaultNodeIDValue" }, "NodeID is empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := valid
			tt.mutate(&params)
			_, err := NewDriver(params)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("error = %v, want InvalidArgument", err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want message containing %q", err, tt.want)
			}
		})
	}
}

func TestNewDriverDefaultsRoleToNode(t *testing.T) {
	// An empty role defaults to node, so a missing node ID must be rejected
	// even though the caller never asked for the node role explicitly.
	_, err := NewDriver(InputParameters{
		Ip:       "10.0.0.1",
		PoolName: "pool1",
		ApiKey:   "test-key",
		Endpoint: "unix:///csi/csi.sock",
	})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "NodeID is empty") {
		t.Fatalf("error = %v, want InvalidArgument NodeID default-role rejection", err)
	}
}

func TestInputParametersStringRedactsAPIKey(t *testing.T) {
	p := InputParameters{
		Endpoint: "unix:///csi/csi.sock",
		Ip:       "10.0.0.1",
		PoolName: "pool1",
		ApiKey:   "super-secret-key-value",
		NodeId:   "node-1",
		Role:     "node",
	}
	out := p.String()
	if strings.Contains(out, "super-secret-key-value") {
		t.Fatalf("String() leaked the API key: %s", out)
	}
	if !strings.Contains(out, "[REDACTED]") {
		t.Fatalf("String() missing redaction marker: %s", out)
	}
}

func TestNewDriverPoolNotFoundMapsNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/status/cluster"):
			w.Write([]byte(`{"connected":1,"status":"Master","peers":[]}`))
		case strings.HasSuffix(r.URL.Path, "/api/v2/pool"):
			w.Write([]byte(`[]`))
		default:
			w.Write([]byte(`{"connected":1,"status":"Master"}`))
		}
	}))
	defer server.Close()
	transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		req.URL.Scheme = "http"
		req.URL.Host = server.Listener.Addr().String()
		return http.DefaultTransport.RoundTrip(req)
	})
	_, err := NewDriverWithHTTPClient(InputParameters{
		Ip:            "10.0.0.1",
		PoolName:      "absent-pool",
		ApiKey:        "test-key",
		Endpoint:      "http://127.0.0.1",
		NodeId:        "node-1",
		Role:          "controller",
		FcTargetNames: "fc-target-1",
	}, &http.Client{Transport: transport})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("error = %v, want NotFound for absent configured pool", err)
	}
}

func TestNewDriverFCTargetNamesRequired(t *testing.T) {
	// FC targets are pre-created and configured; an empty target list is a
	// local configuration error, rejected before any backend call.
	_, err := NewDriver(InputParameters{
		Ip:       "10.0.0.1",
		PoolName: "pool1",
		ApiKey:   "test-key",
		Endpoint: "unix:///csi/csi.sock",
		NodeId:   "node-1",
		Role:     "controller",
	})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "FC target names are required") {
		t.Fatalf("error = %v, want InvalidArgument FC target names required", err)
	}
}

func TestPrepareCoordinationUsesInjectedCoordinatorAndFailsClosed(t *testing.T) {
	coord := newFakeCoordinator()
	d := &Driver{Role: "node", coordination: coord}
	if err := d.prepareCoordination(context.Background()); err != nil {
		t.Fatalf("preflight success rejected: %v", err)
	}
	coord.preflightErr = errors.New("forbidden")
	if err := d.prepareCoordination(context.Background()); err == nil {
		t.Fatal("preflight failure must fail closed")
	}
}
