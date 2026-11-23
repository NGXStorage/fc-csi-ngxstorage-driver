package helper

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	ngxstorage "github.com/ngxstorage/ngxstorage-sdk-go/pkg/ngxstorage"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// verifyRoundTripper adapts a func to http.RoundTripper for the fake backend.
type verifyRoundTripper func(*http.Request) (*http.Response, error)

func (f verifyRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// newVerifyTestReconciler builds a Reconciler whose SDK client points at a
// mock NGX API. failTransport, when non-nil, injects a transport error for
// matching requests before they reach the server.
func newVerifyTestReconciler(t *testing.T, timeout, interval time.Duration, failTransport func(*http.Request) bool, handler http.HandlerFunc) *Reconciler {
	t.Helper()
	wrap := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Default routing for SDK controller selection: cluster status and
		// pool list/detail. Operation-specific paths delegate to handler.
		switch {
		case strings.Contains(r.URL.Path, "/status/cluster"):
			w.Write([]byte(`{"connected":1,"status":"Master","peers":[]}`))
		case strings.HasSuffix(r.URL.Path, "/api/v2/pool"):
			w.Write([]byte(`[{"id":"pool1","name":"pool1","owner":"o"}]`))
		case strings.HasSuffix(r.URL.Path, "/api/v2/pool/pool1"):
			w.Write([]byte(`{"id":"pool1","name":"pool1","owner":"o"}`))
		default:
			handler(w, r)
		}
	})
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on 127.0.0.1: %v", err)
	}
	server := &httptest.Server{Listener: listener, Config: &http.Server{Handler: wrap}}
	server.Start()
	t.Cleanup(server.Close)

	transport := verifyRoundTripper(func(req *http.Request) (*http.Response, error) {
		if failTransport != nil && failTransport(req) {
			return nil, fmt.Errorf("read tcp: EOF")
		}
		req.URL.Scheme = "http"
		req.URL.Host = server.Listener.Addr().String()
		return http.DefaultTransport.RoundTrip(req)
	})
	client, err := ngxstorage.NewClient(ngxstorage.Config{
		Controllers: []string{"127.0.0.1"},
		APIKey:      "test-key",
		PoolName:    "pool1",
		HTTPClient:  &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatalf("ngxstorage.NewClient: %v", err)
	}
	return &Reconciler{Client: client, DeleteVerifyTimeout: timeout, DeleteVerifyInterval: interval}
}

const verifyLUNPresentBody = `{"id":"lun-1","name":"pvcvol1","size":1073741824,"protocols":{}}`

func writeVerifyLUNAbsent(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNotFound)
	w.Write([]byte(`{"code":5011,"error":"not found"}`))
}

// TestVerifyLUNDeleted pins the bounded read-after-delete semantics: only an
// authoritative NotFound proves absence; a present LUN or an ambiguous GET
// exhausts the budget and fails closed, never a fake success.
func TestVerifyLUNDeleted(t *testing.T) {
	t.Run("canonical NotFound is success", func(t *testing.T) {
		r := newVerifyTestReconciler(t, 150*time.Millisecond, 20*time.Millisecond, nil,
			func(w http.ResponseWriter, req *http.Request) { writeVerifyLUNAbsent(w) })
		if err := r.VerifyLUNDeleted(context.Background(), "lun-1"); err != nil {
			t.Fatalf("VerifyLUNDeleted: %v", err)
		}
	})

	t.Run("code-less HTTP 404 is success", func(t *testing.T) {
		r := newVerifyTestReconciler(t, 150*time.Millisecond, 20*time.Millisecond, nil,
			func(w http.ResponseWriter, req *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"error":"not found"}`))
			})
		if err := r.VerifyLUNDeleted(context.Background(), "lun-1"); err != nil {
			t.Fatalf("VerifyLUNDeleted: %v", err)
		}
	})

	t.Run("zero budgets fall back to package defaults", func(t *testing.T) {
		r := newVerifyTestReconciler(t, 0, 0, nil,
			func(w http.ResponseWriter, req *http.Request) { writeVerifyLUNAbsent(w) })
		if err := r.VerifyLUNDeleted(context.Background(), "lun-1"); err != nil {
			t.Fatalf("VerifyLUNDeleted with default budgets: %v", err)
		}
	})

	t.Run("present through budget fails closed", func(t *testing.T) {
		gets := 0
		r := newVerifyTestReconciler(t, 150*time.Millisecond, 20*time.Millisecond, nil,
			func(w http.ResponseWriter, req *http.Request) {
				gets++
				w.Write([]byte(verifyLUNPresentBody))
			})
		err := r.VerifyLUNDeleted(context.Background(), "lun-1")
		if err == nil {
			t.Fatal("VerifyLUNDeleted succeeded while the LUN is still present")
		}
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("code = %v, want DeadlineExceeded (err=%v)", status.Code(err), err)
		}
		if !strings.Contains(err.Error(), "absence NOT proven") {
			t.Fatalf("error must state absence is not proven: %v", err)
		}
		if gets < 2 {
			t.Fatalf("verification GETs = %d, want bounded retries within the budget", gets)
		}
	})

	t.Run("delayed absence converges", func(t *testing.T) {
		gets := 0
		r := newVerifyTestReconciler(t, 2*time.Second, 10*time.Millisecond, nil,
			func(w http.ResponseWriter, req *http.Request) {
				gets++
				if gets < 3 {
					w.Write([]byte(verifyLUNPresentBody))
					return
				}
				writeVerifyLUNAbsent(w)
			})
		if err := r.VerifyLUNDeleted(context.Background(), "lun-1"); err != nil {
			t.Fatalf("VerifyLUNDeleted should converge on delayed absence: %v", err)
		}
		if gets != 3 {
			t.Fatalf("verification GETs = %d, want 3", gets)
		}
	})

	t.Run("transient transport error then absence converges", func(t *testing.T) {
		failed := false
		gets := 0
		r := newVerifyTestReconciler(t, 2*time.Second, 10*time.Millisecond,
			func(req *http.Request) bool {
				if !failed && req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/api/v2/lun/lun-1") {
					failed = true
					return true
				}
				return false
			},
			func(w http.ResponseWriter, req *http.Request) {
				gets++
				if gets == 1 {
					w.Write([]byte(verifyLUNPresentBody))
					return
				}
				writeVerifyLUNAbsent(w)
			})
		if err := r.VerifyLUNDeleted(context.Background(), "lun-1"); err != nil {
			t.Fatalf("VerifyLUNDeleted should retry an ambiguous GET and converge: %v", err)
		}
	})

	t.Run("persistent transport error fails closed", func(t *testing.T) {
		r := newVerifyTestReconciler(t, 150*time.Millisecond, 20*time.Millisecond,
			func(req *http.Request) bool {
				return req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/api/v2/lun/lun-1")
			},
			func(w http.ResponseWriter, req *http.Request) {
				t.Error("transport-failed GET must not reach the handler")
			})
		err := r.VerifyLUNDeleted(context.Background(), "lun-1")
		if err == nil {
			t.Fatal("VerifyLUNDeleted treated a persistent transport error as absence")
		}
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("code = %v, want DeadlineExceeded (err=%v)", status.Code(err), err)
		}
		if !strings.Contains(err.Error(), "absence NOT proven") {
			t.Fatalf("error must state absence is not proven: %v", err)
		}
	})

	t.Run("persistent 5xx fails closed", func(t *testing.T) {
		r := newVerifyTestReconciler(t, 150*time.Millisecond, 20*time.Millisecond, nil,
			func(w http.ResponseWriter, req *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(`{"code":5000,"error":"backend unavailable"}`))
			})
		err := r.VerifyLUNDeleted(context.Background(), "lun-1")
		if err == nil || status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("VerifyLUNDeleted = %v, want DeadlineExceeded on persistent 5xx", err)
		}
	})

	t.Run("403 is never absence", func(t *testing.T) {
		r := newVerifyTestReconciler(t, 150*time.Millisecond, 20*time.Millisecond, nil,
			func(w http.ResponseWriter, req *http.Request) {
				w.WriteHeader(http.StatusForbidden)
				w.Write([]byte(`{"code":4030,"error":"forbidden"}`))
			})
		err := r.VerifyLUNDeleted(context.Background(), "lun-1")
		if err == nil || status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("VerifyLUNDeleted = %v, want DeadlineExceeded on persistent 403", err)
		}
	})

	t.Run("parent context cancellation propagates", func(t *testing.T) {
		r := newVerifyTestReconciler(t, time.Second, 10*time.Millisecond, nil,
			func(w http.ResponseWriter, req *http.Request) {
				w.Write([]byte(verifyLUNPresentBody))
			})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := r.VerifyLUNDeleted(ctx, "lun-1")
		if status.Code(err) != codes.Canceled {
			t.Fatalf("code = %v, want Canceled from the parent context (err=%v)", status.Code(err), err)
		}
	})
}

// TestVerifyLUNDeletedHTTPErrorNotAbsence pins the narrow safety fix on the
// REAL helper/SDK path (SDK client against a fake NGX API): an HTTP error
// status must never be converted into absence-success through the NotFound
// code path. Only an authoritative HTTP 404 carrying the canonical NotFound
// code (5011) or no code proves absence. A 404 with a different explicit
// code, any non-404 error status (even one carrying code 5011), or a
// transport failure stays ambiguous and fails closed.
func TestVerifyLUNDeletedHTTPErrorNotAbsence(t *testing.T) {
	notAbsence := []struct {
		name   string
		status int
		body   string
	}{
		{"404 with AlreadyExists code", 404, `{"code":710,"error":"exists"}`},
		{"404 with AttachedBusy code", 404, `{"code":534,"error":"busy"}`},
		{"404 with unknown explicit code", 404, `{"code":9999,"error":"other"}`},
		{"403 carrying NotFound code", 403, `{"code":5011,"error":"forbidden"}`},
		{"403 forbidden", 403, `{"code":5013,"error":"forbidden"}`},
		{"500 carrying NotFound code", 500, `{"code":5011,"error":"internal"}`},
		{"500 internal", 500, `{"code":5000,"error":"internal"}`},
		{"503 unavailable", 503, `{"error":"unavailable"}`},
	}
	for _, tc := range notAbsence {
		t.Run(tc.name, func(t *testing.T) {
			gets := 0
			r := newVerifyTestReconciler(t, 150*time.Millisecond, 20*time.Millisecond, nil,
				func(w http.ResponseWriter, req *http.Request) {
					gets++
					w.WriteHeader(tc.status)
					w.Write([]byte(tc.body))
				})
			err := r.VerifyLUNDeleted(context.Background(), "lun-1")
			if err == nil {
				t.Fatalf("HTTP %d body %s was converted to absence-success", tc.status, tc.body)
			}
			if status.Code(err) != codes.DeadlineExceeded || !strings.Contains(err.Error(), "absence NOT proven") {
				t.Fatalf("want fail-closed DeadlineExceeded 'absence NOT proven', got %v", err)
			}
			if gets < 1 {
				t.Fatalf("expected bounded verification GETs, got %d", gets)
			}
		})
	}
	t.Run("persistent transport failure is not absence", func(t *testing.T) {
		r := newVerifyTestReconciler(t, 150*time.Millisecond, 20*time.Millisecond,
			func(req *http.Request) bool {
				return req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/api/v2/lun/lun-1")
			},
			func(w http.ResponseWriter, req *http.Request) { writeVerifyLUNAbsent(w) })
		err := r.VerifyLUNDeleted(context.Background(), "lun-1")
		if err == nil || status.Code(err) != codes.DeadlineExceeded || !strings.Contains(err.Error(), "absence NOT proven") {
			t.Fatalf("persistent transport must fail closed, got %v", err)
		}
	})
	t.Run("authoritative 404 controls still succeed", func(t *testing.T) {
		for _, body := range []string{`{"code":5011,"error":"not found"}`, `{"error":"not found"}`} {
			r := newVerifyTestReconciler(t, 150*time.Millisecond, 20*time.Millisecond, nil,
				func(w http.ResponseWriter, req *http.Request) {
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte(body))
				})
			if err := r.VerifyLUNDeleted(context.Background(), "lun-1"); err != nil {
				t.Fatalf("authoritative absence %s: %v", body, err)
			}
		}
	})
}

const verifySnapshotPresentBody = `{"id":"snap-1","name":"snap1","volume_id":"lun-src","ready":true}`

func writeVerifySnapshotAbsent(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNotFound)
	w.Write([]byte(`{"code":5011,"error":"not found"}`))
}

// TestVerifySnapshotDeleted pins the bounded read-after-delete semantics for
// snapshots: only an authoritative NotFound proves absence; a present
// snapshot or an ambiguous GET exhausts the budget and fails closed, never a
// fake success.
func TestVerifySnapshotDeleted(t *testing.T) {
	t.Run("canonical NotFound is success", func(t *testing.T) {
		r := newVerifyTestReconciler(t, 150*time.Millisecond, 20*time.Millisecond, nil,
			func(w http.ResponseWriter, req *http.Request) { writeVerifySnapshotAbsent(w) })
		if err := r.VerifySnapshotDeleted(context.Background(), "snap-1"); err != nil {
			t.Fatalf("VerifySnapshotDeleted: %v", err)
		}
	})

	t.Run("code-less HTTP 404 is success", func(t *testing.T) {
		r := newVerifyTestReconciler(t, 150*time.Millisecond, 20*time.Millisecond, nil,
			func(w http.ResponseWriter, req *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"error":"not found"}`))
			})
		if err := r.VerifySnapshotDeleted(context.Background(), "snap-1"); err != nil {
			t.Fatalf("VerifySnapshotDeleted: %v", err)
		}
	})

	t.Run("zero budgets fall back to package defaults", func(t *testing.T) {
		r := newVerifyTestReconciler(t, 0, 0, nil,
			func(w http.ResponseWriter, req *http.Request) { writeVerifySnapshotAbsent(w) })
		if err := r.VerifySnapshotDeleted(context.Background(), "snap-1"); err != nil {
			t.Fatalf("VerifySnapshotDeleted with default budgets: %v", err)
		}
	})

	t.Run("present through budget fails closed", func(t *testing.T) {
		gets := 0
		r := newVerifyTestReconciler(t, 150*time.Millisecond, 20*time.Millisecond, nil,
			func(w http.ResponseWriter, req *http.Request) {
				gets++
				w.Write([]byte(verifySnapshotPresentBody))
			})
		err := r.VerifySnapshotDeleted(context.Background(), "snap-1")
		if err == nil {
			t.Fatal("VerifySnapshotDeleted succeeded while the snapshot is still present")
		}
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("code = %v, want DeadlineExceeded (err=%v)", status.Code(err), err)
		}
		if !strings.Contains(err.Error(), "absence NOT proven") {
			t.Fatalf("error must state absence is not proven: %v", err)
		}
		if gets < 2 {
			t.Fatalf("verification GETs = %d, want bounded retries within the budget", gets)
		}
	})

	t.Run("delayed absence converges", func(t *testing.T) {
		gets := 0
		r := newVerifyTestReconciler(t, 2*time.Second, 10*time.Millisecond, nil,
			func(w http.ResponseWriter, req *http.Request) {
				gets++
				if gets < 3 {
					w.Write([]byte(verifySnapshotPresentBody))
					return
				}
				writeVerifySnapshotAbsent(w)
			})
		if err := r.VerifySnapshotDeleted(context.Background(), "snap-1"); err != nil {
			t.Fatalf("VerifySnapshotDeleted should converge on delayed absence: %v", err)
		}
		if gets != 3 {
			t.Fatalf("verification GETs = %d, want 3", gets)
		}
	})

	t.Run("transient transport error then absence converges", func(t *testing.T) {
		failed := false
		r := newVerifyTestReconciler(t, 2*time.Second, 10*time.Millisecond,
			func(req *http.Request) bool {
				if !failed && req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/api/v2/snapshot/snap-1") {
					failed = true
					return true
				}
				return false
			},
			func(w http.ResponseWriter, req *http.Request) { writeVerifySnapshotAbsent(w) })
		if err := r.VerifySnapshotDeleted(context.Background(), "snap-1"); err != nil {
			t.Fatalf("VerifySnapshotDeleted should retry an ambiguous GET and converge: %v", err)
		}
	})

	t.Run("persistent transport error fails closed", func(t *testing.T) {
		r := newVerifyTestReconciler(t, 150*time.Millisecond, 20*time.Millisecond,
			func(req *http.Request) bool {
				return req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/api/v2/snapshot/snap-1")
			},
			func(w http.ResponseWriter, req *http.Request) {
				t.Error("transport-failed GET must not reach the handler")
			})
		err := r.VerifySnapshotDeleted(context.Background(), "snap-1")
		if err == nil {
			t.Fatal("VerifySnapshotDeleted treated a persistent transport error as absence")
		}
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("code = %v, want DeadlineExceeded (err=%v)", status.Code(err), err)
		}
		if !strings.Contains(err.Error(), "absence NOT proven") {
			t.Fatalf("error must state absence is not proven: %v", err)
		}
	})

	t.Run("persistent 5xx fails closed", func(t *testing.T) {
		r := newVerifyTestReconciler(t, 150*time.Millisecond, 20*time.Millisecond, nil,
			func(w http.ResponseWriter, req *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(`{"code":5000,"error":"backend unavailable"}`))
			})
		err := r.VerifySnapshotDeleted(context.Background(), "snap-1")
		if err == nil || status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("VerifySnapshotDeleted = %v, want DeadlineExceeded on persistent 5xx", err)
		}
	})

	t.Run("403 is never absence", func(t *testing.T) {
		r := newVerifyTestReconciler(t, 150*time.Millisecond, 20*time.Millisecond, nil,
			func(w http.ResponseWriter, req *http.Request) {
				w.WriteHeader(http.StatusForbidden)
				w.Write([]byte(`{"code":4030,"error":"forbidden"}`))
			})
		err := r.VerifySnapshotDeleted(context.Background(), "snap-1")
		if err == nil || status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("VerifySnapshotDeleted = %v, want DeadlineExceeded on persistent 403", err)
		}
	})

	t.Run("parent context cancellation propagates", func(t *testing.T) {
		r := newVerifyTestReconciler(t, time.Second, 10*time.Millisecond, nil,
			func(w http.ResponseWriter, req *http.Request) {
				w.Write([]byte(verifySnapshotPresentBody))
			})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := r.VerifySnapshotDeleted(ctx, "snap-1")
		if status.Code(err) != codes.Canceled {
			t.Fatalf("code = %v, want Canceled from the parent context (err=%v)", status.Code(err), err)
		}
	})
}

// TestVerifySnapshotDeletedHTTPErrorNotAbsence pins the snapshot absence
// predicate on the REAL helper/SDK path (SDK client against a fake NGX API):
// an HTTP error status must never be converted into absence-success through
// the NotFound code path. Only an authoritative HTTP 404 carrying the
// canonical NotFound code (5011) or no code proves absence. A 404 with a
// different explicit code, any non-404 error status (even one carrying code
// 5011), or a transport failure stays ambiguous and fails closed.
func TestVerifySnapshotDeletedHTTPErrorNotAbsence(t *testing.T) {
	notAbsence := []struct {
		name   string
		status int
		body   string
	}{
		{"404 with AlreadyExists code", 404, `{"code":710,"error":"exists"}`},
		{"404 with AttachedBusy code", 404, `{"code":534,"error":"busy"}`},
		{"404 with unknown explicit code", 404, `{"code":9999,"error":"other"}`},
		{"403 carrying NotFound code", 403, `{"code":5011,"error":"forbidden"}`},
		{"403 forbidden", 403, `{"code":5013,"error":"forbidden"}`},
		{"500 carrying NotFound code", 500, `{"code":5011,"error":"internal"}`},
		{"500 internal", 500, `{"code":5000,"error":"internal"}`},
		{"503 unavailable", 503, `{"error":"unavailable"}`},
	}
	for _, tc := range notAbsence {
		t.Run(tc.name, func(t *testing.T) {
			gets := 0
			r := newVerifyTestReconciler(t, 150*time.Millisecond, 20*time.Millisecond, nil,
				func(w http.ResponseWriter, req *http.Request) {
					gets++
					w.WriteHeader(tc.status)
					w.Write([]byte(tc.body))
				})
			err := r.VerifySnapshotDeleted(context.Background(), "snap-1")
			if err == nil {
				t.Fatalf("HTTP %d body %s was converted to absence-success", tc.status, tc.body)
			}
			if status.Code(err) != codes.DeadlineExceeded || !strings.Contains(err.Error(), "absence NOT proven") {
				t.Fatalf("want fail-closed DeadlineExceeded 'absence NOT proven', got %v", err)
			}
			if gets < 1 {
				t.Fatalf("expected bounded verification GETs, got %d", gets)
			}
		})
	}
	t.Run("persistent transport failure is not absence", func(t *testing.T) {
		r := newVerifyTestReconciler(t, 150*time.Millisecond, 20*time.Millisecond,
			func(req *http.Request) bool {
				return req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/api/v2/snapshot/snap-1")
			},
			func(w http.ResponseWriter, req *http.Request) { writeVerifySnapshotAbsent(w) })
		err := r.VerifySnapshotDeleted(context.Background(), "snap-1")
		if err == nil || status.Code(err) != codes.DeadlineExceeded || !strings.Contains(err.Error(), "absence NOT proven") {
			t.Fatalf("persistent transport must fail closed, got %v", err)
		}
	})
	t.Run("authoritative 404 controls still succeed", func(t *testing.T) {
		for _, body := range []string{`{"code":5011,"error":"not found"}`, `{"error":"not found"}`} {
			r := newVerifyTestReconciler(t, 150*time.Millisecond, 20*time.Millisecond, nil,
				func(w http.ResponseWriter, req *http.Request) {
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte(body))
				})
			if err := r.VerifySnapshotDeleted(context.Background(), "snap-1"); err != nil {
				t.Fatalf("authoritative absence %s: %v", body, err)
			}
		}
	})
}

// TestObjectAbsenceProvenPredicate pins the authoritative-absence predicate
// directly: only an HTTP 404 carrying the canonical NotFound code (5011) or
// no code proves absence. A contradictory status/code pair, a non-404 error,
// a pool/controller discovery error, or a transport failure is NEVER absence
// (another object's or discovery error must not count as LUN absence).
func TestObjectAbsenceProvenPredicate(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"404 canonical NotFound", &ngxstorage.APIError{StatusCode: 404, Code: ngxstorage.CodeNotFound}, true},
		{"404 code-less", &ngxstorage.APIError{StatusCode: 404, Code: 0}, true},
		{"wrapped 404 canonical NotFound", fmt.Errorf("get LUN: %w", &ngxstorage.APIError{StatusCode: 404, Code: ngxstorage.CodeNotFound}), true},
		{"404 with AlreadyExists code", &ngxstorage.APIError{StatusCode: 404, Code: ngxstorage.CodeAlreadyExists}, false},
		{"403 carrying NotFound code", &ngxstorage.APIError{StatusCode: 403, Code: ngxstorage.CodeNotFound}, false},
		{"500 carrying NotFound code", &ngxstorage.APIError{StatusCode: 500, Code: ngxstorage.CodeNotFound}, false},
		{"503 code-less", &ngxstorage.APIError{StatusCode: 503, Code: 0}, false},
		{"pool discovery error", ngxstorage.ErrPoolNotFound, false},
		{"cluster not ready", ngxstorage.ErrClusterNotReady, false},
		{"transport error", &ngxstorage.TransportError{}, false},
		{"plain error", fmt.Errorf("boom"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := objectAbsenceProven(tc.err); got != tc.want {
				t.Fatalf("objectAbsenceProven(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
