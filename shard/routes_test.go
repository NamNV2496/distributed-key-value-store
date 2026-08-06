package shard

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/namnv2496/go-redis-raft/dashboard"
	"github.com/namnv2496/go-redis-raft/proxy"
)

func mustRegister(t *testing.T, mode string, register func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("%s: registering routes panicked: %v", mode, r)
		}
	}()
	register()
}

func statusCode(t *testing.T, mux *http.ServeMux, method, target string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(method, target, nil))
	return rec.Code
}

func TestNodeRouteWiringIsClusterSurfaceOnly(t *testing.T) {
	tc := twoShardCluster(t)
	m := tc.node("node1").manager

	mux := http.NewServeMux()
	mustRegister(t, "node", func() { m.Routes(mux) })

	// The writable control plane belongs to a node and must stay writable.
	if got := statusCode(t, mux, http.MethodPost, "/cluster/topology"); got == http.StatusMethodNotAllowed {
		t.Error("node rejects POST /cluster/topology; a node installs topologies")
	}
	if got := statusCode(t, mux, http.MethodPost, "/cluster/rebalance"); got == http.StatusNotFound {
		t.Error("node has no /cluster/rebalance; rebalancing lives here")
	}
	for _, path := range []string{"/status", "/health", "/cluster/status", "/cluster/topology"} {
		if got := statusCode(t, mux, http.MethodGet, path); got != http.StatusOK {
			t.Errorf("node GET %s = %d, want 200 — the dashboard reads this", path, got)
		}
	}

	// But no UI: a node has one job.
	if got := statusCode(t, mux, http.MethodGet, "/dashboard/"); got != http.StatusNotFound {
		t.Errorf("node GET /dashboard/ = %d, want 404 — the dashboard is its own process", got)
	}
	if got := statusCode(t, mux, http.MethodGet, "/"); got != http.StatusNotFound {
		t.Errorf("node GET / = %d, want 404 — there is no UI to redirect to", got)
	}
}

func TestProxyRouteWiringIsDataPathOnly(t *testing.T) {
	p, err := proxy.New(proxy.Config{
		ID:              "px",
		Seeds:           []string{deadSeed},
		RefreshInterval: time.Hour,
		RequestTimeout:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Stop)

	mux := http.NewServeMux()
	mustRegister(t, "proxy", func() { p.Routes(mux) })

	for _, tc := range []struct {
		method, path string
		want         int
		why          string
	}{
		{http.MethodGet, "/dashboard/", http.StatusNotFound, "a proxy serves no UI"},
		{http.MethodGet, "/", http.StatusNotFound, "and so has no root redirect"},
		{http.MethodPost, "/cluster/rebalance", http.StatusNotFound, "rebalancing stays on the nodes"},
		{http.MethodPost, "/cluster/shards", http.StatusNotFound, "shard membership stays on the nodes"},
		{http.MethodPost, "/raft/append", http.StatusNotFound, "Raft RPC stays on the nodes"},
		{http.MethodPost, "/cluster/topology", http.StatusMethodNotAllowed, "the topology is read-only here"},
	} {
		if got := statusCode(t, mux, tc.method, tc.path); got != tc.want {
			t.Errorf("proxy %s %s = %d, want %d — %s", tc.method, tc.path, got, tc.want, tc.why)
		}
	}
}

func TestWatcherRouteWiring(t *testing.T) {
	w, err := dashboard.New([]string{deadSeed})
	if err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	mustRegister(t, "watcher", func() { w.Routes(mux) })

	if got := statusCode(t, mux, http.MethodGet, "/dashboard/"); got != http.StatusOK {
		t.Errorf("watcher GET /dashboard/ = %d, want 200", got)
	}
	if got := statusCode(t, mux, http.MethodPost, "/kv"); got != http.StatusNotFound {
		t.Errorf("watcher POST /kv = %d, want 404 — a dashboard is not a data path", got)
	}
	if got := statusCode(t, mux, http.MethodPost, "/cluster/topology"); got != http.StatusMethodNotAllowed {
		t.Errorf("watcher POST /cluster/topology = %d, want 405", got)
	}
}
