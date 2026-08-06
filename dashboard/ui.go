package dashboard

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed assets/*
var assetFS embed.FS

// registerUI adds the embedded page and its assets.
func registerUI(mux *http.ServeMux) {
	mux.HandleFunc("/dashboard", serveAsset)
	mux.HandleFunc("/dashboard/{asset...}", serveAsset)
	mux.HandleFunc("/{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/dashboard/", http.StatusFound)
	})
}

func serveAsset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	assets, err := fs.Sub(assetFS, "assets")
	if err != nil {
		http.Error(w, "dashboard assets unavailable", http.StatusInternalServerError)
		return
	}

	name := r.PathValue("asset")
	if name == "" {
		name = "index.html"
	}
	if _, err := fs.Stat(assets, name); err != nil {
		name = "index.html"
	}
	// No caching: a dashboard that shows a stale cluster is worse than none.
	w.Header().Set("Cache-Control", "no-store")
	http.ServeFileFS(w, r, assets, name)
}
