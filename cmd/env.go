package cmd

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

func Get(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func Port(key string, defaultValue int) (int, error) {
	raw := Get(key, strconv.Itoa(defaultValue))
	port, err := strconv.Atoi(raw)
	if err != nil || port <= 0 || port > 65535 {
		return 0, fmt.Errorf("%s must be a port number, got %q", key, raw)
	}
	return port, nil
}

func TopologyPath(dataDir, id string) string {
	name := fmt.Sprintf(".cluster-%s.json", id)
	if dataDir == "" {
		return name
	}
	return filepath.Join(dataDir, name)
}

func Serve(id, name string, port int, handler http.Handler) {
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	log.Printf("[%s] %s HTTP server listening on %s", id, name, srv.Addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("[%s] %s HTTP server error: %v", id, name, err)
	}
}

func WaitForSignal() os.Signal {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	return <-sigChan
}

func DashboardPortRequested() bool {
	dash := Get("DASHBOARD_PORT", "")
	return dash != "" && dash != "off" && dash != "0"
}
