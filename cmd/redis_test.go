package cmd

import "testing"

func TestGetThreadCountUsesEnvValue(t *testing.T) {
	t.Setenv("THREADS", "8")

	if got := getThreadCount(); got != 8 {
		t.Fatalf("expected 8 worker threads, got %d", got)
	}
}

func TestGetThreadCountDefaultsToPositiveValue(t *testing.T) {
	t.Setenv("THREADS", "")

	if got := getThreadCount(); got <= 0 {
		t.Fatalf("expected a positive worker thread count, got %d", got)
	}
}
