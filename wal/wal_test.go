package wal

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func newTestWAL(t *testing.T, path string) *WAL {
	t.Helper()
	w, err := NewWAL(path)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	return w
}

// An appended entry must be readable after reopening the file. Before the fix
// Append only flushed the bufio layer, leaving the bytes in the OS page cache,
// so a crash could lose an entry Raft had already acknowledged as committed.
func TestAppendSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")

	w := newTestWAL(t, path)
	if err := w.Append("SET", 1, 0, []byte(`{"cmd":"SET"}`)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// Deliberately do NOT Close: Append alone must have made it durable.
	size, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if size.Size() == 0 {
		t.Fatal("Append returned but nothing reached the file")
	}

	reopened := newTestWAL(t, path)
	defer reopened.Close()
	if got := reopened.Len(); got != 1 {
		t.Fatalf("expected 1 entry after reopen, got %d", got)
	}
	if e := reopened.GetEntry(0); e == nil || e.Cmd != "SET" {
		t.Fatalf("entry did not round-trip: %+v", e)
	}
}

func TestAppendBatchIsDurableAndOrdered(t *testing.T) {
	path := filepath.Join(t.TempDir(), "batch.wal")

	w := newTestWAL(t, path)
	batch := []LogEntry{
		{Cmd: "A", Term: 1, Index: 0},
		{Cmd: "B", Term: 1, Index: 1},
		{Cmd: "C", Term: 2, Index: 2},
	}
	if err := w.AppendBatch(batch); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	reopened := newTestWAL(t, path)
	defer reopened.Close()
	if got := reopened.Len(); got != 3 {
		t.Fatalf("expected 3 entries, got %d", got)
	}
	for i, want := range []string{"A", "B", "C"} {
		e := reopened.GetEntry(int64(i))
		if e == nil || e.Cmd != want {
			t.Fatalf("entry %d: got %+v, want Cmd=%s", i, e, want)
		}
	}
}

// A crash partway through writing a record leaves a truncated tail. load()
// must drop it and keep every complete entry before it, rather than erroring
// out or (as the old reader.Read loop did) silently stopping early and
// treating an incomplete read as a clean end of file.
func TestLoadRecoversFromTornTailRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "torn.wal")

	w := newTestWAL(t, path)
	for i := 0; i < 3; i++ {
		if err := w.Append("SET", 1, int64(i), []byte(`{"cmd":"SET"}`)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	w.Close()

	// Simulate a crash mid-append: a length prefix promising more bytes than follow.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	lengthBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBuf, 500)
	if _, err := f.Write(append(lengthBuf, []byte("only a few bytes")...)); err != nil {
		t.Fatalf("write torn record: %v", err)
	}
	f.Close()

	recovered := newTestWAL(t, path)
	defer recovered.Close()

	if got := recovered.Len(); got != 3 {
		t.Fatalf("expected the 3 complete entries to survive, got %d", got)
	}

	// The file must have been truncated back to a clean boundary, so the next
	// append lands where the reader expects it.
	if err := recovered.Append("SET", 1, 3, []byte(`{"cmd":"SET"}`)); err != nil {
		t.Fatalf("Append after recovery: %v", err)
	}
	again := newTestWAL(t, path)
	defer again.Close()
	if got := again.Len(); got != 4 {
		t.Fatalf("expected 4 entries after appending post-recovery, got %d", got)
	}
}

// A corrupt length prefix must not make load() allocate wildly.
func TestLoadRejectsAbsurdEntryLength(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.wal")

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	lengthBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBuf, 0xFFFFFFFF) // ~4 GiB
	f.Write(lengthBuf)
	f.Write([]byte("junk"))
	f.Close()

	if _, err := NewWAL(path); err == nil {
		t.Fatal("expected an error for an entry length beyond the sanity bound")
	}
}

// GetEntries must hand back a copy. Returning a sub-slice aliased the backing
// array, so a concurrent TruncateAfter or Append could mutate entries the
// caller was still reading.
func TestGetEntriesReturnsACopy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "copy.wal")
	w := newTestWAL(t, path)
	defer w.Close()

	for i := 0; i < 3; i++ {
		if err := w.Append("SET", 1, int64(i), nil); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	got := w.GetEntries(0, 0)
	got[0].Cmd = "MUTATED"

	if e := w.GetEntry(0); e.Cmd != "SET" {
		t.Fatalf("mutating the returned slice changed the log: %q", e.Cmd)
	}
}

func TestGetEntriesRespectsLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "limit.wal")
	w := newTestWAL(t, path)
	defer w.Close()

	for i := 0; i < 10; i++ {
		if err := w.Append("SET", 1, int64(i), nil); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	if got := len(w.GetEntries(2, 3)); got != 3 {
		t.Fatalf("expected 3 entries, got %d", got)
	}
	if got := len(w.GetEntries(2, 0)); got != 8 {
		t.Fatalf("limit 0 should mean unbounded; got %d", got)
	}
	if got := len(w.GetEntries(20, 5)); got != 0 {
		t.Fatalf("reading past the end should be empty; got %d", got)
	}
}

func TestTruncateAfterPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trunc.wal")
	w := newTestWAL(t, path)

	for i := 0; i < 5; i++ {
		if err := w.Append("SET", 1, int64(i), nil); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.TruncateAfter(2); err != nil {
		t.Fatalf("TruncateAfter: %v", err)
	}
	w.Close()

	reopened := newTestWAL(t, path)
	defer reopened.Close()
	if got := reopened.Len(); got != 3 {
		t.Fatalf("expected 3 entries on disk after truncation, got %d", got)
	}
}
