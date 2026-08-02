package wal

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
)

// maxEntryBytes bounds a single record so a corrupt length prefix cannot make
// load() allocate an arbitrary amount of memory.
const maxEntryBytes = 64 << 20 // 64 MiB

// LogEntry represents a WAL entry
type LogEntry struct {
	Cmd   string `json:"cmd"`
	Term  int64  `json:"term"`
	Index int64  `json:"index"`
	Data  []byte `json:"data"`
}

// WAL manages write-ahead logging for persistence
type WAL struct {
	mu       sync.Mutex
	file     *os.File
	writer   *bufio.Writer
	filename string
	entries  []LogEntry
}

// NewWAL creates a new WAL instance
func NewWAL(filename string) (*WAL, error) {
	wal := &WAL{
		filename: filename,
		entries:  make([]LogEntry, 0),
	}

	// Open or create the file
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	wal.file = file
	wal.writer = bufio.NewWriter(file)

	// Load existing entries
	if err := wal.load(); err != nil {
		return nil, fmt.Errorf("failed to load WAL: %w", err)
	}

	return wal, nil
}

// Append adds a new entry to the WAL and fsyncs it to stable storage before
// returning. Raft may only acknowledge an entry once it survives a crash, so
// this call is deliberately synchronous — buffered writes that live in the OS
// page cache are not durable.
func (w *WAL) Append(cmd string, term, index int64, data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	entry := LogEntry{
		Cmd:   cmd,
		Term:  term,
		Index: index,
		Data:  data,
	}
	// Write to file
	if err := w.writeEntry(entry); err != nil {
		return err
	}
	if err := w.sync(); err != nil {
		return err
	}
	w.entries = append(w.entries, entry)
	return nil
}

// AppendBatch appends every entry and fsyncs ONCE at the end.
//
// A follower receiving N entries in one AppendEntries RPC would otherwise pay N
// fsyncs while the Raft mutex is held, which is both slow and enough to make
// heartbeats miss their deadline and trigger spurious elections. One RPC, one
// sync: the batch is still durable before the follower acknowledges it.
func (w *WAL) AppendBatch(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, entry := range entries {
		if err := w.writeEntry(entry); err != nil {
			return err
		}
	}
	if err := w.sync(); err != nil {
		return err
	}
	w.entries = append(w.entries, entries...)
	return nil
}

// sync flushes the bufio layer and fsyncs the file. Caller must hold w.mu.
func (w *WAL) sync() error {
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to fsync WAL: %w", err)
	}
	return nil
}

// GetEntries returns at most limit entries starting at fromIndex.
//
// The result is a copy: returning a sub-slice of w.entries would alias the
// backing array, which TruncateAfter and Append are free to reallocate or
// overwrite while a caller still holds it.
func (w *WAL) GetEntries(fromIndex int64, limit int) []LogEntry {
	w.mu.Lock()
	defer w.mu.Unlock()

	if fromIndex < 0 {
		fromIndex = 0
	}
	if fromIndex >= int64(len(w.entries)) {
		return []LogEntry{}
	}
	end := int64(len(w.entries))
	if limit > 0 && fromIndex+int64(limit) < end {
		end = fromIndex + int64(limit)
	}
	out := make([]LogEntry, end-fromIndex)
	copy(out, w.entries[fromIndex:end])
	return out
}

// GetEntry returns a copy of a specific entry, or nil if the index is absent.
func (w *WAL) GetEntry(index int64) *LogEntry {
	w.mu.Lock()
	defer w.mu.Unlock()

	if index < 0 || index >= int64(len(w.entries)) {
		return nil
	}
	entry := w.entries[index]
	return &entry
}

// LastEntry returns a copy of the last entry in the log.
func (w *WAL) LastEntry() *LogEntry {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.entries) == 0 {
		return nil
	}
	entry := w.entries[len(w.entries)-1]
	return &entry
}

// TruncateAfter removes all entries after the given index
func (w *WAL) TruncateAfter(index int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if index < 0 {
		w.entries = w.entries[:0]
		return w.rewrite()
	}

	if index >= int64(len(w.entries)) {
		return nil
	}

	w.entries = w.entries[:index+1]

	return w.rewrite()
}

// load reads existing entries from file.
//
// It uses io.ReadFull so a short read can never silently truncate the log.
// A partial record at the tail is the expected result of a crash mid-append:
// that record is discarded and the file is truncated back to the last complete
// entry, so the next Append starts from a clean boundary. A malformed record
// anywhere before the tail means real corruption and is reported as an error.
func (w *WAL) load() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	reader := bufio.NewReader(w.file)
	var validBytes int64

	for {
		lengthBuf := make([]byte, 4)
		if _, err := io.ReadFull(reader, lengthBuf); err != nil {
			if err == io.EOF {
				break // clean end of file
			}
			if err == io.ErrUnexpectedEOF {
				break // torn length prefix — drop it
			}
			return fmt.Errorf("failed to read entry length: %w", err)
		}

		length := binary.BigEndian.Uint32(lengthBuf)
		if length > maxEntryBytes {
			return fmt.Errorf("wal corrupt: entry at offset %d declares %d bytes (max %d)",
				validBytes, length, maxEntryBytes)
		}

		dataBuf := make([]byte, length)
		if _, err := io.ReadFull(reader, dataBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break // torn payload — drop it
			}
			return fmt.Errorf("failed to read entry payload: %w", err)
		}

		var entry LogEntry
		if err := json.Unmarshal(dataBuf, &entry); err != nil {
			return fmt.Errorf("wal corrupt: failed to unmarshal entry at offset %d: %w", validBytes, err)
		}

		w.entries = append(w.entries, entry)
		validBytes += 4 + int64(length)
	}

	// Drop any trailing partial record left behind by a crash.
	size, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if size != validBytes {
		if err := w.file.Truncate(validBytes); err != nil {
			return fmt.Errorf("failed to truncate partial tail record: %w", err)
		}
		if _, err := w.file.Seek(validBytes, io.SeekStart); err != nil {
			return err
		}
		w.writer.Reset(w.file)
		if err := w.file.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// writeEntry writes a single entry to the file
func (w *WAL) writeEntry(entry LogEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	lengthBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBuf, uint32(len(data)))

	if _, err := w.writer.Write(lengthBuf); err != nil {
		return err
	}

	if _, err := w.writer.Write(data); err != nil {
		return err
	}

	return nil
}

// rewrite truncates and rewrites the entire file. Caller must hold w.mu.
func (w *WAL) rewrite() error {
	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	w.writer.Reset(w.file)

	for _, entry := range w.entries {
		if err := w.writeEntry(entry); err != nil {
			return err
		}
	}

	return w.sync()
}

// Close closes the WAL file
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.writer != nil {
		if err := w.writer.Flush(); err != nil {
			return err
		}
	}

	if w.file != nil {
		return w.file.Close()
	}

	return nil
}

// Clear removes all entries
func (w *WAL) Clear() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.entries = make([]LogEntry, 0)
	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	w.writer.Reset(w.file)

	return w.sync()
}

// Len returns the number of entries
func (w *WAL) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.entries)
}
