package wal

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

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

// Append adds a new entry to the WAL
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
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL: %w", err)
	}
	w.entries = append(w.entries, entry)
	return nil
}

// GetEntries returns all entries from a given index
func (w *WAL) GetEntries(fromIndex int64) []LogEntry {
	w.mu.Lock()
	defer w.mu.Unlock()

	if fromIndex >= int64(len(w.entries)) {
		return []LogEntry{}
	}
	return w.entries[fromIndex:]
}

// GetEntry returns a specific entry
func (w *WAL) GetEntry(index int64) *LogEntry {
	w.mu.Lock()
	defer w.mu.Unlock()

	if index < 0 || index >= int64(len(w.entries)) {
		return nil
	}
	return &w.entries[index]
}

// LastEntry returns the last entry in the log
func (w *WAL) LastEntry() *LogEntry {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.entries) == 0 {
		return nil
	}
	return &w.entries[len(w.entries)-1]
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

// Load reads existing entries from file
func (w *WAL) load() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}

	reader := bufio.NewReader(w.file)
	for {
		lengthBuf := make([]byte, 4)
		n, err := reader.Read(lengthBuf)
		if n == 0 || err != nil {
			break
		}
		if n != 4 {
			break
		}

		length := binary.BigEndian.Uint32(lengthBuf)
		dataBuf := make([]byte, length)
		n, err = reader.Read(dataBuf)
		if n != int(length) || err != nil {
			break
		}

		var entry LogEntry
		if err := json.Unmarshal(dataBuf, &entry); err != nil {
			return fmt.Errorf("failed to unmarshal entry: %w", err)
		}

		w.entries = append(w.entries, entry)
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

// rewrite truncates and rewrites the entire file
func (w *WAL) rewrite() error {
	w.file.Truncate(0)
	w.file.Seek(0, 0)
	w.writer.Reset(w.file)

	for _, entry := range w.entries {
		if err := w.writeEntry(entry); err != nil {
			return err
		}
	}

	return w.writer.Flush()
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
	w.file.Truncate(0)
	w.file.Seek(0, 0)
	w.writer.Reset(w.file)

	return w.writer.Flush()
}

// Len returns the number of entries
func (w *WAL) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.entries)
}
