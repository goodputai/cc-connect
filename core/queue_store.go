package core

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// A journal record is committed before the item files are replaced. This
// also covers changes to multiple items, such as a finish plus its receipt.
type queueStoreEvent struct {
	At      time.Time    `json:"at"`
	Note    string       `json:"note"`
	Changes []*queueItem `json:"changes"`
}

func (c *queueCenter) itemPath(id string) string {
	return filepath.Join(c.dir, "items", id+".json")
}

func (c *queueCenter) eventsPath() string {
	return filepath.Join(c.dir, "events.jsonl")
}

func (c *queueCenter) writeItem(it *queueItem) error {
	data, err := json.Marshal(it)
	if err != nil {
		return err
	}
	return queueWriteAtomic(c.itemPath(it.ID), data)
}

func (c *queueCenter) commitLocked(note string, items ...*queueItem) error {
	for _, it := range items {
		it.Revision++
	}
	event := queueStoreEvent{At: c.now(), Note: note, Changes: items}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if err := appendQueueEvent(c.eventsPath(), append(data, '\n')); err != nil {
		return err
	}
	for _, it := range items {
		c.indexEvent(event, it)
		c.indexItem(it)
		if err := c.writeItem(it); err != nil {
			// The journal already committed the change. Startup repairs any
			// item file left behind by a crash or a failed snapshot write.
			slog.Error("queue item snapshot not written", "item_id", it.ID, "error", err)
		}
	}
	return nil
}

func appendQueueEvent(path string, line []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	offset, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if offset == 0 {
		if err := syncQueueDir(dir); err != nil {
			return err
		}
	}
	if _, err = f.Write(line); err == nil {
		err = f.Sync()
	}
	if err != nil {
		if rollback := f.Truncate(offset); rollback != nil {
			return errors.Join(err, rollback)
		}
		return errors.Join(err, f.Sync())
	}
	return nil
}

func syncQueueDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (c *queueCenter) indexItem(it *queueItem) {
	c.items[it.ID] = it
	c.seq = max(c.seq, it.Seq)
	if it.IdempotencyKey != "" {
		c.keys[it.IdempotencyKey] = it.ID
	}
}

func validQueueItemID(id string) bool {
	return id != "" && id != "." && id != ".." &&
		strings.Trim(id, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") == ""
}

func (c *queueCenter) loadStore() error {
	files, err := os.ReadDir(filepath.Join(c.dir, "items"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	for _, file := range files {
		id, ok := strings.CutSuffix(file.Name(), ".json")
		if !ok || file.IsDir() {
			continue
		}
		data, err := os.ReadFile(c.itemPath(id))
		if err != nil {
			return err
		}
		var it queueItem
		if err := json.Unmarshal(data, &it); err != nil {
			return fmt.Errorf("item %s: %w", id, err)
		}
		if it.ID != id || !validQueueItemID(id) || !validQueueState(it.Status) {
			return fmt.Errorf("invalid queue snapshot %q", id)
		}
		c.indexItem(&it)
	}

	if err := c.replayEvents(); err != nil {
		return err
	}
	if err := c.importLegacy(); err != nil {
		return err
	}
	for _, it := range c.items {
		if it.Kind != queueKindCompletion || c.now().Sub(it.CreatedAt) >= queueWakeWindow {
			continue
		}
		chain := it.WakeChain
		if chain == "" {
			id, _, _ := strings.Cut(strings.TrimPrefix(it.IdempotencyKey, "cb:"), ":")
			if order := c.items[id]; order != nil {
				chain = it.lane() + "\x00>" + order.lane()
			}
		}
		if chain != "" {
			c.wakes[chain] = append(c.wakes[chain], it.CreatedAt)
		}
	}
	return nil
}

func (c *queueCenter) replayEvents() error {
	f, err := os.OpenFile(c.eventsPath(), os.O_RDWR, 0600)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	repaired := make(map[string]*queueItem)
	reader := bufio.NewReader(f)
	var offset int64
	for {
		line, err := reader.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			// A process may die partway through an append. Only the trailing
			// incomplete record is discarded; committed lines stay immutable.
			if len(line) != 0 {
				if err := f.Truncate(offset); err != nil {
					return err
				}
				if err := f.Sync(); err != nil {
					return err
				}
			}
			break
		}
		if err != nil {
			return err
		}
		var event queueStoreEvent
		if err := json.Unmarshal(line, &event); err != nil {
			return fmt.Errorf("queue event at byte %d: %w", offset, err)
		}
		offset += int64(len(line))
		for _, it := range event.Changes {
			if it == nil || !validQueueItemID(it.ID) || !validQueueState(it.Status) {
				return fmt.Errorf("invalid queue change at byte %d", offset)
			}
			c.indexEvent(event, it)
			prior := c.items[it.ID]
			if prior == nil || prior.Revision < it.Revision {
				c.indexItem(it)
				repaired[it.ID] = it
			}
		}
	}
	for _, it := range repaired {
		if err := c.writeItem(it); err != nil {
			return err
		}
	}
	return nil
}

type legacyQueueItem struct {
	queueItem
	Status string          `json:"status"`
	Board  json.RawMessage `json:"board"`
}

func (old *legacyQueueItem) convert() (*queueItem, error) {
	if old == nil {
		return nil, errors.New("empty legacy queue item")
	}
	it := &old.queueItem
	if !validQueueItemID(it.ID) {
		return nil, fmt.Errorf("invalid legacy queue ID %q", it.ID)
	}
	it.Status = old.Status
	switch old.Status {
	case "queued":
		it.Status = queueStatusQueued
	case "running":
		it.Status = queueStatusRunning
	case "done":
		it.Status, it.Result = queueStatusDone, queueResultDelivered
		// The old archive contains the tail only. It cannot classify first lines.
		if it.Tail != "" {
			it.Evidence = append(it.Evidence, finalReplyTail(it.Tail))
		}
	case "cancelled":
		it.Status = queueStatusCancelled
	case "failed", "timed_out":
		it.Status, it.LastError = queueStatusQueued, it.Reason
		if it.LastError == "" {
			it.LastError = old.Status
		}
		it.FinishedAt = nil
	default:
		return nil, fmt.Errorf("unknown legacy queue status %q", old.Status)
	}
	for i := range it.History {
		if state := queueStateFilter(it.History[i].Status); validQueueState(state) {
			it.History[i].Status = state
		}
	}
	if old.Status != "failed" && old.Status != "timed_out" && it.Instance != "" {
		it.Executor = &queueSource{Instance: it.Instance, Project: it.Project, SessionKey: it.SessionKey}
	}
	it.Priority = queuePriority(it.Priority)
	if len(old.Board) != 0 {
		var board queueBoard
		if err := json.Unmarshal(old.Board, &board); err != nil {
			return nil, err
		}
		it.applyBoard(&board)
	}
	return it, nil
}

func (c *queueCenter) importLegacy() error {
	path := filepath.Join(c.dir, "state.json")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var disk struct {
		Seq   int64              `json:"seq"`
		Items []*legacyQueueItem `json:"items"`
	}
	if err := json.Unmarshal(data, &disk); err != nil {
		return fmt.Errorf("decode legacy queue state: %w", err)
	}
	c.seq = max(c.seq, disk.Seq)
	imported := make(map[string]*queueItem)
	for _, old := range disk.Items {
		it, err := old.convert()
		if err != nil {
			return err
		}
		imported[it.ID] = it
	}
	archives, err := filepath.Glob(filepath.Join(c.dir, "archive-*.jsonl"))
	if err != nil {
		return err
	}
	for _, path := range archives {
		if err := loadLegacyArchive(path, imported); err != nil {
			return err
		}
	}
	for _, it := range imported {
		if c.items[it.ID] == nil {
			if err := c.commitLocked("legacy import", it); err != nil {
				return err
			}
			// Keep the import marker pending if the item snapshot failed.
			if _, err := os.Stat(c.itemPath(it.ID)); err != nil {
				return err
			}
		}
	}
	if err := os.Rename(path, path+".imported"); err != nil {
		return err
	}
	return syncQueueDir(c.dir)
}

func loadLegacyArchive(path string, imported map[string]*queueItem) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	reader := bufio.NewReader(f)
	for {
		line, err := reader.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			// Every complete archive write ends in a newline.
			return nil
		}
		if err != nil {
			return err
		}
		if len(bytes.TrimSpace(line)) != 0 {
			var old legacyQueueItem
			if decodeErr := json.Unmarshal(line, &old); decodeErr != nil {
				return fmt.Errorf("decode legacy archive %s: %w", path, decodeErr)
			}
			it, convertErr := old.convert()
			if convertErr != nil {
				return convertErr
			}
			imported[it.ID] = it
		}
	}
}

func queueWriteAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".queue-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(name, path)
	}
	if err == nil {
		var d *os.File
		if d, err = os.Open(dir); err == nil {
			err = d.Sync()
			d.Close()
		}
	}
	return err
}
