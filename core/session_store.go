package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Split session store layout.
//
// storePath holds a small "meta" snapshot (everything except the sessions)
// plus session_files: session ID → file name. Each session lives in
// <storePath without .json>.d/sessions/<id>-<content hash>.json. A Save only
// writes the files whose content changed, then the meta file. Because a
// changed session gets a new file name, the meta rename is the single commit
// point: a crash before it leaves the previous meta pointing at the previous,
// still-present files. Files no longer referenced are removed after the commit.
//
// Rotation records (mostly terminal, each carrying its handoff text) live in
// <stem>.d/rotations-<content hash>.json, rewritten only when a rotation
// changes (detected by a fingerprint that skips the immutable handoff text).
//
// A legacy single-file store (sessions inline) is migrated on load; its bytes
// are kept as <storePath>.pre-split.bak. ExportLegacySessionStore writes the
// legacy format back for a rollback to an older binary.

const sessionStoreLayoutSplit = "split"

var (
	// rotationRetention: terminal rotation records (anything but SCHEDULED)
	// older than this are not persisted any more.
	rotationRetention = 7 * 24 * time.Hour
	// sessionStoreOrphanAge: unreferenced files in the session dir older than
	// this are removed on load (younger ones may belong to a concurrent writer).
	sessionStoreOrphanAge = time.Hour
)

var safeSessionFileID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// sessionStoreDir returns the per-session directory for a meta path.
func sessionStoreDir(storePath string) string {
	return strings.TrimSuffix(storePath, ".json") + ".d/sessions"
}

func rotationsFilePath(storePath, name string) string {
	return filepath.Join(filepath.Dir(sessionStoreDir(storePath)), filepath.Base(name))
}

// rotationsFingerprint identifies a rotation set without hashing handoff
// texts: Handoff is fixed when a request is registered (PrepareRotation
// rejects a different handoff for the same request ID); only status and
// error change afterwards. Its length is still included.
func rotationsFingerprint(rotations map[string]*RotationOperation) [32]byte {
	keys := make([]string, 0, len(rotations))
	for k := range rotations {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		op := rotations[k]
		fmt.Fprintf(h, "%q|%q|%q|%q|%q|%q|%d|%d|%q|%q|%d\n", k, op.RequestID, op.SessionKey, op.SourceID, op.TargetID, op.Name,
			len(op.Handoff), op.Revision, op.Status, op.Error, op.CreatedAt.UnixNano())
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum
}

func sessionFileName(id string, data []byte) string {
	sum := sha256.Sum256(data)
	if !safeSessionFileID.MatchString(id) {
		idSum := sha256.Sum256([]byte(id))
		id = "x" + hex.EncodeToString(idSum[:8])
	}
	return id + "-" + hex.EncodeToString(sum[:8]) + ".json"
}

// readSessionStore reads storePath in either layout without side effects.
// For the split layout, snap.Sessions is filled from the per-session files;
// a referenced file that is missing or unreadable becomes a minimal record
// (ID only) so key → session mappings stay valid. loaded holds the file names
// that were read successfully.
func readSessionStore(storePath string) (snap sessionSnapshot, meta []byte, loaded map[string]string, err error) {
	snap, meta, loaded, _, err = readSessionStoreFull(storePath)
	return
}

// readSessionStoreFull also reports the rotations file that was read.
func readSessionStoreFull(storePath string) (snap sessionSnapshot, meta []byte, loaded map[string]string, rotFile string, err error) {
	meta, err = os.ReadFile(storePath)
	if err != nil {
		return snap, nil, nil, "", err
	}
	if err = json.Unmarshal(meta, &snap); err != nil {
		return snap, meta, nil, "", fmt.Errorf("unmarshal %s: %w", storePath, err)
	}
	if snap.Layout != sessionStoreLayoutSplit {
		return snap, meta, nil, "", nil
	}
	if snap.RotationsFile != "" {
		data, rerr := os.ReadFile(rotationsFilePath(storePath, snap.RotationsFile))
		if rerr == nil {
			rerr = json.Unmarshal(data, &snap.Rotations)
		}
		if rerr != nil {
			slog.Warn("session: rotations file missing or invalid; starting without rotation records",
				"path", storePath, "file", snap.RotationsFile, "error", rerr)
			snap.Rotations = nil
		} else {
			rotFile = snap.RotationsFile
		}
	}
	dir := sessionStoreDir(storePath)
	snap.Sessions = make(map[string]*Session, len(snap.SessionFiles))
	loaded = make(map[string]string, len(snap.SessionFiles))
	for id, name := range snap.SessionFiles {
		data, rerr := os.ReadFile(filepath.Join(dir, filepath.Base(name)))
		var s Session
		if rerr == nil {
			rerr = json.Unmarshal(data, &s)
		}
		if rerr != nil || s.ID != id {
			slog.Warn("session: session file missing or invalid; keeping a minimal record",
				"path", storePath, "session", id, "file", name, "error", rerr)
			snap.Sessions[id] = &Session{ID: id}
			continue
		}
		snap.Sessions[id] = &s
		loaded[id] = name
	}
	return snap, meta, loaded, rotFile, nil
}

// writeSnapshotLocked persists snap: changed session files first, then the
// meta file (commit point), then removal of files no longer referenced.
// Caller holds sm.mu.
func (sm *SessionManager) writeSnapshotLocked(snap sessionSnapshot) error {
	if sm.storePath == "" {
		return nil
	}
	dir := sessionStoreDir(sm.storePath)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create sessions directory: %w", err)
	}
	names := make(map[string]string, len(snap.Sessions))
	for id, s := range snap.Sessions {
		data, err := json.Marshal(s)
		if err != nil {
			return fmt.Errorf("marshal session %s: %w", id, err)
		}
		name := sessionFileName(id, data)
		names[id] = name
		if sm.storeFiles[id] == name {
			continue
		}
		if err := sm.storeWriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			return fmt.Errorf("commit session %s: %w", id, err)
		}
	}
	rotFile, rotFP := "", rotationsFingerprint(snap.Rotations)
	if len(snap.Rotations) > 0 {
		rotFile = sm.storeRotFile
		if rotFile == "" || rotFP != sm.storeRotFP {
			data, err := json.Marshal(snap.Rotations)
			if err != nil {
				return fmt.Errorf("marshal rotations: %w", err)
			}
			sum := sha256.Sum256(data)
			rotFile = "rotations-" + hex.EncodeToString(sum[:8]) + ".json"
			if rotFile != sm.storeRotFile {
				if err := sm.storeWriteFile(rotationsFilePath(sm.storePath, rotFile), data, 0o600); err != nil {
					return fmt.Errorf("commit rotations: %w", err)
				}
			}
		}
	}
	meta := snap
	meta.Sessions = nil
	meta.Rotations = nil
	meta.RotationsFile = rotFile
	meta.SessionFiles = names
	meta.Layout = sessionStoreLayoutSplit
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal sessions: %w", err)
	}
	if sm.storeMeta == nil || !bytes.Equal(sm.storeMeta, data) {
		if err := sm.storeWriteFile(sm.storePath, data, 0o600); err != nil {
			return fmt.Errorf("commit sessions: %w", err)
		}
	}
	for id, old := range sm.storeFiles {
		if names[id] != old {
			if err := os.Remove(filepath.Join(dir, old)); err != nil && !os.IsNotExist(err) {
				slog.Warn("session: remove superseded session file", "file", old, "error", err)
			}
		}
	}
	if old := sm.storeRotFile; old != "" && old != rotFile {
		if err := os.Remove(rotationsFilePath(sm.storePath, old)); err != nil && !os.IsNotExist(err) {
			slog.Warn("session: remove superseded rotations file", "file", old, "error", err)
		}
	}
	sm.storeFiles = names
	sm.storeMeta = data
	sm.storeRotFile, sm.storeRotFP = rotFile, rotFP
	return nil
}

// storeWriteFile writes one store file atomically (tests may replace
// sm.storeWrite to count or fail writes).
func (sm *SessionManager) storeWriteFile(path string, data []byte, perm os.FileMode) error {
	if sm.storeWrite != nil {
		return sm.storeWrite(path, data, perm)
	}
	return AtomicWriteFile(path, data, perm)
}

// removeOrphanSessionFiles deletes files in the session dir that the
// committed meta does not reference (left by a crash between session-file
// writes and the meta commit). Recent files are kept.
func removeOrphanSessionFiles(storePath string, referenced map[string]string, rotFile string) {
	removed := removeUnreferenced(sessionStoreDir(storePath), referenced, func(string) bool { return true })
	removed += removeUnreferenced(filepath.Dir(sessionStoreDir(storePath)), map[string]string{"": rotFile},
		func(name string) bool { return strings.HasPrefix(name, "rotations-") })
	if removed > 0 {
		slog.Info("session: removed orphan session files", "path", storePath, "removed", removed)
	}
}

func removeUnreferenced(dir string, referenced map[string]string, match func(string) bool) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	keep := make(map[string]bool, len(referenced))
	for _, name := range referenced {
		keep[name] = true
	}
	removed := 0
	for _, e := range entries {
		if e.IsDir() || keep[e.Name()] || !match(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil || time.Since(info.ModTime()) < sessionStoreOrphanAge {
			continue
		}
		if os.Remove(filepath.Join(dir, e.Name())) == nil {
			removed++
		}
	}
	return removed
}

// legacySessionStoreJSON renders a store (either layout) in the legacy
// single-file format, sessions inline.
func legacySessionStoreJSON(storePath string) ([]byte, error) {
	snap, meta, _, err := readSessionStore(storePath)
	if err != nil {
		return nil, err
	}
	if snap.Layout != sessionStoreLayoutSplit {
		return meta, nil
	}
	snap.Layout = ""
	snap.SessionFiles = nil
	snap.RotationsFile = ""
	return json.Marshal(snap)
}

// ReadSessionStoreJSON returns the store at path in the legacy single-file
// JSON shape (sessions inline) regardless of its on-disk layout. Read-only;
// for tools that parse the session file directly.
func ReadSessionStoreJSON(path string) ([]byte, error) {
	return legacySessionStoreJSON(path)
}

// ExportLegacySessionStore rewrites path in the legacy single-file format so
// an older cc-connect binary can read it (rollback). The per-session
// directory is left in place; a newer binary migrates the file again.
// cc-connect must not be running on this store.
func ExportLegacySessionStore(path string) error {
	data, err := legacySessionStoreJSON(path)
	if err != nil {
		return err
	}
	return AtomicWriteFile(path, data, 0o600)
}
