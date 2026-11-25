package core

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordStoreWrites replaces the store writer; fail, when non-nil, may reject
// a write by returning an error.
func recordStoreWrites(t *testing.T, sm *SessionManager, fail func(path string) error) func() []string {
	t.Helper()
	var mu sync.Mutex
	var paths []string
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.storeWrite = func(path string, data []byte, perm os.FileMode) error {
		if fail != nil {
			if err := fail(path); err != nil {
				return err
			}
		}
		mu.Lock()
		paths = append(paths, filepath.Base(path))
		mu.Unlock()
		return AtomicWriteFile(path, data, perm)
	}
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		out := append([]string(nil), paths...)
		paths = nil
		return out
	}
}

func sessionDirFiles(t *testing.T, storePath string) []string {
	t.Helper()
	entries, err := os.ReadDir(sessionStoreDir(storePath))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out
}

func newSplitStore(t *testing.T, n int) (string, *SessionManager, []*Session) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "proj.json")
	sm := NewSessionManager(path)
	var ss []*Session
	for i := 0; i < n; i++ {
		s := sm.GetOrCreateActive("key" + string(rune('a'+i)))
		s.SetAgentSessionID("agent-"+s.ID, "claude")
		s.AddHistory("user", "hello "+s.ID)
		ss = append(ss, s)
	}
	if err := sm.SaveError(); err != nil {
		t.Fatal(err)
	}
	return path, sm, ss
}

func TestSplitStore_OnlyChangedSessionsRewritten(t *testing.T) {
	path, sm, ss := newSplitStore(t, 5)
	writes := recordStoreWrites(t, sm, nil)

	ss[2].AddHistory("assistant", "reply")
	if err := sm.SaveError(); err != nil {
		t.Fatal(err)
	}
	got := writes()
	if len(got) != 2 || !strings.HasPrefix(got[0], ss[2].ID+"-") || got[1] != "proj.json" {
		t.Fatalf("writes after one-session change = %v, want [%s-<hash>.json proj.json]", got, ss[2].ID)
	}
	if files := sessionDirFiles(t, path); len(files) != 5 {
		t.Fatalf("superseded file not removed: %v", files)
	}
	if err := sm.SaveError(); err != nil {
		t.Fatal(err)
	}
	if got := writes(); len(got) != 0 {
		t.Fatalf("unchanged save wrote %v", got)
	}
	// A change not routed through any setter is still detected (content hash).
	ss[0].mu.Lock()
	ss[0].Name = "renamed"
	ss[0].mu.Unlock()
	sm.Save()
	if got := writes(); len(got) != 2 {
		t.Fatalf("direct mutation not persisted, writes=%v", got)
	}
	if NewSessionManager(path).FindByID(ss[0].ID).GetName() != "renamed" {
		t.Fatal("reload lost the mutation")
	}
}

func TestSplitStore_RemovedSessionFileDeleted(t *testing.T) {
	path, sm, ss := newSplitStore(t, 3)
	if !sm.DeleteByID(ss[1].ID) {
		t.Fatal("delete failed")
	}
	sm.Save()
	files := sessionDirFiles(t, path)
	if len(files) != 2 {
		t.Fatalf("files after delete = %v", files)
	}
	for _, f := range files {
		if strings.HasPrefix(f, ss[1].ID+"-") {
			t.Fatalf("deleted session file still present: %v", files)
		}
	}
	if NewSessionManager(path).FindByID(ss[1].ID) != nil {
		t.Fatal("deleted session reloaded")
	}
}

func TestSplitStore_MigratesLegacyRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "proj.json")
	now := time.Now().UTC().Truncate(time.Second)
	legacy := sessionSnapshot{
		Sessions: map[string]*Session{
			"s1": {ID: "s1", Name: "main", AgentSessionID: "a1", AgentType: "claude", PastAgentSessionIDs: []string{"a0"},
				ActiveProvider: "p1", History: []HistoryEntry{{Role: "user", Content: "hi", Timestamp: now}},
				CreatedAt: now.Add(-time.Hour), UpdatedAt: now, LastUserActivity: now,
				PendingFirstInput: &FirstInput{RequestID: "r1", Text: "handoff", State: "accepted"}},
			"s2": {ID: "s2", Name: "old", AgentSessionID: "a2", Retired: true, CreatedAt: now.Add(-2 * time.Hour), UpdatedAt: now.Add(-time.Hour)},
		},
		ActiveSession:   map[string]string{"k": "s1"},
		UserSessions:    map[string][]string{"k": {"s2", "s1"}},
		Counter:         2,
		SessionNames:    map[string]string{"a1": "main"},
		UserMeta:        map[string]*UserMeta{"k": {UserName: "u", ChatName: "c"}},
		PastIDTracking:  true,
		Rotations:       map[string]*RotationOperation{"r1": {RequestID: "r1", SessionKey: "k", SourceID: "s2", TargetID: "s1", Handoff: "handoff", Revision: 1, Status: "SWITCHED", CreatedAt: now}},
		RetiredAgentIDs: map[string]bool{"a2": true},
		Version:         snapshotVersion,
	}
	orig, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, orig, 0o600); err != nil {
		t.Fatal(err)
	}

	NewSessionManager(path)
	if bak, err := os.ReadFile(path + ".pre-split.bak"); err != nil || string(bak) != string(orig) {
		t.Fatalf("legacy bytes not kept as .pre-split.bak (err=%v)", err)
	}
	var meta map[string]json.RawMessage
	raw, _ := os.ReadFile(path)
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatal(err)
	}
	if string(meta["layout"]) != `"split"` || string(meta["sessions"]) != "null" {
		t.Fatalf("store not migrated to split layout: layout=%s sessions=%.40s", meta["layout"], meta["sessions"])
	}

	exported, err := ReadSessionStoreJSON(path)
	if err != nil {
		t.Fatal(err)
	}
	var back sessionSnapshot
	if err := json.Unmarshal(exported, &back); err != nil {
		t.Fatal(err)
	}
	var want sessionSnapshot
	_ = json.Unmarshal(orig, &want)
	back.Layout, back.SessionFiles = "", nil
	if !reflect.DeepEqual(back, want) {
		gb, _ := json.Marshal(back)
		t.Fatalf("round trip changed the store:\n got %s\nwant %s", gb, orig)
	}

	// Rollback: export-legacy writes the single-file format in place.
	if err := ExportLegacySessionStore(path); err != nil {
		t.Fatal(err)
	}
	var rolled sessionSnapshot
	raw, _ = os.ReadFile(path)
	if err := json.Unmarshal(raw, &rolled); err != nil || rolled.Layout != "" || len(rolled.Sessions) != 2 || rolled.Sessions["s1"].AgentSessionID != "a1" {
		t.Fatalf("export-legacy did not write the legacy format: %.200s", raw)
	}
	// ...and a newer binary migrates it again.
	if s := NewSessionManager(path).FindByID("s1"); s == nil || s.GetAgentSessionID() != "a1" || s.HistoryLen() != 1 {
		t.Fatal("re-migration after rollback lost data")
	}
}

func TestSplitStore_CrashBeforeMetaCommitKeepsPreviousState(t *testing.T) {
	path, sm, ss := newSplitStore(t, 3)
	errCrash := errors.New("crash before meta")
	recordStoreWrites(t, sm, func(p string) error {
		if p == path {
			return errCrash
		}
		return nil
	})
	ss[1].AddHistory("assistant", "UNCOMMITTED")
	ss[1].SetAgentSessionID("agent-new", "claude")
	if err := sm.SaveError(); !errors.Is(err, errCrash) {
		t.Fatalf("SaveError = %v, want the meta write failure", err)
	}

	re := NewSessionManager(path)
	got := re.FindByID(ss[1].ID)
	if got == nil || got.GetAgentSessionID() != "agent-"+ss[1].ID || got.HistoryLen() != 1 {
		t.Fatalf("reload after crash did not return the last committed session: %+v", got)
	}
	for _, s := range ss {
		if re.FindByID(s.ID) == nil {
			t.Fatalf("session %s lost", s.ID)
		}
	}

	// A referenced file that disappeared leaves a minimal record, not a dangling key.
	for _, f := range sessionDirFiles(t, path) {
		if strings.HasPrefix(f, ss[0].ID+"-") {
			_ = os.Remove(filepath.Join(sessionStoreDir(path), f))
		}
	}
	re2 := NewSessionManager(path)
	if s := re2.FindByID(ss[0].ID); s == nil || re2.ActiveSessionID("keya") != ss[0].ID {
		t.Fatal("missing session file dropped the record")
	}
}

func TestSplitStore_RotationPruneKeepsScheduled(t *testing.T) {
	path, sm, _ := newSplitStore(t, 1)
	old := time.Now().Add(-8 * 24 * time.Hour)
	sm.mu.Lock()
	sm.rotations = map[string]*RotationOperation{
		"old-failed":    {RequestID: "old-failed", Status: "FAILED", Handoff: "x", CreatedAt: old},
		"old-switched":  {RequestID: "old-switched", Status: "SWITCHED", Handoff: "x", CreatedAt: old},
		"old-scheduled": {RequestID: "old-scheduled", Status: "SCHEDULED", Handoff: "x", CreatedAt: old},
		"new-failed":    {RequestID: "new-failed", Status: "FAILED", Handoff: "x", CreatedAt: time.Now()},
	}
	sm.mu.Unlock()
	sm.Save()
	var meta sessionSnapshot
	raw, _ := ReadSessionStoreJSON(path)
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for k := range meta.Rotations {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if !reflect.DeepEqual(keys, []string{"new-failed", "old-scheduled"}) {
		t.Fatalf("persisted rotations = %v, want [new-failed old-scheduled]", keys)
	}
}

func TestSplitStore_RotationsFileRewrittenOnlyOnChange(t *testing.T) {
	path, sm, ss := newSplitStore(t, 2)
	sm.mu.Lock()
	sm.rotations = map[string]*RotationOperation{
		"r1": {RequestID: "r1", SessionKey: "keya", SourceID: ss[0].ID, Handoff: strings.Repeat("h", 5000), Status: "SCHEDULED", CreatedAt: time.Now()},
	}
	sm.mu.Unlock()
	writes := recordStoreWrites(t, sm, nil)
	sm.Save()
	if got := writes(); len(got) != 2 || !strings.HasPrefix(got[0], "rotations-") {
		t.Fatalf("first save with a rotation wrote %v", got)
	}
	ss[1].AddHistory("assistant", "more")
	sm.Save()
	for _, w := range writes() {
		if strings.HasPrefix(w, "rotations-") {
			t.Fatal("rotations rewritten for a session-only change")
		}
	}
	if err := sm.FinishRotation("r1", "FAILED", "boom"); err != nil {
		t.Fatal(err)
	}
	if got := writes(); len(got) != 2 || !strings.HasPrefix(got[0], "rotations-") {
		t.Fatalf("rotation status change wrote %v", got)
	}
	op, err := NewSessionManager(path).Rotation("r1")
	if err != nil || op.Status != "FAILED" || len(op.Handoff) != 5000 {
		t.Fatalf("reloaded rotation = %+v, %v", op.Status, err)
	}
	var n int
	entries, _ := os.ReadDir(filepath.Dir(sessionStoreDir(path)))
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "rotations-") {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("superseded rotations files kept: %d", n)
	}
}
