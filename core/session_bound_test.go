package core

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func withHistoryBounds(t *testing.T, entries, bytes int, retain time.Duration) {
	t.Helper()
	pe, pb, pr := historyMaxEntries, historyMaxEntryBytes, historyRetainActiveFor
	historyMaxEntries, historyMaxEntryBytes, historyRetainActiveFor = entries, bytes, retain
	t.Cleanup(func() { historyMaxEntries, historyMaxEntryBytes, historyRetainActiveFor = pe, pb, pr })
}

func TestAddHistory_CapsEntries(t *testing.T) {
	withHistoryBounds(t, 5, 4000, 48*time.Hour)
	s := &Session{}
	for i := 0; i < 12; i++ {
		s.AddHistory("user", fmt.Sprintf("m%d", i))
	}
	h := s.GetHistory(0)
	if len(h) != 5 || h[0].Content != "m7" || h[4].Content != "m11" {
		t.Fatalf("history = %+v, want last 5 (m7..m11)", h)
	}
	if got := s.HistoryAddedCount(); got != 12 {
		t.Fatalf("HistoryAddedCount = %d, want 12 (monotonic past the cap)", got)
	}
}

func TestAddHistory_TruncatesEntryRuneSafe(t *testing.T) {
	withHistoryBounds(t, 50, 101, 48*time.Hour) // 101-40=61: cut lands inside a 3-byte rune
	s := &Session{}
	long := strings.Repeat("中", 100) // 300 bytes
	s.AddHistory("assistant", long)
	s.AddHistory("user", "short")
	h := s.GetHistory(0)
	got := h[0].Content
	if len(got) > 101 {
		t.Fatalf("stored %d bytes, want <= 101", len(got))
	}
	if !utf8.ValidString(got) {
		t.Fatalf("truncation split a rune: %q", got)
	}
	if !strings.HasPrefix(got, strings.Repeat("中", 20)) || !strings.Contains(got, "…[truncated 240 bytes]") {
		t.Fatalf("unexpected truncated content %q", got)
	}
	if again := truncateHistoryContent(got); again != got {
		t.Fatalf("truncation not idempotent: %q -> %q", got, again)
	}
	if h[1].Content != "short" {
		t.Fatalf("short entry changed: %q", h[1].Content)
	}
}

func writeSessionSnapshot(t *testing.T, path string, snap map[string]any) {
	t.Helper()
	b, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
}

func histEntries(n int, content string) []map[string]any {
	out := make([]map[string]any, n)
	for i := range out {
		out[i] = map[string]any{"role": "user", "content": fmt.Sprintf("%s-%d", content, i), "timestamp": time.Now()}
	}
	return out
}

// Load bounds history: active+recent sessions keep a capped/truncated history,
// non-active (retired) and stale active sessions lose it; nothing else changes.
func TestSessionLoad_BoundsHistory(t *testing.T) {
	withHistoryBounds(t, 50, 4000, 48*time.Hour)
	path := filepath.Join(t.TempDir(), "sessions.json")
	now := time.Now()
	recent := histEntries(60, "recent")
	recent[59]["content"] = strings.Repeat("x", 10000)
	writeSessionSnapshot(t, path, map[string]any{
		"sessions": map[string]any{
			"s1": map[string]any{"id": "s1", "agent_session_id": "a1", "history": recent, "updated_at": now.Add(-time.Hour)},
			"s2": map[string]any{"id": "s2", "agent_session_id": "a2", "past_agent_session_ids": []string{"a2old"}, "history": histEntries(3, "old"), "updated_at": now.Add(-time.Hour)},
			"s3": map[string]any{"id": "s3", "agent_session_id": "a3", "history": histEntries(3, "stale"), "updated_at": now.Add(-72 * time.Hour)},
		},
		"active_session":    map[string]string{"k1": "s1", "k3": "s3"},
		"user_sessions":     map[string][]string{"k1": {"s2", "s1"}, "k3": {"s3"}},
		"counter":           3,
		"retired_agent_ids": map[string]bool{"a0": true},
		"user_meta":         map[string]any{"k1": map[string]string{"user_name": "u"}},
		"past_id_tracking":  true,
		"version":           1,
	})

	check := func(sm *SessionManager, stage string) {
		t.Helper()
		s1, s2, s3 := sm.FindByID("s1"), sm.FindByID("s2"), sm.FindByID("s3")
		if s1 == nil || s2 == nil || s3 == nil {
			t.Fatalf("%s: session records removed", stage)
		}
		h1 := s1.GetHistory(0)
		if len(h1) != 50 || h1[0].Content != "recent-10" {
			t.Fatalf("%s: active recent history len=%d, want 50 entries from recent-10: %.80v", stage, len(h1), h1)
		}
		if last := h1[49].Content; len(last) > 4000 || !strings.Contains(last, "[truncated") {
			t.Fatalf("%s: long entry not truncated (%d bytes)", stage, len(last))
		}
		if n := s2.HistoryLen(); n != 0 {
			t.Fatalf("%s: non-active session kept %d history entries", stage, n)
		}
		if n := s3.HistoryLen(); n != 0 {
			t.Fatalf("%s: stale active session kept %d history entries", stage, n)
		}
		if s1.GetAgentSessionID() != "a1" || s2.GetAgentSessionID() != "a2" || s3.GetAgentSessionID() != "a3" {
			t.Fatalf("%s: agent session ids changed", stage)
		}
		if len(s2.PastAgentSessionIDs) != 1 || s2.PastAgentSessionIDs[0] != "a2old" {
			t.Fatalf("%s: past ids changed: %v", stage, s2.PastAgentSessionIDs)
		}
		if !sm.IsRetiredAgentID("a0") || sm.GetUserMeta("k1") == nil || sm.ActiveSessionID("k3") != "s3" {
			t.Fatalf("%s: manager metadata changed", stage)
		}
		if got, err := sm.SwitchSession("k1", "s2"); err != nil || got.GetAgentSessionID() != "a2" {
			t.Fatalf("%s: switch to non-active session failed: %v", stage, err)
		}
	}
	sm := NewSessionManager(path)
	check(sm, "load")
	// SwitchSession above made s2 active; switch back before saving.
	if _, err := sm.SwitchSession("k1", "s1"); err != nil {
		t.Fatal(err)
	}
	if err := sm.SaveError(); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(path)
	if strings.Contains(string(raw), "\n  ") {
		t.Fatal("snapshot still written with MarshalIndent")
	}
	check(NewSessionManager(path), "reload")
}

// Saving drops the history of a session that stopped being active, without
// touching the in-memory record or the active session's history.
func TestSessionSave_DropsHistoryOfInactiveSessions(t *testing.T) {
	withHistoryBounds(t, 50, 4000, 48*time.Hour)
	path := filepath.Join(t.TempDir(), "sessions.json")
	sm := NewSessionManager(path)
	old := sm.GetOrCreateActive("k")
	old.SetAgentSessionID("agent-old", "claude")
	old.AddHistory("user", "old-hello")
	fresh := sm.NewSession("k", "fresh")
	fresh.AddHistory("user", "fresh-hello")
	sm.Save()

	var snap struct {
		Sessions map[string]struct {
			AgentSessionID string         `json:"agent_session_id"`
			History        []HistoryEntry `json:"history"`
		} `json:"sessions"`
	}
	raw, err := ReadSessionStoreJSON(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &snap); err != nil {
		t.Fatal(err)
	}
	if got := snap.Sessions[old.ID]; len(got.History) != 0 || got.AgentSessionID != "agent-old" {
		t.Fatalf("inactive session on disk: %+v", got)
	}
	if got := snap.Sessions[fresh.ID]; len(got.History) != 1 || got.History[0].Content != "fresh-hello" {
		t.Fatalf("active session history on disk: %+v", got)
	}
	if old.HistoryLen() != 1 {
		t.Fatal("save mutated in-memory history")
	}
	// The inactive but resumable record must survive an empty-session prune.
	sm2 := NewSessionManager(path)
	sm2.PruneEmptySessions()
	if sm2.FindByID(old.ID) == nil {
		t.Fatal("PruneEmptySessions deleted a resumable session whose history was dropped")
	}
}
