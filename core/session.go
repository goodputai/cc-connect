package core

import (
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// Session history is display/bookkeeping only (the agent keeps the real
// transcript), but the whole store is rewritten under sm.mu on every Save, so
// its size is bounded. Variables so tests can override them.
var (
	// historyMaxEntries keeps only the last N entries per session.
	historyMaxEntries = 50
	// historyMaxEntryBytes caps each stored entry's Content (including the
	// truncation marker, so re-applying it is a no-op).
	historyMaxEntryBytes = 4000
	// historyRetainActiveFor: History is dropped (on load and in every saved
	// snapshot) for sessions that are not active for any key, and for active
	// sessions not updated for this long. Session records are kept.
	historyRetainActiveFor = 48 * time.Hour
)

// historyTruncateReserve leaves room for the truncation marker.
const historyTruncateReserve = 40

// truncateHistoryContent cuts content to at most historyMaxEntryBytes bytes on
// a rune boundary, appending a marker with the number of dropped bytes.
func truncateHistoryContent(content string) string {
	limit := historyMaxEntryBytes
	if limit <= 0 || len(content) <= limit {
		return content
	}
	cut := limit - historyTruncateReserve
	if cut < 0 {
		cut = 0
	}
	for cut > 0 && !utf8.RuneStart(content[cut]) {
		cut--
	}
	return content[:cut] + fmt.Sprintf("…[truncated %d bytes]", len(content)-cut)
}

// trimHistoryLocked applies the entry-count and entry-size caps.
// Caller holds s.mu or owns s exclusively.
func (s *Session) trimHistoryLocked() {
	if n := historyMaxEntries; n > 0 && len(s.History) > n {
		s.History = append([]HistoryEntry(nil), s.History[len(s.History)-n:]...)
	}
	for i := range s.History {
		s.History[i].Content = truncateHistoryContent(s.History[i].Content)
	}
}

// boundHistories trims every session's history and drops it entirely for
// sessions that are not active (by activeSession values) or are active but
// idle longer than historyRetainActiveFor. Caller owns the sessions (load) or
// passes snapshot clones.
func boundHistories(sessions map[string]*Session, activeSession map[string]string, now time.Time) {
	active := make(map[string]bool, len(activeSession))
	for _, id := range activeSession {
		active[id] = true
	}
	for id, s := range sessions {
		if s == nil {
			continue
		}
		stale := !s.UpdatedAt.IsZero() && now.Sub(s.UpdatedAt) > historyRetainActiveFor
		if !active[id] || stale {
			s.History = nil
			continue
		}
		s.trimHistoryLocked()
	}
}

// ContinueSession is a sentinel value for AgentSessionID that tells the agent
// to use --continue (resume most recent session) instead of a specific session ID.
const ContinueSession = "__continue__"

// Session tracks one conversation between a user and the agent.
type Session struct {
	ID                  string   `json:"id"`
	Name                string   `json:"name"`
	AgentSessionID      string   `json:"agent_session_id"`
	AgentType           string   `json:"agent_type,omitempty"`
	PastAgentSessionIDs []string `json:"past_agent_session_ids,omitempty"`
	// ActiveProvider is the agent provider name that was active when this
	// session last took a turn. It is restored before --resume so that a
	// cc-connect process restart does not silently drop a user's
	// `/provider switch` (the agent_session_id survives on disk while the
	// in-memory active provider does not). Empty means "no explicit choice
	// — use whatever the agent's default is".
	ActiveProvider string         `json:"active_provider,omitempty"`
	History        []HistoryEntry `json:"history"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
	// LastUserActivity records when a real user message was last received.
	// Unlike UpdatedAt (bumped by every session.Unlock including heartbeats and
	// unsolicited agent output), this field is only updated when the engine
	// processes an actual incoming user message. It is used by reset_on_idle_mins
	// so that automated activity cannot prevent idle session rotation.
	LastUserActivity time.Time `json:"last_user_activity,omitempty"`

	Retired           bool        `json:"retired,omitempty"`
	PendingFirstInput *FirstInput `json:"pending_first_input,omitempty"`
	retirement        *sync.Map
	mu                sync.Mutex `json:"-"`
	busy              bool       `json:"-"`
	queueTurnStart    time.Time
	queueTurnSource   string
	queueItemID       string
	// historyAdded counts AddHistory calls (not persisted). Unlike
	// len(History) it keeps growing once the entry cap is reached.
	historyAdded uint64
}

func (s *Session) TryLock() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.busy || s.Retired {
		return false
	}
	s.busy = true
	return true
}

// Busy reports whether the session is currently locked for an in-flight turn.
// Used by commands (e.g. /ps) that only make sense while a task is running.
func (s *Session) Busy() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.busy
}

func (s *Session) Unlock() {
	s.unlock(true)
}

func (s *Session) UnlockWithoutUpdate() {
	s.unlock(false)
}

func (s *Session) unlock(update bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.busy = false
	s.queueTurnStart = time.Time{}
	s.queueTurnSource, s.queueItemID = "", ""
	if update {
		s.UpdatedAt = time.Now()
	}
}

func (s *Session) setQueueTurn(start time.Time, sender, itemID string) {
	source := "human"
	switch {
	case itemID != "":
		source = "center"
	case sender == "timer" || sender == "cron":
		source = sender
	}
	s.mu.Lock()
	s.queueTurnStart, s.queueTurnSource, s.queueItemID = start, source, itemID
	s.mu.Unlock()
}

func (s *Session) AddHistory(role, content string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.History = append(s.History, HistoryEntry{
		Role:      role,
		Content:   truncateHistoryContent(content),
		Timestamp: time.Now(),
	})
	if n := historyMaxEntries; n > 0 && len(s.History) > n {
		s.History = s.History[len(s.History)-n:]
	}
	s.historyAdded++
}

// HistoryAddedCount returns how many entries were ever added in this process
// (monotonic; use it for "did this turn add entries" checks, since
// len(History) stops growing at historyMaxEntries).
func (s *Session) HistoryAddedCount() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.historyAdded
}

// recordPastAgentSessionID saves the current AgentSessionID to PastAgentSessionIDs
// so it remains visible in KnownAgentSessionIDs after the ID is replaced or cleared.
// Must be called with s.mu held.
func (s *Session) recordPastAgentSessionID() {
	if s.AgentSessionID == "" || s.AgentSessionID == ContinueSession {
		return
	}
	for _, past := range s.PastAgentSessionIDs {
		if past == s.AgentSessionID {
			return
		}
	}
	s.PastAgentSessionIDs = append(s.PastAgentSessionIDs, s.AgentSessionID)
}

// SetAgentInfo atomically sets the agent session ID, agent type, and name.
func (s *Session) SetAgentInfo(agentSessionID, agentType, name string) {
	if agentSessionID == ContinueSession {
		agentSessionID = ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.AgentSessionID != agentSessionID {
		s.recordPastAgentSessionID()
	}
	if s.Retired || s.isRetiredID(agentSessionID) {
		return
	}
	s.AgentSessionID = agentSessionID
	s.AgentType = agentType
	s.Name = name
}

// GetAgentSessionID atomically reads the agent session ID.
func (s *Session) GetAgentSessionID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Retired || s.isRetiredID(s.AgentSessionID) {
		return ""
	}
	return s.AgentSessionID
}

// SetName atomically updates the session's display name. The management
// API used to write s.Name directly, which raced with GetName / listing
// handlers that read it under s.mu.
func (s *Session) SetName(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Name = name
}

// GetName atomically reads the session name.
func (s *Session) GetName() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Name
}

// TouchUserActivity records the current time as the last user-driven activity
// on this session. Call this once per incoming user message, after the idle
// reset check, so that only real user interactions reset the idle timer.
func (s *Session) TouchUserActivity() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastUserActivity = time.Now()
}

// GetLastUserActivity returns when the last real user message was received.
// Returns zero time if no user message has been processed yet.
func (s *Session) GetLastUserActivity() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.LastUserActivity
}

func (s *Session) GetUpdatedAt() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.UpdatedAt
}

// SetActiveProvider atomically records the provider name the user last
// switched to for this session. Pass an empty string to clear the choice
// (e.g. on `/provider clear`). Callers are expected to call
// SessionManager.Save() afterwards so the value survives a process restart.
func (s *Session) SetActiveProvider(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ActiveProvider = name
}

// GetActiveProvider atomically reads the provider name the user last
// switched to for this session. Empty means "no explicit choice".
func (s *Session) GetActiveProvider() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ActiveProvider
}

// SetAgentSessionID atomically sets the agent session ID and agent type.
// The ContinueSession sentinel is never persisted — it is only used transiently
// when starting an agent (see engine); storing it on disk breaks resume (#255).
// When the existing ID is replaced or cleared, it is saved to PastAgentSessionIDs
// so filterOwnedSessions continues to recognise the session.
func (s *Session) SetAgentSessionID(id, agentType string) {
	if id == ContinueSession {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.AgentSessionID != id {
		s.recordPastAgentSessionID()
	}
	if s.Retired || s.isRetiredID(id) {
		return
	}
	s.AgentSessionID = id
	s.AgentType = agentType
}

// CompareAndSetAgentSessionID sets the agent session ID only if it is currently
// empty or still holds the erroneous persisted ContinueSession sentinel.
// Returns true if the value was set, false if a real session ID was already stored.
func (s *Session) CompareAndSetAgentSessionID(id, agentType string) bool {
	if id == "" || id == ContinueSession {
		return false
	}
	if s.isRetiredID(id) {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Retired {
		return false
	}
	if s.AgentSessionID != "" && s.AgentSessionID != ContinueSession {
		return false
	}
	s.AgentSessionID = id
	s.AgentType = agentType
	return true
}

func (s *Session) stripContinueSessionSentinel() {
	s.mu.Lock()
	if s.AgentSessionID == ContinueSession {
		s.AgentSessionID = ""
	}
	s.mu.Unlock()
}

func (s *Session) ClearHistory() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.History = nil
}

// hasAgentBindingLocked reports whether the session has (or had) an agent
// session ID that it can be resumed or switched to. Caller holds s.mu.
func (s *Session) hasAgentBindingLocked() bool {
	return (s.AgentSessionID != "" && s.AgentSessionID != ContinueSession) || len(s.PastAgentSessionIDs) > 0
}

// HistoryLen returns the current number of history entries.
func (s *Session) HistoryLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.History)
}

// GetHistory returns the last n entries. If n <= 0, returns all.
func (s *Session) GetHistory(n int) []HistoryEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := len(s.History)
	if n <= 0 || n > total {
		n = total
	}
	out := make([]HistoryEntry, n)
	copy(out, s.History[total-n:])
	return out
}

// UserMeta stores human-readable display info for a session key.
type UserMeta struct {
	UserName string `json:"user_name,omitempty"`
	ChatName string `json:"chat_name,omitempty"`
}

// snapshotVersion tracks the schema version so we can detect data saved by
// older code that didn't persist all migration flags.
//   - 0 (missing): original format or early PastIDTracking-only format
//   - 1: full LegacyData persistence
const snapshotVersion = 1

// sessionSnapshot is the JSON-serializable state of the SessionManager.
type sessionSnapshot struct {
	Sessions        map[string]*Session           `json:"sessions"`
	ActiveSession   map[string]string             `json:"active_session"`
	UserSessions    map[string][]string           `json:"user_sessions"`
	Counter         int64                         `json:"counter"`
	SessionNames    map[string]string             `json:"session_names,omitempty"`    // agent session ID → custom name
	UserMeta        map[string]*UserMeta          `json:"user_meta,omitempty"`        // sessionKey → display info
	PastIDTracking  bool                          `json:"past_id_tracking,omitempty"` // true once PastAgentSessionIDs is supported
	LegacyData      bool                          `json:"legacy_data,omitempty"`      // true while pre-fix sessions exist
	Rotations       map[string]*RotationOperation `json:"rotations,omitempty"`
	RetiredAgentIDs map[string]bool               `json:"retired_agent_ids,omitempty"`
	Version         int                           `json:"version,omitempty"` // schema version for migration detection
	// Split layout (see session_store.go): sessions live in per-session files.
	Layout        string            `json:"layout,omitempty"`
	SessionFiles  map[string]string `json:"session_files,omitempty"`
	RotationsFile string            `json:"rotations_file,omitempty"`
}

// SessionManager supports multiple named sessions per user with active-session tracking.
// It can persist state to a JSON file and reload on startup.
type SessionManager struct {
	rotations       map[string]*RotationOperation
	retiredAgentIDs map[string]bool
	retirement      sync.Map
	mu              sync.RWMutex
	sessions        map[string]*Session
	activeSession   map[string]string
	userSessions    map[string][]string
	sessionNames    map[string]string    // agent session ID → custom name
	userMeta        map[string]*UserMeta // sessionKey → display info
	counter         int64
	storePath       string // empty = no persistence

	// legacyData is true when sessions were loaded from a snapshot that
	// predates PastAgentSessionIDs tracking. In this state, many sessions
	// may have lost their AgentSessionID through /new or provider switches.
	// KnownAgentSessionIDs returns nil to disable filterOwnedSessions.
	legacyData bool

	// Split-store write cache (guarded by mu): file name per session as
	// committed on disk, and the last committed meta bytes.
	storeFiles map[string]string
	storeMeta  []byte
	storeWrite func(path string, data []byte, perm os.FileMode) error // test hook
	// committed rotations file and the fingerprint of its content
	storeRotFile string
	storeRotFP   [32]byte
}

func NewSessionManager(storePath string) *SessionManager {
	sm := &SessionManager{
		sessions:      make(map[string]*Session),
		activeSession: make(map[string]string),
		userSessions:  make(map[string][]string),
		sessionNames:  make(map[string]string),
		userMeta:      make(map[string]*UserMeta),
		storePath:     storePath,
	}
	if storePath != "" {
		sm.load()
	}
	return sm
}

// StorePath returns the file path used for session persistence.
func (sm *SessionManager) StorePath() string {
	return sm.storePath
}

func (sm *SessionManager) nextID() string {
	sm.counter++
	return fmt.Sprintf("s%d", sm.counter)
}

func (sm *SessionManager) GetOrCreateActive(userKey string) *Session {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sid, ok := sm.activeSession[userKey]; ok {
		if s, ok := sm.sessions[sid]; ok && !s.Retired {
			return s
		}
	}
	s := sm.createLocked(userKey, "default")
	sm.saveLocked()
	return s
}

func (sm *SessionManager) NewSession(userKey, name string) *Session {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	s := sm.createLocked(userKey, name)
	sm.saveLocked()
	return s
}

// NewSideSession registers a new session for userKey without changing the active
// session. Used for isolated one-off runs (e.g. cron with session_mode=new_per_run)
// so the user's current chat remains the default target for normal messages.
func (sm *SessionManager) NewSideSession(userKey, name string) *Session {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	id := sm.nextID()
	now := time.Now()
	s := &Session{
		ID:         id,
		retirement: &sm.retirement,
		Name:       name,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	sm.sessions[id] = s
	sm.userSessions[userKey] = append(sm.userSessions[userKey], id)
	sm.saveLocked()
	return s
}

func (sm *SessionManager) createLocked(userKey, name string) *Session {
	id := sm.nextID()
	now := time.Now()
	s := &Session{
		ID:         id,
		retirement: &sm.retirement,
		Name:       name,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	sm.sessions[id] = s
	sm.activeSession[userKey] = id
	sm.userSessions[userKey] = append(sm.userSessions[userKey], id)
	return s
}

func (sm *SessionManager) SwitchSession(userKey, target string) (*Session, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for _, sid := range sm.userSessions[userKey] {
		s := sm.sessions[sid]
		if s != nil && !s.Retired && (s.ID == target || s.Name == target) {
			sm.activeSession[userKey] = s.ID
			sm.saveLocked()
			return s, nil
		}
	}
	return nil, fmt.Errorf("session %q not found", target)
}

// SwitchToAgentSession finds or creates an internal session that maps to the
// given agent session ID. If an existing session already references agentSID,
// it becomes the active session. Otherwise a new session is created so the
// previous session's AgentSessionID is preserved in KnownAgentSessionIDs.
func (sm *SessionManager) SwitchToAgentSession(userKey, agentSID, agentName, summary string) *Session {
	if sm.IsRetiredAgentID(agentSID) {
		return nil
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.retiredAgentIDs[agentSID] {
		return nil
	}

	for _, sid := range sm.userSessions[userKey] {
		s := sm.sessions[sid]
		if s == nil {
			continue
		}
		s.mu.Lock()
		aid := s.AgentSessionID
		s.mu.Unlock()
		if aid == agentSID {
			sm.activeSession[userKey] = s.ID
			sm.saveLocked()
			return s
		}
	}

	s := sm.createLocked(userKey, summary)
	s.SetAgentInfo(agentSID, agentName, summary)
	sm.saveLocked()
	return s
}

func (sm *SessionManager) ListSessions(userKey string) []*Session {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	ids := sm.userSessions[userKey]
	out := make([]*Session, 0, len(ids))
	for _, sid := range ids {
		if s, ok := sm.sessions[sid]; ok {
			out = append(out, s)
		}
	}
	return out
}

func (sm *SessionManager) ActiveSessionID(userKey string) string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.activeSession[userKey]
}

// SetSessionName sets a custom display name for an agent session.
func (sm *SessionManager) SetSessionName(agentSessionID, name string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if name == "" {
		delete(sm.sessionNames, agentSessionID)
	} else {
		sm.sessionNames[agentSessionID] = name
	}
	sm.saveLocked()
}

// GetSessionName returns the custom name for an agent session, or "".
func (sm *SessionManager) GetSessionName(agentSessionID string) string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.sessionNames[agentSessionID]
}

// UpdateUserMeta updates the human-readable metadata for a session key.
// Only non-empty fields are applied (merge behavior).
func (sm *SessionManager) UpdateUserMeta(sessionKey, userName, chatName string) {
	if userName == "" && chatName == "" {
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	meta, ok := sm.userMeta[sessionKey]
	if !ok {
		meta = &UserMeta{}
		sm.userMeta[sessionKey] = meta
	}
	if userName != "" {
		meta.UserName = userName
	}
	if chatName != "" {
		meta.ChatName = chatName
	}
}

// GetUserMeta returns a copy of the stored metadata for a session key, or nil.
func (sm *SessionManager) GetUserMeta(sessionKey string) *UserMeta {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	m := sm.userMeta[sessionKey]
	if m == nil {
		return nil
	}
	cp := *m
	return &cp
}

// AllSessions returns all sessions across all user keys.
func (sm *SessionManager) AllSessions() []*Session {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	out := make([]*Session, 0, len(sm.sessions))
	for _, s := range sm.sessions {
		out = append(out, s)
	}
	return out
}

// KnownAgentSessionIDs returns the set of agent session IDs tracked by cc-connect.
// This is used to filter agent.ListSessions() output to only sessions owned by
// cc-connect, excluding sessions created by external CLI usage in the same work_dir.
// It includes both current and historical agent session IDs so that sessions whose
// IDs were cleared (e.g. after /new or provider switch) remain visible.
//
// Legacy data: when the snapshot was written before PastAgentSessionIDs tracking
// existed, many sessions may have silently lost their IDs through /new or provider
// switches. Returns nil unconditionally while legacyData is true, disabling
// filterOwnedSessions. legacyData is only cleared once every session has at least
// one tracked ID (current or past), meaning the data has been fully migrated.
func (sm *SessionManager) KnownAgentSessionIDs() map[string]struct{} {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if sm.legacyData {
		return nil
	}
	ids := make(map[string]struct{})
	for _, s := range sm.sessions {
		s.mu.Lock()
		if s.AgentSessionID != "" {
			ids[s.AgentSessionID] = struct{}{}
		}
		for _, past := range s.PastAgentSessionIDs {
			ids[past] = struct{}{}
		}
		s.mu.Unlock()
	}
	return ids
}

// SessionKeyMap returns a mapping from session ID to the user key (session_key) it belongs to,
// plus active session IDs for each user key.
func (sm *SessionManager) SessionKeyMap() (idToKey map[string]string, activeIDs map[string]bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	idToKey = make(map[string]string, len(sm.sessions))
	activeIDs = make(map[string]bool)
	for userKey, ids := range sm.userSessions {
		for _, sid := range ids {
			idToKey[sid] = userKey
		}
		if aid, ok := sm.activeSession[userKey]; ok {
			activeIDs[aid] = true
		}
	}
	return
}

// FindByID looks up a session by its internal ID across all users.
func (sm *SessionManager) FindByID(id string) *Session {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.sessions[id]
}

// DeleteByID removes a session by its internal ID from all tracking structures.
func (sm *SessionManager) DeleteByID(id string) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if _, ok := sm.sessions[id]; !ok {
		return false
	}
	sm.deleteByIDLocked(id)
	sm.saveLocked()
	return true
}

// DeleteByAgentSessionID removes all local sessions mapped to the given
// agent session ID. It returns the number of removed local sessions.
func (sm *SessionManager) DeleteByAgentSessionID(agentSessionID string) int {
	if agentSessionID == "" {
		return 0
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	removed := 0
	for id, s := range sm.sessions {
		s.mu.Lock()
		matched := s.AgentSessionID == agentSessionID
		s.mu.Unlock()
		if !matched {
			continue
		}
		sm.deleteByIDLocked(id)
		removed++
	}
	if removed > 0 {
		sm.saveLocked()
	}
	return removed
}

func (sm *SessionManager) deleteByIDLocked(id string) {
	delete(sm.sessions, id)
	for userKey, ids := range sm.userSessions {
		for i, sid := range ids {
			if sid == id {
				sm.userSessions[userKey] = append(ids[:i], ids[i+1:]...)
				break
			}
		}
		if sm.activeSession[userKey] == id {
			delete(sm.activeSession, userKey)
		}
	}
}

// Save persists current state to disk. Safe to call from outside (e.g. after message processing).
func (sm *SessionManager) Save() {
	if err := sm.SaveError(); err != nil {
		slog.Error("session: failed to save", "error", err)
	}
}

// SaveError serializes writers and reports durability failures to lifecycle callers.
func (sm *SessionManager) SaveError() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.saveLocked()
}

func (sm *SessionManager) saveLocked() error {
	snap := sm.snapshotLocked()
	if err := sm.writeSnapshotLocked(snap); err != nil {
		return err
	}
	sm.legacyData = snap.LegacyData
	return nil
}

func (sm *SessionManager) load() {
	snap, raw, loadedFiles, rotFile, err := readSessionStoreFull(sm.storePath)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Error("session: failed to load", "path", sm.storePath, "error", err)
		}
		return
	}
	split := snap.Layout == sessionStoreLayoutSplit
	// Fingerprint what is on disk before recoverRotationsLocked edits it.
	rotFP := rotationsFingerprint(snap.Rotations)
	sm.sessions = snap.Sessions
	sm.activeSession = snap.ActiveSession
	sm.userSessions = snap.UserSessions
	sm.sessionNames = snap.SessionNames
	sm.userMeta = snap.UserMeta
	sm.counter = snap.Counter
	sm.rotations = snap.Rotations
	sm.retiredAgentIDs = snap.RetiredAgentIDs
	sm.recoverRotationsLocked()
	if snap.Version >= snapshotVersion {
		sm.legacyData = snap.LegacyData
	} else {
		// Snapshot was written before LegacyData persistence existed.
		sm.legacyData = !snap.PastIDTracking
		if !sm.legacyData {
			// PastIDTracking was set by a prior code version but LegacyData
			// wasn't persisted. Check for sessions that lost their IDs before
			// PastAgentSessionIDs tracking was available.
			for _, s := range sm.sessions {
				if s.AgentSessionID == "" && len(s.PastAgentSessionIDs) == 0 {
					sm.legacyData = true
					slog.Info("session: detected untracked sessions from prior data loss, enabling legacy mode")
					break
				}
			}
		}
	}

	if sm.sessions == nil {
		sm.sessions = make(map[string]*Session)
	}
	if sm.activeSession == nil {
		sm.activeSession = make(map[string]string)
	}
	if sm.userSessions == nil {
		sm.userSessions = make(map[string][]string)
	}
	if sm.sessionNames == nil {
		sm.sessionNames = make(map[string]string)
	}
	if sm.userMeta == nil {
		sm.userMeta = make(map[string]*UserMeta)
	}

	for _, s := range sm.sessions {
		s.stripContinueSessionSentinel()
	}
	boundHistories(sm.sessions, sm.activeSession, time.Now())

	if split {
		sm.storeFiles = loadedFiles
		sm.storeMeta = raw
		if rotFile != "" {
			sm.storeRotFile, sm.storeRotFP = rotFile, rotFP
		}
		removeOrphanSessionFiles(sm.storePath, loadedFiles, rotFile)
	} else if len(sm.sessions) > 0 {
		// Legacy single-file store: keep its bytes, then write the split layout.
		bak := sm.storePath + ".pre-split.bak"
		if err := AtomicWriteFile(bak, raw, 0o600); err != nil {
			slog.Error("session: keep legacy store before split", "path", bak, "error", err)
		} else if err := sm.saveLocked(); err != nil {
			slog.Error("session: migrate to split layout", "path", sm.storePath, "error", err)
		} else {
			slog.Info("session: migrated store to split layout", "path", sm.storePath, "backup", bak, "sessions", len(sm.sessions))
		}
	}

	slog.Info("session: loaded from disk", "path", sm.storePath, "sessions", len(sm.sessions), "split", split)
}

// InvalidateForAgent clears AgentSessionID on all sessions whose AgentType
// does not match the current agent. This handles the case where the user
// switches agent types (e.g. opencode → pi) and stale session IDs from the
// old agent would cause errors.
func (sm *SessionManager) InvalidateForAgent(agentType string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	invalidated := 0
	for _, s := range sm.sessions {
		s.mu.Lock()
		if s.AgentSessionID != "" && s.AgentType != "" && s.AgentType != agentType {
			slog.Info("session: invalidating stale agent session",
				"session", s.ID,
				"old_agent", s.AgentType,
				"new_agent", agentType,
				"old_agent_session_id", s.AgentSessionID,
			)
			s.recordPastAgentSessionID()
			s.AgentSessionID = ""
			s.AgentType = agentType
			invalidated++
		}
		s.mu.Unlock()
	}
	if invalidated > 0 {
		sm.saveLocked()
	}
}

// ParseSessionKey extracts the base chat identifier from a sessionKey.
// SessionKey formats:
//   - "platform:chatID:userID" → baseChat="platform:chatID", userOrThread="userID"
//   - "platform:chatID:root:rootID" → baseChat="platform:chatID", userOrThread="root:rootID"
//   - "platform:chatID" → baseChat="platform:chatID", userOrThread=""
func ParseSessionKey(sessionKey string) (platform, baseChat, userOrThread string) {
	parts := strings.SplitN(sessionKey, ":", 4)
	if len(parts) < 2 {
		return sessionKey, "", ""
	}
	platform = parts[0]
	if len(parts) == 2 {
		// "platform:chatID" - shared session mode
		return platform, sessionKey, ""
	}
	if len(parts) == 3 {
		// "platform:chatID:userID" - default mode
		return platform, platform + ":" + parts[1], parts[2]
	}
	// "platform:chatID:root:rootID" - thread isolation mode
	return platform, platform + ":" + parts[1], parts[2] + ":" + parts[3]
}

// PruneResult reports the outcome of a prune operation.
type PruneResult struct {
	RemovedSessions []string // IDs of removed sessions
	MergedHistory   int      // Total history entries merged
	ChatsAffected   int      // Number of chat groups with duplicates
}

// PruneDuplicateSessions removes duplicate sessions for the same chat_id,
// keeping only the most recently active one per base chat. History from
// older sessions is merged into the kept session.
//
// This addresses the issue where the same chat_id can have multiple session
// records due to:
//  1. Different users sending messages (different sessionKeys)
//  2. Thread isolation creating per-thread sessions
//  3. Accidental duplicate creation via race conditions
//
// When mergeHistory=true, history entries from removed sessions are appended
// to the kept session (sorted by timestamp). When false, only empty sessions
// are removed.
func (sm *SessionManager) PruneDuplicateSessions(mergeHistory bool) PruneResult {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Group sessions by baseChat
	chatSessions := make(map[string][]*Session)  // baseChat -> sessions
	sessionToBaseChat := make(map[string]string) // session.ID -> baseChat

	for userKey, sessionIDs := range sm.userSessions {
		_, baseChat, _ := ParseSessionKey(userKey)
		for _, sid := range sessionIDs {
			s, ok := sm.sessions[sid]
			if !ok || s == nil {
				continue
			}
			chatSessions[baseChat] = append(chatSessions[baseChat], s)
			sessionToBaseChat[sid] = baseChat
		}
	}

	result := PruneResult{}
	kept := make(map[string]*Session) // baseChat -> session to keep

	// For each baseChat with multiple sessions, decide which to keep
	for baseChat, sessions := range chatSessions {
		if len(sessions) <= 1 {
			continue
		}
		result.ChatsAffected++

		// Sort by UpdatedAt descending (most recent first)
		sort.Slice(sessions, func(i, j int) bool {
			return sessions[i].GetUpdatedAt().After(sessions[j].GetUpdatedAt())
		})

		// Find the best session to keep
		// Priority: most recent session with history, or most recent if none has history
		var keep *Session
		for _, s := range sessions {
			s.mu.Lock()
			hasHistory := len(s.History) > 0
			s.mu.Unlock()
			if hasHistory {
				keep = s
				break
			}
		}
		// If no session has history, keep the most recent one
		if keep == nil {
			keep = sessions[0]
		}
		kept[baseChat] = keep

		// Process other sessions for removal
		for _, old := range sessions {
			if old.ID == keep.ID {
				continue // Skip the one we're keeping
			}

			old.mu.Lock()
			hasHistory := len(old.History) > 0
			oldHistoryLen := len(old.History)
			resumable := old.hasAgentBindingLocked()
			old.mu.Unlock()

			// When not merging: only remove empty sessions. History of
			// non-active sessions is dropped on save/load, so a backend binding
			// also counts as content (keeps /switch and --resume working).
			if !mergeHistory && (hasHistory || resumable) {
				continue // Keep sessions with history when not merging
			}

			// Merge history before removal
			if mergeHistory && hasHistory {
				keep.mu.Lock()
				old.mu.Lock()
				// Append old history to keep, then sort by timestamp.
				// Use SliceStable so that entries with equal timestamps
				// preserve their original relative order — relevant for
				// IM platforms that timestamp at second precision.
				keep.History = append(keep.History, old.History...)
				sort.SliceStable(keep.History, func(i, j int) bool {
					return keep.History[i].Timestamp.Before(keep.History[j].Timestamp)
				})
				result.MergedHistory += oldHistoryLen
				old.mu.Unlock()
				keep.mu.Unlock()
			}

			// Remove old session
			sm.deleteByIDLocked(old.ID)
			result.RemovedSessions = append(result.RemovedSessions, old.ID)

			slog.Info("session: pruned duplicate",
				"removed_session", old.ID,
				"kept_session", keep.ID,
				"base_chat", baseChat,
				"history_merged", oldHistoryLen,
			)
		}
	}

	// Update activeSession: point each userKey to the kept session
	for userKey, sessionIDs := range sm.userSessions {
		if len(sessionIDs) == 0 {
			continue
		}
		_, baseChat, _ := ParseSessionKey(userKey)
		if keep, ok := kept[baseChat]; ok {
			sm.activeSession[userKey] = keep.ID
		}
	}

	if len(result.RemovedSessions) > 0 {
		sm.saveLocked()
		slog.Info("session: prune complete",
			"removed", len(result.RemovedSessions),
			"merged_history", result.MergedHistory,
			"chats_affected", result.ChatsAffected,
		)
	}

	return result
}

// PruneEmptySessions removes sessions with no history entries. Returns count of removed.
func (sm *SessionManager) PruneEmptySessions() int {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	removed := 0
	for _, s := range sm.sessions {
		s.mu.Lock()
		// History of non-active sessions is dropped on save/load; a backend
		// binding means the session is still resumable, so it is not empty.
		isEmpty := len(s.History) == 0 && !s.hasAgentBindingLocked()
		s.mu.Unlock()

		if isEmpty {
			sm.deleteByIDLocked(s.ID)
			removed++
		}
	}

	if removed > 0 {
		sm.saveLocked()
		slog.Info("session: pruned empty sessions", "removed", removed)
	}
	return removed
}
