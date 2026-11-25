package core

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// drainTestSession replies to every Send with a terminal EventResult and lets a
// test observe Alive() calls (used to find code running after session unlock).
type drainTestSession struct {
	controllableAgentSession
	reply   string
	onAlive func()
}

func newDrainTestSession(id, reply string) *drainTestSession {
	return &drainTestSession{controllableAgentSession: *newControllableSession(id), reply: reply}
}

func (s *drainTestSession) Send(string, string, []ImageAttachment, []FileAttachment) error {
	s.events <- Event{Type: EventResult, Content: s.reply, Done: true}
	return nil
}

func (s *drainTestSession) Alive() bool {
	if s.onAlive != nil {
		s.onAlive()
	}
	return true
}

// slowTypingPlatform delays StartTyping. The queued-turn path calls it after
// Send has been issued but before processInteractiveEvents reads Events(), so
// any other reader of Events() deterministically gets the EventResult first.
type slowTypingPlatform struct {
	stubPlatformEngine
	delay time.Duration
}

func (p *slowTypingPlatform) StartTyping(context.Context, any) func() {
	time.Sleep(p.delay)
	return func() {}
}

func sentContains(sent []string, sub string) bool {
	for _, s := range sent {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// Production incident 11-25: the turn-end path starts the unsolicited reader
// and then drains queued messages. The reader kept reading Events() during the
// queued turn, consumed its EventResult, and the foreground turn waited for the
// 2h event idle timeout with the session locked ("is busy").
func TestDrainPendingMessages_StopsUnsolicitedReaderBeforeQueuedTurn(t *testing.T) {
	p := &slowTypingPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}, delay: 100 * time.Millisecond}
	sess := newDrainTestSession("drain-race", "QUEUED_REPLY")
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "", LangEnglish)
	e.streamPreview.Enabled = false
	t.Cleanup(func() { _ = e.Stop() })

	key := "test:ch1:u1"
	session := e.sessions.GetOrCreateActive(key)
	if !session.TryLock() {
		t.Fatal("expected TryLock to succeed")
	}
	state := &interactiveState{
		agentSession: sess,
		platform:     p,
		replyCtx:     "ctx",
		pendingMessages: []queuedMessage{
			{platform: p, replyCtx: "ctx-q", content: "queued"},
		},
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	// Same order as processInteractiveMessageWith's turn end.
	e.startUnsolicitedReader(state, session, e.sessions, key, "")
	t.Cleanup(func() { e.stopUnsolicitedReader(state) })

	done := make(chan bool, 1)
	go func() { done <- e.drainPendingMessages(state, session, e.sessions, key) }()
	select {
	case unlocked := <-done:
		if !unlocked {
			t.Fatal("drainPendingMessages did not unlock the session")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("queued turn never finished (reader stole its EventResult?); session busy=%v sent=%q", session.Busy(), p.getSent())
	}
	if session.Busy() {
		t.Fatal("session still locked after drain")
	}
	if !sentContains(p.getSent(), "QUEUED_REPLY") {
		t.Fatalf("queued reply not delivered: %q", p.getSent())
	}
	var assistant []string
	for _, h := range session.GetHistory(0) {
		if h.Role == "assistant" {
			assistant = append(assistant, h.Content)
		}
	}
	if len(assistant) != 1 || !strings.Contains(assistant[0], "QUEUED_REPLY") {
		t.Fatalf("expected the reply recorded once by the foreground turn, history=%q", assistant)
	}
}

// drainOrphanedQueue must not start the unsolicited reader after
// drainPendingMessages released the session lock: a new foreground turn may
// already own Events() by then.
func TestDrainOrphanedQueue_NoReaderStartAfterUnlock(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newDrainTestSession("orphan-unlock", "ORPHAN_REPLY")
	agent := &controllableAgent{nextSession: sess}
	e := NewEngine("test", agent, []Platform{p}, "", LangEnglish)
	e.streamPreview.Enabled = false
	t.Cleanup(func() { _ = e.Stop() })

	key := "test:ch2:u1"
	session := e.sessions.GetOrCreateActive(key)
	if !session.TryLock() {
		t.Fatal("expected TryLock to succeed")
	}
	// Emulate a new message's foreground turn winning the session lock in the
	// window right after the drain unlocked it: any engine code that touches
	// the agent after unlock observes that turn as the owner.
	var foregroundOwns atomic.Bool
	sess.onAlive = func() {
		if !foregroundOwns.Load() && session.TryLock() {
			foregroundOwns.Store(true)
		}
	}
	state := &interactiveState{
		agentSession: sess,
		platform:     p,
		replyCtx:     "ctx",
		pendingMessages: []queuedMessage{
			{platform: p, replyCtx: "ctx-q", content: "queued-orphan"},
		},
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()
	t.Cleanup(func() { e.stopUnsolicitedReader(state) })

	done := make(chan struct{})
	go func() {
		e.drainOrphanedQueue(session, e.sessions, key, agent, "")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("drainOrphanedQueue did not complete")
	}
	if !sentContains(p.getSent(), "ORPHAN_REPLY") {
		t.Fatalf("queued reply not delivered: %q", p.getSent())
	}
	state.mu.Lock()
	readerRunning := state.unsolicitedCancel != nil
	state.mu.Unlock()
	if foregroundOwns.Load() && readerRunning {
		t.Fatal("unsolicited reader started after the session was unlocked, concurrently with a new foreground turn")
	}
}

// An externally killed agent process must not drop queued user messages: the
// queue is retained and the next message is processed on a --resume'd process.
func TestExternalKillDuringQueuedTurnResumesQueue(t *testing.T) {
	e, a, p, old := rotStart(t)
	e.ReceiveMessage(p, &Message{Platform: "test", SessionKey: "test:chat:user", Content: "Q1_KILLED", ReplyCtx: "chat"})
	e.ReceiveMessage(p, &Message{Platform: "test", SessionKey: "test:chat:user", Content: "Q2_KEEP", ReplyCtx: "chat"})
	a.at(0).events <- Event{Type: EventResult, Done: true, Content: "A_DONE"}
	// Q1 is now the running (queued) turn on backend-0; Q2 waits behind it.
	rotWait(t, func() bool { return len(a.at(0).sent()) == 2 })
	if !old.Busy() {
		t.Fatal("expected Q1 turn to hold the session")
	}
	src := a.at(0)
	src.mu.Lock()
	src.alive = false
	src.mu.Unlock()
	close(src.events) // external kill: stdout closes

	rotWait(t, func() bool { return a.count() == 2 && sentContains(a.at(1).sent(), "Q2_KEEP") })
	a.mu.Lock()
	resumed := a.resumeIDs[1]
	a.mu.Unlock()
	if resumed != "backend-0" {
		t.Fatalf("queue recovered on resume id %q, want backend-0", resumed)
	}
	rotWait(t, func() bool { return !old.Busy() })
	if sentContains(p.getSent(), "agent process exited") {
		t.Fatalf("queued messages reported dropped: %q", p.getSent())
	}
	if !sentContains(p.getSent(), "HANDOFF_DONE") {
		t.Fatalf("Q2 reply not delivered: %q", p.getSent())
	}
	// The session keeps working on the resumed process.
	e.ReceiveMessage(p, &Message{Platform: "test", SessionKey: "test:chat:user", Content: "Q3_AFTER", ReplyCtx: "chat"})
	rotWait(t, func() bool { return sentContains(a.at(1).sent(), "Q3_AFTER") && !old.Busy() })
	if a.count() != 2 {
		t.Fatalf("unexpected extra process start: %d", a.count())
	}
}

// A pending session switch must converge even when the foreground turn has
// gone silent, instead of staying SCHEDULED until the 2h event idle timeout.
func TestRotationStalledForegroundTurnConverges(t *testing.T) {
	prev := rotationTurnStallTimeout.Swap(int64(150 * time.Millisecond))
	t.Cleanup(func() { rotationTurnStallTimeout.Store(prev) })

	e, _, _, old := rotStart(t)
	if _, err := e.ScheduleRotation(RotateRequest{SessionKey: "test:chat:user", ExpectedSessionID: old.ID, RequestID: "stall", Handoff: "handoff"}); err != nil {
		t.Fatal(err)
	}
	// backend-0 never answers ORIGINAL.
	rotWait(t, func() bool { op, _ := e.sessions.Rotation("stall"); return op.Status != "SCHEDULED" })
	if op, _ := e.sessions.Rotation("stall"); op.Status != "FAILED" {
		t.Fatalf("rotation status = %q, want FAILED", op.Status)
	}
	rotWait(t, func() bool { return !old.Busy() })
}

// Without a pending switch, a quiet foreground turn is left alone (only the
// general event idle timeout applies).
func TestRotationStallTimeoutIgnoredWithoutPendingSwitch(t *testing.T) {
	prev := rotationTurnStallTimeout.Swap(int64(50 * time.Millisecond))
	t.Cleanup(func() { rotationTurnStallTimeout.Store(prev) })

	_, a, _, old := rotStart(t)
	time.Sleep(300 * time.Millisecond)
	if !old.Busy() || !a.at(0).Alive() {
		t.Fatal("quiet turn without a pending switch was ended")
	}
}
