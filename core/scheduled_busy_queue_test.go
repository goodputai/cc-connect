package core

import (
	"errors"
	"path/filepath"
	"testing"
)

// tqPlatform adds proactive messaging to the topic test platform so timers
// can target it.
type tqPlatform struct{ *tfPlatform }

func (p *tqPlatform) ReconstructReplyCtx(string) (any, error) {
	return tfContext{"chat", "", "root"}, nil
}

// A timer that fires while the target session is mid-turn must be queued for
// that turn (and delivered right after it), not fail with "is busy".
// Negative control: without queueScheduledForBusySession the call returns
// `session "test:chat:commander" is busy` and the prompt is lost.
func TestScheduledPrompt_BusySessionQueuesAndDrains(t *testing.T) {
	p := &tqPlatform{&tfPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}, limit: 4}}
	a := &tfAgent{started: make(chan tfStart, 32)}
	e := NewEngine("test", a, []Platform{p}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	e.dataDir = t.TempDir()
	t.Cleanup(func() { _ = e.Stop() })

	const key = "test:chat:commander"
	e.ReceiveMessage(p, &Message{Platform: "test", SessionKey: key, MessageID: "MAIN", Content: "MAIN", UserID: "commander", ReplyCtx: tfContext{"chat", "", "root"}})
	main := tfNext(t, a, "MAIN")

	err := e.ExecuteTimerJob(&TimerJob{ID: "t1", SessionKey: key, Prompt: "TIMER-PROMPT"})
	if !errors.Is(err, ErrQueuedForBusySession) {
		t.Fatalf("timer on busy session: err=%v, want ErrQueuedForBusySession", err)
	}

	main.s.finish("MAIN-DONE")
	next := tfNext(t, a, "TIMER-PROMPT")
	if next.s != main.s {
		t.Fatal("queued timer prompt started a second agent session instead of the main one")
	}
	next.s.finish("TIMER-DONE")
	tfWait(t, func() bool { return tfSaw(p.tfPlatform, "TIMER-DONE") })
}
