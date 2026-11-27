package core

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type qxReport struct {
	item, event string
	body        queueTurnReport
}

func qxEnv(t *testing.T) (*Engine, *tqPlatform, *tfAgent) {
	t.Helper()
	p := &tqPlatform{&tfPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}, limit: 4}}
	a := &tfAgent{started: make(chan tfStart, 32)}
	e := NewEngine("test", a, []Platform{p}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	e.dataDir = t.TempDir()
	t.Cleanup(func() { _ = e.Stop() })
	return e, p, a
}

func qxIdle(t *testing.T, e *Engine, key string) {
	t.Helper()
	tfWait(t, func() bool { return e.queueSessionSnapshot()[key].State == "idle" })
}

// The executor opens a turn only on an idle session (202), answers 409 while
// that turn runs, and reports started/finished until the center acknowledges.
// Negative controls: drop the busy checks in accept (x.items scan and
// active.TryLock) and the second item gets 202 on a busy session; make
// reportUntilAcknowledged give up on the first error and the item is dropped
// before it is reported, so the repeat accept no longer returns the same turn.
func TestQueueExecutorTurnLifecycle(t *testing.T) {
	e, p, a := qxEnv(t)
	const key = "test:chat:commander"
	e.ReceiveMessage(p, &Message{Platform: "test", SessionKey: key, MessageID: "MAIN", Content: "MAIN", UserID: "commander", ReplyCtx: tfContext{"chat", "", "root"}})
	tfNext(t, a, "MAIN").s.finish("MAIN-DONE")
	qxIdle(t, e, key)

	var mu sync.Mutex
	failures := 2
	reports := make(chan qxReport, 8)
	report := func(_ context.Context, item, event string, body queueTurnReport) error {
		mu.Lock()
		defer mu.Unlock()
		if event == "started" && failures > 0 {
			failures--
			return errors.New("center down")
		}
		reports <- qxReport{item, event, body}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	x := newQueueExecutor(&APIServer{engines: map[string]*Engine{"test": e}}, ctx, report)
	x.halt = func(string) (bool, error) { return false, nil }

	first := queueTurnRequest{ItemID: "q_1", Project: "test", SessionKey: key, Prompt: "ORDER-ONE"}
	start, code, err := x.accept(first)
	if code != http.StatusAccepted || err != nil || start.TurnID == "" {
		t.Fatalf("accept idle = %d %v %+v", code, err, start)
	}
	turn := tfNext(t, a, "ORDER-ONE")
	if again, code, _ := x.accept(first); code != http.StatusAccepted || again.TurnID != start.TurnID {
		t.Fatalf("repeat accept = %d %+v; want same turn", code, again)
	}
	second := queueTurnRequest{ItemID: "q_2", Project: "test", SessionKey: key, Prompt: "ORDER-TWO"}
	if _, code, _ := x.accept(second); code != http.StatusConflict {
		t.Fatalf("accept busy = %d; want 409", code)
	}
	if got := <-reports; got.event != "started" || got.item != "q_1" || got.body.TurnID != start.TurnID {
		t.Fatalf("first report = %+v", got)
	}
	rows := x.api.queueSessions("henry")
	if len(rows) != 1 || rows[0].State != "busy" || rows[0].ItemID != "q_1" {
		t.Fatalf("sessions while running = %+v", rows)
	}
	turn.s.finish("ORDER-ONE-DONE")
	got := <-reports
	if got.event != "finished" || got.body.Status != "done" || !strings.Contains(got.body.Tail, "ORDER-ONE-DONE") {
		t.Fatalf("finished report = %+v", got)
	}
	tfWait(t, func() bool {
		_, code, _ := x.accept(second)
		return code == http.StatusAccepted
	})
	tfNext(t, a, "ORDER-TWO").s.finish("ORDER-TWO-DONE")
	if got := <-reports; got.event != "started" || got.item != "q_2" {
		t.Fatalf("second started = %+v", got)
	}
	if got := <-reports; got.event != "finished" || got.item != "q_2" {
		t.Fatalf("second finished = %+v", got)
	}
	// Let the turn's session save land before TempDir cleanup.
	qxIdle(t, e, key)
	tfWait(t, func() bool { x.mu.Lock(); defer x.mu.Unlock(); return len(x.items) == 0 })
}

// Negative control: remove the halt or session lookup in accept and these
// requests start a turn (202) instead of being rejected.
func TestQueueExecutorRejects(t *testing.T) {
	e, p, a := qxEnv(t)
	const key = "test:chat:commander"
	e.ReceiveMessage(p, &Message{Platform: "test", SessionKey: key, MessageID: "MAIN", Content: "MAIN", UserID: "commander", ReplyCtx: tfContext{"chat", "", "root"}})
	tfNext(t, a, "MAIN").s.finish("MAIN-DONE")
	qxIdle(t, e, key)
	x := newQueueExecutor(&APIServer{engines: map[string]*Engine{"test": e}}, context.Background(),
		func(context.Context, string, string, queueTurnReport) error { return nil })
	halted := false
	x.halt = func(string) (bool, error) { return halted, nil }
	for _, tc := range []struct {
		name string
		req  queueTurnRequest
		halt bool
		want int
	}{
		{"missing prompt", queueTurnRequest{ItemID: "q", Project: "test", SessionKey: key}, false, http.StatusBadRequest},
		{"unknown project", queueTurnRequest{ItemID: "q", Project: "nope", SessionKey: key, Prompt: "x"}, false, http.StatusNotFound},
		{"unknown session", queueTurnRequest{ItemID: "q", Project: "test", SessionKey: "test:chat:other", Prompt: "x"}, false, http.StatusNotFound},
		{"halted group", queueTurnRequest{ItemID: "q", Project: "test", SessionKey: key, Prompt: "x"}, true, http.StatusLocked},
	} {
		halted = tc.halt
		if _, code, _ := x.accept(tc.req); code != tc.want {
			t.Errorf("%s: got %d; want %d", tc.name, code, tc.want)
		}
	}
}
