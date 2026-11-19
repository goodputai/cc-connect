package antigravity

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

type observedAgy struct {
	bin     string
	args    []string
	env     []string
	workDir string
	started chan *antigravitySession
}

func (a *observedAgy) Name() string                                                  { return "antigravity" }
func (a *observedAgy) Stop() error                                                   { return nil }
func (a *observedAgy) ListSessions(context.Context) ([]core.AgentSessionInfo, error) { return nil, nil }
func (a *observedAgy) StartSession(ctx context.Context, id string) (core.AgentSession, error) {
	session, err := newAntigravitySession(ctx, a.bin, a.args, a.workDir, "", "yolo", id, a.env, 0)
	if err == nil {
		a.started <- session
	}
	return session, err
}

type agyTestReply struct{ key, content string }
type agyTestPlatform struct{ replies chan agyTestReply }

func (*agyTestPlatform) Name() string                    { return "agy-test" }
func (*agyTestPlatform) Start(core.MessageHandler) error { return nil }
func (*agyTestPlatform) Stop() error                     { return nil }
func (p *agyTestPlatform) Reply(_ context.Context, ctx any, content string) error {
	p.replies <- agyTestReply{ctx.(string), content}
	return nil
}
func (p *agyTestPlatform) Send(ctx context.Context, replyCtx any, content string) error {
	return p.Reply(ctx, replyCtx, content)
}
func (*agyTestPlatform) TopicSession(ctx any) (string, string, int) {
	key := ctx.(string)
	if group, thread, ok := strings.Cut(key, ":thread:"); ok {
		return group, thread, 2
	}
	return "", "", 0
}

type agyEngineTest struct {
	engine   *core.Engine
	agent    *observedAgy
	platform *agyTestPlatform
	seq      atomic.Int64
}

func newAgyEngineTest(t *testing.T, extraEnv ...string) *agyEngineTest {
	t.Helper()
	bin, args, env := fakeAgyArgs(t)
	a := &observedAgy{bin: bin, args: args, env: append(env, extraEnv...), workDir: t.TempDir(), started: make(chan *antigravitySession, 8)}
	p := &agyTestPlatform{replies: make(chan agyTestReply, 32)}
	e := core.NewEngine("antigravity-test", a, []core.Platform{p}, filepath.Join(t.TempDir(), "sessions.json"), core.Language("en"))
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Stop() })
	return &agyEngineTest{engine: e, agent: a, platform: p}
}

func (h *agyEngineTest) message(key, prompt string) {
	msg := &core.Message{
		Platform: h.platform.Name(), MessageID: fmt.Sprint(h.seq.Add(1)),
		SessionKey: key, UserID: "test-user", Content: prompt, ReplyCtx: key,
	}
	go h.engine.ReceiveMessage(h.platform, msg)
}

func (h *agyEngineTest) started(t *testing.T) *antigravitySession {
	t.Helper()
	select {
	case session := <-h.agent.started:
		return session
	case <-time.After(5 * time.Second):
		t.Fatal("no agy process started")
		return nil
	}
}

func (h *agyEngineTest) reply(t *testing.T, contains string) agyTestReply {
	t.Helper()
	deadline := time.After(6 * time.Second)
	for {
		select {
		case r := <-h.platform.replies:
			if strings.Contains(strings.ToLower(r.content), strings.ToLower(contains)) {
				return r
			}
		case <-deadline:
			t.Fatalf("no reply containing %q", contains)
			return agyTestReply{}
		}
	}
}

func assertAgyClosed(t *testing.T, s *antigravitySession) {
	t.Helper()
	select {
	case <-s.done:
		if s.Alive() {
			t.Fatal("exited session reported alive")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("agy process still running")
	}
}

func assertAgyResumed(t *testing.T, previous, next *antigravitySession) {
	t.Helper()
	id := previous.CurrentSessionID()
	if id == "" || next.cmd.Process.Pid == previous.cmd.Process.Pid || next.CurrentSessionID() != id {
		t.Fatalf("expected a new PID resuming %q: old=%d new=%d current=%q", id, previous.cmd.Process.Pid, next.cmd.Process.Pid, next.CurrentSessionID())
	}
	for i, arg := range next.cmd.Args {
		if arg == "--conversation" && next.cmd.Args[i+1] == id {
			return
		}
	}
	t.Fatalf("missing --conversation %s in %v", id, next.cmd.Args)
}

func TestAntigravityEngine_IdleReapsAndResumes(t *testing.T) {
	h := newAgyEngineTest(t)
	h.engine.SetAgentSessionIdleTimeout(150 * time.Millisecond)
	const key = "agy-test:group:user"
	h.message(key, "first turn")
	first := h.started(t)
	h.reply(t, "OK-1")
	assertAgyClosed(t, first)
	h.message(key, "more after idle")
	second := h.started(t)
	h.reply(t, "OK-1")
	assertAgyResumed(t, first, second)
}

func TestAntigravityEngine_CrashRestartsWithConversation(t *testing.T) {
	h := newAgyEngineTest(t)
	const key = "agy-test:group:user"
	h.message(key, "CRASH-TEST in a new conversation")
	first := h.started(t)
	h.reply(t, "process exited before result")
	assertAgyClosed(t, first)
	h.message(key, "try the next turn")
	second := h.started(t)
	h.reply(t, "OK-1")
	assertAgyResumed(t, first, second)
}

func TestAntigravityEngine_StopInterruptsActiveTurn(t *testing.T) {
	release := filepath.Join(t.TempDir(), "never-released")
	h := newAgyEngineTest(t, "AGY_RELEASE_FILE="+release)
	const key = "agy-test:group:user"
	h.message(key, "first turn")
	first := h.started(t)
	h.reply(t, "OK-1")
	h.message(key, "STOP-TEST")
	// The helper emits "started" while the turn stays active. Wait for the
	// session to observe an active turn before delivering the real command.
	deadline := time.Now().Add(4 * time.Second)
	for {
		first.mu.Lock()
		active := first.turn != nil
		first.mu.Unlock()
		if active {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second turn never started")
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.message(key, "/stop")
	h.reply(t, "execution stopped")
	assertAgyClosed(t, first)
	h.message(key, "resume after stop")
	second := h.started(t)
	h.reply(t, "OK-1")
	assertAgyResumed(t, first, second)
}

func TestAntigravityEngine_TopicsUseIndependentProcesses(t *testing.T) {
	release := filepath.Join(t.TempDir(), "release")
	h := newAgyEngineTest(t, "AGY_RELEASE_FILE="+release)
	firstKey, secondKey := "agy-test:group:thread:1", "agy-test:group:thread:2"
	h.message(firstKey, "TOPIC-HOLD")
	first := h.started(t)
	h.message(secondKey, "TOPIC-HOLD")
	second := h.started(t)
	if first.cmd.Process.Pid == second.cmd.Process.Pid || !first.Alive() || !second.Alive() {
		t.Fatal("topics did not start independent agy processes concurrently")
	}
	if err := os.WriteFile(release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	a, b := h.reply(t, "OK-1"), h.reply(t, "OK-1")
	if a.key == b.key || first.CurrentSessionID() == second.CurrentSessionID() {
		t.Fatalf("topics shared delivery or conversation: %+v %+v", a, b)
	}
}
