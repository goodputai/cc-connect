package core

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const qcLocal, qcProject, qcKey, qcSrcKey = "vps", "proj", "feishu:oc_target:ou_x", "feishu:oc_source:ou_y"

// qcEnv is a center with a fake executor: sessions are whatever the test
// says, and startTurn answers from a script.
type qcEnv struct {
	t      *testing.T
	c      *queueCenter
	mu     sync.Mutex
	clock  time.Time
	rows   map[string]queueSession // lane key (project\x00session) → row
	reach  bool
	answer func(queueTurnRequest) (int, error)
	sent   []queueTurnRequest
	notes  []string
	halted map[string]bool
}

func newQCEnv(t *testing.T, dir string) *qcEnv {
	t.Helper()
	c, err := newQueueCenter(dir)
	if err != nil {
		t.Fatal(err)
	}
	e := &qcEnv{t: t, c: c, clock: time.Date(2026, 11, 27, 18, 0, 0, 0, queueZone), rows: map[string]queueSession{}, reach: true, halted: map[string]bool{}}
	e.answer = func(queueTurnRequest) (int, error) { return http.StatusAccepted, nil }
	e.idle(qcProject, qcKey)
	e.idle(qcProject, qcSrcKey)
	c.now = func() time.Time { e.mu.Lock(); defer e.mu.Unlock(); return e.clock }
	c.spawn = func(f func()) { f() }
	c.halt = func(k string) (bool, error) { e.mu.Lock(); defer e.mu.Unlock(); return e.halted[k], nil }
	c.notify = func(_, text string) { e.mu.Lock(); e.notes = append(e.notes, text); e.mu.Unlock() }
	c.views = func() map[string]queueView {
		e.mu.Lock()
		defer e.mu.Unlock()
		now := e.clock
		var rows []queueSession
		for _, r := range e.rows {
			rows = append(rows, r)
		}
		v := queueView{Known: true, Reachable: e.reach, Sessions: rows}
		if e.reach {
			v.FetchedAt = &now
		}
		return map[string]queueView{qcLocal: v}
	}
	c.startTurn = func(_ context.Context, _ string, req queueTurnRequest) (queueTurnReport, int, error) {
		e.mu.Lock()
		e.sent = append(e.sent, req)
		answer := e.answer
		e.mu.Unlock()
		status, err := answer(req)
		if err == nil {
			// The executor now holds the turn, as applySessions would show.
			e.busy(req.Project, req.SessionKey, req.ItemID)
		}
		return queueTurnReport{TurnID: "t_" + req.ItemID}, status, err
	}
	return e
}

func (e *qcEnv) idle(project, key string) {
	e.mu.Lock()
	e.rows[project+"\x00"+key] = queueSession{Project: project, SessionKey: key, State: "idle"}
	e.mu.Unlock()
}

func (e *qcEnv) busy(project, key, item string) {
	e.mu.Lock()
	e.rows[project+"\x00"+key] = queueSession{Project: project, SessionKey: key, State: "busy", ItemID: item}
	e.mu.Unlock()
}

func (e *qcEnv) advance(d time.Duration) {
	e.mu.Lock()
	e.clock = e.clock.Add(d)
	e.mu.Unlock()
}

func (e *qcEnv) enqueue(key string, mutate ...func(*queueEnqueueRequest)) (queueEnqueueResponse, int) {
	e.t.Helper()
	req := queueEnqueueRequest{IdempotencyKey: key, Instance: qcLocal, Project: qcProject, SessionKey: qcKey, Prompt: "do " + key}
	for _, m := range mutate {
		m(&req)
	}
	res, status, _ := e.c.enqueue(req)
	return res, status
}

func (e *qcEnv) mustEnqueue(key string, mutate ...func(*queueEnqueueRequest)) string {
	e.t.Helper()
	res, status := e.enqueue(key, mutate...)
	if status != http.StatusCreated {
		e.t.Fatalf("enqueue %s: HTTP %d", key, status)
	}
	return res.ID
}

func (e *qcEnv) finish(id, status, tail string) {
	e.t.Helper()
	if code, err := e.c.report(id, "finished", queueTurnReport{Status: status, Tail: tail}); code != http.StatusOK {
		e.t.Fatalf("finish %s: HTTP %d %v", id, code, err)
	}
	it := e.c.lookup(id)
	if it == nil {
		e.t.Fatalf("finished item %s vanished", id)
	}
	e.idle(it.Project, it.SessionKey)
}

func (e *qcEnv) status(id string) string {
	e.t.Helper()
	it := e.c.lookup(id)
	if it == nil {
		e.t.Fatalf("item %s not found", id)
	}
	return it.Status
}

func (e *qcEnv) sentIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	var ids []string
	for _, r := range e.sent {
		ids = append(ids, r.ItemID)
	}
	return ids
}

func withWake(r *queueEnqueueRequest) {
	r.WakeSourceOnFinish = true
	r.Source = &queueSource{Instance: qcLocal, Project: qcProject, SessionKey: qcSrcKey, Agent: "总管A"}
}

func (e *qcEnv) completions() []*queueItem {
	e.c.mu.Lock()
	defer e.c.mu.Unlock()
	var out []*queueItem
	for _, it := range e.c.items {
		if it.Kind == queueKindCompletion {
			out = append(out, it.clone())
		}
	}
	return out
}

func TestQueueCenterEnqueueIdempotentAndRejects(t *testing.T) {
	e := newQCEnv(t, t.TempDir())
	first, status := e.enqueue("A1")
	if status != http.StatusCreated || !strings.HasPrefix(first.ID, "q_") || first.Status != "queued" || first.Position != 1 {
		t.Fatalf("first enqueue = %+v, HTTP %d", first, status)
	}
	again, status := e.enqueue("A1", func(r *queueEnqueueRequest) { r.Prompt = "different" })
	if status != http.StatusOK || again.ID != first.ID {
		t.Fatalf("duplicate enqueue = %+v, HTTP %d; want 200 with %s", again, status, first.ID)
	}

	cases := []struct {
		name   string
		mutate func(*queueEnqueueRequest)
		want   int
	}{
		{"missing prompt", func(r *queueEnqueueRequest) { r.Prompt = " " }, http.StatusBadRequest},
		{"reserved key", func(r *queueEnqueueRequest) { r.IdempotencyKey = "cb:q_x" }, http.StatusBadRequest},
		{"wake without source", func(r *queueEnqueueRequest) { r.WakeSourceOnFinish = true }, http.StatusBadRequest},
		{"bad priority", func(r *queueEnqueueRequest) { r.Priority = "urgent" }, http.StatusBadRequest},
		{"unknown instance", func(r *queueEnqueueRequest) { r.Instance = "nowhere" }, http.StatusNotFound},
		{"unknown session", func(r *queueEnqueueRequest) { r.SessionKey = "feishu:oc_none:ou_x" }, http.StatusNotFound},
		{"halted group", func(r *queueEnqueueRequest) { r.SessionKey = "feishu:oc_halted:ou_x" }, http.StatusLocked},
	}
	e.halted["feishu:oc_halted:ou_x"] = true
	e.idle(qcProject, "feishu:oc_halted:ou_x")
	for i, tc := range cases {
		if _, got := e.enqueue("R"+string(rune('a'+i)), tc.mutate); got != tc.want {
			t.Errorf("%s: HTTP %d, want %d", tc.name, got, tc.want)
		}
	}

	for i := 2; i <= queueLaneLimit; i++ {
		e.mustEnqueue("F" + string(rune('a'+i)))
	}
	if _, got := e.enqueue("overflow"); got != http.StatusTooManyRequests {
		t.Fatalf("21st queued item: HTTP %d, want 429", got)
	}

	e.reach = false
	e2 := newQCEnv(t, t.TempDir())
	e2.reach = false
	if _, got := e2.enqueue("never-reached"); got != http.StatusServiceUnavailable {
		t.Fatalf("never reachable executor: HTTP %d, want 503", got)
	}
}

func TestQueueCenterHTTPAuthAndWhitelist(t *testing.T) {
	e := newQCEnv(t, t.TempDir())
	mux := http.NewServeMux()
	e.c.register(mux)
	srv := httptest.NewServer(queueBearerHandler(mux, "good-token"))
	defer srv.Close()
	do := func(method, path, token, body string) int {
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		return res.StatusCode
	}
	body := `{"idempotency_key":"H1","instance":"vps","project":"proj","session_key":"` + qcKey + `","prompt":"p"}`
	if got := do("POST", "/v1/queue/items", "wrong", body); got != http.StatusUnauthorized {
		t.Fatalf("bad token: HTTP %d, want 401", got)
	}
	if got := do("POST", "/v1/queue/items", "", body); got != http.StatusUnauthorized {
		t.Fatalf("no token: HTTP %d, want 401", got)
	}
	for _, path := range []string{"/x/turns", "/v1/queue", "/v1/board", "/api/sessions"} {
		if got := do("POST", path, "good-token", "{}"); got != http.StatusNotFound {
			t.Fatalf("POST %s outside whitelist: HTTP %d, want 404", path, got)
		}
	}
	if got := do("POST", "/v1/queue/items", "good-token", body); got != http.StatusCreated {
		t.Fatalf("good token: HTTP %d, want 201", got)
	}
	if got := do("GET", "/v1/queue/items", "good-token", ""); got != http.StatusOK {
		t.Fatalf("list: HTTP %d", got)
	}
}

func TestQueueCenterDispatchLifecycle(t *testing.T) {
	e := newQCEnv(t, t.TempDir())
	a := e.mustEnqueue("L1")
	b := e.mustEnqueue("L2")
	e.c.step(context.Background())
	if got := e.sentIDs(); len(got) != 1 || got[0] != a {
		t.Fatalf("first pass sent %v, want only %s", got, a)
	}
	if e.status(a) != "running" || e.c.lookup(a).StartedAt == nil || e.status(b) != "queued" {
		t.Fatalf("after first pass: a=%s b=%s", e.status(a), e.status(b))
	}
	high := e.mustEnqueue("L3", func(r *queueEnqueueRequest) { r.Priority = "high" })
	e.c.step(context.Background())
	if len(e.sentIDs()) != 1 {
		t.Fatalf("busy lane dispatched again: %v", e.sentIDs())
	}

	e.finish(a, "done", "all good")
	if got := e.c.lookup(a); got.Status != "done" || got.Tail != "all good" || got.FinishedAt == nil {
		t.Fatalf("archived item = %+v", got)
	}
	e.c.mu.Lock()
	_, stillActive := e.c.items[a]
	e.c.mu.Unlock()
	if stillActive {
		t.Fatal("finished item still in active state")
	}
	e.c.step(context.Background())
	if got := e.sentIDs(); len(got) != 2 || got[1] != high {
		t.Fatalf("after finish sent %v, want high-priority %s next", got, high)
	}
	if code, _ := e.c.report(a, "finished", queueTurnReport{Status: "done"}); code != http.StatusOK {
		t.Fatalf("repeated finished for archived item: HTTP %d, want 200", code)
	}
	if res, status := e.enqueue("L1"); status != http.StatusOK || res.ID != a || res.Status != "done" {
		t.Fatalf("duplicate of finished item = %+v HTTP %d", res, status)
	}
	items, err := e.c.list(map[string][]string{"status": {"done"}})
	if err != nil || len(items) != 1 || items[0].ID != a {
		t.Fatalf("list done = %v, %v", items, err)
	}
}

func TestQueueCenterRequeuesBusySession(t *testing.T) {
	e := newQCEnv(t, t.TempDir())
	calls := 0
	e.answer = func(queueTurnRequest) (int, error) {
		calls++
		if calls == 1 {
			return http.StatusConflict, errors.New("session is busy")
		}
		return http.StatusAccepted, nil
	}
	id := e.mustEnqueue("B1")
	e.c.step(context.Background())
	if got := e.status(id); got != "queued" {
		t.Fatalf("after 409 status = %s, want queued", got)
	}
	e.c.step(context.Background())
	if got := e.status(id); got != "running" || calls != 2 {
		t.Fatalf("after retry status = %s calls = %d", got, calls)
	}

	// A human turn on the session holds the item back without a push.
	e2 := newQCEnv(t, t.TempDir())
	e2.busy(qcProject, qcKey, "")
	id2 := e2.mustEnqueue("B2")
	e2.c.step(context.Background())
	if len(e2.sentIDs()) != 0 || e2.status(id2) != "queued" {
		t.Fatalf("busy session got a push: %v", e2.sentIDs())
	}
	e2.idle(qcProject, qcKey)
	e2.c.step(context.Background())
	if e2.status(id2) != "running" {
		t.Fatalf("idle session not dispatched: %s", e2.status(id2))
	}
}

func TestQueueCenterRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	e := newQCEnv(t, dir)
	otherKey := "feishu:oc_other:ou_z"
	lostKey := "feishu:oc_lost:ou_z"
	e.idle(qcProject, otherKey)
	e.idle(qcProject, lostKey)
	done := e.mustEnqueue("D1")
	e.c.step(context.Background())
	e.finish(done, "done", "finished before crash")
	alive := e.mustEnqueue("D2")
	lost := e.mustEnqueue("D3", func(r *queueEnqueueRequest) { r.SessionKey = lostKey })
	e.c.step(context.Background())
	queued := e.mustEnqueue("D4")
	pending := e.mustEnqueue("D5", func(r *queueEnqueueRequest) { r.SessionKey = otherKey })
	e.halted[otherKey] = true
	if e.status(alive) != "running" || e.status(lost) != "running" || e.status(queued) != "queued" {
		t.Fatalf("before crash: %s %s %s", e.status(alive), e.status(lost), e.status(queued))
	}

	// Crash: a new center reads only what is on disk. The executor still
	// holds alive's turn; lost's turn died with it.
	e.advance(time.Second)
	r := newQCEnv(t, dir)
	r.clock = e.clock
	r.idle(qcProject, otherKey)
	r.idle(qcProject, lostKey)
	r.busy(qcProject, qcKey, alive)
	r.c.step(context.Background())
	if got := r.c.lookup(done); got == nil || got.Status != "done" || got.Tail != "finished before crash" {
		t.Fatalf("finished item after restart = %+v", got)
	}
	if got := r.status(alive); got != "running" {
		t.Fatalf("live turn after restart = %s, want running (no false lost-finish)", got)
	}
	if got := r.c.lookup(lost); got.Status != "failed" || got.Reason != "lost-finish" {
		t.Fatalf("dead turn after restart = %s/%s, want failed/lost-finish", got.Status, got.Reason)
	}
	if got := r.status(queued); got != "queued" {
		t.Fatalf("queued behind live turn = %s", got)
	}
	r.finish(alive, "done", "")
	r.c.step(context.Background())
	if got := r.status(queued); got != "running" {
		t.Fatalf("queued item not pushed after restart: %s", got)
	}
	r.c.step(context.Background())
	if got := r.status(pending); got != "running" {
		t.Fatalf("item queued before crash not pushed: %s", got)
	}
	info, err := os.Stat(filepath.Join(dir, "state.json"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("state file mode = %v, %v", info, err)
	}
}

func TestQueueCenterCompletionWake(t *testing.T) {
	e := newQCEnv(t, t.TempDir())
	id := e.mustEnqueue("W1", withWake, func(r *queueEnqueueRequest) { r.Board = &queueBoard{Line: "cc-connect", Item: "M2"} })
	e.c.step(context.Background())
	e.finish(id, "done", "tail text")
	cbs := e.completions()
	if len(cbs) != 1 {
		t.Fatalf("completion items = %d, want 1", len(cbs))
	}
	cb := cbs[0]
	if cb.IdempotencyKey != "cb:"+id || cb.SessionKey != qcSrcKey || !strings.Contains(cb.Prompt, "tail text") ||
		!strings.Contains(cb.Prompt, id) || !strings.Contains(cb.Prompt, "done") {
		t.Fatalf("completion item = %+v", cb)
	}
	e.c.step(context.Background())
	e.finish(cb.ID, "done", "ack")
	if got := e.completions(); len(got) != 0 {
		t.Fatalf("completion woke someone: %+v", got[0])
	}

	// Without wake_source_on_finish nobody is woken.
	plain := e.mustEnqueue("W2")
	e.c.step(context.Background())
	e.finish(plain, "failed", "")
	if got := e.completions(); len(got) != 0 {
		t.Fatalf("unrequested wake: %+v", got[0])
	}

	// One chain gets at most queueWakeLimit wakes an hour; the next one is
	// reported to a person instead.
	for i := 1; i < queueWakeLimit; i++ {
		x := e.mustEnqueue("WL"+string(rune('a'+i)), withWake)
		e.c.step(context.Background())
		e.finish(x, "done", "")
	}
	if got := len(e.completions()); got != queueWakeLimit-1 {
		t.Fatalf("wakes within limit = %d, want %d", got, queueWakeLimit-1)
	}
	over := e.mustEnqueue("WL-over", withWake)
	e.c.step(context.Background())
	e.finish(over, "done", "")
	if got := len(e.completions()); got != queueWakeLimit-1 {
		t.Fatalf("wake past limit was queued: %d", got)
	}
	if len(e.notes) != 1 || !strings.Contains(e.notes[0], over) {
		t.Fatalf("suppressed wake notices = %v", e.notes)
	}
	if h := e.c.lookup(over).History; h[len(h)-1].Note != "wake_suppressed" {
		t.Fatalf("suppression not in history: %+v", h)
	}
	e.advance(queueWakeWindow)
	later := e.mustEnqueue("WL-later", withWake)
	e.c.step(context.Background())
	e.finish(later, "done", "")
	if got := len(e.completions()); got != queueWakeLimit {
		t.Fatalf("wake after the window = %d completions, want %d", got, queueWakeLimit)
	}
}

func TestQueueCenterTimeouts(t *testing.T) {
	e := newQCEnv(t, t.TempDir())
	id := e.mustEnqueue("T1", withWake)
	next := e.mustEnqueue("T2")
	e.c.step(context.Background())
	e.advance(59 * time.Minute)
	e.c.step(context.Background())
	if e.status(id) != "running" {
		t.Fatalf("timed out early: %s", e.status(id))
	}
	e.advance(time.Minute)
	e.c.step(context.Background())
	if e.status(id) != "timed_out" {
		t.Fatalf("after 1h status = %s, want timed_out", e.status(id))
	}
	cbs := e.completions()
	if len(cbs) != 1 || cbs[0].IdempotencyKey != "cb:"+id+":timed_out" || !strings.Contains(cbs[0].Prompt, "timed_out") {
		t.Fatalf("timeout wake = %+v", cbs)
	}
	if e.status(next) != "queued" {
		t.Fatalf("lane freed while the timed-out turn may still run: %s", e.status(next))
	}
	// The turn was not stopped; its late finish still lands and wakes again.
	e.finish(id, "done", "late")
	if got := e.c.lookup(id); got.Status != "done" || got.Tail != "late" {
		t.Fatalf("late finish = %+v", got)
	}
	if got := len(e.completions()); got != 2 {
		t.Fatalf("completions after late finish = %d, want 2", got)
	}

	// Dispatched but never started: dispatch_failed after 5 minutes.
	d := newQCEnv(t, t.TempDir())
	d.answer = func(queueTurnRequest) (int, error) { return 0, errors.New("connection refused") }
	stuck := d.mustEnqueue("T3", withWake)
	d.c.step(context.Background())
	d.advance(4 * time.Minute)
	d.c.step(context.Background())
	if d.status(stuck) != "running" || len(d.sentIDs()) != 2 {
		t.Fatalf("before 5 min: %s, %d pushes", d.status(stuck), len(d.sentIDs()))
	}
	d.advance(time.Minute)
	d.c.step(context.Background())
	if got := d.c.lookup(stuck); got.Status != "failed" || got.Reason != "dispatch_failed" {
		t.Fatalf("after 5 min = %s/%s, want failed/dispatch_failed", got.Status, got.Reason)
	}
	if cbs := d.completions(); len(cbs) != 1 || !strings.Contains(cbs[0].Prompt, "dispatch_failed") {
		t.Fatalf("dispatch_failed wake = %+v", cbs)
	}
}

func TestQueueCenterCancelAndPatch(t *testing.T) {
	e := newQCEnv(t, t.TempDir())
	running := e.mustEnqueue("C1")
	e.c.step(context.Background())
	queued := e.mustEnqueue("C2")
	if _, code, _ := e.c.cancel(running); code != http.StatusConflict {
		t.Fatalf("cancel running: HTTP %d, want 409", code)
	}
	prompt, high := "new prompt", "high"
	if _, code, _ := e.c.patch(running, queuePatchRequest{Prompt: &prompt}); code != http.StatusConflict {
		t.Fatalf("patch running: HTTP %d, want 409", code)
	}
	it, code, _ := e.c.patch(queued, queuePatchRequest{Prompt: &prompt, Priority: &high})
	if code != http.StatusOK || it.Prompt != prompt || it.Priority != "high" ||
		!strings.Contains(it.History[len(it.History)-1].Note, "prompt") {
		t.Fatalf("patch queued = %+v HTTP %d", it, code)
	}
	if _, code, _ := e.c.cancel(queued); code != http.StatusOK || e.status(queued) != "cancelled" {
		t.Fatalf("cancel queued: HTTP %d status %s", code, e.status(queued))
	}
	if _, code, _ := e.c.cancel(queued); code != http.StatusConflict {
		t.Fatalf("cancel again: HTTP %d, want 409", code)
	}
}

// TestQueueCenterExecutorHTTP checks the center ↔ executor wire: the center
// pushes over POST /x/turns and the executor's reports land through the
// center routes.
func TestQueueCenterExecutorHTTP(t *testing.T) {
	e := newQCEnv(t, t.TempDir())
	mux := http.NewServeMux()
	e.c.register(mux)
	center := httptest.NewServer(queueBearerHandler(mux, "ct"))
	defer center.Close()
	var got queueTurnRequest
	execMux := http.NewServeMux()
	execMux.HandleFunc("POST /x/turns", func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&got)
		apiJSON(w, http.StatusAccepted, queueTurnReport{TurnID: "t_remote"})
	})
	exec := httptest.NewServer(queueBearerHandler(execMux, "et"))
	defer exec.Close()
	tokens := t.TempDir()
	os.WriteFile(filepath.Join(tokens, "et"), []byte("et\n"), 0600)
	os.WriteFile(filepath.Join(tokens, "ct"), []byte("ct\n"), 0600)
	remote, err := newQueueRemote(QueuePeerConfig{Name: qcLocal, URL: exec.URL, TokenFile: filepath.Join(tokens, "et")})
	if err != nil {
		t.Fatal(err)
	}
	e.c.startTurn = func(ctx context.Context, _ string, req queueTurnRequest) (queueTurnReport, int, error) {
		return remote.startTurn(ctx, req)
	}
	id := e.mustEnqueue("X1", func(r *queueEnqueueRequest) { r.Visible = &queueVisible{PostToChat: true} })
	e.c.step(context.Background())
	if got.ItemID != id || !got.Visible || e.c.lookup(id).TurnID != "t_remote" {
		t.Fatalf("pushed %+v, item %+v", got, e.c.lookup(id))
	}
	back, err := newQueueRemote(QueuePeerConfig{URL: center.URL, TokenFile: filepath.Join(tokens, "ct")})
	if err != nil {
		t.Fatal(err)
	}
	if err := back.reportTurn(context.Background(), id, "finished", queueTurnReport{Status: "done", Tail: "remote tail"}); err != nil {
		t.Fatal(err)
	}
	if it := e.c.lookup(id); it.Status != "done" || it.Tail != "remote tail" {
		t.Fatalf("after remote finish = %+v", it)
	}
	var gone queueReportGone
	if err := back.reportTurn(context.Background(), "q_unknown", "finished", queueTurnReport{Status: "done"}); !errors.As(err, &gone) {
		t.Fatalf("report for unknown item = %v, want permanent refusal", err)
	}
}

// TestQueueCenterWakeOncePerEvent: the same finish event never queues two
// completion turns, even if the wake path runs twice.
func TestQueueCenterWakeOncePerEvent(t *testing.T) {
	e := newQCEnv(t, t.TempDir())
	id := e.mustEnqueue("O1", withWake)
	e.c.step(context.Background())
	e.c.mu.Lock()
	it := e.c.items[id].clone()
	it.Status = queueStatusDone
	e.c.wakeLocked(it, "")
	e.c.wakeLocked(it, "")
	e.c.mu.Unlock()
	if got := len(e.completions()); got != 1 {
		t.Fatalf("completions for one event = %d, want 1", got)
	}
}
