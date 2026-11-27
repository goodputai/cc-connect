package core

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func setQueueState(state, evidence string) queuePatchRequest {
	return queuePatchRequest{State: state, Evidence: []string{evidence}}
}

func requireQueuePatch(t *testing.T, c *queueCenter, id string, req queuePatchRequest, want int) *queueItem {
	t.Helper()
	got, code, err := c.patch(id, req)
	if code != want {
		t.Fatalf("patch %s: HTTP %d %v, want %d", id, code, err, want)
	}
	return got
}

func TestQueueBoardStatesAndDependencies(t *testing.T) {
	env := newQCEnv(t, t.TempDir())
	ctx := context.Background()
	decision := env.mustEnqueue("decision", func(r *queueEnqueueRequest) {
		r.Instance, r.Project, r.SessionKey, r.Title, r.Prompt = "", "", "", "User decision", ""
	})
	runnable := env.mustEnqueue("depends", func(r *queueEnqueueRequest) { r.WaitsOn = &queueWaits{IDs: []string{decision}} })
	env.c.step(ctx)
	if sent := env.sentIDs(); len(sent) != 0 {
		t.Fatalf("unassigned and dependent items must stay pending: %v", sent)
	}
	requireQueuePatch(t, env.c, decision, setQueueState(queueStatusDone, ""), http.StatusBadRequest)
	requireQueuePatch(t, env.c, runnable, setQueueState(queueStatusCancelled, ""), http.StatusBadRequest)
	requireQueuePatch(t, env.c, decision, queuePatchRequest{Evidence: []string{"proposal"}}, http.StatusOK)
	requireQueuePatch(t, env.c, decision, setQueueState(queueStatusRunning, ""), http.StatusOK)
	done := requireQueuePatch(t, env.c, decision, setQueueState(queueStatusDone, "approved"), http.StatusOK)
	if !reflect.DeepEqual(done.Evidence, []string{"proposal", "approved"}) {
		t.Fatalf("evidence did not append: %v", done.Evidence)
	}
	requireQueuePatch(t, env.c, decision, setQueueState(queueStatusQueued, ""), http.StatusConflict)
	requireQueuePatch(t, env.c, decision, queuePatchRequest{Evidence: []string{"late edit"}}, http.StatusConflict)
	env.c.step(ctx)
	if got := env.sentIDs(); !reflect.DeepEqual(got, []string{runnable}) {
		t.Fatalf("dependency completed, dispatches = %v", got)
	}
	cancel := env.mustEnqueue("cancel")
	it, code, err := env.c.cancel(cancel, "no longer needed")
	if code != http.StatusOK || it.Status != queueStatusCancelled || it.Reason != "no longer needed" {
		t.Fatalf("cancel: HTTP %d %v item %+v", code, err, it)
	}
	requireQueuePatch(t, env.c, cancel, setQueueState(queueStatusQueued, ""), http.StatusConflict)
}

func TestQueueBoardFailureRequiresReassignment(t *testing.T) {
	dir := t.TempDir()
	env := newQCEnv(t, dir)
	ctx := context.Background()
	id := env.mustEnqueue("assigned", withWake)
	env.c.step(ctx)
	before := env.c.lookup(id)
	fail := queueTurnReport{Attempt: before.Attempt, TurnID: before.TurnID, Status: queueReportFailed, Reason: "quota exhausted"}
	if code, err := env.c.report(id, "finished", fail); code != http.StatusOK {
		t.Fatalf("report failed attempt: HTTP %d %v", code, err)
	}
	restarted := newQCEnv(t, dir)
	restarted.c.step(ctx)
	got := restarted.c.lookup(id)
	if got.Status != queueStatusQueued || got.Executor != nil || got.LastError != "quota exhausted" {
		t.Fatalf("failed attempt after restart: %+v", got)
	}
	sent := restarted.sentIDs()
	if len(sent) != 1 || sent[0] == id {
		t.Fatalf("only the receipt may be dispatched after failure: %v", sent)
	}
	assignment, err := json.Marshal(queueSource{Instance: qcLocal, Project: qcProject, SessionKey: qcKey})
	if err != nil {
		t.Fatal(err)
	}
	requireQueuePatch(t, restarted.c, id, queuePatchRequest{Executor: assignment}, http.StatusOK)
	restarted.c.step(ctx)
	again := restarted.c.lookup(id)
	if again.Status != queueStatusRunning || again.Attempt == before.Attempt {
		t.Fatalf("assignment did not open a new attempt: %+v", again)
	}
	if code, err := restarted.c.report(id, "finished", fail); code != http.StatusOK ||
		restarted.c.lookup(id).Status != queueStatusRunning {
		t.Fatalf("late report changed reassigned item: HTTP %d %v, item %+v", code, err, restarted.c.lookup(id))
	}
}

func TestQueueBoardWritesOnlyChangedItem(t *testing.T) {
	dir := t.TempDir()
	env := newQCEnv(t, dir)
	first := env.mustEnqueue("first")
	second := env.mustEnqueue("second")
	a, b := env.c.itemPath(first), env.c.itemPath(second)
	aBefore, err := os.Stat(a)
	if err != nil {
		t.Fatal(err)
	}
	bBefore, err := os.Stat(b)
	if err != nil {
		t.Fatal(err)
	}
	logBefore, err := os.ReadFile(env.c.eventsPath())
	if err != nil {
		t.Fatal(err)
	}
	requireQueuePatch(t, env.c, first, queuePatchRequest{Evidence: []string{"changed only first"}}, http.StatusOK)
	aAfter, err := os.Stat(a)
	if err != nil {
		t.Fatal(err)
	}
	bAfter, err := os.Stat(b)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(aBefore, aAfter) || !os.SameFile(bBefore, bAfter) {
		t.Fatal("the update must atomically replace the changed item file alone")
	}
	if aAfter.Mode().Perm() != 0600 || bAfter.Mode().Perm() != 0600 {
		t.Fatalf("unexpected item modes %v / %v", aAfter.Mode(), bAfter.Mode())
	}
	logAfter, err := os.ReadFile(env.c.eventsPath())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(logAfter, logBefore) || bytes.Count(logAfter[len(logBefore):], []byte("\n")) != 1 {
		t.Fatal("update did not append exactly one event")
	}

	// A crash after the log append and before the snapshot rename must
	// restore the changed item without rewriting any unrelated item.
	if err := os.Remove(a); err != nil {
		t.Fatal(err)
	}
	restarted := newQCEnv(t, dir)
	if got := restarted.c.lookup(first); !reflect.DeepEqual(got.Evidence, []string{"changed only first"}) {
		t.Fatalf("missing snapshot not recovered from journal: %+v", got)
	}
	bRecovered, err := os.Stat(b)
	if err != nil || !os.SameFile(bBefore, bRecovered) {
		t.Fatalf("unrelated snapshot changed during replay: %v", err)
	}
}

func TestQueueBoardBottleneckPriorityTransitiveAndCycles(t *testing.T) {
	env := newQCEnv(t, t.TempDir())
	root := env.mustEnqueue("root")
	left := env.mustEnqueue("left", func(r *queueEnqueueRequest) { r.WaitsOn = &queueWaits{IDs: []string{root}} })
	right := env.mustEnqueue("right", func(r *queueEnqueueRequest) { r.WaitsOn = &queueWaits{IDs: []string{left}} })
	tip := env.mustEnqueue("tip", func(r *queueEnqueueRequest) { r.WaitsOn = &queueWaits{IDs: []string{right}} })
	wait, err := json.Marshal([]string{root, right})
	if err != nil {
		t.Fatal(err)
	}
	requireQueuePatch(t, env.c, left, queuePatchRequest{WaitsOn: wait}, http.StatusOK)
	env.advance(time.Minute)
	wait, err = json.Marshal([]string{left})
	if err != nil {
		t.Fatal(err)
	}
	requireQueuePatch(t, env.c, right, queuePatchRequest{WaitsOn: json.RawMessage("null")}, http.StatusOK)
	requireQueuePatch(t, env.c, right, queuePatchRequest{WaitsOn: wait}, http.StatusOK)
	urgent := env.mustEnqueue("urgent", func(r *queueEnqueueRequest) { r.Priority = "P0" })
	env.advance(time.Minute)
	items, err := env.c.bottleneck(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 5 || items[0].Item.ID != urgent || items[1].Item.ID != root ||
		items[2].Item.ID != left || items[3].Item.ID != right {
		t.Fatalf("wrong bottleneck order: %+v", items)
	}
	dependents := items[1].DependentIDs
	if len(dependents) != 3 || !strings.Contains(strings.Join(dependents, ","), tip) ||
		len(items[2].DependentIDs) != 2 || items[2].WaitSeconds <= items[3].WaitSeconds {
		t.Fatalf("transitive dependencies or wait age are wrong: %+v", items)
	}
}

func TestQueueBoardImportsLegacyOnce(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	legacy := `{"seq":8,"items":[{"id":"q_queued","seq":7,"status":"queued","instance":"vps","project":"proj","session_key":"feishu:oc_target:ou_x","idempotency_key":"old","kind":"order","prompt":"keep me","priority":"high","board":{"line":"merge","item":"Old title"}}]}`
	archive := `{"id":"q_archived","seq":8,"status":"done","idempotency_key":"old-done","kind":"order","tail":"old evidence","finished_at":"2026-11-27T00:02:00+08:00"}` + "\n"
	if err := os.WriteFile(path, []byte(legacy), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "archive-2026-11-27.jsonl"), []byte(archive), 0600); err != nil {
		t.Fatal(err)
	}
	env := newQCEnv(t, dir)
	if got := env.c.lookup("q_queued"); got == nil || got.Status != queueStatusQueued || got.Executor == nil ||
		got.Priority != "P0" || got.Line != "merge" || got.Title != "Old title" {
		t.Fatalf("queued item was not imported: %+v", got)
	}
	if got := env.c.lookup("q_archived"); got == nil || got.Status != queueStatusDone ||
		!reflect.DeepEqual(got.Evidence, []string{"old evidence"}) {
		t.Fatalf("archive item was not imported: %+v", got)
	}
	if _, err := os.Stat(path + ".imported"); err != nil {
		t.Fatalf("legacy file was not marked imported: %v", err)
	}
	env.c.step(context.Background())
	if got := env.sentIDs(); !reflect.DeepEqual(got, []string{"q_queued"}) {
		t.Fatalf("legacy item not dispatched: %v", got)
	}
	env.finish("q_queued", queueReportDone, "after migration")
	restarted := newQCEnv(t, dir)
	if got := restarted.c.lookup("q_queued"); got.Status != queueStatusDone || got.Tail != "after migration" {
		t.Fatalf("legacy file replaced the migrated item: %+v", got)
	}
}

func TestQueueBoardOnlyFirstLineMarksBlockerAndFollowsResolves(t *testing.T) {
	dir := t.TempDir()
	env := newQCEnv(t, dir)
	ctx := context.Background()
	board := func(r *queueEnqueueRequest) { r.Board = &queueBoard{Name: "A"}; r.Line = "queue" }
	blocked := env.mustEnqueue("blocked", board, func(r *queueEnqueueRequest) {
		r.Source = &queueSource{Instance: qcLocal, Project: qcProject, SessionKey: qcSrcKey}
	})
	normal := env.mustEnqueue("normal", board)
	env.c.step(ctx)
	report := func(id, content string) {
		t.Helper()
		obs := queueTurnObservation{ended: make(chan queueTurnReport, 1)}
		obs.finish(queueReportDone, "", content)
		if code, err := env.c.report(id, "finished", <-obs.ended); code != http.StatusOK {
			t.Fatalf("finish %s: HTTP %d %v", id, code, err)
		}
		env.idle(qcProject, qcKey)
	}
	report(blocked, "【阻塞】 等用户定范围\r\n"+strings.Repeat("尾", 315))
	env.c.step(ctx)
	report(normal, "【交付】完成\n【阻塞】只是引用，不能判成阻塞")
	if got := env.c.lookup(blocked); got.Status != queueStatusDone || got.Result != queueResultBlocked ||
		got.WaitsOn.Text != "等用户定范围" || got.WaitsSince == nil ||
		!reflect.DeepEqual(got.Evidence, []string{strings.Repeat("尾", 300)}) {
		t.Fatalf("blocked reply: %+v", got)
	}
	if got := env.c.lookup(normal); got.Status != queueStatusDone || got.Result != queueResultDelivered || got.WaitsOn != nil {
		t.Fatalf("body text was misclassified: %+v", got)
	}
	if wakes := env.completions(); len(wakes) != 1 || !strings.Contains(wakes[0].Prompt, "等用户定范围") {
		t.Fatalf("blocked item did not wake its issuer: %v", wakes)
	}
	env.advance(8 * time.Minute)
	q := map[string][]string{"board": {"A"}, "line": {"queue"}, "state": {queueStatusDone}}
	items, err := env.c.bottleneck(q)
	if err != nil || len(items) != 1 || items[0].Item.ID != blocked || items[0].WaitSeconds != 480 {
		t.Fatalf("unresolved blockers = %+v, %v", items, err)
	}
	list, err := env.c.list(q)
	if err != nil || len(list) != 2 {
		t.Fatalf("completed items filtered by board, line, state = %v, %v", list, err)
	}
	follow := env.mustEnqueue("follow", board, func(r *queueEnqueueRequest) { r.Follows = blocked })
	restarted := newQCEnv(t, dir)
	if got := restarted.c.lookup(blocked); got.Status != queueStatusDone || got.ResolvedBy != follow {
		t.Fatalf("follows did not resolve completed blocker: %+v", got)
	}
	if got := restarted.c.lookup(follow); got.Follows != blocked {
		t.Fatalf("successor lost follows: %+v", got)
	}
	items, err = restarted.c.bottleneck(q)
	if err != nil || len(items) != 0 {
		t.Fatalf("resolved blocker still visible: %+v, %v", items, err)
	}
}
