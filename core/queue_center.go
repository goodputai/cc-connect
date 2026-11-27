package core

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	queueLaneLimit       = 20
	queueTurnTimeout     = time.Hour
	queueDispatchTimeout = 5 * time.Minute
	queueWakeLimit       = 12
	queueWakeWindow      = time.Hour
	queueExecutorGrace   = 2 * queuePollInterval

	queueStatusQueued    = "待办"
	queueStatusRunning   = "进行中"
	queueStatusDone      = "完成"
	queueStatusCancelled = "取消"

	queueReportDone   = "done"
	queueReportFailed = "failed"

	queueResultDelivered = "交付"
	queueResultBlocked   = "阻塞"

	queueKindOrder      = "order"
	queueKindCompletion = "completion"
)

var queueZone = time.FixedZone("+08:00", 8*3600)

type queueSource struct {
	Instance   string `json:"instance"`
	Project    string `json:"project"`
	SessionKey string `json:"session_key"`
	Agent      string `json:"agent,omitempty"`
}

type queueVisible struct {
	PostToChat bool `json:"post_to_chat"`
}

type queueEvent struct {
	At     time.Time `json:"at"`
	Status string    `json:"status"`
	Note   string    `json:"note,omitempty"`
}

type queueItem struct {
	ID                 string        `json:"id"`
	IdempotencyKey     string        `json:"idempotency_key"`
	Kind               string        `json:"kind"`
	Instance           string        `json:"instance,omitempty"`
	Project            string        `json:"project,omitempty"`
	SessionKey         string        `json:"session_key,omitempty"`
	NewSession         bool          `json:"new_session"`
	Executor           *queueSource  `json:"executor"`
	Priority           string        `json:"priority"`
	Prompt             string        `json:"prompt"`
	Source             *queueSource  `json:"source,omitempty"`
	WakeSourceOnFinish bool          `json:"wake_source_on_finish"`
	Visible            *queueVisible `json:"visible,omitempty"`
	Title              string        `json:"title,omitempty"`
	Line               string        `json:"line,omitempty"`
	Board              string        `json:"board,omitempty"`
	Owner              string        `json:"owner,omitempty"`
	RegisteredBy       string        `json:"by,omitempty"`
	SourceRef          string        `json:"source_ref,omitempty"`
	Due                string        `json:"due,omitempty"`
	WaitsOn            *queueWaits   `json:"waits_on,omitempty"`
	WaitsSince         *time.Time    `json:"waits_since,omitempty"`
	Evidence           []string      `json:"evidence,omitempty"`
	LastError          string        `json:"last_error,omitempty"`
	Result             string        `json:"result,omitempty"`
	Follows            string        `json:"follows,omitempty"`
	ResolvedBy         string        `json:"resolved_by,omitempty"`
	Status             string        `json:"state"`
	Reason             string        `json:"reason,omitempty"`
	Seq                int64         `json:"seq"`
	Revision           uint64        `json:"revision"`
	CreatedAt          time.Time     `json:"created_at"`
	DispatchedAt       *time.Time    `json:"dispatched_at,omitempty"`
	StartedAt          *time.Time    `json:"started_at,omitempty"`
	FinishedAt         *time.Time    `json:"finished_at,omitempty"`
	TurnID             string        `json:"turn_id,omitempty"`
	Attempt            string        `json:"attempt,omitempty"`
	Tail               string        `json:"tail,omitempty"`
	ReplyMessageID     string        `json:"reply_message_id,omitempty"`
	WakeChain          string        `json:"wake_chain,omitempty"`
	History            []queueEvent  `json:"history"`
}

// Keep the M2 response key available to existing clients while both keys
// expose the same four states.
func (it *queueItem) MarshalJSON() ([]byte, error) {
	type item queueItem
	return json.Marshal(struct {
		*item
		Status string `json:"status"`
	}{(*item)(it), it.Status})
}

func (it *queueItem) lane() string {
	return it.Instance + "\x00" + it.Project + "\x00" + it.SessionKey
}

func (it *queueItem) note(at time.Time, status, note string) {
	it.History = append(it.History, queueEvent{At: at, Status: status, Note: note})
}

func (it *queueItem) clone() *queueItem {
	// All referenced values are treated as immutable. Copy the two slices
	// that a state transition can append to.
	c := *it
	c.History = append([]queueEvent(nil), it.History...)
	c.Evidence = append([]string(nil), it.Evidence...)
	return &c
}

func (it *queueItem) assign(executor *queueSource) {
	it.Executor = executor
	if executor != nil {
		it.Instance, it.Project, it.SessionKey = executor.Instance, executor.Project, executor.SessionKey
	}
}

type queueEnqueueRequest struct {
	IdempotencyKey     string        `json:"idempotency_key"`
	Instance           string        `json:"instance"`
	Project            string        `json:"project"`
	SessionKey         string        `json:"session_key"`
	Executor           *queueSource  `json:"executor"`
	NewSession         bool          `json:"new_session"`
	Priority           string        `json:"priority"`
	Prompt             string        `json:"prompt"`
	Source             *queueSource  `json:"source"`
	WakeSourceOnFinish bool          `json:"wake_source_on_finish"`
	Board              *queueBoard   `json:"board"`
	Visible            *queueVisible `json:"visible"`
	Title              string        `json:"title"`
	Line               string        `json:"line"`
	Due                string        `json:"due"`
	Owner              string        `json:"owner"`
	RegisteredBy       string        `json:"by"`
	SourceRef          string        `json:"source_ref"`
	WaitsOn            *queueWaits   `json:"waits_on"`
	Evidence           []string      `json:"evidence"`
	Follows            string        `json:"follows"`
}

type queueEnqueueResponse struct {
	ID       string `json:"id"`
	Status   string `json:"status"`
	Position int    `json:"position"`
}

func (r queueEnqueueResponse) MarshalJSON() ([]byte, error) {
	type response queueEnqueueResponse
	return json.Marshal(struct {
		response
		State string `json:"state"`
	}{response(r), r.Status})
}

// queueView is the center's latest knowledge of one instance's sessions.
// FetchedAt is when the request that produced Sessions was sent.
type queueView struct {
	Known, Reachable bool
	FetchedAt        *time.Time
	Sessions         []queueSession
	UnreachableSince *time.Time
}

func (v queueView) session(project, key string) (queueSession, bool) {
	for _, row := range v.Sessions {
		if row.Project == project && row.SessionKey == key {
			return row, true
		}
	}
	return queueSession{}, false
}

type queueStartTurn func(context.Context, string, queueTurnRequest) (queueTurnReport, int, error)

// queueReportGone tells an executor that retrying a report cannot succeed.
type queueReportGone struct{ status int }

func (e queueReportGone) Error() string {
	return fmt.Sprintf("center refused report with HTTP %d", e.status)
}

type queueCenter struct {
	dir             string
	now             func() time.Time
	turnTimeout     time.Duration
	dispatchTimeout time.Duration
	tick            time.Duration
	views           func() map[string]queueView
	startTurn       queueStartTurn
	halt            func(string) (bool, error)
	notify          func(sessionKey, text string)
	spawn           func(func())
	kick            chan struct{}

	mu       sync.Mutex
	seq      int64
	items    map[string]*queueItem
	keys     map[string]string
	wakes    map[string][]time.Time
	inflight map[string]bool
}

func newQueueCenter(dir string) (*queueCenter, error) {
	c := &queueCenter{
		dir:             dir,
		now:             func() time.Time { return time.Now().In(queueZone) },
		turnTimeout:     queueTurnTimeout,
		dispatchTimeout: queueDispatchTimeout,
		tick:            queuePollInterval,
		halt:            queueGroupHalted,
		notify:          func(string, string) {},
		spawn:           func(f func()) { go f() },
		kick:            make(chan struct{}, 1),
		items:           make(map[string]*queueItem),
		keys:            make(map[string]string),
		wakes:           make(map[string][]time.Time),
		inflight:        make(map[string]bool),
	}
	if err := c.loadStore(); err != nil {
		return nil, fmt.Errorf("load queue store: %w", err)
	}
	return c, nil
}

func (c *queueCenter) signal() {
	select {
	case c.kick <- struct{}{}:
	default:
	}
}

func (c *queueCenter) lookup(id string) *queueItem {
	c.mu.Lock()
	defer c.mu.Unlock()
	if it := c.items[id]; it != nil {
		return it.clone()
	}
	return nil
}

func (c *queueCenter) laneOrderLocked(lane string) []*queueItem {
	var queued []*queueItem
	for _, it := range c.items {
		if it.Status == queueStatusQueued && it.Executor != nil && it.lane() == lane {
			queued = append(queued, it)
		}
	}
	sort.Slice(queued, func(i, j int) bool {
		if a, b := queued[i].Priority, queued[j].Priority; a != b {
			return a < b
		}
		return queued[i].Seq < queued[j].Seq
	})
	return queued
}

func (c *queueCenter) positionLocked(it *queueItem) int {
	if it.Executor != nil && it.Status == queueStatusQueued {
		for i, q := range c.laneOrderLocked(it.lane()) {
			if q.ID == it.ID {
				return i + 1
			}
		}
	}
	return 0
}

func (c *queueCenter) duplicateLocked(key string) (queueEnqueueResponse, bool) {
	if it := c.items[c.keys[key]]; it != nil {
		return queueEnqueueResponse{it.ID, it.Status, c.positionLocked(it)}, true
	}
	return queueEnqueueResponse{}, false
}

func (c *queueCenter) prepareLocked(it *queueItem) {
	it.ID = "q_" + strings.ToLower(rand.Text()[:20])
	it.Seq, it.Status, it.CreatedAt = c.seq+1, queueStatusQueued, c.now()
	it.Priority = queuePriority(it.Priority)
	if it.WaitsOn != nil && !it.WaitsOn.empty() {
		it.WaitsSince = &it.CreatedAt
	}
	it.note(it.CreatedAt, queueStatusQueued, "")
}

func queueTargetValid(src *queueSource) bool {
	return src != nil && src.Instance != "" && src.Project != "" && src.SessionKey != ""
}

func (c *queueCenter) checkTarget(src *queueSource) (int, error) {
	halted, err := c.halt(src.SessionKey)
	if err != nil {
		return http.StatusServiceUnavailable, errors.New("cannot check group stop marker")
	}
	if halted {
		return http.StatusLocked, errors.New("group is stopped")
	}
	view, known := c.views()[src.Instance]
	switch {
	case !known || !view.Known:
		return http.StatusNotFound, errors.New("instance not found")
	case !view.Reachable && view.FetchedAt == nil:
		return http.StatusServiceUnavailable, errors.New("executor has never been reachable")
	case view.Reachable:
		if _, ok := view.session(src.Project, src.SessionKey); !ok {
			return http.StatusNotFound, errors.New("session not found")
		}
	}
	return http.StatusOK, nil
}

func (c *queueCenter) enqueue(req queueEnqueueRequest) (queueEnqueueResponse, int, error) {
	req.Prompt = strings.TrimRight(req.Prompt, " \t\r\n")
	if req.Executor == nil && (req.Instance != "" || req.Project != "" || req.SessionKey != "") {
		req.Executor = &queueSource{Instance: req.Instance, Project: req.Project, SessionKey: req.SessionKey}
	}
	it := &queueItem{
		IdempotencyKey: req.IdempotencyKey, Kind: queueKindOrder,
		NewSession: req.NewSession, Priority: req.Priority, Prompt: req.Prompt,
		Source: req.Source, WakeSourceOnFinish: req.WakeSourceOnFinish, Visible: req.Visible,
		Title: req.Title, Line: req.Line, Due: req.Due, Owner: req.Owner, RegisteredBy: req.RegisteredBy,
		SourceRef: req.SourceRef, WaitsOn: req.WaitsOn, Evidence: queueEvidence(nil, req.Evidence, ""),
		Follows: req.Follows,
	}
	it.applyBoard(req.Board)
	it.assign(req.Executor)
	switch {
	case req.IdempotencyKey == "":
		return queueEnqueueResponse{}, http.StatusBadRequest, errors.New("idempotency_key is required")
	case strings.HasPrefix(req.IdempotencyKey, "cb:"):
		return queueEnqueueResponse{}, http.StatusBadRequest, errors.New("idempotency keys starting with cb: are reserved")
	case !validQueuePriority(req.Priority):
		return queueEnqueueResponse{}, http.StatusBadRequest, errors.New("priority must be P0, P1, P2 or P3")
	case it.Executor != nil && (!queueTargetValid(it.Executor) || strings.TrimSpace(it.Prompt) == ""):
		return queueEnqueueResponse{}, http.StatusBadRequest, errors.New("executor instance, project, session_key and prompt are required")
	case it.Executor == nil && strings.TrimSpace(it.Title) == "":
		return queueEnqueueResponse{}, http.StatusBadRequest, errors.New("unassigned items require a title")
	case req.WakeSourceOnFinish && !queueTargetValid(req.Source):
		return queueEnqueueResponse{}, http.StatusBadRequest, errors.New("wake_source_on_finish needs source instance, project and session_key")
	}
	if err := it.validateBoard(); err != nil {
		return queueEnqueueResponse{}, http.StatusBadRequest, err
	}

	c.mu.Lock()
	dup, ok := c.duplicateLocked(req.IdempotencyKey)
	c.mu.Unlock()
	if ok {
		return dup, http.StatusOK, nil
	}
	if it.Executor != nil {
		if code, err := c.checkTarget(it.Executor); err != nil {
			return queueEnqueueResponse{}, code, err
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if dup, ok := c.duplicateLocked(req.IdempotencyKey); ok {
		return dup, http.StatusOK, nil
	}
	if it.Executor != nil && len(c.laneOrderLocked(it.lane())) >= queueLaneLimit {
		return queueEnqueueResponse{}, http.StatusTooManyRequests, errors.New("session queue is full")
	}
	c.prepareLocked(it)
	changes := []*queueItem{it}
	if it.Follows != "" {
		prev := c.items[it.Follows]
		if prev == nil {
			return queueEnqueueResponse{}, http.StatusNotFound, errors.New("followed item not found")
		}
		if prev.Status != queueStatusDone || prev.Result != queueResultBlocked || prev.ResolvedBy != "" {
			return queueEnqueueResponse{}, http.StatusConflict, errors.New("follows requires an unresolved completed blocker")
		}
		prev = prev.clone()
		prev.ResolvedBy = it.ID
		prev.note(c.now(), prev.Status, "resolved by "+it.ID)
		changes = append(changes, prev)
	}
	if err := c.commitLocked("enqueued", changes...); err != nil {
		slog.Error("queue enqueue not persisted", "error", err)
		return queueEnqueueResponse{}, http.StatusServiceUnavailable, errors.New("queue state not persisted")
	}
	c.signal()
	return queueEnqueueResponse{it.ID, it.Status, c.positionLocked(it)}, http.StatusCreated, nil
}

func pruneQueueWakes(times []time.Time, now time.Time) []time.Time {
	kept := times[:0:0]
	for _, at := range times {
		if now.Sub(at) < queueWakeWindow {
			kept = append(kept, at)
		}
	}
	return kept
}

// Persist the outcome and its completion receipt together. Replaying a
// report or recovering the center can then neither lose a wake nor duplicate it.
func (c *queueCenter) finishLocked(it *queueItem, note string, wake bool) error {
	changes := []*queueItem{it}
	var cb *queueItem
	var suppressed string
	src := it.Source
	now := c.now()
	if wake && it.Kind != queueKindCompletion && queueTargetValid(src) {
		key := "cb:" + it.ID
		if it.Attempt != "" {
			key += ":" + it.Attempt
		}
		if _, dup := c.keys[key]; !dup {
			chain := src.Instance + "\x00" + src.Project + "\x00" + src.SessionKey + "\x00>" + it.lane()
			recent := pruneQueueWakes(c.wakes[chain], now)
			if len(recent) < queueWakeLimit {
				cb = &queueItem{IdempotencyKey: key, Kind: queueKindCompletion,
					Priority: "P2", Prompt: queueCompletionPrompt(it), WakeChain: chain}
				cb.assign(src)
				c.prepareLocked(cb)
				changes = append(changes, cb)
			} else {
				it.note(now, it.Status, "wake_suppressed")
				suppressed = fmt.Sprintf("【队列回执暂停】令 %s 已到 %s，本派令链 1 小时内已叫醒 %d 次。详情：GET /v1/queue/items/%s",
					it.ID, it.Status, queueWakeLimit, it.ID)
			}
		}
	}
	if err := c.commitLocked(note, changes...); err != nil {
		return err
	}
	if cb != nil {
		c.wakes[cb.WakeChain] = append(pruneQueueWakes(c.wakes[cb.WakeChain], now), now)
	}
	if suppressed != "" {
		notify, sessionKey := c.notify, src.SessionKey
		c.spawn(func() { notify(sessionKey, suppressed) })
	}
	c.signal()
	return nil
}

func queueCompletionPrompt(it *queueItem) string {
	var b strings.Builder
	fmt.Fprintf(&b, "【队列回执】令 %s（%s）：%s", it.ID, it.IdempotencyKey, it.Status)
	if it.Result != "" {
		fmt.Fprintf(&b, " / %s", it.Result)
	}
	if it.LastError != "" {
		fmt.Fprintf(&b, "，原因 %s", it.LastError)
	}
	b.WriteString("。\n")
	fmt.Fprintf(&b, "目标：%s / %s / %s\n", it.Instance, it.Project, it.SessionKey)
	if it.Line != "" || it.Title != "" {
		fmt.Fprintf(&b, "看板：%s / %s\n", it.Line, it.Title)
	}
	if it.Result == queueResultBlocked && it.WaitsOn != nil {
		fmt.Fprintf(&b, "等待：%s\n", it.WaitsOn.Text)
	}
	if it.Tail != "" {
		fmt.Fprintf(&b, "回复末段：\n%s\n", it.Tail)
	}
	fmt.Fprintf(&b, "详情：GET /v1/queue/items/%s", it.ID)
	return b.String()
}

func (c *queueCenter) markStartedLocked(it *queueItem, turnID string) error {
	next, now := it.clone(), c.now()
	next.StartedAt, next.TurnID = &now, turnID
	next.note(now, queueStatusRunning, "started "+turnID)
	return c.commitLocked("started", next)
}

func (c *queueCenter) failedItem(it *queueItem, reason string) *queueItem {
	next := it.clone()
	next.Status, next.LastError, next.Reason = queueStatusQueued, reason, ""
	next.Executor, next.FinishedAt = nil, nil
	next.note(c.now(), next.Status, reason)
	return next
}

// The executor still reports done/failed on the M2 wire.
func (c *queueCenter) report(itemID, event string, body queueTurnReport) (int, error) {
	if event != "started" && event != "finished" {
		return http.StatusNotFound, errors.New("unknown report")
	}
	if event == "finished" && body.Status != queueReportDone && body.Status != queueReportFailed {
		return http.StatusBadRequest, errors.New("status must be done or failed")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	it := c.items[itemID]
	if it == nil {
		return http.StatusNotFound, errors.New("item not found")
	}
	if (body.Attempt != "" && body.Attempt != it.Attempt) ||
		(it.TurnID != "" && body.TurnID != "" && body.TurnID != it.TurnID) ||
		it.Status != queueStatusRunning || it.DispatchedAt == nil {
		// The turn may report late or retry an already-committed outcome.
		return http.StatusOK, nil
	}
	var err error
	switch {
	case event == "started":
		if it.StartedAt == nil {
			err = c.markStartedLocked(it, body.TurnID)
		}
	case body.Status == queueReportFailed:
		reason := strings.TrimSpace(body.Reason)
		if reason == "" {
			reason = "executor_failed"
		}
		next := c.failedItem(it, reason)
		next.Tail, next.ReplyMessageID = finalReplyTail(body.Tail), body.ReplyMessageID
		err = c.finishLocked(next, reason, true)
	default:
		next, now := it.clone(), c.now()
		next.Status, next.Result, next.FinishedAt = queueStatusDone, queueResultDelivered, &now
		if body.TurnID != "" {
			next.TurnID = body.TurnID
		}
		next.LastError, next.Reason = "", ""
		next.Tail, next.ReplyMessageID = finalReplyTail(body.Tail), body.ReplyMessageID
		next.WaitsOn, next.WaitsSince = nil, nil
		if waiting, blocked := strings.CutPrefix(body.FirstLine, "【阻塞】"); blocked {
			next.Result, next.WaitsOn, next.WaitsSince = queueResultBlocked, &queueWaits{Text: strings.TrimSpace(waiting)}, &now
		}
		evidence := next.Tail
		if evidence == "" {
			evidence = "执行端已上报正常结束"
		}
		next.Evidence = append(next.Evidence, evidence)
		next.note(now, next.Status, next.Result)
		err = c.finishLocked(next, "finished", true)
	}
	if err != nil {
		slog.Error("queue report not persisted", "item_id", itemID, "event", event, "error", err)
		return http.StatusServiceUnavailable, errors.New("report not persisted")
	}
	return http.StatusOK, nil
}

// step runs timeouts and lost-finish checks before dispatching the first
// eligible item in each lane.
func (c *queueCenter) step(ctx context.Context) {
	views := c.views()
	var launch []*queueItem
	c.mu.Lock()
	now := c.now()
	items := make([]*queueItem, 0, len(c.items))
	for _, it := range c.items {
		items = append(items, it)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Seq < items[j].Seq })
	busy := make(map[string]bool)
	for _, it := range items {
		if it.Status != queueStatusRunning || it.Executor == nil || it.DispatchedAt == nil {
			continue
		}
		if c.checkRunningLocked(it, views[it.Instance], now) {
			continue
		}
		busy[it.lane()] = true
		if it.StartedAt == nil && !c.inflight[it.ID] {
			view := views[it.Instance]
			if !view.Reachable {
				continue
			}
			if row, ok := view.session(it.Project, it.SessionKey); ok && row.ItemID == it.ID {
				if err := c.markStartedLocked(it, ""); err != nil {
					slog.Error("queue start reconciliation not persisted", "item_id", it.ID, "error", err)
				}
				continue
			}
			launch = append(launch, it)
		}
	}
	for _, it := range items {
		if it.Status != queueStatusQueued || it.Executor == nil || busy[it.lane()] {
			continue
		}
		lane := it.lane()
		busy[lane] = true
		view := views[it.Instance]
		if !view.Reachable {
			continue
		}
		if halted, err := c.halt(it.SessionKey); err != nil || halted {
			continue
		}
		if row, ok := view.session(it.Project, it.SessionKey); ok && row.State != "idle" {
			continue
		}
		for _, head := range c.laneOrderLocked(lane) {
			if !c.dependenciesDoneLocked(head.WaitsOn) {
				continue
			}
			next := head.clone()
			next.Status, next.DispatchedAt, next.WaitsSince = queueStatusRunning, &now, nil
			next.StartedAt, next.TurnID, next.Attempt = nil, "", rand.Text()
			next.note(now, queueStatusRunning, "dispatched")
			if err := c.commitLocked("dispatched", next); err != nil {
				slog.Error("queue dispatch not persisted", "item_id", head.ID, "error", err)
			} else {
				launch = append(launch, next)
			}
			break
		}
	}
	for _, it := range launch {
		c.inflight[it.ID] = true
	}
	c.mu.Unlock()
	for _, it := range launch {
		c.spawn(func() { c.dispatch(ctx, it) })
	}
}

func (c *queueCenter) checkRunningLocked(it *queueItem, view queueView, now time.Time) bool {
	var reason string
	if it.StartedAt == nil {
		if now.Sub(*it.DispatchedAt) >= c.dispatchTimeout && !c.inflight[it.ID] {
			reason = "dispatch_failed"
		}
	} else {
		if now.Sub(*it.StartedAt) >= c.turnTimeout {
			reason = "timed_out: no finished report within " + c.turnTimeout.String()
		}
		if view.Reachable && view.FetchedAt != nil && view.FetchedAt.After(*it.StartedAt) {
			if row, ok := view.session(it.Project, it.SessionKey); !ok || row.ItemID != it.ID {
				reason = "lost-finish"
			}
		}
		if !view.Reachable && view.UnreachableSince != nil &&
			now.Sub(*view.UnreachableSince) >= queueExecutorGrace {
			reason = "lost-finish: executor unreachable"
		}
	}
	return reason != "" && c.persistFailureLocked(it, reason)
}

func (c *queueCenter) persistFailureLocked(it *queueItem, reason string) bool {
	if err := c.finishLocked(c.failedItem(it, reason), reason, true); err != nil {
		slog.Error("queue failure not persisted", "item_id", it.ID, "error", err)
		return false
	}
	return true
}

func (c *queueCenter) dispatch(ctx context.Context, snapshot *queueItem) {
	req := queueTurnRequest{ItemID: snapshot.ID, Attempt: snapshot.Attempt, Project: snapshot.Project, SessionKey: snapshot.SessionKey,
		NewSession: snapshot.NewSession, Prompt: snapshot.Prompt, Visible: snapshot.Visible != nil && snapshot.Visible.PostToChat}
	report, status, err := c.startTurn(ctx, snapshot.Instance, req)

	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.inflight, snapshot.ID)
	it := c.items[snapshot.ID]
	if it == nil || it.Status != queueStatusRunning || it.StartedAt != nil || it.Attempt != snapshot.Attempt {
		return
	}
	switch {
	case err == nil:
		if err := c.markStartedLocked(it, report.TurnID); err != nil {
			slog.Error("queue start not persisted", "item_id", it.ID, "error", err)
		}
	case status == http.StatusConflict || status == http.StatusLocked:
		next, now := it.clone(), c.now()
		next.Status, next.DispatchedAt = queueStatusQueued, nil
		next.note(now, queueStatusQueued, fmt.Sprintf("requeued: executor HTTP %d", status))
		if err := c.commitLocked("executor busy or stopped", next); err != nil {
			slog.Error("queue requeue not persisted", "item_id", it.ID, "error", err)
		}
	case status == http.StatusNotFound || status == http.StatusBadRequest:
		c.persistFailureLocked(it, fmt.Sprintf("rejected by executor: %v", err))
	default:
		// Unknown outcomes are retried with the same attempt until the
		// dispatch deadline. The execution endpoint deduplicates the request.
		slog.Warn("queue dispatch pending retry", "item_id", it.ID, "status", status, "error", err)
	}
}

func (c *queueCenter) run(ctx context.Context) {
	ticker := time.NewTicker(c.tick)
	defer ticker.Stop()
	for {
		c.step(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-c.kick:
		}
	}
}

func (c *queueCenter) applyDepth(nodes []queueFleetNode) {
	c.mu.Lock()
	defer c.mu.Unlock()
	depth := make(map[string]int)
	for _, it := range c.items {
		if it.Status == queueStatusQueued && it.Executor != nil {
			depth[it.lane()]++
		}
	}
	for i := range nodes {
		for j := range nodes[i].Sessions {
			row := &nodes[i].Sessions[j]
			row.QueueDepth = depth[nodes[i].Instance+"\x00"+row.Project+"\x00"+row.SessionKey]
		}
	}
}
