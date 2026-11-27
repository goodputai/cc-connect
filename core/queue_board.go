package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The current board is a name. The M2 caller also sends an object containing
// the old board display metadata.
type queueBoard struct {
	Name string `json:"name"`
	Line string `json:"line"`
	Item string `json:"item"`
	Due  string `json:"due"`
}

func (b *queueBoard) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] == '"' {
		return json.Unmarshal(data, &b.Name)
	}
	type board queueBoard
	return json.Unmarshal(data, (*board)(b))
}

func (it *queueItem) applyBoard(board *queueBoard) {
	if board == nil {
		return
	}
	it.Board = board.Name
	if it.Title == "" {
		it.Title = board.Item
	}
	if it.Line == "" {
		it.Line = board.Line
	}
	if it.Due == "" {
		it.Due = board.Due
	}
}

// Waiting on queue entries is structured; a completed blocker records the
// literal explanation from the executor's first line.
type queueWaits struct {
	IDs  []string
	Text string
}

func (w *queueWaits) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] == '"' {
		return json.Unmarshal(data, &w.Text)
	}
	return json.Unmarshal(data, &w.IDs)
}

func (w *queueWaits) MarshalJSON() ([]byte, error) {
	if w.IDs == nil {
		return json.Marshal(w.Text)
	}
	return json.Marshal(w.IDs)
}

func (w *queueWaits) empty() bool {
	return w == nil || (w.Text == "" && len(w.IDs) == 0)
}

func (w *queueWaits) dependencies() []string {
	if w == nil || w.empty() {
		return nil
	}
	if w.IDs == nil {
		return []string{w.Text}
	}
	return w.IDs
}

func (c *queueCenter) dependenciesDoneLocked(w *queueWaits) bool {
	for _, id := range w.dependencies() {
		if dep := c.items[id]; dep == nil || dep.Status != queueStatusDone {
			return false
		}
	}
	return true
}

func validQueueState(s string) bool {
	switch s {
	case queueStatusQueued, queueStatusRunning, queueStatusDone, queueStatusCancelled:
		return true
	}
	return false
}

func queueStateFilter(s string) string {
	switch s {
	case "queued", "timed_out", "failed":
		return queueStatusQueued
	case "running":
		return queueStatusRunning
	case "done":
		return queueStatusDone
	case "cancelled":
		return queueStatusCancelled
	}
	return s
}

func validQueuePriority(s string) bool {
	switch s {
	case "", "high", "normal", "P0", "P1", "P2", "P3":
		return true
	}
	return false
}

func queuePriority(s string) string {
	switch s {
	case "high":
		return "P0"
	case "", "normal":
		return "P2"
	}
	return s
}

func queueDue(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if t, err := time.ParseInLocation("2006-01-02 15:04", s, queueZone); err == nil {
		return t, nil
	}
	if t, err := time.ParseInLocation("2006-01-02", s, queueZone); err == nil {
		return t, nil
	}
	return time.Time{}, errors.New("due must be RFC3339 or YYYY-MM-DD HH:MM (Beijing)")
}

func (it *queueItem) validateBoard() error {
	if it.Board != "" && it.Board != "A" && it.Board != "B" && it.Board != "DH" {
		return errors.New("board must be A, B or DH")
	}
	_, err := queueDue(it.Due)
	return err
}

func queueEvidence(existing, added []string, reason string) []string {
	for _, text := range added {
		if text = strings.TrimSpace(text); text != "" {
			existing = append(existing, text)
		}
	}
	if reason != "" {
		existing = append(existing, reason)
	}
	return existing
}

type queuePatchRequest struct {
	Prompt   *string         `json:"prompt"`
	Priority *string         `json:"priority"`
	Board    *queueBoard     `json:"board"`
	Line     *string         `json:"line"`
	Due      *string         `json:"due"`
	State    string          `json:"state"`
	Reason   string          `json:"reason"`
	Evidence []string        `json:"evidence"`
	WaitsOn  json.RawMessage `json:"waits_on"`
	Executor json.RawMessage `json:"executor"`
}

func (c *queueCenter) cancel(id, reason string) (*queueItem, int, error) {
	return c.patch(id, queuePatchRequest{State: queueStatusCancelled, Reason: reason})
}

func (c *queueCenter) patch(id string, req queuePatchRequest) (*queueItem, int, error) {
	bad := func(text string) (*queueItem, int, error) { return nil, http.StatusBadRequest, errors.New(text) }
	if req.Prompt != nil && strings.TrimSpace(*req.Prompt) == "" {
		return bad("prompt cannot be empty")
	}
	if req.Priority != nil && !validQueuePriority(*req.Priority) {
		return bad("priority must be P0, P1, P2 or P3")
	}
	if req.State != "" && !validQueueState(req.State) {
		return bad("unknown state")
	}
	reason := strings.TrimSpace(req.Reason)
	added := queueEvidence(nil, req.Evidence, "")
	if req.Due != nil && reason == "" {
		return bad("changing due requires a reason")
	}
	var waits *queueWaits
	if req.WaitsOn != nil && json.Unmarshal(req.WaitsOn, &waits) != nil {
		return bad("waits_on must be a string or a list of item IDs")
	}
	var executor *queueSource
	if req.Executor != nil {
		if err := json.Unmarshal(req.Executor, &executor); err != nil || (executor != nil && !queueTargetValid(executor)) {
			return bad("executor must have instance, project and session_key")
		}
		if executor != nil {
			if code, err := c.checkTarget(executor); err != nil {
				return nil, code, err
			}
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	it := c.items[id]
	if it == nil {
		return nil, http.StatusNotFound, errors.New("item not found")
	}
	conflict := func(text string) (*queueItem, int, error) { return nil, http.StatusConflict, errors.New(text) }
	if it.Status == queueStatusDone || it.Status == queueStatusCancelled {
		return conflict("finished items cannot be changed")
	}
	if it.Status == queueStatusRunning && it.DispatchedAt != nil &&
		(req.State != "" || req.Prompt != nil || req.Priority != nil || req.Executor != nil || req.WaitsOn != nil) {
		return conflict("turn is running; use /stop")
	}
	if req.State == queueStatusRunning && it.Status == queueStatusQueued && (it.Executor != nil || executor != nil) {
		return conflict("assigned items enter 进行中 through dispatch")
	}
	if (req.State == queueStatusDone || req.State == queueStatusCancelled) && reason == "" && len(added) == 0 {
		return bad("completion or cancellation requires evidence or a reason")
	}

	next, now := it.clone(), c.now()
	var changed []string
	if req.Prompt != nil && *req.Prompt != next.Prompt {
		next.Prompt = *req.Prompt
		changed = append(changed, "prompt")
	}
	if req.Priority != nil && queuePriority(*req.Priority) != next.Priority {
		next.Priority = queuePriority(*req.Priority)
		changed = append(changed, "priority")
	}
	if req.Board != nil {
		if req.Board.Name != "" {
			next.Board = req.Board.Name
		}
		if req.Board.Line != "" {
			next.Line = req.Board.Line
		}
		if req.Board.Item != "" {
			next.Title = req.Board.Item
		}
		if req.Board.Due != "" {
			if reason == "" {
				return bad("changing due requires a reason")
			}
			next.Due = req.Board.Due
		}
		changed = append(changed, "board")
	}
	if req.Line != nil {
		next.Line = *req.Line
		changed = append(changed, "line")
	}
	if req.Due != nil {
		next.Due = *req.Due
		changed = append(changed, "due")
	}
	if req.WaitsOn != nil {
		next.WaitsOn = waits
		if waits.empty() {
			next.WaitsSince = nil
		} else if next.WaitsSince == nil {
			next.WaitsSince = &now
		}
		changed = append(changed, "waits_on")
	}
	if req.Executor != nil {
		if next.Status != queueStatusQueued {
			return conflict("only pending items can be reassigned")
		}
		next.assign(executor)
		if executor != nil {
			if strings.TrimSpace(next.Prompt) == "" {
				return bad("an assigned item requires a prompt")
			}
			keepsLane := it.Executor != nil && it.lane() == next.lane()
			if !keepsLane && len(c.laneOrderLocked(next.lane())) >= queueLaneLimit {
				return nil, http.StatusTooManyRequests, errors.New("session queue is full")
			}
		}
		changed = append(changed, "executor")
	}
	if req.State != "" && req.State != next.Status {
		next.Status = req.State
		switch next.Status {
		case queueStatusRunning:
			next.StartedAt = &now
		case queueStatusQueued:
			next.StartedAt = nil
		case queueStatusDone:
			next.Result, next.FinishedAt, next.Reason = queueResultDelivered, &now, reason
		case queueStatusCancelled:
			next.FinishedAt, next.Reason = &now, reason
			if reason == "" {
				next.Reason = added[0]
			}
		}
		changed = append(changed, "state")
	}
	if reason != "" || len(added) != 0 {
		next.Evidence = queueEvidence(next.Evidence, added, reason)
		changed = append(changed, "evidence")
	}
	if err := next.validateBoard(); err != nil {
		return nil, http.StatusBadRequest, err
	}
	if len(changed) == 0 {
		return it.clone(), http.StatusOK, nil
	}
	note := "patched: " + strings.Join(changed, ", ")
	next.note(now, next.Status, note)
	if err := c.commitLocked(note, next); err != nil {
		return nil, http.StatusServiceUnavailable, fmt.Errorf("queue state not persisted: %w", err)
	}
	c.signal()
	return next.clone(), http.StatusOK, nil
}

type queueFilter struct {
	board, line, state, instance, sessionKey string
	limit                                    int
	after                                    int64
	since                                    time.Time
	open, overdue                            bool
}

func newQueueFilter(q map[string][]string, limit int) (queueFilter, error) {
	get := func(k string) string {
		if v := q[k]; len(v) > 0 {
			return v[0]
		}
		return ""
	}
	f := queueFilter{
		board: get("board"), line: get("line"), state: get("state"), limit: limit,
		instance: get("instance"), sessionKey: get("session_key"),
		open: get("open") == "true", overdue: get("overdue") == "true",
	}
	if f.state == "" {
		f.state = queueStateFilter(get("status"))
		if f.state == "all" {
			f.state = ""
		}
	}
	if f.state != "" && !validQueueState(f.state) {
		return f, errors.New("unknown state filter")
	}
	if s := get("since"); s != "" {
		var err error
		f.since, err = time.Parse(time.RFC3339, s)
		if err != nil {
			if f.since, err = time.ParseInLocation("2006-01-02", s, queueZone); err != nil {
				return f, errors.New("since must be RFC3339 or YYYY-MM-DD")
			}
		}
	}
	if s := get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 {
			return f, errors.New("limit must be a positive integer")
		}
		f.limit = min(n, 1000)
	}
	if s := get("after"); s != "" {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return f, errors.New("after must be an item seq")
		}
		f.after = n
	}
	return f, nil
}

func (f queueFilter) matches(it *queueItem, now time.Time) bool {
	if (f.board != "" && it.Board != f.board) || (f.line != "" && it.Line != f.line) ||
		(f.state != "" && it.Status != f.state) || (f.instance != "" && it.Instance != f.instance) ||
		(f.sessionKey != "" && it.SessionKey != f.sessionKey) || it.Seq <= f.after ||
		(!f.since.IsZero() && it.CreatedAt.Before(f.since) && (it.FinishedAt == nil || it.FinishedAt.Before(f.since))) {
		return false
	}
	if (f.open || f.overdue) && (it.Status == queueStatusDone || it.Status == queueStatusCancelled) {
		return false
	}
	if f.overdue {
		due, _ := queueDue(it.Due)
		return !due.IsZero() && due.Before(now)
	}
	return true
}

func (c *queueCenter) list(q map[string][]string) ([]*queueItem, error) {
	f, err := newQueueFilter(q, 200)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	result := make([]*queueItem, 0)
	for _, it := range c.items {
		if f.matches(it, now) {
			result = append(result, it.clone())
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Seq < result[j].Seq })
	return result[:min(len(result), f.limit)], nil
}

type queueBottleneck struct {
	Item         *queueItem `json:"item"`
	DependentIDs []string   `json:"dependent_ids"`
	WaitSeconds  int64      `json:"wait_seconds"`
	Reason       string     `json:"reason"`
}

func queueBottleneckCandidate(it *queueItem) bool {
	return it.Kind != queueKindCompletion && (it.Status == queueStatusQueued || it.Status == queueStatusRunning ||
		(it.Status == queueStatusDone && it.Result == queueResultBlocked && it.ResolvedBy == ""))
}

func (c *queueCenter) bottleneck(q map[string][]string) ([]queueBottleneck, error) {
	f, err := newQueueFilter(q, 10)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	reverse := make(map[string][]string)
	var candidates []*queueItem
	for _, it := range c.items {
		if !queueBottleneckCandidate(it) {
			continue
		}
		for _, dep := range it.WaitsOn.dependencies() {
			reverse[dep] = append(reverse[dep], it.ID)
		}
		if f.matches(it, now) {
			candidates = append(candidates, it)
		}
	}
	result := make([]queueBottleneck, 0, len(candidates))
	for _, it := range candidates {
		dependents := queueDependents(it.ID, reverse)
		since := it.WaitsSince
		if it.Result != queueResultBlocked {
			if it.Status == queueStatusRunning && it.StartedAt != nil && (since == nil || it.StartedAt.Before(*since)) {
				since = it.StartedAt
			}
			if due, err := queueDue(it.Due); err == nil && !due.IsZero() && due.Before(now) &&
				(since == nil || due.Before(*since)) {
				since = &due
			}
		}
		var seconds int64
		if since != nil && since.Before(now) {
			seconds = int64(now.Sub(*since) / time.Second)
		}
		reason := fmt.Sprintf("%s · 等待 %d 分钟 · %d 条依赖", it.Status, seconds/60, len(dependents))
		if !it.WaitsOn.empty() {
			reason += " · 等：" + strings.Join(it.WaitsOn.dependencies(), "、")
		}
		if it.LastError != "" {
			reason += " · " + it.LastError
		}
		result = append(result, queueBottleneck{it.clone(), dependents, seconds, reason})
	}
	sort.Slice(result, func(i, j int) bool {
		a, b := result[i], result[j]
		if a.Item.Priority != b.Item.Priority {
			return a.Item.Priority < b.Item.Priority
		}
		if len(a.DependentIDs) != len(b.DependentIDs) {
			return len(a.DependentIDs) > len(b.DependentIDs)
		}
		if a.WaitSeconds != b.WaitSeconds {
			return a.WaitSeconds > b.WaitSeconds
		}
		return a.Item.Seq < b.Item.Seq
	})
	return result[:min(len(result), f.limit)], nil
}

func queueDependents(id string, reverse map[string][]string) []string {
	seen := map[string]bool{id: true}
	stack := append([]string(nil), reverse[id]...)
	ids := make([]string, 0)
	for len(stack) > 0 {
		dep := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[dep] {
			continue
		}
		seen[dep] = true
		ids = append(ids, dep)
		stack = append(stack, reverse[dep]...)
	}
	sort.Strings(ids)
	return ids
}
