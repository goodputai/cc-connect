package core

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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
	queueKeyRetention    = 7 * 24 * time.Hour
	queueArchiveDays     = 30

	queueStatusQueued    = "queued"
	queueStatusRunning   = "running"
	queueStatusTimedOut  = "timed_out"
	queueStatusDone      = "done"
	queueStatusFailed    = "failed"
	queueStatusCancelled = "cancelled"

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

type queueBoard struct {
	Line string `json:"line,omitempty"`
	Item string `json:"item,omitempty"`
	Due  string `json:"due,omitempty"`
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
	Instance           string        `json:"instance"`
	Project            string        `json:"project"`
	SessionKey         string        `json:"session_key"`
	NewSession         bool          `json:"new_session"`
	Priority           string        `json:"priority"`
	Prompt             string        `json:"prompt"`
	Source             *queueSource  `json:"source,omitempty"`
	WakeSourceOnFinish bool          `json:"wake_source_on_finish"`
	Board              *queueBoard   `json:"board,omitempty"`
	Visible            *queueVisible `json:"visible,omitempty"`
	Status             string        `json:"status"`
	Reason             string        `json:"reason,omitempty"`
	Seq                int64         `json:"seq"`
	CreatedAt          time.Time     `json:"created_at"`
	DispatchedAt       *time.Time    `json:"dispatched_at,omitempty"`
	StartedAt          *time.Time    `json:"started_at,omitempty"`
	FinishedAt         *time.Time    `json:"finished_at,omitempty"`
	TurnID             string        `json:"turn_id,omitempty"`
	Tail               string        `json:"tail,omitempty"`
	ReplyMessageID     string        `json:"reply_message_id,omitempty"`
	History            []queueEvent  `json:"history"`
}

func (it *queueItem) lane() string {
	return it.Instance + "\x00" + it.Project + "\x00" + it.SessionKey
}

func (it *queueItem) note(at time.Time, status, note string) {
	it.History = append(it.History, queueEvent{At: at, Status: status, Note: note})
}

func (it *queueItem) clone() *queueItem {
	c := *it
	c.History = append([]queueEvent(nil), it.History...)
	if it.Source != nil {
		s := *it.Source
		c.Source = &s
	}
	if it.Board != nil {
		b := *it.Board
		c.Board = &b
	}
	if it.Visible != nil {
		v := *it.Visible
		c.Visible = &v
	}
	return &c
}

type queueEnqueueRequest struct {
	IdempotencyKey     string        `json:"idempotency_key"`
	Instance           string        `json:"instance"`
	Project            string        `json:"project"`
	SessionKey         string        `json:"session_key"`
	NewSession         bool          `json:"new_session"`
	Priority           string        `json:"priority"`
	Prompt             string        `json:"prompt"`
	Source             *queueSource  `json:"source"`
	WakeSourceOnFinish bool          `json:"wake_source_on_finish"`
	Board              *queueBoard   `json:"board"`
	Visible            *queueVisible `json:"visible"`
}

type queueEnqueueResponse struct {
	ID       string `json:"id"`
	Status   string `json:"status"`
	Position int    `json:"position"`
}

type queueKeyRef struct {
	ID     string    `json:"id"`
	Status string    `json:"status"`
	At     time.Time `json:"at"`
}

type queueDisk struct {
	Seq   int64                  `json:"seq"`
	Items []*queueItem           `json:"items"`
	Keys  map[string]queueKeyRef `json:"keys"`
	Wakes map[string][]time.Time `json:"wakes"`
}

// queueView is the center's latest knowledge of one instance's sessions.
// FetchedAt is when the request that produced Sessions was sent.
type queueView struct {
	Known, Reachable bool
	FetchedAt        *time.Time
	Sessions         []queueSession
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
	keys     map[string]queueKeyRef
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
		keys:            make(map[string]queueKeyRef),
		wakes:           make(map[string][]time.Time),
		inflight:        make(map[string]bool),
	}
	data, err := os.ReadFile(c.statePath())
	if errors.Is(err, os.ErrNotExist) {
		return c, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read queue state: %w", err)
	}
	var disk queueDisk
	if err := json.Unmarshal(data, &disk); err != nil {
		return nil, fmt.Errorf("decode queue state: %w", err)
	}
	c.seq = disk.Seq
	for _, it := range disk.Items {
		c.items[it.ID] = it
	}
	for k, ref := range disk.Keys {
		c.keys[k] = ref
	}
	for k, v := range disk.Wakes {
		c.wakes[k] = v
	}
	return c, nil
}

func (c *queueCenter) statePath() string { return filepath.Join(c.dir, "state.json") }

func (c *queueCenter) archivePath(day time.Time) string {
	return filepath.Join(c.dir, "archive-"+day.In(queueZone).Format("2006-01-02")+".jsonl")
}

func (c *queueCenter) signal() {
	select {
	case c.kick <- struct{}{}:
	default:
	}
}

// saveLocked writes the whole active state durably: tmp file, fsync, rename,
// directory fsync.
func (c *queueCenter) saveLocked() error {
	now := c.now()
	disk := queueDisk{Seq: c.seq, Items: make([]*queueItem, 0, len(c.items)), Keys: c.keys, Wakes: c.wakes}
	for k, ref := range c.keys {
		if _, active := c.items[ref.ID]; !active && now.Sub(ref.At) > queueKeyRetention {
			delete(c.keys, k)
		}
	}
	for k, times := range c.wakes {
		if kept := pruneQueueWakes(times, now); len(kept) == 0 {
			delete(c.wakes, k)
		} else {
			c.wakes[k] = kept
		}
	}
	for _, it := range c.items {
		disk.Items = append(disk.Items, it)
	}
	sort.Slice(disk.Items, func(i, j int) bool { return disk.Items[i].Seq < disk.Items[j].Seq })
	data, err := json.Marshal(disk)
	if err != nil {
		return err
	}
	return queueWriteAtomic(c.statePath(), data)
}

func queueWriteAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".queue-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(name, path)
	}
	if err == nil {
		var d *os.File
		if d, err = os.Open(dir); err == nil {
			err = d.Sync()
			d.Close()
		}
	}
	return err
}

func (c *queueCenter) appendArchive(it *queueItem) error {
	if err := os.MkdirAll(c.dir, 0700); err != nil {
		return err
	}
	data, err := json.Marshal(it)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(c.archivePath(*it.FinishedAt), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(append(data, '\n')); err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	return err
}

// readArchive returns archived items finished on or after since, the latest
// record per id winning.
func (c *queueCenter) readArchive(since time.Time) []*queueItem {
	today := c.now()
	if since.IsZero() || today.Sub(since) > queueArchiveDays*24*time.Hour {
		since = today.Add(-queueArchiveDays * 24 * time.Hour)
	}
	byID := make(map[string]*queueItem)
	for day := since.In(queueZone); !day.After(today.Add(24 * time.Hour)); day = day.Add(24 * time.Hour) {
		f, err := os.Open(c.archivePath(day))
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 64*1024), 4*queueBodyLimit)
		for scanner.Scan() {
			var it queueItem
			if json.Unmarshal(scanner.Bytes(), &it) == nil && it.ID != "" &&
				it.FinishedAt != nil && !it.FinishedAt.Before(since) {
				byID[it.ID] = &it
			}
		}
		f.Close()
	}
	result := make([]*queueItem, 0, len(byID))
	for _, it := range byID {
		result = append(result, it)
	}
	return result
}

func (c *queueCenter) lookup(id string) *queueItem {
	c.mu.Lock()
	if it := c.items[id]; it != nil {
		defer c.mu.Unlock()
		return it.clone()
	}
	c.mu.Unlock()
	for _, it := range c.readArchive(time.Time{}) {
		if it.ID == id {
			return it
		}
	}
	return nil
}

func (c *queueCenter) laneOrderLocked(lane string) []*queueItem {
	var queued []*queueItem
	for _, it := range c.items {
		if it.Status == queueStatusQueued && it.lane() == lane {
			queued = append(queued, it)
		}
	}
	sort.Slice(queued, func(i, j int) bool {
		hi, hj := queued[i].Priority == "high", queued[j].Priority == "high"
		if hi != hj {
			return hi
		}
		return queued[i].Seq < queued[j].Seq
	})
	return queued
}

func (c *queueCenter) positionLocked(it *queueItem) int {
	for i, q := range c.laneOrderLocked(it.lane()) {
		if q.ID == it.ID {
			return i + 1
		}
	}
	return 0
}

func (c *queueCenter) duplicateLocked(key string) (queueEnqueueResponse, bool) {
	ref, ok := c.keys[key]
	if !ok {
		return queueEnqueueResponse{}, false
	}
	if it := c.items[ref.ID]; it != nil {
		return queueEnqueueResponse{it.ID, it.Status, c.positionLocked(it)}, true
	}
	return queueEnqueueResponse{ID: ref.ID, Status: ref.Status}, true
}

// insertLocked adds an item and persists it before anyone is told it exists.
func (c *queueCenter) insertLocked(it *queueItem) error {
	c.seq++
	it.ID, it.Seq, it.Status, it.CreatedAt = "q_"+strings.ToLower(rand.Text()[:20]), c.seq, queueStatusQueued, c.now()
	if it.Priority == "" {
		it.Priority = "normal"
	}
	it.note(it.CreatedAt, queueStatusQueued, "")
	c.items[it.ID] = it
	c.keys[it.IdempotencyKey] = queueKeyRef{it.ID, it.Status, it.CreatedAt}
	if err := c.saveLocked(); err != nil {
		delete(c.items, it.ID)
		delete(c.keys, it.IdempotencyKey)
		c.seq--
		return err
	}
	c.signal()
	return nil
}

func (c *queueCenter) enqueue(req queueEnqueueRequest) (queueEnqueueResponse, int, error) {
	req.Prompt = strings.TrimRight(req.Prompt, " \t\r\n")
	switch {
	case req.IdempotencyKey == "" || req.Instance == "" || req.Project == "" || req.SessionKey == "" || strings.TrimSpace(req.Prompt) == "":
		return queueEnqueueResponse{}, http.StatusBadRequest, errors.New("idempotency_key, instance, project, session_key and prompt are required")
	case strings.HasPrefix(req.IdempotencyKey, "cb:"):
		return queueEnqueueResponse{}, http.StatusBadRequest, errors.New("idempotency keys starting with cb: are reserved")
	case req.Priority != "" && req.Priority != "normal" && req.Priority != "high":
		return queueEnqueueResponse{}, http.StatusBadRequest, errors.New("priority must be normal or high")
	case req.WakeSourceOnFinish && (req.Source == nil || req.Source.Instance == "" || req.Source.Project == "" || req.Source.SessionKey == ""):
		return queueEnqueueResponse{}, http.StatusBadRequest, errors.New("wake_source_on_finish needs source instance, project and session_key")
	}
	c.mu.Lock()
	dup, ok := c.duplicateLocked(req.IdempotencyKey)
	c.mu.Unlock()
	if ok {
		return dup, http.StatusOK, nil
	}
	halted, err := c.halt(req.SessionKey)
	if err != nil {
		return queueEnqueueResponse{}, http.StatusServiceUnavailable, errors.New("cannot check group stop marker")
	}
	if halted {
		return queueEnqueueResponse{}, http.StatusLocked, errors.New("group is stopped")
	}
	view, known := c.views()[req.Instance]
	switch {
	case !known || !view.Known:
		return queueEnqueueResponse{}, http.StatusNotFound, errors.New("instance not found")
	case !view.Reachable && view.FetchedAt == nil:
		return queueEnqueueResponse{}, http.StatusServiceUnavailable, errors.New("executor has never been reachable")
	case view.Reachable:
		if _, ok := view.session(req.Project, req.SessionKey); !ok {
			return queueEnqueueResponse{}, http.StatusNotFound, errors.New("session not found")
		}
	}

	it := &queueItem{
		IdempotencyKey: req.IdempotencyKey, Kind: queueKindOrder,
		Instance: req.Instance, Project: req.Project, SessionKey: req.SessionKey,
		NewSession: req.NewSession, Priority: req.Priority, Prompt: req.Prompt,
		Source: req.Source, WakeSourceOnFinish: req.WakeSourceOnFinish, Board: req.Board, Visible: req.Visible,
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if dup, ok := c.duplicateLocked(req.IdempotencyKey); ok {
		return dup, http.StatusOK, nil
	}
	if len(c.laneOrderLocked(it.lane())) >= queueLaneLimit {
		return queueEnqueueResponse{}, http.StatusTooManyRequests, errors.New("session queue is full")
	}
	if err := c.insertLocked(it); err != nil {
		slog.Error("queue enqueue not persisted", "error", err)
		return queueEnqueueResponse{}, http.StatusServiceUnavailable, errors.New("queue state not persisted")
	}
	return queueEnqueueResponse{it.ID, it.Status, c.positionLocked(it)}, http.StatusCreated, nil
}

// finishLocked moves an item to a terminal state. The archive line is
// written before the active state drops the item, so a crash between the two
// leaves the item active rather than lost.
func (c *queueCenter) finishLocked(it *queueItem, status, reason string) error {
	next := it.clone()
	now := c.now()
	next.Status, next.Reason, next.FinishedAt = status, reason, &now
	next.note(now, status, reason)
	if status != queueStatusCancelled {
		// A crash after this wake but before the archive leaves the item
		// active; the retried report finds the cb: key and does not wake twice.
		c.wakeLocked(next, "")
	}
	if err := c.appendArchive(next); err != nil {
		return err
	}
	delete(c.items, it.ID)
	c.keys[it.IdempotencyKey] = queueKeyRef{it.ID, status, now}
	c.signal()
	return c.saveLocked()
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

// wakeLocked queues a completion turn for the dispatcher. Completion items
// never wake anyone, and one dispatcher→executor chain gets at most
// queueWakeLimit wakes per hour.
func (c *queueCenter) wakeLocked(it *queueItem, suffix string) {
	if it.Kind == queueKindCompletion || !it.WakeSourceOnFinish || it.Source == nil {
		return
	}
	key := "cb:" + it.ID + suffix
	if _, dup := c.keys[key]; dup {
		return
	}
	src := it.Source
	chain := src.Instance + "\x00" + src.Project + "\x00" + src.SessionKey + "\x00>" + it.lane()
	now := c.now()
	recent := pruneQueueWakes(c.wakes[chain], now)
	if len(recent) >= queueWakeLimit {
		c.wakes[chain] = recent
		it.note(now, it.Status, "wake_suppressed")
		slog.Warn("queue completion wake suppressed", "item_id", it.ID, "source", src.SessionKey)
		text := fmt.Sprintf("【队列回执暂停】令 %s 已到 %s，但这条派令链 1 小时内已叫醒 %d 次，本次不再自动叫醒派令方，请人工查看：GET /v1/queue/items/%s",
			it.ID, it.Status, queueWakeLimit, it.ID)
		notify, sessionKey := c.notify, src.SessionKey
		c.spawn(func() { notify(sessionKey, text) })
		return
	}
	c.wakes[chain] = append(recent, now)
	cb := &queueItem{
		IdempotencyKey: key, Kind: queueKindCompletion,
		Instance: src.Instance, Project: src.Project, SessionKey: src.SessionKey,
		Priority: "normal", Prompt: queueCompletionPrompt(it, c.turnTimeout),
	}
	if err := c.insertLocked(cb); err != nil {
		slog.Error("queue completion wake not persisted", "item_id", it.ID, "error", err)
	}
}

func queueCompletionPrompt(it *queueItem, timeout time.Duration) string {
	var b strings.Builder
	if it.Status == queueStatusTimedOut {
		fmt.Fprintf(&b, "【队列回执】令 %s（%s）开轮超过 %s 仍未结束，判 timed_out。执行方这一轮没有被停；要停用 /stop，要继续等不用操作，结束后会再报一次。\n", it.ID, it.IdempotencyKey, timeout)
	} else {
		fmt.Fprintf(&b, "【队列回执】令 %s（%s）已结束：%s", it.ID, it.IdempotencyKey, it.Status)
		if it.Reason != "" {
			fmt.Fprintf(&b, "，原因 %s", it.Reason)
		}
		b.WriteString("。\n")
	}
	fmt.Fprintf(&b, "目标：%s / %s / %s\n", it.Instance, it.Project, it.SessionKey)
	if it.Board != nil && (it.Board.Line != "" || it.Board.Item != "") {
		fmt.Fprintf(&b, "看板：%s / %s\n", it.Board.Line, it.Board.Item)
	}
	if it.Tail != "" {
		fmt.Fprintf(&b, "回复末段：\n%s\n", it.Tail)
	}
	fmt.Fprintf(&b, "详情：GET /v1/queue/items/%s", it.ID)
	return b.String()
}

func (c *queueCenter) markStartedLocked(it *queueItem, turnID string) error {
	now := c.now()
	it.StartedAt, it.TurnID = &now, turnID
	if it.DispatchedAt == nil {
		it.DispatchedAt = &now
	}
	it.note(now, queueStatusRunning, "started "+turnID)
	return c.saveLocked()
}

// report applies an executor's started/finished report and acknowledges it
// only after the change is on disk.
func (c *queueCenter) report(itemID, event string, body queueTurnReport) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	it := c.items[itemID]
	if it == nil {
		for _, ref := range c.keys {
			if ref.ID == itemID {
				return http.StatusOK, nil
			}
		}
		return http.StatusNotFound, errors.New("item not found")
	}
	var err error
	switch event {
	case "started":
		if it.Status == queueStatusRunning && it.StartedAt == nil {
			err = c.markStartedLocked(it, body.TurnID)
		}
	case "finished":
		if body.Status != queueStatusDone && body.Status != queueStatusFailed {
			return http.StatusBadRequest, errors.New("status must be done or failed")
		}
		if it.Status != queueStatusRunning && it.Status != queueStatusTimedOut {
			return http.StatusConflict, errors.New("item is not running")
		}
		it.Tail, it.ReplyMessageID = body.Tail, body.ReplyMessageID
		if it.TurnID == "" {
			it.TurnID = body.TurnID
		}
		err = c.finishLocked(it, body.Status, body.Reason)
	default:
		return http.StatusNotFound, errors.New("unknown report")
	}
	if err != nil {
		slog.Error("queue report not persisted", "item_id", itemID, "event", event, "error", err)
		return http.StatusServiceUnavailable, errors.New("report not persisted")
	}
	return http.StatusOK, nil
}

func (c *queueCenter) cancel(id string) (*queueItem, int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	it := c.items[id]
	if it == nil {
		if c.lookupKeyLocked(id) {
			return nil, http.StatusConflict, errors.New("item already finished")
		}
		return nil, http.StatusNotFound, errors.New("item not found")
	}
	if it.Status != queueStatusQueued {
		return nil, http.StatusConflict, errors.New("only queued items can be cancelled; use /stop for the running turn")
	}
	if err := c.finishLocked(it, queueStatusCancelled, "cancelled"); err != nil {
		return nil, http.StatusServiceUnavailable, errors.New("queue state not persisted")
	}
	done := it.clone()
	done.Status = queueStatusCancelled
	return done, http.StatusOK, nil
}

func (c *queueCenter) lookupKeyLocked(id string) bool {
	for _, ref := range c.keys {
		if ref.ID == id {
			return true
		}
	}
	return false
}

type queuePatchRequest struct {
	Prompt   *string `json:"prompt"`
	Priority *string `json:"priority"`
	Board    *struct {
		Line *string `json:"line"`
		Item *string `json:"item"`
		Due  *string `json:"due"`
	} `json:"board"`
}

func (c *queueCenter) patch(id string, req queuePatchRequest) (*queueItem, int, error) {
	if req.Prompt != nil && strings.TrimSpace(*req.Prompt) == "" {
		return nil, http.StatusBadRequest, errors.New("prompt cannot be empty")
	}
	if req.Priority != nil && *req.Priority != "normal" && *req.Priority != "high" {
		return nil, http.StatusBadRequest, errors.New("priority must be normal or high")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	it := c.items[id]
	if it == nil {
		return nil, http.StatusNotFound, errors.New("item not found")
	}
	if it.Status != queueStatusQueued {
		return nil, http.StatusConflict, errors.New("only queued items can be changed")
	}
	before := it.clone()
	var changed []string
	if req.Prompt != nil && *req.Prompt != it.Prompt {
		it.Prompt = *req.Prompt
		changed = append(changed, "prompt")
	}
	if req.Priority != nil && *req.Priority != it.Priority {
		changed = append(changed, "priority "+it.Priority+"→"+*req.Priority)
		it.Priority = *req.Priority
	}
	if req.Board != nil {
		if it.Board == nil {
			it.Board = &queueBoard{}
		}
		for _, f := range []struct {
			name string
			src  *string
			dst  *string
		}{{"board.line", req.Board.Line, &it.Board.Line}, {"board.item", req.Board.Item, &it.Board.Item}, {"board.due", req.Board.Due, &it.Board.Due}} {
			if f.src != nil && *f.src != *f.dst {
				*f.dst = *f.src
				changed = append(changed, f.name)
			}
		}
	}
	if len(changed) > 0 {
		it.note(c.now(), it.Status, "patched: "+strings.Join(changed, ", "))
		if err := c.saveLocked(); err != nil {
			c.items[id] = before
			return nil, http.StatusServiceUnavailable, errors.New("queue state not persisted")
		}
		c.signal()
	}
	return it.clone(), http.StatusOK, nil
}

// step runs one scheduling pass: timeouts, lost finishes, then dispatch.
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
		if it.Status != queueStatusRunning && it.Status != queueStatusTimedOut {
			continue
		}
		if c.checkRunningLocked(it, views[it.Instance], now) {
			continue
		}
		busy[it.lane()] = true
		if it.Status == queueStatusRunning && it.StartedAt == nil && !c.inflight[it.ID] {
			launch = append(launch, it.clone())
		}
	}
	for _, it := range items {
		lane := it.lane()
		if it.Status != queueStatusQueued || busy[lane] {
			continue
		}
		busy[lane] = true
		head := c.laneOrderLocked(lane)[0]
		if halted, err := c.halt(head.SessionKey); err != nil || halted {
			continue
		}
		view := views[head.Instance]
		if !view.Reachable {
			continue
		}
		if row, ok := view.session(head.Project, head.SessionKey); ok && row.State != "idle" {
			continue
		}
		head.Status, head.DispatchedAt = queueStatusRunning, &now
		head.note(now, queueStatusRunning, "dispatched")
		if err := c.saveLocked(); err != nil {
			head.Status, head.DispatchedAt = queueStatusQueued, nil
			slog.Error("queue dispatch not persisted", "item_id", head.ID, "error", err)
			continue
		}
		launch = append(launch, head.clone())
	}
	for _, it := range launch {
		c.inflight[it.ID] = true
	}
	c.mu.Unlock()
	for _, it := range launch {
		c.spawn(func() { c.dispatch(ctx, it) })
	}
}

// checkRunningLocked applies timeouts and lost-finish detection, reporting
// whether the item left the running set.
func (c *queueCenter) checkRunningLocked(it *queueItem, view queueView, now time.Time) bool {
	if it.StartedAt == nil {
		if it.DispatchedAt != nil && now.Sub(*it.DispatchedAt) >= c.dispatchTimeout && !c.inflight[it.ID] {
			c.persistFinish(it, queueStatusFailed, "dispatch_failed")
			return true
		}
		return false
	}
	if view.Reachable && view.FetchedAt != nil && view.FetchedAt.After(*it.StartedAt) {
		if row, ok := view.session(it.Project, it.SessionKey); !ok || row.ItemID != it.ID {
			c.persistFinish(it, queueStatusFailed, "lost-finish")
			return true
		}
	}
	if it.Status == queueStatusRunning && now.Sub(*it.StartedAt) >= c.turnTimeout {
		it.Status, it.Reason = queueStatusTimedOut, "no finished report within "+c.turnTimeout.String()
		it.note(now, queueStatusTimedOut, it.Reason)
		c.keys[it.IdempotencyKey] = queueKeyRef{it.ID, it.Status, now}
		c.wakeLocked(it, ":timed_out")
		if err := c.saveLocked(); err != nil {
			slog.Error("queue timeout not persisted", "item_id", it.ID, "error", err)
		}
	}
	return false
}

func (c *queueCenter) persistFinish(it *queueItem, status, reason string) {
	if err := c.finishLocked(it, status, reason); err != nil {
		slog.Error("queue terminal state not persisted", "item_id", it.ID, "status", status, "error", err)
	}
}

func (c *queueCenter) dispatch(ctx context.Context, snapshot *queueItem) {
	req := queueTurnRequest{ItemID: snapshot.ID, Project: snapshot.Project, SessionKey: snapshot.SessionKey,
		NewSession: snapshot.NewSession, Prompt: snapshot.Prompt, Visible: snapshot.Visible != nil && snapshot.Visible.PostToChat}
	report, status, err := c.startTurn(ctx, snapshot.Instance, req)

	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.inflight, snapshot.ID)
	it := c.items[snapshot.ID]
	if it == nil || it.Status != queueStatusRunning || it.StartedAt != nil {
		return
	}
	now := c.now()
	switch {
	case err == nil:
		if err := c.markStartedLocked(it, report.TurnID); err != nil {
			slog.Error("queue start not persisted", "item_id", it.ID, "error", err)
		}
	case status == http.StatusConflict || status == http.StatusLocked:
		it.Status, it.DispatchedAt = queueStatusQueued, nil
		it.note(now, queueStatusQueued, fmt.Sprintf("requeued: executor HTTP %d", status))
		if err := c.saveLocked(); err != nil {
			slog.Error("queue requeue not persisted", "item_id", it.ID, "error", err)
		}
	case status == http.StatusNotFound || status == http.StatusBadRequest:
		c.persistFinish(it, queueStatusFailed, fmt.Sprintf("rejected by executor: %v", err))
	default:
		// Unknown outcome: retried on later passes. The executor deduplicates
		// by item id, and dispatchTimeout bounds the retries.
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
		if it.Status == queueStatusQueued {
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

func (c *queueCenter) list(q map[string][]string) ([]*queueItem, error) {
	get := func(k string) string {
		if v := q[k]; len(v) > 0 {
			return v[0]
		}
		return ""
	}
	var since time.Time
	if s := get("since"); s != "" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			if t, err = time.ParseInLocation("2006-01-02", s, queueZone); err != nil {
				return nil, errors.New("since must be RFC3339 or YYYY-MM-DD")
			}
		}
		since = t
	}
	limit := 200
	if s := get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 {
			return nil, errors.New("limit must be a positive integer")
		}
		limit = min(n, 1000)
	}
	var after int64
	if s := get("after"); s != "" {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, errors.New("after must be an item seq")
		}
		after = n
	}
	status := get("status")
	c.mu.Lock()
	var all []*queueItem
	for _, it := range c.items {
		all = append(all, it.clone())
	}
	c.mu.Unlock()
	if !since.IsZero() || status == queueStatusDone || status == queueStatusFailed || status == queueStatusCancelled {
		active := make(map[string]bool, len(all))
		for _, it := range all {
			active[it.ID] = true
		}
		for _, it := range c.readArchive(since) {
			if !active[it.ID] {
				all = append(all, it)
			}
		}
	}
	result := make([]*queueItem, 0)
	for _, it := range all {
		if (get("instance") != "" && it.Instance != get("instance")) ||
			(get("session_key") != "" && it.SessionKey != get("session_key")) ||
			(status != "" && it.Status != status) ||
			(get("line") != "" && (it.Board == nil || it.Board.Line != get("line"))) ||
			(!since.IsZero() && it.CreatedAt.Before(since) && (it.FinishedAt == nil || it.FinishedAt.Before(since))) ||
			it.Seq <= after {
			continue
		}
		result = append(result, it)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Seq < result[j].Seq })
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (c *queueCenter) register(mux *http.ServeMux) {
	fail := func(w http.ResponseWriter, status int, err error) { http.Error(w, err.Error(), status) }
	mux.HandleFunc("POST /v1/queue/items", func(w http.ResponseWriter, r *http.Request) {
		var req queueEnqueueRequest
		if !decodeQueueJSON(w, r, &req) {
			return
		}
		res, status, err := c.enqueue(req)
		if err != nil {
			fail(w, status, err)
			return
		}
		apiJSON(w, status, res)
	})
	mux.HandleFunc("GET /v1/queue/items", func(w http.ResponseWriter, r *http.Request) {
		items, err := c.list(r.URL.Query())
		if err != nil {
			fail(w, http.StatusBadRequest, err)
			return
		}
		apiJSON(w, http.StatusOK, items)
	})
	mux.HandleFunc("GET /v1/queue/items/{id}", func(w http.ResponseWriter, r *http.Request) {
		it := c.lookup(r.PathValue("id"))
		if it == nil {
			fail(w, http.StatusNotFound, errors.New("item not found"))
			return
		}
		apiJSON(w, http.StatusOK, it)
	})
	mux.HandleFunc("POST /v1/queue/items/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		it, status, err := c.cancel(r.PathValue("id"))
		if err != nil {
			fail(w, status, err)
			return
		}
		apiJSON(w, status, it)
	})
	mux.HandleFunc("PATCH /v1/queue/items/{id}", func(w http.ResponseWriter, r *http.Request) {
		var req queuePatchRequest
		if !decodeQueueJSON(w, r, &req) {
			return
		}
		it, status, err := c.patch(r.PathValue("id"), req)
		if err != nil {
			fail(w, status, err)
			return
		}
		apiJSON(w, status, it)
	})
	mux.HandleFunc("POST /v1/turns/{id}/{event}", func(w http.ResponseWriter, r *http.Request) {
		var body queueTurnReport
		if !decodeQueueJSON(w, r, &body) {
			return
		}
		status, err := c.report(r.PathValue("id"), r.PathValue("event"), body)
		if err != nil {
			fail(w, status, err)
			return
		}
		apiJSON(w, status, map[string]bool{"ok": true})
	})
}

func (p *queueRemote) startTurn(ctx context.Context, req queueTurnRequest) (queueTurnReport, int, error) {
	res, err := p.postJSON(ctx, "/x/turns", req)
	if err != nil {
		return queueTurnReport{}, 0, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusAccepted {
		msg, _ := io.ReadAll(io.LimitReader(res.Body, 512))
		return queueTurnReport{}, res.StatusCode, fmt.Errorf("executor returned HTTP %d: %s", res.StatusCode, strings.TrimSpace(string(msg)))
	}
	var report queueTurnReport
	if err := json.NewDecoder(res.Body).Decode(&report); err != nil {
		return queueTurnReport{}, 0, fmt.Errorf("decode executor turn: %w", err)
	}
	return report, res.StatusCode, nil
}
