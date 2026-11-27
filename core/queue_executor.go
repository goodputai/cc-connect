package core

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type queueTurnRequest struct {
	ItemID     string `json:"item_id"`
	Project    string `json:"project"`
	SessionKey string `json:"session_key"`
	NewSession bool   `json:"new_session"`
	Prompt     string `json:"prompt"`
	Visible    bool   `json:"visible,omitempty"`
}

type queueTurnReport struct {
	TurnID         string    `json:"turn_id"`
	StartedAt      time.Time `json:"started_at"`
	Status         string    `json:"status,omitempty"`
	Reason         string    `json:"reason,omitempty"`
	Tail           string    `json:"tail,omitempty"`
	ReplyMessageID string    `json:"reply_message_id,omitempty"`
}

type queueTurnObservation struct {
	once    sync.Once
	mu      sync.Mutex
	replyID string
	ended   chan queueTurnReport
}

func (o *queueTurnObservation) recordReplyID(id string) {
	o.mu.Lock()
	o.replyID = id
	o.mu.Unlock()
}

func (o *queueTurnObservation) finish(status, reason, content string) {
	o.once.Do(func() {
		o.mu.Lock()
		id := o.replyID
		o.mu.Unlock()
		o.ended <- queueTurnReport{Status: status, Reason: reason, Tail: finalReplyTail(content), ReplyMessageID: id}
	})
}

// QueueReplyTracking is optional. It records the ID returned by the platform
// when a queued turn delivers a reply.
type QueueReplyTracking interface {
	WithQueueReplyTracking(replyCtx any, record func(string)) any
}

type queueExecution struct {
	request     queueTurnRequest
	start       queueTurnReport
	observation *queueTurnObservation
}

type queueDestination struct {
	Instance, Project, SessionKey string
}

type queueTurnReporter func(context.Context, string, string, queueTurnReport) error

type queueExecutor struct {
	api    *APIServer
	ctx    context.Context
	report queueTurnReporter
	halt   func(string) (bool, error)
	mu     sync.Mutex
	items  map[string]*queueExecution
}

func newQueueExecutor(api *APIServer, ctx context.Context, report queueTurnReporter) *queueExecutor {
	return &queueExecutor{api: api, ctx: ctx, report: report, halt: queueGroupHalted, items: make(map[string]*queueExecution)}
}

func queueGroupHalted(sessionKey string) (bool, error) {
	platform, chat, _ := ParseSessionKey(sessionKey)
	if platform != "feishu" || !strings.HasPrefix(chat, "oc_") || len(chat) != 35 ||
		strings.Trim(chat[3:], "0123456789abcdef") != "" {
		return false, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return false, err
	}
	_, err = os.Stat(filepath.Join(home, "agent-share", "state", "halt", chat+".json"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func (x *queueExecutor) applySessions(rows []queueSession) {
	x.mu.Lock()
	defer x.mu.Unlock()
	for i := range rows {
		for _, active := range x.items {
			if rows[i].Project == active.request.Project && rows[i].SessionKey == active.request.SessionKey {
				startedAt := active.start.StartedAt
				rows[i].ItemID, rows[i].TurnStartedAt = active.request.ItemID, &startedAt
				if rows[i].State == "idle" || rows[i].State == "starting" {
					// Keep the item visible while its finished report is
					// waiting for a durable acknowledgement from the center.
					rows[i].State, rows[i].TurnSource = "busy", "center"
				}
			}
		}
	}
}

func (x *queueExecutor) accept(req queueTurnRequest) (queueTurnReport, int, error) {
	if req.ItemID == "" || req.Project == "" || req.SessionKey == "" || strings.TrimSpace(req.Prompt) == "" {
		return queueTurnReport{}, http.StatusBadRequest, errors.New("item_id, project, session_key and prompt are required")
	}
	if err := x.ctx.Err(); err != nil {
		return queueTurnReport{}, http.StatusServiceUnavailable, errors.New("executor is stopping")
	}
	halted, err := x.halt(req.SessionKey)
	if err != nil {
		return queueTurnReport{}, http.StatusServiceUnavailable, errors.New("cannot check group stop marker")
	}
	if halted {
		return queueTurnReport{}, http.StatusLocked, errors.New("group is stopped")
	}
	x.api.mu.RLock()
	engine := x.api.engines[req.Project]
	x.api.mu.RUnlock()
	if engine == nil {
		return queueTurnReport{}, http.StatusNotFound, errors.New("session not found")
	}
	agent, manager := engine.sessionContextForKey(req.SessionKey)
	active := manager.LookupActive(req.SessionKey)
	if active == nil {
		return queueTurnReport{}, http.StatusNotFound, errors.New("session not found")
	}
	_, platform, replyCtx, err := engine.resolveOutboundSessionTarget(req.SessionKey, false)
	if err != nil {
		return queueTurnReport{}, http.StatusServiceUnavailable, errors.New("session reply target unavailable")
	}

	x.mu.Lock()
	defer x.mu.Unlock()
	if existing := x.items[req.ItemID]; existing != nil {
		if existing.request != req {
			return queueTurnReport{}, http.StatusConflict, errors.New("item id already assigned")
		}
		return existing.start, http.StatusAccepted, nil
	}
	for _, existing := range x.items {
		if req.Project == existing.request.Project && req.SessionKey == existing.request.SessionKey {
			return queueTurnReport{}, http.StatusConflict, errors.New("session is busy")
		}
	}
	if !active.TryLock() {
		return queueTurnReport{}, http.StatusConflict, errors.New("session is busy")
	}
	start := queueTurnReport{TurnID: "t_" + rand.Text(), StartedAt: time.Now()}
	entry := &queueExecution{request: req, start: start, observation: &queueTurnObservation{ended: make(chan queueTurnReport, 1)}}
	x.items[req.ItemID] = entry
	go x.run(engine, agent, manager, active, platform, replyCtx, entry)
	return start, http.StatusAccepted, nil
}

func (x *queueExecutor) run(engine *Engine, agent Agent, manager *SessionManager, active *Session, platform Platform, replyCtx any, entry *queueExecution) {
	req, observation := entry.request, entry.observation
	go x.reportTurn(entry)
	defer observation.finish("failed", "error_or_interrupted", "")
	key := engine.interactiveKeyForSessionKey(req.SessionKey)
	workspace := strings.TrimSuffix(key, ":"+req.SessionKey)
	if workspace == key {
		workspace = ""
	}

	session := active
	if req.NewSession {
		session = manager.NewSideSession(req.SessionKey, "queue-"+req.ItemID)
		session.TryLock()
		sideKey := key + "#queue:" + session.ID
		defer func() {
			engine.cleanupInteractiveState(sideKey)
			active.Unlock()
			if active.TryLock() {
				engine.drainOrphanedQueue(active, manager, key, agent, workspace)
			}
		}()
		key = sideKey
	}
	if req.Visible {
		// Feishu resolves literal @names as well as native mention markup.
		plain := strings.NewReplacer("@", "＠", "<", "＜").Replace(req.Prompt)
		if err := platform.Send(engine.ctx, replyCtx, plain); err != nil {
			slog.Warn("queue prompt copy failed", "item_id", req.ItemID, "error", err)
		}
	}
	if tracker, ok := platform.(QueueReplyTracking); ok {
		replyCtx = tracker.WithQueueReplyTracking(replyCtx, observation.recordReplyID)
	}
	msg := &Message{
		SessionKey: req.SessionKey, Platform: platform.Name(), UserID: "center_queue",
		Content: req.Prompt, ReplyCtx: replyCtx, queueItemID: req.ItemID, queueTurn: observation,
	}
	engine.processInteractiveMessageWith(platform, msg, session, agent, manager, key, workspace, req.SessionKey)
}

func (x *queueExecutor) reportTurn(entry *queueExecution) {
	defer func() {
		x.mu.Lock()
		delete(x.items, entry.request.ItemID)
		x.mu.Unlock()
	}()

	itemID := entry.request.ItemID
	if !x.reportUntilAcknowledged(itemID, "started", entry.start) {
		return
	}
	select {
	case <-x.ctx.Done():
	case result := <-entry.observation.ended:
		result.TurnID, result.StartedAt = entry.start.TurnID, entry.start.StartedAt
		x.reportUntilAcknowledged(itemID, "finished", result)
	}
}

func (x *queueExecutor) reportUntilAcknowledged(itemID, event string, body queueTurnReport) bool {
	delay := 250 * time.Millisecond
	for x.ctx.Err() == nil {
		if err := x.report(x.ctx, itemID, event, body); err == nil {
			return true
		} else if delay == 250*time.Millisecond {
			slog.Warn("queue report pending retry", "item_id", itemID, "event", event, "error", err)
		}
		timer := time.NewTimer(delay)
		select {
		case <-x.ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
		delay *= 2
		if delay > 15*time.Second {
			delay = 15 * time.Second
		}
	}
	return false
}

func (x *queueExecutor) serveTurns(w http.ResponseWriter, r *http.Request) {
	var request queueTurnRequest
	if !decodeQueueJSON(w, r, &request) {
		return
	}
	report, status, err := x.accept(request)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	apiJSON(w, status, report)
}

func decodeQueueJSON(w http.ResponseWriter, r *http.Request, body any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, queueBodyLimit)
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(body); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return false
	}
	return true
}

func (p *queueRemote) postJSON(ctx context.Context, path string, value any) (*http.Response, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url+path, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+p.token)
	req.Header.Set("Content-Type", "application/json")
	return p.client.Do(req)
}

func (p *queueRemote) reportTurn(ctx context.Context, itemID, event string, report queueTurnReport) error {
	res, err := p.postJSON(ctx, "/v1/turns/"+itemID+"/"+event, report)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("center rejected report with HTTP %d", res.StatusCode)
	}
	return nil
}
