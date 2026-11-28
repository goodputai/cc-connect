package core

import (
	_ "embed"
	"net/http"
	"time"
)

//go:embed queue_board.html
var queueBoardHTML []byte

// Public reads are limited to the board and its data on the queue center.
// The executor listener and unrelated center routes still require a token.
func queueCenterHandler(mux *http.ServeMux, token string) http.Handler {
	return queueBearerHandler(mux, token,
		"GET /board",
		"GET /v1/queue/items",
		"GET /v1/queue/items/{id}",
		"GET /v1/queue/items/{id}/events",
		"GET /v1/queue/bottleneck",
	)
}

func (c *queueCenter) registerBoard(mux *http.ServeMux) {
	mux.HandleFunc("GET /board", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'")
		if r.Method == http.MethodGet {
			w.Write(queueBoardHTML)
		}
	})
	mux.HandleFunc("GET /v1/queue/items/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		events, ok := c.itemEvents(r.PathValue("id"))
		if !ok {
			http.NotFound(w, r)
			return
		}
		apiJSON(w, http.StatusOK, events)
	})
}

type queueItemEvent struct {
	At       time.Time `json:"at"`
	State    string    `json:"state"`
	Note     string    `json:"note"`
	Revision uint64    `json:"revision"`
}

func (c *queueCenter) indexEvent(event queueStoreEvent, it *queueItem) {
	c.events[it.ID] = append(c.events[it.ID], queueItemEvent{event.At, it.Status, event.Note, it.Revision})
}

func (c *queueCenter) itemEvents(id string) ([]queueItemEvent, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.items[id] == nil {
		return nil, false
	}
	return append([]queueItemEvent{}, c.events[id]...), true
}

// QueueDisplayStatus returns the human-readable state and outcome shown by
// both the web board and the queue CLI.
func QueueDisplayStatus(state string, assigned bool, lastError, result, resolvedBy string) string {
	switch state {
	case queueStatusQueued:
		if assigned {
			state += "/排队"
		}
		if lastError != "" {
			state += "/失败退回"
		}
	case queueStatusDone:
		switch {
		case result == queueResultBlocked && resolvedBy != "":
			return state + "/阻塞（已被 " + resolvedBy + " 解决）"
		case result == queueResultBlocked:
			return state + "/阻塞（未解决）"
		case result == queueResultDelivered:
			return state + "/交付"
		}
	}
	return state
}
