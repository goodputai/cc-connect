package core

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestQueueHTTPWhitelist(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/fleet", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	// Negative control: register /send on the TCP mux or use the legacy
	// API mux. A valid queue token would then reach a non-queue route.
	handler := queueBearerHandler(mux, "test-token")
	for _, path := range []string{"/send", "/inject", "/cron/del", "/v1/board", "/v1/events", "/x/sessions"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("Authorization", "Bearer test-token")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNotFound {
			t.Errorf("%s returned %d; want 404", path, response.Code)
		}
	}
}

func TestQueueHTTPRejectsBadToken(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/fleet", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := queueBearerHandler(mux, "test-token")
	for _, tc := range []struct {
		token string
		want  int
	}{{"", 401}, {"Bearer next-token", 401}, {"Bearer test-token", 200}} {
		request := httptest.NewRequest(http.MethodGet, "/v1/fleet", nil)
		request.Header.Set("Authorization", tc.token)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		// Negative control: bypass the bearer middleware. Bad tokens get 200.
		if response.Code != tc.want {
			t.Errorf("token %q returned %d; want %d", tc.token, response.Code, tc.want)
		}
	}
}

func TestQueueFleetDropsStaleSessions(t *testing.T) {
	reachable := true
	executor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !reachable {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "[{\"project\":\"p\",\"session_key\":\"test:chat:user\",\"state\":\"idle\"}]")
	}))
	defer executor.Close()
	fleet := queueFleet{
		peers: []*queueRemote{{name: "exec", url: executor.URL, client: executor.Client()}},
		nodes: make(map[string]queueFleetNode),
	}
	fleet.poll(context.Background())
	first := fleet.snapshot()[0]
	reachable = false
	fleet.poll(context.Background())
	failed := fleet.snapshot()[0]
	if !first.Reachable || first.FetchedAt == nil || len(first.Sessions) != 1 ||
		failed.Reachable || len(failed.Sessions) != 0 || failed.FetchedAt == nil ||
		!failed.FetchedAt.Equal(*first.FetchedAt) {
		t.Fatalf("first = %+v; failed = %+v", first, failed)
	}
}

func TestQueueSessionSnapshotColdAndBusy(t *testing.T) {
	manager := NewSessionManager("")
	key := "test:chat:user"
	session := manager.GetOrCreateActive(key)
	manager.UpdateUserMeta(key, "user", "chat")
	rows := make(map[string]queueSession)
	manager.appendQueueSessions(rows)
	if row := rows[key]; row.State != "idle" || row.ChatName != "chat" {
		t.Fatalf("idle session = %+v", row)
	}
	if !session.TryLock() {
		t.Fatal("session is busy before test")
	}
	started := time.Now()
	session.setQueueTurn(started, "timer", "")
	manager.appendQueueSessions(make(map[string]queueSession))
	rows = make(map[string]queueSession)
	manager.appendQueueSessions(rows)
	if row := rows[key]; row.State != "busy" || row.TurnSource != "timer" ||
		row.TurnStartedAt == nil || !row.TurnStartedAt.Equal(started) {
		t.Fatalf("busy session = %+v", row)
	}
}
