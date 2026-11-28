package core

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestQueueBoardPublicReadsAndWriteAuthentication(t *testing.T) {
	e := newQCEnv(t, t.TempDir())
	id := e.mustEnqueue("board-route", func(req *queueEnqueueRequest) { req.Board = &queueBoard{Name: "A"} })
	itemPath := "/v1/queue/items/" + id
	mux := http.NewServeMux()
	e.c.register(mux)
	mux.HandleFunc("GET /v1/fleet", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	handler := queueCenterHandler(mux, "center-token")
	patch := `{"evidence":["operator appended this"]}`

	cases := []struct {
		method, path, token string
		body, contains      string
		want                int
	}{
		{"GET", "/board", "", "", "队列看板", http.StatusOK},
		{"GET", "/v1/queue/items?board=A&priority=P2", "", "", id, http.StatusOK},
		{"GET", "/v1/queue/items?board=A&priority=P0", "", "", "[]", http.StatusOK},
		{"GET", "/v1/queue/bottleneck?board=A&priority=P2", "", "", id, http.StatusOK},
		{"GET", "/v1/queue/bottleneck?board=A&priority=P0", "", "", "[]", http.StatusOK},
		{"GET", itemPath, "", "", `"executor":{`, http.StatusOK},
		{"GET", itemPath + "/events", "", "", `"note":"enqueued"`, http.StatusOK},
		{"GET", "/v1/queue/items/unknown/events", "", "", "", http.StatusNotFound},
		{"GET", "/v1/fleet", "", "", "", http.StatusUnauthorized},
		{"GET", "/v1/fleet", "center-token", "", "", http.StatusOK},
		// Exemptions follow the matched mux pattern, not a path prefix.
		{"HEAD", "/v1/queue/items", "", "", "", http.StatusOK},
		{"OPTIONS", "/v1/queue/items", "", "", "", http.StatusNotFound},
		{"PUT", "/board", "", "", "", http.StatusNotFound},
		{"DELETE", itemPath, "", "", "", http.StatusNotFound},
		{"GET", "/board/", "", "", "", http.StatusNotFound},
		{"GET", "/board/../v1/queue/items", "", "", "", http.StatusTemporaryRedirect},
		{"GET", "/board/../v1/fleet", "", "", "", http.StatusUnauthorized},
		{"POST", "/board/../v1/queue/items", "", "{}", "", http.StatusUnauthorized},
		{"PATCH", "/v1/queue/items/" + id + "/events", "", patch, "", http.StatusNotFound},
		{"POST", "/v1/queue/items", "", "{}", "", http.StatusUnauthorized},
		{"PATCH", itemPath, "", patch, "", http.StatusUnauthorized},
		{"PATCH", itemPath, "wrong", patch, "", http.StatusUnauthorized},
		{"POST", itemPath + "/cancel", "", "{}", "", http.StatusUnauthorized},
		{"POST", "/v1/turns/" + id + "/finished", "", "{}", "", http.StatusUnauthorized},
		{"PATCH", itemPath, "center-token", patch, "operator appended this", http.StatusOK},
		{"GET", itemPath, "", "", `"resolved_by":""`, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path+"/"+tc.token, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			if tc.token != "" {
				req.Header.Set("Authorization", "Bearer "+tc.token)
			}
			res := httptest.NewRecorder()
			handler.ServeHTTP(res, req)
			if res.Code != tc.want || !strings.Contains(res.Body.String(), tc.contains) {
				t.Fatalf("got HTTP %d %q; want HTTP %d containing %q", res.Code, res.Body.String(), tc.want, tc.contains)
			}
			if tc.method == "GET" && tc.path == "/board" && !strings.HasPrefix(res.Header().Get("Content-Type"), "text/html") {
				t.Fatalf("board Content-Type: %s", res.Header().Get("Content-Type"))
			}
		})
	}
}

func TestQueueBoardEventsReplayedFromJournal(t *testing.T) {
	dir := t.TempDir()
	e := newQCEnv(t, dir)
	item := e.mustEnqueue("journal-entry")
	other := e.mustEnqueue("separate-entry")
	requireQueuePatch(t, e.c, item, queuePatchRequest{Evidence: []string{"first proof", "second proof"}}, http.StatusOK)
	requireQueuePatch(t, e.c, other, setQueueState(queueStatusCancelled, "cancelled"), http.StatusOK)
	e = newQCEnv(t, dir)

	mux := http.NewServeMux()
	e.c.register(mux)
	res := httptest.NewRecorder()
	queueCenterHandler(mux, "secret").ServeHTTP(res, httptest.NewRequest("GET", "/v1/queue/items/"+item+"/events", nil))
	if res.Code != http.StatusOK {
		t.Fatalf("event timeline HTTP %d: %s", res.Code, res.Body)
	}
	var timeline []queueItemEvent
	if err := json.Unmarshal(res.Body.Bytes(), &timeline); err != nil {
		t.Fatal(err)
	}
	if len(timeline) != 2 || timeline[0].Note != "enqueued" || timeline[0].Revision != 1 ||
		timeline[1].Note != "patched: evidence" || timeline[1].Revision != 2 ||
		timeline[1].State != queueStatusQueued || timeline[1].At.IsZero() {
		t.Fatalf("unexpected event timeline: %+v", timeline)
	}
	if got := e.c.lookup(item).Evidence; !reflect.DeepEqual(got, []string{"first proof", "second proof"}) {
		t.Fatalf("full evidence = %v", got)
	}
}
