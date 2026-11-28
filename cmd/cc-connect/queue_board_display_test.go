package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestQueueCommandsShowBoardLabels(t *testing.T) {
	items := []map[string]any{
		{"id": "q_pending", "state": "待办"},
		{"id": "q_queued", "state": "待办", "executor": map[string]string{"instance": "henry"}},
		{"id": "q_failed", "state": "待办", "last_error": "lost-finish"},
		{"id": "q_active", "state": "进行中"},
		{"id": "q_delivery", "state": "完成", "result": "交付"},
		{"id": "q_blocked", "state": "完成", "result": "阻塞"},
		{"id": "q_resolved", "state": "完成", "result": "阻塞", "resolved_by": "q_next"},
		{"id": "q_cancelled", "state": "取消"},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/queue/items", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(items)
	})
	mux.HandleFunc("GET /v1/queue/bottleneck", func(w http.ResponseWriter, r *http.Request) {
		var rows []map[string]any
		for _, item := range items {
			rows = append(rows, map[string]any{"item": item, "reason": "ranking"})
		}
		json.NewEncoder(w).Encode(rows)
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("token"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"list", "bottleneck"} {
		t.Run(command, func(t *testing.T) {
			var output bytes.Buffer
			if err := queueCommand([]string{command, "--board", "A", "--center", server.URL, "--token-file", tokenFile}, &output); err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{"q_pending [待办]", "q_queued [待办/排队]", "q_failed [待办/失败退回]",
				"q_active [进行中]", "q_delivery [完成/交付]", "q_blocked [完成/阻塞（未解决）]",
				"q_resolved [完成/阻塞（已被 q_next 解决）]", "q_cancelled [取消]"} {
				if !strings.Contains(output.String(), want) {
					t.Errorf("missing %q in %s", want, output.String())
				}
			}
		})
	}
}
