package antigravity

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/agent/antigravityhook"
	"github.com/chenhg5/cc-connect/core"
)

func TestSlugify(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"cc-connect", "cc-connect"},
		{"Daily", "daily"},
		{"My Project", "my-project"},
		{"hello_world", "hello-world"},
		{"Test.123", "test-123"},
		{"---weird---", "weird"},
		{"", "project"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := slugify(tt.input)
			if got != tt.want {
				t.Errorf("slugify(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestNormalizeMode(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"default", "default"},
		{"yolo", "yolo"},
		{"auto", "yolo"},
		{"force", "yolo"},
		{"plan", "plan"},
		{"sandbox", "plan"},
		{"invalid", "default"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := normalizeMode(tt.input)
			if got != tt.want {
				t.Errorf("normalizeMode(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestBuildAntigravityArgs_StreamAndWrapper(t *testing.T) {
	got := buildAntigravityArgs("agy-carbon", []string{"--verbose"}, "config-model", "plan", "sid-1", "/tmp/agy-config")
	want := []string{"--verbose", "--gemini_dir=/tmp/agy-config", "--print-timeout=24h", "--conversation", "sid-1", "--sandbox", "--input-format", "stream-json", "--output-format", "stream-json", "-p="}
	if !slices.Equal(got, want) {
		t.Fatalf("args = %q, want %q", got, want)
	}
	for _, tc := range []struct {
		extra []string
		model string
	}{
		{nil, "configured"}, {[]string{"--model=explicit"}, "explicit"},
	} {
		args := buildAntigravityArgs("/usr/bin/agy", tc.extra, "configured", "yolo", "", "")
		if strings.Count(strings.Join(args, " "), "--model") != 1 || !strings.Contains(strings.Join(args, " "), tc.model) {
			t.Fatalf("model args = %q, want only %q", args, tc.model)
		}
	}
}

const fixtureConversationID = "00000000-0000-4000-8000-000000000001"

func TestAgyHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_AGY_HELPER") != "1" {
		return
	}
	data, err := os.ReadFile(os.Getenv("AGY_FIXTURE"))
	if err != nil {
		t.Fatal(err)
	}
	recordings := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(recordings) != 9 {
		t.Fatalf("unexpected fixture count: %d", len(recordings))
	}

	id := fmt.Sprintf("fixture-pid-%d", os.Getpid())
	for i, arg := range os.Args {
		if arg == "--conversation" {
			id = os.Args[i+1]
		}
	}
	printRecord := func(line string) {
		fmt.Println(strings.ReplaceAll(line, fixtureConversationID, id))
	}
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 4096), 1<<20)
	for i := 0; in.Scan(); i++ {
		var request struct {
			Event   string
			Message struct {
				Role    string
				Content []struct{ Text string }
			}
		}
		if err := json.Unmarshal(in.Bytes(), &request); err != nil {
			t.Fatal(err)
		}
		if request.Event != "user" || request.Message.Role != "user" || len(request.Message.Content) != 1 {
			t.Fatalf("bad input: %s", in.Text())
		}
		prompt := request.Message.Content[0].Text
		if i == 0 {
			printRecord(recordings[0])
		}
		switch {
		case strings.Contains(prompt, "CRASH-TEST"):
			os.Exit(9)
		case strings.Contains(prompt, "STOP-TEST"), strings.Contains(prompt, "TOPIC-HOLD"):
			// ACK only after the subprocess has read the input and started its
			// turn, then wait for Close (or a test-controlled release file).
			fmt.Printf("{\"event\":\"step_update\",\"step_update\":{\"step_type\":\"agent_response\",\"text_delta\":\"started\"}}\n")
			for {
				if _, err := os.Stat(os.Getenv("AGY_RELEASE_FILE")); err == nil {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
		case strings.Contains(prompt, "PERMISSION-TEST"):
			hookInput := fmt.Sprintf(`{"conversationId":%q,"stepIdx":1,"toolCall":{"name":"view_file","args":{"AbsolutePath":"/test/fixture.txt"}}}`, id)
			var response bytes.Buffer
			err := antigravityhook.Relay(strings.NewReader(hookInput), &response, os.Getenv(antigravityhook.EnvAddress), os.Getenv(antigravityhook.EnvToken))
			if err != nil || !strings.Contains(response.String(), `"decision":"allow"`) {
				t.Fatalf("permission round-trip: %v, %s", err, response.String())
			}
		}
		start, end := 1, 5
		if i > 0 {
			start, end = 5, 9
		}
		for _, record := range recordings[start:end] {
			printRecord(record)
		}
	}
	if err := in.Err(); err != nil {
		t.Fatal(err)
	}
	os.Exit(0)
}

func fakeAgyArgs(t *testing.T) (string, []string, []string) {
	t.Helper()
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	fixture, err := filepath.Abs("testdata/agy-1.5.12.stdout.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	return bin, []string{"-test.run=^TestAgyHelperProcess$", "--"}, []string{"GO_WANT_AGY_HELPER=1", "AGY_FIXTURE=" + fixture}
}

func startFakeAgy(t *testing.T, mode, resume string) *antigravitySession {
	t.Helper()
	bin, args, env := fakeAgyArgs(t)
	s, err := newAntigravitySession(context.Background(), bin, args, t.TempDir(), "", mode, resume, env, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func nextAgyEvent(t *testing.T, s *antigravitySession) core.Event {
	t.Helper()
	select {
	case event, ok := <-s.Events():
		if !ok {
			t.Fatal("agy session closed early")
		}
		return event
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for agy event")
		return core.Event{}
	}
}

func awaitAgyResult(t *testing.T, s *antigravitySession, want string) core.Event {
	t.Helper()
	var text strings.Builder
	for {
		event := nextAgyEvent(t, s)
		switch event.Type {
		case core.EventText:
			text.WriteString(event.Content)
		case core.EventError:
			t.Fatal(event.Error)
		case core.EventResult:
			if !event.Done || event.Content != want || text.String() != want {
				t.Fatalf("text = %q, result = %+v, want %q", text.String(), event, want)
			}
			return event
		}
	}
}

func TestAntigravitySession_ReusesProcess(t *testing.T) {
	s := startFakeAgy(t, "yolo", core.ContinueSession)
	if got := s.CurrentSessionID(); got != "" {
		t.Fatalf("ContinueSession should be fresh, got %q", got)
	}
	pid := s.cmd.Process.Pid
	if err := s.Send("first prompt", "", nil, nil); err != nil {
		t.Fatal(err)
	}
	first := awaitAgyResult(t, s, "OK-1\n")
	if err := s.Send("second prompt", "", nil, nil); err != nil {
		t.Fatal(err)
	}
	second := awaitAgyResult(t, s, "OK-2\n")
	if !s.Alive() || first.SessionID != second.SessionID || second.SessionID != fmt.Sprintf("fixture-pid-%d", pid) {
		t.Fatalf("process/conversation changed across turns: %d, %q, %q", pid, first.SessionID, second.SessionID)
	}
	if first.InputTokens != 20574 || second.InputTokens != 2169 || second.CacheReadInputTokens != 18517 {
		t.Fatalf("per-turn usage: %+v / %+v", first, second)
	}
}

func TestAntigravitySession_ResumePassesConversationID(t *testing.T) {
	s := startFakeAgy(t, "yolo", "conversation-1")
	if err := s.Send("resume", "", nil, nil); err != nil {
		t.Fatal(err)
	}
	if event := awaitAgyResult(t, s, "OK-1\n"); event.SessionID != "conversation-1" {
		t.Fatalf("resume = %q", event.SessionID)
	}
}

func TestAntigravitySession_PermissionBridge(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	s := startFakeAgy(t, "default", "")
	if s.permissionBridge == nil {
		t.Fatal("missing default-mode bridge")
	}
	if _, err := os.Stat(filepath.Join(s.permissionBridge.AgyConfigDir(), "config", "hooks.json")); err != nil {
		t.Fatal(err)
	}
	if err := s.Send("PERMISSION-TEST", "", nil, nil); err != nil {
		t.Fatal(err)
	}
	for {
		event := nextAgyEvent(t, s)
		if event.Type == core.EventError || event.Type == core.EventResult {
			t.Fatalf("missing permission request: %+v", event)
		}
		if event.Type == core.EventPermissionRequest {
			if event.ToolName != "view_file" || event.ToolInputRaw["AbsolutePath"] != "/test/fixture.txt" {
				t.Fatalf("permission event = %+v", event)
			}
			if err := s.RespondPermission(event.RequestID, core.PermissionResult{Behavior: "allow"}); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	awaitAgyResult(t, s, "OK-1\n")
	if err := s.Send("PERMISSION-TEST again", "", nil, nil); err != nil {
		t.Fatal(err)
	}
	event := nextAgyEvent(t, s)
	if event.Type != core.EventPermissionRequest {
		t.Fatalf("second permission event = %+v", event)
	}
	if err := s.RespondPermission(event.RequestID, core.PermissionResult{Behavior: "allow"}); err != nil {
		t.Fatal(err)
	}
	awaitAgyResult(t, s, "OK-2\n")
}

func TestAntigravitySession_NonDefaultModes(t *testing.T) {
	for _, mode := range []string{"yolo", "plan"} {
		t.Run(mode, func(t *testing.T) {
			s := startFakeAgy(t, mode, "")
			if s.permissionBridge != nil {
				t.Fatal("unexpected permission bridge")
			}
			if err := s.RespondPermission("req", core.PermissionResult{Behavior: "allow"}); err == nil {
				t.Fatal("unexpected permission response accepted")
			}
		})
	}
}
