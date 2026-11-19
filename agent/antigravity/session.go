package antigravity

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

// antigravitySession owns one agy stream-json process. The engine creates a
// new session with the last conversation ID if the process exits or is reaped.
type antigravitySession struct {
	cmd              *exec.Cmd
	stdin            io.WriteCloser
	workDir          string
	timeout          time.Duration
	events           chan core.Event
	done             chan struct{}
	closeOnce        sync.Once
	chatID           atomic.Value
	ctx              context.Context
	cancel           context.CancelFunc
	cancelCommand    context.CancelFunc
	alive            atomic.Bool
	permissionBridge *agyPermissionBridge

	mu   sync.Mutex
	turn *agyTurn
}

type agyTurn struct {
	files     []string
	timer     *time.Timer
	timeout   bool
	firstText bool
}

type agyUsage struct {
	Input     int `json:"input_tokens"`
	Output    int `json:"output_tokens"`
	CacheRead int `json:"cache_read_tokens"`
}

type agyEvent struct {
	Event          string `json:"event"`
	ConversationID string `json:"conversation_id"`
	StepUpdate     *struct {
		ConversationID string `json:"conversation_id"`
		Type           string `json:"step_type"`
		TextDelta      string `json:"text_delta"`
	} `json:"step_update"`
	Result *struct {
		ConversationID string   `json:"conversation_id"`
		Status         string   `json:"status"`
		Response       string   `json:"response"`
		Error          string   `json:"error"`
		Usage          agyUsage `json:"usage"`
	} `json:"result"`
}

func newAntigravitySession(ctx context.Context, cliBin string, extraArgs []string, workDir, model, mode, resumeID string, extraEnv []string, timeout time.Duration) (*antigravitySession, error) {
	sessionCtx, cancel := context.WithCancel(ctx)
	as := &antigravitySession{
		workDir: workDir,
		timeout: timeout,
		events:  make(chan core.Event, 64),
		done:    make(chan struct{}),
		ctx:     sessionCtx,
		cancel:  cancel,
	}
	if resumeID == core.ContinueSession {
		resumeID = ""
	}
	as.chatID.Store(resumeID)

	agyConfigDir := ""
	if mode == "default" {
		bridge, err := newAgyPermissionBridge(sessionCtx, as.events)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("antigravity: initialize permission bridge: %w", err)
		}
		as.permissionBridge = bridge
		agyConfigDir = bridge.AgyConfigDir()
	}
	started := false
	defer func() {
		if !started {
			cancel()
			if as.permissionBridge != nil {
				as.permissionBridge.Close()
			}
		}
	}()

	args := buildAntigravityArgs(cliBin, extraArgs, model, mode, resumeID, agyConfigDir)
	commandCtx, cancelCommand := context.WithCancel(sessionCtx)
	as.cancelCommand = cancelCommand
	cmd := exec.CommandContext(commandCtx, cliBin, args...)
	prepareCmdForKill(cmd)
	cmd.Cancel = func() error { return forceKillCmd(cmd) }
	cmd.WaitDelay = time.Second
	cmd.Dir = workDir
	cmd.Env = core.MergeEnv(os.Environ(), extraEnv)
	if as.permissionBridge != nil {
		cmd.Env = core.MergeEnv(cmd.Env, as.permissionBridge.Env())
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("antigravity: stdin pipe: %w", err)
	}
	// Own the read end so Wait cannot discard final NDJSON buffered in the pipe.
	stdout, stdoutWriter, err := os.Pipe()
	if err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("antigravity: stdout pipe: %w", err)
	}
	cmd.Stdout = stdoutWriter
	var stderr agyStderr
	cmd.Stderr = &stderr

	slog.Debug("antigravitySession: launching", "resume", resumeID != "", "args", core.RedactArgs(args))
	err = cmd.Start()
	_ = stdoutWriter.Close()
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, fmt.Errorf("antigravity: start: %w", err)
	}

	as.cmd = cmd
	as.stdin = stdin
	as.alive.Store(true)
	started = true
	go as.readLoop(stdout, &stderr)
	return as, nil
}

func buildAntigravityArgs(cliBin string, extraArgs []string, model, mode, resumeID, agyConfigDir string) []string {
	args := append([]string{}, extraArgs...)
	if agyConfigDir != "" {
		args = append(args, "--gemini_dir="+agyConfigDir, "--print-timeout=24h")
	}
	if resumeID != "" {
		args = append(args, "--conversation", resumeID)
	}
	// Existing wrapper commands own their model flags. Direct agy supports
	// the configured model, unless explicit CLI arguments already select one.
	if filepath.Base(cliBin) == "agy" && strings.TrimSpace(model) != "" {
		explicitModel := false
		for _, arg := range extraArgs {
			if arg == "--model" || strings.HasPrefix(arg, "--model=") {
				explicitModel = true
			}
		}
		if !explicitModel {
			args = append(args, "--model", model)
		}
	}
	switch mode {
	case "yolo":
		args = append(args, "--dangerously-skip-permissions")
	case "plan":
		args = append(args, "--sandbox")
	}
	// agy 1.5.12 requires a value for -p. An explicit empty value tells
	// print mode to take each prompt from the NDJSON stream instead.
	return append(args, "--input-format", "stream-json", "--output-format", "stream-json", "-p=")
}

func (as *antigravitySession) Send(prompt string, messageID string, images []core.ImageAttachment, files []core.FileAttachment) error {
	if !as.Alive() {
		return fmt.Errorf("antigravity: session process is not running")
	}

	// Save images and files into the workspace
	attachDir := filepath.Join(as.workDir, ".cc-connect", "attachments")
	if (len(images) > 0 || len(files) > 0) && os.MkdirAll(attachDir, 0o755) != nil {
		attachDir = os.TempDir()
	}

	var imageRefs []string
	for i, img := range images {
		ext := ".png"
		switch img.MimeType {
		case "image/jpeg":
			ext = ".jpg"
		case "image/gif":
			ext = ".gif"
		case "image/webp":
			ext = ".webp"
		}
		fname := fmt.Sprintf("img_%d_%d%s", time.Now().UnixMilli(), i, ext)
		fpath := filepath.Join(attachDir, fname)
		if err := os.WriteFile(fpath, img.Data, 0o600); err == nil {
			imageRefs = append(imageRefs, fpath)
		}
	}

	var fileRefs []string
	for i, f := range files {
		fname := filepath.Base(f.FileName)
		if fname == "" || fname == "." || fname == ".." {
			fname = fmt.Sprintf("file_%d_%d", time.Now().UnixMilli(), i)
		}
		fpath := filepath.Join(attachDir, fname)
		if err := os.WriteFile(fpath, f.Data, 0o600); err == nil {
			fileRefs = append(fileRefs, fpath)
		}
	}

	// Attach image and file references to prompt
	fullPrompt := prompt
	if len(imageRefs) > 0 {
		if fullPrompt == "" {
			fullPrompt = "Please analyze the attached image(s)."
		}
		fullPrompt += "\n\n[Attached images saved at: " + strings.Join(imageRefs, ", ") + "]"
	}
	if len(fileRefs) > 0 {
		if fullPrompt == "" {
			fullPrompt = "Please analyze the attached file(s)."
		}
		fullPrompt += "\n\n[Attached files saved at: " + strings.Join(fileRefs, ", ") + "]"
	}
	turn := &agyTurn{files: append(imageRefs, fileRefs...)}
	as.mu.Lock()
	if !as.Alive() || as.turn != nil {
		as.mu.Unlock()
		turn.cleanup()
		return fmt.Errorf("antigravity: session process is busy or closed")
	}
	as.turn = turn
	if as.timeout > 0 {
		turn.timer = time.AfterFunc(as.timeout, func() {
			as.mu.Lock()
			defer as.mu.Unlock()
			if as.turn == turn {
				turn.timeout = true
				as.cancelCommand()
			}
		})
	}
	as.mu.Unlock()

	if err := as.writeJSON(map[string]any{
		"event": "user",
		"message": map[string]any{
			"role":    "user",
			"content": []map[string]string{{"type": "text", "text": fullPrompt}},
		},
	}); err != nil {
		as.takeTurn(turn)
		as.cancel()
		return fmt.Errorf("antigravity: write prompt: %w", err)
	}
	return nil
}

func (as *antigravitySession) writeJSON(value any) error {
	return json.NewEncoder(as.stdin).Encode(value)
}

func (as *antigravitySession) takeTurn(expected *agyTurn) *agyTurn {
	as.mu.Lock()
	defer as.mu.Unlock()
	turn := as.turn
	if turn == nil || (expected != nil && expected != turn) {
		return nil
	}
	as.turn = nil
	turn.cleanup()
	return turn
}

func (turn *agyTurn) cleanup() {
	if turn.timer != nil {
		turn.timer.Stop()
	}
	for _, name := range turn.files {
		_ = os.Remove(name)
	}
}

func (as *antigravitySession) readLoop(stdout *os.File, stderr *agyStderr) {
	waitErrCh := make(chan error, 1)
	drained := make(chan struct{})
	go func() {
		waitErrCh <- as.cmd.Wait()
		// A tool descendant may inherit stdout. Give the scanner time to
		// consume buffered records, then unblock it if the pipe is still open.
		select {
		case <-drained:
		case <-time.After(time.Second):
			_ = stdout.Close()
		}
	}()
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	var usage agyUsage
	for scanner.Scan() {
		as.handleReadLoopLine(scanner.Bytes(), &usage)
	}
	close(drained)
	_ = stdout.Close()
	if scanner.Err() != nil {
		as.cancelCommand()
	}
	err := <-waitErrCh
	as.alive.Store(false)
	turn := as.takeTurn(nil)
	if turn != nil {
		if turn.timeout {
			as.emit(core.Event{Type: core.EventError, Error: fmt.Errorf("antigravity: turn timed out after %s", as.timeout)})
		} else if as.ctx.Err() == nil {
			detail := strings.TrimSpace(stderr.String())
			switch {
			case detail != "":
				err = fmt.Errorf("antigravity: process exited before result: %s", detail)
			case scanner.Err() != nil:
				err = fmt.Errorf("antigravity: read stdout: %w", scanner.Err())
			case err != nil:
				err = fmt.Errorf("antigravity: process exited before result: %w", err)
			default:
				err = errors.New("antigravity: process exited before result")
			}
			as.emit(core.Event{Type: core.EventError, Error: err})
		}
	}
	as.cancel()
	if as.permissionBridge != nil {
		as.permissionBridge.Close()
	}
	close(as.events)
	close(as.done)
}

func (as *antigravitySession) handleReadLoopLine(line []byte, usage *agyUsage) {
	var record agyEvent
	if err := json.Unmarshal(line, &record); err != nil {
		slog.Debug("antigravitySession: ignoring non-JSON stdout", "error", err)
		return
	}
	switch record.Event {
	case "init":
		as.setConversationID(record.ConversationID)
	case "step_update":
		step := record.StepUpdate
		if step == nil {
			return
		}
		as.setConversationID(step.ConversationID)
		if step.Type == "agent_response" && step.TextDelta != "" {
			as.mu.Lock()
			if turn := as.turn; turn != nil && !turn.firstText && strings.TrimSpace(step.TextDelta) != "" {
				turn.firstText = true
				slog.Debug("antigravitySession: first response text", "pid", as.cmd.Process.Pid, "session_id", as.CurrentSessionID())
			}
			as.mu.Unlock()
			as.emit(core.Event{Type: core.EventText, Content: step.TextDelta, SessionID: as.CurrentSessionID()})
		}
	case "result":
		result := record.Result
		if result == nil {
			return
		}
		as.setConversationID(result.ConversationID)
		turn := as.takeTurn(nil)
		if turn == nil {
			return
		}
		tokens := result.Usage
		event := core.Event{
			Type: core.EventResult, Content: result.Response, Done: true, SessionID: as.CurrentSessionID(),
			InputTokens:          max(0, tokens.Input-usage.Input),
			OutputTokens:         max(0, tokens.Output-usage.Output),
			CacheReadInputTokens: max(0, tokens.CacheRead-usage.CacheRead),
		}
		*usage = tokens
		if turn.timeout {
			as.emit(core.Event{Type: core.EventError, Error: fmt.Errorf("antigravity: turn timed out after %s", as.timeout)})
			return
		}
		if result.Status != "SUCCESS" {
			detail := result.Error
			if detail == "" {
				detail = fmt.Sprintf("turn ended with status %q", result.Status)
			}
			event = core.Event{Type: core.EventError, Error: fmt.Errorf("antigravity: %s", detail)}
		}
		as.emit(event)
	default:
		slog.Debug("antigravitySession: unrecognized event type", "event", record.Event)
	}
}

func (as *antigravitySession) setConversationID(id string) {
	if id != "" && id != as.CurrentSessionID() {
		as.chatID.Store(id)
		// Persist the ID even if agy dies before the first text delta.
		as.emit(core.Event{Type: core.EventText, SessionID: id})
	}
}

func (as *antigravitySession) emit(event core.Event) {
	select {
	case as.events <- event:
	case <-as.ctx.Done():
	}
}

// agyStderr retains a bounded tail for a possible exit error. cmd.Wait
// finishes the stderr copy before readLoop reads it.
type agyStderr struct{ bytes.Buffer }

func (s *agyStderr) Write(data []byte) (int, error) {
	const limit = 32 * 1024
	n := len(data)
	if n >= limit {
		s.Reset()
		data = data[n-limit:]
	} else if s.Len()+n > limit {
		s.Next(s.Len() + n - limit)
	}
	_, err := s.Buffer.Write(data)
	return n, err
}

func (as *antigravitySession) RespondPermission(requestID string, result core.PermissionResult) error {
	if !as.alive.Load() {
		return fmt.Errorf("session is closed")
	}
	if as.permissionBridge == nil {
		return fmt.Errorf("antigravity: permission responses are only available in default mode")
	}
	return as.permissionBridge.RespondPermission(requestID, result)
}

func (as *antigravitySession) Events() <-chan core.Event {
	return as.events
}

func (as *antigravitySession) CurrentSessionID() string {
	v, _ := as.chatID.Load().(string)
	return v
}

func (as *antigravitySession) Alive() bool {
	return as.alive.Load()
}

func (as *antigravitySession) Close() error {
	as.closeOnce.Do(func() {
		as.alive.Store(false)
		as.mu.Lock()
		active := as.turn != nil
		as.mu.Unlock()

		// EOF lets idle agy flush the conversation. /stop must interrupt an
		// active turn immediately rather than waiting for it to finish.
		_ = as.stdin.Close()
		if !active {
			select {
			case <-as.done:
				return
			case <-time.After(2 * time.Second):
			}
		}
		if err := signalProcessGroup(as.cmd, syscall.SIGTERM); err != nil {
			slog.Warn("antigravitySession: signal SIGTERM", "error", err)
		}
		select {
		case <-as.done:
			return
		case <-time.After(2 * time.Second):
		}
		as.cancel()
		<-as.done
	})
	return nil
}
