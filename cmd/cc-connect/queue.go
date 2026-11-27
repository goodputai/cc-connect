package main

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type queueCommandItem struct {
	ID, State, Result, Board, Line, Title, Due, Owner, Prompt string
	Seq                                                       int64
}

type queueCommandClient struct {
	http   *http.Client
	origin string
	token  string
}

func newQueueCommandClient(center, tokenFile string) (*queueCommandClient, error) {
	addr, err := url.Parse(center)
	if err != nil || addr.Host == "" || addr.User != nil ||
		(addr.Scheme != "http" && addr.Scheme != "https") ||
		(addr.Path != "" && addr.Path != "/") || addr.RawQuery != "" || addr.Fragment != "" {
		return nil, errors.New("set CCQ_CENTER or --center to the queue HTTP(S) origin")
	}
	data, err := os.ReadFile(tokenFile)
	if err != nil {
		return nil, fmt.Errorf("read queue token file: %w", err)
	}
	token := strings.TrimSpace(string(data))
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return nil, errors.New("queue token file must contain one nonempty token")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	return &queueCommandClient{client, strings.TrimSuffix(addr.String(), "/"), token}, nil
}

func (c *queueCommandClient) call(method, path string, payload any) (json.RawMessage, error) {
	var data []byte
	var err error
	if payload != nil {
		if data, err = json.Marshal(payload); err != nil {
			return nil, err
		}
	}
	req, err := http.NewRequest(method, c.origin+"/v1/queue/"+path, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("queue HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var raw json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func runQueue(args []string) {
	if err := queueCommand(args, os.Stdout); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func queueCommand(args []string, out io.Writer) error {
	if len(args) == 0 || args[0] == "--help" || args[0] == "-h" {
		fmt.Fprintln(out, "Usage: cc-connect queue add|move|due|list|bottleneck [flags]")
		return nil
	}
	cmd := args[0]
	home, _ := os.UserHomeDir()
	tokenDefault := os.Getenv("CCQ_TOKEN_FILE")
	if tokenDefault == "" {
		tokenDefault = filepath.Join(home, ".cc-connect", "queue", "center.token")
	}
	f := flag.NewFlagSet("cc-connect queue "+cmd, flag.ContinueOnError)
	board := f.String("board", "", "board name (A, B or DH)")
	center := f.String("center", os.Getenv("CCQ_CENTER"), "queue center HTTP(S) origin")
	tokenFile := f.String("token-file", tokenDefault, "queue bearer token file")
	var id, title, line, owner, by, due, source, waiting, priority, follows, state, evidence, reason, idem string
	var instance, project, session, prompt string
	var open, overdue, jsonOutput bool
	var limit int
	switch cmd {
	case "add":
		f.StringVar(&title, "item", "", "item title")
		f.StringVar(&line, "line", "", "work line")
		f.StringVar(&owner, "owner", "", "item owner")
		f.StringVar(&by, "by", "", "who registered the item")
		f.StringVar(&source, "source", "", "source link or reference")
		f.StringVar(&priority, "priority", "", "priority P0 through P3")
		f.StringVar(&follows, "follows", "", "completed blocker this item resolves")
		f.StringVar(&idem, "idempotency-key", "", "reuse this key when retrying an add")
		f.StringVar(&due, "due", "", "due date (Beijing YYYY-MM-DD HH:MM or RFC3339)")
		f.StringVar(&waiting, "waiting", "", "dependency ID(s), comma-separated, or waiting reason")
	case "move":
		f.StringVar(&id, "id", "", "item ID")
		f.StringVar(&state, "to", "", "new state (待办, 进行中, 完成 or 取消)")
		f.StringVar(&evidence, "evidence", "", "completion evidence or cancellation reason")
		f.StringVar(&waiting, "waiting", "", "dependency ID(s), comma-separated, or waiting reason")
	case "due":
		f.StringVar(&id, "id", "", "item ID")
		f.StringVar(&due, "due", "", "new due date (Beijing YYYY-MM-DD HH:MM or RFC3339)")
		f.StringVar(&reason, "reason", "", "reason for changing the due date")
	case "list", "bottleneck":
		f.StringVar(&line, "line", "", "filter by work line")
		f.StringVar(&state, "state", "", "filter by state")
		f.BoolVar(&jsonOutput, "json", false, "print JSON")
		if cmd == "list" {
			f.BoolVar(&open, "open", false, "show open items")
			f.BoolVar(&overdue, "overdue", false, "show overdue open items")
		} else {
			f.IntVar(&limit, "limit", 10, "maximum number of bottlenecks")
		}
	default:
		return fmt.Errorf("unknown queue command %q", cmd)
	}
	if cmd == "add" || cmd == "move" {
		f.StringVar(&instance, "instance", "", "executor instance")
		f.StringVar(&project, "project", "", "executor project")
		f.StringVar(&session, "session", "", "executor session key")
		f.StringVar(&prompt, "prompt", "", "instructions to send to the executor")
	}
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return fmt.Errorf("unexpected argument %q", f.Arg(0))
	}
	if *board == "" && cmd != "bottleneck" {
		return errors.New("--board is required")
	}
	var executor map[string]string
	if instance != "" || project != "" || session != "" {
		if instance == "" || project == "" || session == "" {
			return errors.New("--instance, --project and --session must be supplied together")
		}
		executor = map[string]string{"instance": instance, "project": project, "session_key": session}
	}
	client, err := newQueueCommandClient(*center, *tokenFile)
	if err != nil {
		return err
	}

	switch cmd {
	case "add":
		if strings.TrimSpace(title) == "" || line == "" || owner == "" || by == "" {
			return errors.New("--item, --line, --owner and --by are required")
		}
		if idem == "" {
			idem = "board:" + rand.Text()
		}
		body := map[string]any{"idempotency_key": idem, "board": *board, "title": title,
			"line": line, "owner": owner, "by": by, "due": due, "source_ref": source,
			"priority": priority, "follows": follows}
		if executor != nil {
			body["executor"] = executor
			if prompt == "" {
				prompt = title
			}
			body["prompt"] = prompt
		}
		if waiting != "" {
			body["waits_on"] = queueCommandWaits(waiting)
		}
		raw, err := client.call(http.MethodPost, "items", body)
		if err != nil {
			return err
		}
		var item queueCommandItem
		if err := json.Unmarshal(raw, &item); err != nil {
			return err
		}
		fmt.Fprintln(out, item.ID)
		return nil
	case "move", "due":
		if id == "" {
			return errors.New("--id is required")
		}
		path := "items/" + url.PathEscape(id)
		raw, err := client.call(http.MethodGet, path, nil)
		if err != nil {
			return err
		}
		var prior queueCommandItem
		if err := json.Unmarshal(raw, &prior); err != nil {
			return err
		}
		if prior.Board != *board {
			return fmt.Errorf("item %s is on board %q", id, prior.Board)
		}
		body := map[string]any{}
		if cmd == "move" {
			if state == "" {
				return errors.New("--to is required")
			}
			body["state"] = state
			if evidence != "" {
				body["evidence"] = []string{evidence}
			}
			f.Visit(func(v *flag.Flag) {
				if v.Name == "waiting" {
					body["waits_on"] = queueCommandWaits(waiting)
				}
			})
			if executor != nil {
				body["executor"] = executor
			}
			if prompt != "" {
				body["prompt"] = prompt
			}
		} else {
			if strings.TrimSpace(due) == "" || strings.TrimSpace(reason) == "" {
				return errors.New("--due and --reason are required")
			}
			body["due"], body["reason"] = due, reason
		}
		if _, err := client.call(http.MethodPatch, path, body); err != nil {
			return err
		}
		if cmd == "move" {
			fmt.Fprintf(out, "%s %s -> %s\n", id, prior.State, state)
		} else {
			fmt.Fprintf(out, "%s 截止 %s -> %s（%s）\n", id, prior.Due, due, reason)
		}
		return nil
	}
	q := url.Values{"board": {*board}, "line": {line}, "state": {state}}
	if cmd == "list" {
		q.Set("open", strconv.FormatBool(open))
		q.Set("overdue", strconv.FormatBool(overdue))
		return queueCommandList(out, client, q, jsonOutput)
	}
	q.Set("limit", strconv.Itoa(limit))
	raw, err := client.call(http.MethodGet, "bottleneck?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	if jsonOutput {
		fmt.Fprintln(out, string(raw))
		return nil
	}
	var rows []struct {
		Item   queueCommandItem
		Reason string
	}
	if err := json.Unmarshal(raw, &rows); err != nil {
		return err
	}
	for _, row := range rows {
		fmt.Fprintf(out, "%s %s %s | %s\n", row.Item.ID, row.Item.Line, row.Item.Title, row.Reason)
	}
	return nil
}

func queueCommandWaits(value string) any {
	ids := strings.Split(value, ",")
	for i := range ids {
		ids[i] = strings.TrimSpace(ids[i])
		if !strings.HasPrefix(ids[i], "q_") {
			return value
		}
	}
	return ids
}

func queueCommandList(out io.Writer, client *queueCommandClient, q url.Values, jsonOutput bool) error {
	type row struct {
		item queueCommandItem
		raw  json.RawMessage
		due  time.Time
	}
	q.Set("limit", "1000")
	var all []row
	for {
		raw, err := client.call(http.MethodGet, "items?"+q.Encode(), nil)
		if err != nil {
			return err
		}
		var records []json.RawMessage
		if err := json.Unmarshal(raw, &records); err != nil {
			return err
		}
		for _, record := range records {
			var item queueCommandItem
			if err := json.Unmarshal(record, &item); err != nil {
				return err
			}
			due, err := time.Parse(time.RFC3339, item.Due)
			if err != nil {
				due, _ = time.ParseInLocation("2006-01-02 15:04", item.Due, time.FixedZone("+08:00", 8*3600))
			}
			all = append(all, row{item, record, due})
		}
		if len(records) < 1000 {
			break
		}
		q.Set("after", strconv.FormatInt(all[len(all)-1].item.Seq, 10))
	}
	sort.Slice(all, func(i, j int) bool {
		a, b := all[i], all[j]
		if a.due.Equal(b.due) {
			return a.item.ID < b.item.ID
		}
		return !a.due.IsZero() && (b.due.IsZero() || a.due.Before(b.due))
	})
	for _, row := range all {
		if jsonOutput {
			fmt.Fprintln(out, string(row.raw))
			continue
		}
		item := row.item
		fmt.Fprintf(out, "%s [%s] 截止 %s %s | %s | 负责人 %s\n", item.ID, item.State, item.Due, item.Line, item.Title, item.Owner)
	}
	return nil
}
