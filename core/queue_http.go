package core

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	queuePollInterval = 2 * time.Second
	queueHTTPTimeout  = 5 * time.Second
	queueBodyLimit    = 1 << 20
)

// QueuePeerConfig identifies an executor as seen by the center.
type QueuePeerConfig struct {
	Name, URL, TokenFile string
}

type QueueCenterConfig struct {
	Address, TokenFile, Instance string
	Peers                        []QueuePeerConfig
}

type QueueExecutorConfig struct {
	Address, TokenFile, Instance string
	CenterURL, CenterTokenFile   string
}

type queueSession struct {
	Instance      string     `json:"instance"`
	Project       string     `json:"project"`
	SessionKey    string     `json:"session_key"`
	ChatName      string     `json:"chat_name"`
	State         string     `json:"state"`
	TurnStartedAt *time.Time `json:"turn_started_at"`
	TurnSource    string     `json:"turn_source"`
	QueueDepth    int        `json:"queue_depth"`
	Halted        bool       `json:"halted"`
	ItemID        string     `json:"item_id,omitempty"`
}

type queueFleetNode struct {
	Instance  string         `json:"instance"`
	Reachable bool           `json:"reachable"`
	FetchedAt *time.Time     `json:"fetched_at"`
	Sessions  []queueSession `json:"sessions"`
}

type queueRemote struct {
	name, url, token string
	client           *http.Client
}

func readQueueToken(path string) (string, error) {
	if path == "" {
		return "", errors.New("queue token file is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read queue token file: %w", err)
	}
	token := strings.TrimSpace(string(data))
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return "", errors.New("queue token file must contain one nonempty token")
	}
	return token, nil
}

func newQueueRemote(cfg QueuePeerConfig) (*queueRemote, error) {
	target, err := url.Parse(cfg.URL)
	if err != nil || target.Host == "" || (target.Scheme != "http" && target.Scheme != "https") ||
		target.User != nil || target.RawQuery != "" || target.Fragment != "" ||
		(target.Path != "" && target.Path != "/") {
		return nil, errors.New("queue peer URL must be an HTTP(S) origin")
	}
	token, err := readQueueToken(cfg.TokenFile)
	if err != nil {
		return nil, err
	}
	// Queue peers talk directly over the private network.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	client := &http.Client{Transport: transport, Timeout: queueHTTPTimeout, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	return &queueRemote{cfg.Name, strings.TrimRight(target.String(), "/"), token, client}, nil
}

func (p *queueRemote) fetchSessions(ctx context.Context) ([]queueSession, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.url+"/x/sessions", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+p.token)
	res, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("executor session lookup returned HTTP %d", res.StatusCode)
	}
	var sessions []queueSession
	if err := json.NewDecoder(res.Body).Decode(&sessions); err != nil {
		return nil, fmt.Errorf("decode executor sessions: %w", err)
	}
	for i := range sessions {
		sessions[i].Instance = p.name
	}
	return sessions, nil
}

func queueBearerHandler(mux *http.ServeMux, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, pattern := mux.Handler(r); pattern == "" {
			http.NotFound(w, r)
			return
		}
		value, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(value), []byte(token)) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (s *APIServer) startQueueHTTP(address, token string, mux *http.ServeMux) (context.Context, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil || host == "" || (net.ParseIP(host) != nil && net.ParseIP(host).IsUnspecified()) {
		return nil, errors.New("queue listener requires an explicit host and port")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen on queue address: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	server := &http.Server{
		Handler:           queueBearerHandler(mux, token),
		ReadHeaderTimeout: queueHTTPTimeout,
		ReadTimeout:       queueHTTPTimeout,
		WriteTimeout:      2 * queueHTTPTimeout,
		IdleTimeout:       time.Minute,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	s.queueHTTP, s.queueCancel = server, cancel
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("queue HTTP server failed", "error", err)
		}
	}()
	slog.Info("queue HTTP server started", "address", listener.Addr().String())
	return ctx, nil
}

type queueFleet struct {
	peers []*queueRemote
	mu    sync.RWMutex
	nodes map[string]queueFleetNode
}

func (f *queueFleet) poll(ctx context.Context) {
	next := make([]queueFleetNode, len(f.peers))
	var wg sync.WaitGroup
	for i, peer := range f.peers {
		wg.Go(func() {
			sessions, err := peer.fetchSessions(ctx)
			if err != nil {
				next[i] = queueFleetNode{Instance: peer.name, Sessions: []queueSession{}}
				return
			}
			now := time.Now()
			next[i] = queueFleetNode{peer.name, true, &now, sessions}
		})
	}
	wg.Wait()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, node := range next {
		if !node.Reachable {
			node.FetchedAt = f.nodes[node.Instance].FetchedAt
		}
		f.nodes[node.Instance] = node
	}
}

func (f *queueFleet) run(ctx context.Context) {
	ticker := time.NewTicker(queuePollInterval)
	defer ticker.Stop()
	for {
		f.poll(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (f *queueFleet) snapshot() []queueFleetNode {
	f.mu.RLock()
	defer f.mu.RUnlock()
	nodes := make([]queueFleetNode, 0, len(f.peers)+1)
	for _, peer := range f.peers {
		node, ok := f.nodes[peer.name]
		if !ok {
			node = queueFleetNode{Instance: peer.name, Sessions: []queueSession{}}
		}
		nodes = append(nodes, node)
	}
	return nodes
}

func (s *APIServer) StartQueueCenter(cfg QueueCenterConfig, dataDir string) error {
	token, err := readQueueToken(cfg.TokenFile)
	if err != nil {
		return err
	}
	if cfg.Instance == "" {
		return errors.New("queue center instance is required")
	}
	fleet := &queueFleet{nodes: make(map[string]queueFleetNode)}
	seen := map[string]bool{cfg.Instance: true}
	for _, peer := range cfg.Peers {
		if peer.Name == "" || seen[peer.Name] {
			return errors.New("queue executor names must be nonempty and unique")
		}
		remote, err := newQueueRemote(peer)
		if err != nil {
			return fmt.Errorf("queue executor %q: %w", peer.Name, err)
		}
		seen[peer.Name] = true
		fleet.peers = append(fleet.peers, remote)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/fleet", func(w http.ResponseWriter, r *http.Request) {
		now := time.Now()
		local := queueFleetNode{cfg.Instance, true, &now, s.queueSessions(cfg.Instance)}
		apiJSON(w, http.StatusOK, append([]queueFleetNode{local}, fleet.snapshot()...))
	})
	ctx, err := s.startQueueHTTP(cfg.Address, token, mux)
	if err != nil {
		return err
	}
	go fleet.run(ctx)
	return nil
}

func (s *APIServer) StartQueueExecutor(cfg QueueExecutorConfig) error {
	token, err := readQueueToken(cfg.TokenFile)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /x/sessions", func(w http.ResponseWriter, r *http.Request) {
		apiJSON(w, http.StatusOK, s.queueSessions(cfg.Instance))
	})
	_, err = s.startQueueHTTP(cfg.Address, token, mux)
	return err
}

func (s *APIServer) queueSessions(instance string) []queueSession {
	s.mu.RLock()
	engines := make(map[string]*Engine, len(s.engines))
	for name, engine := range s.engines {
		engines[name] = engine
	}
	s.mu.RUnlock()

	result := make([]queueSession, 0)
	for name, engine := range engines {
		for key, row := range engine.queueSessionSnapshot() {
			row.Instance, row.Project, row.SessionKey = instance, name, key
			result = append(result, row)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Project != result[j].Project {
			return result[i].Project < result[j].Project
		}
		return result[i].SessionKey < result[j].SessionKey
	})
	return result
}

func (e *Engine) queueSessionSnapshot() map[string]queueSession {
	rows := make(map[string]queueSession)
	e.sessions.appendQueueSessions(rows)
	e.interactiveMu.Lock()
	pool := e.workspacePool
	e.interactiveMu.Unlock()
	if pool != nil {
		for _, workspace := range pool.All() {
			workspace.mu.Lock()
			manager := workspace.sessions
			workspace.mu.Unlock()
			if manager != nil {
				manager.appendQueueSessions(rows)
			}
		}
	}
	return rows
}

func (sm *SessionManager) appendQueueSessions(rows map[string]queueSession) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for key, id := range sm.activeSession {
		session := sm.sessions[id]
		if session == nil {
			continue
		}
		session.mu.Lock()
		if session.Retired {
			session.mu.Unlock()
			continue
		}
		row := queueSession{State: "idle"}
		if session.busy {
			row.State = "starting"
			if !session.queueTurnStart.IsZero() {
				start := session.queueTurnStart
				row.State, row.TurnStartedAt = "busy", &start
				row.TurnSource, row.ItemID = session.queueTurnSource, session.queueItemID
			}
		}
		session.mu.Unlock()
		if meta := sm.userMeta[key]; meta != nil {
			row.ChatName = meta.ChatName
		}
		if previous, ok := rows[key]; !ok || previous.State == "idle" {
			rows[key] = row
		}
	}
}
