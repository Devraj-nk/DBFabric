// Package simulator exposes a control plane for exploring DBFabric's routing
// and failover decisions without requiring a live PostgreSQL cluster.
package simulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"dbfabric/internal/config"
	"dbfabric/internal/router"
	"dbfabric/internal/shardmap"
)

type Server struct {
	httpServer *http.Server
	sim        *Simulation
}

func NewServer(cfg *config.Config, addr string) *Server {
	s := &Server{sim: New(cfg)}
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/state", s.handleState)
	mux.HandleFunc("/api/route", s.handleRoute)
	mux.HandleFunc("/api/simulate", s.handleSimulate)
	s.httpServer = &http.Server{Addr: addr, Handler: cors(mux), ReadHeaderTimeout: 3 * time.Second}
	return s
}

func (s *Server) Run() error { return s.httpServer.ListenAndServe() }

func (s *Server) Shutdown(ctx context.Context) error { return s.httpServer.Shutdown(ctx) }

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(indexHTML))
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, s.sim.State())
}

func (s *Server) handleRoute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var input routeRequest
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	result, err := s.sim.Route(input)
	if err != nil {
		writeJSON(w, routeResponse{Error: err.Error()})
		return
	}
	writeJSON(w, result)
}

func (s *Server) handleSimulate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var input actionRequest
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.sim.Action(input); err != nil {
		writeJSON(w, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, s.sim.State())
}

type routeRequest struct {
	ShardKey    string `json:"shard_key"`
	Consistency string `json:"consistency"`
	MaxLagMS    int64  `json:"max_lag_ms"`
}

type routeResponse struct {
	ShardID     string `json:"shard_id"`
	Node        string `json:"node"`
	Role        string `json:"role"`
	Status      string `json:"status"`
	LagMS       int64  `json:"lag_ms"`
	Consistency string `json:"consistency"`
	Error       string `json:"error,omitempty"`
}

type actionRequest struct {
	Action string `json:"action"`
	Shard  string `json:"shard"`
	Node   string `json:"node"`
	LagMS  int64  `json:"lag_ms"`
}

type Simulation struct {
	mu       sync.RWMutex
	config   *config.Config
	shardMap *shardmap.Map
	router   *router.Router
	events   []string
}

func New(cfg *config.Config) *Simulation {
	s := &Simulation{config: cfg, shardMap: shardmap.New()}
	s.router = router.New(s.shardMap)
	for _, item := range cfg.Shards {
		shard := &shardmap.Shard{ID: item.ID, Primary: shardmap.Node{Addr: item.Primary, Status: shardmap.StatusUp}}
		for _, addr := range item.Replicas {
			shard.Replicas = append(shard.Replicas, shardmap.Node{Addr: addr, Status: shardmap.StatusUp})
		}
		s.router.AddShard(shard)
	}
	return s
}

type state struct {
	ListenAddr string       `json:"listen_addr"`
	MaxLagMS   int64        `json:"max_lag_ms"`
	Shards     []stateShard `json:"shards"`
	Events     []string     `json:"events"`
}

type stateShard struct {
	ID       string      `json:"id"`
	Primary  stateNode   `json:"primary"`
	Replicas []stateNode `json:"replicas"`
}

type stateNode struct {
	Addr   string `json:"addr"`
	Status string `json:"status"`
	LagMS  int64  `json:"lag_ms"`
}

func nodeState(node shardmap.Node) stateNode {
	status := string(node.Status)
	if status == "" {
		status = string(shardmap.StatusUp)
	}
	return stateNode{Addr: node.Addr, Status: status, LagMS: node.LagMS}
}

func (s *Simulation) State() state {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := state{ListenAddr: s.config.ListenAddr, MaxLagMS: s.config.Routing.DefaultMaxLagMS}
	for _, shard := range s.shardMap.All() {
		item := stateShard{ID: shard.ID, Primary: nodeState(shard.Primary)}
		for _, replica := range shard.Replicas {
			item.Replicas = append(item.Replicas, nodeState(replica))
		}
		result.Shards = append(result.Shards, item)
	}
	result.Events = append([]string(nil), s.events...)
	return result
}

func (s *Simulation) Route(input routeRequest) (routeResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	consistency := router.Consistency(strings.ToLower(input.Consistency))
	if consistency == "" {
		consistency = router.Strong
	}
	maxLag := input.MaxLagMS
	if maxLag == 0 {
		maxLag = s.config.Routing.DefaultMaxLagMS
	}
	node, err := s.router.Resolve(input.ShardKey, consistency, maxLag)
	if err != nil {
		return routeResponse{}, err
	}
	shardID, role := "", "replica"
	for _, shard := range s.shardMap.All() {
		if shard.Primary.Addr == node.Addr {
			shardID, role = shard.ID, "primary"
		}
		for _, replica := range shard.Replicas {
			if replica.Addr == node.Addr {
				shardID = shard.ID
			}
		}
	}
	return routeResponse{ShardID: shardID, Node: node.Addr, Role: role, Status: string(node.Status), LagMS: node.LagMS, Consistency: string(consistency)}, nil
}

func (s *Simulation) Action(input actionRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if input.Action == "reset" {
		fresh := New(s.config)
		s.shardMap, s.router, s.events = fresh.shardMap, fresh.router, nil
		return nil
	}
	shard, ok := s.shardMap.Get(input.Shard)
	if !ok {
		return fmt.Errorf("unknown shard %q", input.Shard)
	}
	copy := *shard
	copy.Replicas = append([]shardmap.Node(nil), shard.Replicas...)
	var event string
	switch input.Action {
	case "toggle":
		if copy.Primary.Addr == input.Node {
			copy.Primary.Status = toggleStatus(copy.Primary.Status)
			event = fmt.Sprintf("%s primary is now %s", copy.ID, displayStatus(copy.Primary.Status))
		} else {
			for i := range copy.Replicas {
				if copy.Replicas[i].Addr == input.Node {
					copy.Replicas[i].Status = toggleStatus(copy.Replicas[i].Status)
					event = fmt.Sprintf("%s replica %s is now %s", copy.ID, input.Node, displayStatus(copy.Replicas[i].Status))
				}
			}
		}
	case "lag":
		for i := range copy.Replicas {
			if copy.Replicas[i].Addr == input.Node {
				copy.Replicas[i].LagMS = input.LagMS
				event = fmt.Sprintf("%s lag set to %dms on %s", copy.ID, input.LagMS, input.Node)
			}
		}
	case "promote":
		candidate := -1
		for i, replica := range copy.Replicas {
			if replica.Status != shardmap.StatusDown && (candidate < 0 || replica.LagMS < copy.Replicas[candidate].LagMS) {
				candidate = i
			}
		}
		if candidate < 0 {
			return fmt.Errorf("no healthy replica available for promotion")
		}
		old := copy.Primary
		copy.Primary = copy.Replicas[candidate]
		copy.Primary.Status = shardmap.StatusUp
		copy.Replicas = append(copy.Replicas[:candidate], copy.Replicas[candidate+1:]...)
		old.Status = shardmap.StatusDown
		copy.Replicas = append(copy.Replicas, old)
		event = fmt.Sprintf("%s promoted %s; old primary %s fenced", copy.ID, copy.Primary.Addr, old.Addr)
	default:
		return fmt.Errorf("unknown action %q", input.Action)
	}
	if event == "" {
		return fmt.Errorf("node %q not found in shard %q", input.Node, input.Shard)
	}
	s.shardMap.Set(copy.ID, &copy)
	s.events = append([]string{time.Now().Format("15:04:05") + "  " + event}, s.events...)
	if len(s.events) > 8 {
		s.events = s.events[:8]
	}
	return nil
}

func toggleStatus(status shardmap.NodeStatus) shardmap.NodeStatus {
	if status == shardmap.StatusDown {
		return shardmap.StatusUp
	}
	return shardmap.StatusDown
}

func displayStatus(status shardmap.NodeStatus) string {
	if status == shardmap.StatusDown {
		return "DOWN"
	}
	return "UP"
}
