package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	pb "github.com/distkv/proto"
	"github.com/distkv/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// HTTPServer wraps our store, raft, and gRPC servers to expose a web UI and JSON API.
type HTTPServer struct {
	store      *store.KVStore
	raftNode   *raft.Raft
	grpcServer *GRPCServer
	nodeID     string
	httpAddr   string
}

// NewHTTPServer creates a new HTTPServer instance.
func NewHTTPServer(kv *store.KVStore, r *raft.Raft, grpcSrv *GRPCServer, nodeID, httpAddr string) *HTTPServer {
	return &HTTPServer{
		store:      kv,
		raftNode:   r,
		grpcServer: grpcSrv,
		nodeID:     nodeID,
		httpAddr:   httpAddr,
	}
}

// Start runs the HTTP server. It blocks until context is cancelled.
func (s *HTTPServer) Start(ctx context.Context) error {
	mux := http.NewServeMux()

	// UI Dashboard Page
	mux.HandleFunc("/", s.handleIndex)

	// API Endpoints
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/keys", s.handleKeys)
	mux.HandleFunc("/api/set", s.handleSet)
	mux.HandleFunc("/api/delete", s.handleDelete)
	mux.HandleFunc("/api/shutdown", s.handleShutdown)

	srv := &http.Server{
		Addr:    s.httpAddr,
		Handler: s.corsMiddleware(mux),
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	log.Printf("[HTTPServer] Web Dashboard listening on http://localhost%s", s.httpAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *HTTPServer) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// JSON responses helper
func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (s *HTTPServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	state := "Standalone"
	leader := "None"
	term := uint64(0)
	peers := []string{}

	if s.raftNode != nil {
		state = s.raftNode.State().String()
		leaderAddr, _ := s.raftNode.LeaderWithID()
		leader = string(leaderAddr)
		if termStr, ok := s.raftNode.Stats()["last_log_term"]; ok {
			if t, err := strconv.ParseUint(termStr, 10, 64); err == nil {
				term = t
			}
		}
		
		// Parse peer configuration
		cfgFuture := s.raftNode.GetConfiguration()
		if err := cfgFuture.Error(); err == nil {
			for _, srv := range cfgFuture.Configuration().Servers {
				peers = append(peers, fmt.Sprintf("%s=%s", srv.ID, srv.Address))
			}
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"node_id":   s.nodeID,
		"state":     state,
		"leader":    leader,
		"term":      term,
		"peers":     peers,
		"num_keys":  s.store.Len(),
		"timestamp": time.Now().Unix(),
	})
}

func (s *HTTPServer) handleKeys(w http.ResponseWriter, r *http.Request) {
	consistency := r.URL.Query().Get("consistency")
	if consistency == "strong" && s.raftNode != nil && s.raftNode.State() != raft.Leader {
		leaderAddr, _ := s.raftNode.LeaderWithID()
		if leaderAddr != "" {
			leaderHttpAddr := strings.Replace(string(leaderAddr), "700", "800", 1)
			resp, err := http.Get(fmt.Sprintf("http://%s/api/keys?consistency=local", leaderHttpAddr))
			if err == nil {
				defer resp.Body.Close()
				var result map[string]interface{}
				if json.NewDecoder(resp.Body).Decode(&result) == nil {
					writeJSON(w, http.StatusOK, result)
					return
				}
			}
		}
	}

	// We capture keys and values directly from the store snapshot (JSON bytes)
	snapBytes, err := s.store.Snapshot()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	var data map[string]string
	if err := json.Unmarshal(snapBytes, &data); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"keys": data,
	})
}

func (s *HTTPServer) handleSet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing key"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := s.grpcServer.Set(ctx, &pb.SetRequest{Key: key, Value: value})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	if !resp.Success && resp.LeaderHint != "" {
		// Forward write request to the leader node
		leaderGrpcAddr := strings.Replace(resp.LeaderHint, "700", "900", 1)
		conn, dialErr := grpc.Dial(leaderGrpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if dialErr == nil {
			defer conn.Close()
			client := pb.NewKVClient(conn)
			fwdResp, fwdErr := client.Set(ctx, &pb.SetRequest{Key: key, Value: value})
			if fwdErr == nil && fwdResp.Success {
				writeJSON(w, http.StatusOK, map[string]interface{}{
					"success":   true,
					"forwarded": true,
					"leader":    resp.LeaderHint,
				})
				return
			}
		}
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *HTTPServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing key"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := s.grpcServer.Delete(ctx, &pb.DeleteRequest{Key: key})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	if !resp.Success && resp.LeaderHint != "" {
		// Forward delete request to the leader node
		leaderGrpcAddr := strings.Replace(resp.LeaderHint, "700", "900", 1)
		conn, dialErr := grpc.Dial(leaderGrpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if dialErr == nil {
			defer conn.Close()
			client := pb.NewKVClient(conn)
			fwdResp, fwdErr := client.Delete(ctx, &pb.DeleteRequest{Key: key})
			if fwdErr == nil && fwdResp.Success {
				writeJSON(w, http.StatusOK, map[string]interface{}{
					"success":   true,
					"forwarded": true,
					"leader":    resp.LeaderHint,
				})
				return
			}
		}
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *HTTPServer) handleShutdown(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"message": "Shutting down node..."})
	log.Printf("[HTTPServer] Received UI shutdown request. Terminating process...")
	go func() {
		time.Sleep(500 * time.Millisecond)
		os.Exit(0)
	}()
}

func (s *HTTPServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(indexHTML))
}

// Embedded Web UI Dashboard HTML/CSS/JS code.
// Built with clean Outfit font, dark mode, glassmorphic layout, and live animations.
const indexHTML = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DistKV Web Console</title>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-dark: #0f111a;
            --card-bg: rgba(22, 28, 45, 0.4);
            --border-color: rgba(255, 255, 255, 0.08);
            --primary: #4f46e5;
            --accent-green: #10b981;
            --accent-red: #ef4444;
            --accent-yellow: #f59e0b;
            --text-main: #f3f4f6;
            --text-muted: #9ca3af;
        }

        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
            font-family: 'Outfit', sans-serif;
        }

        body {
            background-color: var(--bg-dark);
            color: var(--text-main);
            min-height: 100vh;
            background-image: radial-gradient(circle at 10% 20%, rgba(79, 70, 229, 0.1) 0%, transparent 40%),
                              radial-gradient(circle at 90% 80%, rgba(16, 185, 129, 0.05) 0%, transparent 40%);
            padding: 2rem;
        }

        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 2.5rem;
            border-bottom: 1px solid var(--border-color);
            padding-bottom: 1.5rem;
        }

        h1 {
            font-size: 2.2rem;
            font-weight: 800;
            background: linear-gradient(135deg, #a5b4fc, #818cf8, #6366f1);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            display: flex;
            align-items: center;
            gap: 0.8rem;
        }

        .badge {
            background: rgba(79, 70, 229, 0.2);
            color: #a5b4fc;
            padding: 0.2rem 0.6rem;
            font-size: 0.8rem;
            border-radius: 9999px;
            border: 1px solid rgba(79, 70, 229, 0.3);
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        .grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 2rem;
        }

        @media (max-width: 1024px) {
            .grid {
                grid-template-columns: 1fr;
            }
        }

        .card {
            background: var(--card-bg);
            backdrop-filter: blur(12px);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            padding: 1.8rem;
            box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.3);
        }

        .card-title {
            font-size: 1.3rem;
            font-weight: 600;
            margin-bottom: 1.5rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid rgba(255, 255, 255, 0.05);
            padding-bottom: 0.8rem;
        }

        .info-row {
            display: flex;
            justify-content: space-between;
            margin-bottom: 1rem;
            font-size: 0.95rem;
        }

        .info-label {
            color: var(--text-muted);
        }

        .info-value {
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }

        .status-indicator {
            width: 10px;
            height: 10px;
            border-radius: 50%;
            display: inline-block;
        }

        .status-online {
            background-color: var(--accent-green);
            box-shadow: 0 0 10px var(--accent-green);
            animation: pulse 2s infinite;
        }

        .status-offline {
            background-color: var(--accent-red);
            box-shadow: 0 0 10px var(--accent-red);
        }

        @keyframes pulse {
            0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
            70% { transform: scale(1); box-shadow: 0 0 0 10px rgba(16, 185, 129, 0); }
            100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
        }

        .cluster-map {
            display: flex;
            justify-content: space-around;
            align-items: center;
            margin-top: 1.5rem;
            padding: 1.5rem;
            background: rgba(255, 255, 255, 0.02);
            border-radius: 12px;
            border: 1px dashed rgba(255, 255, 255, 0.05);
        }

        .node-node {
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 0.5rem;
            cursor: pointer;
            transition: transform 0.2s;
            position: relative;
        }

        .node-node:hover {
            transform: translateY(-4px);
        }

        .node-circle {
            width: 50px;
            height: 50px;
            border-radius: 50%;
            background: rgba(255, 255, 255, 0.05);
            border: 2px solid rgba(255, 255, 255, 0.2);
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: bold;
            font-size: 0.9rem;
        }

        .node-circle.current {
            border-color: var(--primary);
            box-shadow: 0 0 15px rgba(79, 70, 229, 0.4);
        }

        .node-circle.leader {
            border-color: var(--accent-yellow);
            box-shadow: 0 0 15px rgba(245, 158, 11, 0.4);
            background: rgba(245, 158, 11, 0.1);
        }

        .node-crown {
            position: absolute;
            top: -12px;
            font-size: 0.9rem;
        }

        .btn {
            padding: 0.6rem 1.2rem;
            border-radius: 8px;
            border: none;
            cursor: pointer;
            font-weight: 600;
            font-size: 0.9rem;
            transition: all 0.2s;
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
        }

        .btn-primary {
            background: var(--primary);
            color: white;
        }

        .btn-primary:hover {
            background: #4338ca;
            transform: translateY(-1px);
        }

        .btn-danger {
            background: rgba(239, 68, 68, 0.15);
            color: #fca5a5;
            border: 1px solid rgba(239, 68, 68, 0.3);
        }

        .btn-danger:hover {
            background: var(--accent-red);
            color: white;
        }

        .form-group {
            display: flex;
            gap: 1rem;
            margin-bottom: 1.5rem;
        }

        .form-control {
            flex: 1;
            background: rgba(0, 0, 0, 0.2);
            border: 1px solid var(--border-color);
            padding: 0.75rem 1rem;
            border-radius: 8px;
            color: white;
            font-size: 0.95rem;
            outline: none;
            transition: border-color 0.2s;
        }

        .form-control:focus {
            border-color: var(--primary);
        }

        .kv-table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 1rem;
            font-size: 0.95rem;
        }

        .kv-table th, .kv-table td {
            text-align: left;
            padding: 0.75rem 1rem;
            border-bottom: 1px solid rgba(255, 255, 255, 0.05);
        }

        .kv-table th {
            color: var(--text-muted);
            font-weight: 600;
        }

        .action-link {
            color: #f87171;
            cursor: pointer;
            text-decoration: none;
            font-weight: 600;
        }

        .action-link:hover {
            text-decoration: underline;
        }

        .empty-state {
            text-align: center;
            color: var(--text-muted);
            padding: 3rem 0;
            font-style: italic;
        }

        .header-actions {
            display: flex;
            align-items: center;
            gap: 1.5rem;
        }
    </style>
</head>
<body>
    <header>
        <div>
            <h1>DistKV Cluster Console <span class="badge">Raft v1</span></h1>
        </div>
        <div class="header-actions">
            <div class="info-value">
                <span class="status-indicator status-online" id="global-status"></span>
                <span id="node-label">node1</span>
            </div>
            <button class="btn btn-danger" onclick="simulateCrash()">Simulate Crash</button>
        </div>
    </header>

    <div id="toast-banner" style="display: none; max-width: 1200px; margin: 1rem auto; padding: 0.75rem 1.25rem; border-radius: 8px; font-weight: 500; text-align: center; border: 1px solid; animation: fadeIn 0.3s ease;"></div>

    <div class="grid">
        <!-- Cluster Status Card -->
        <div class="card">
            <div class="card-title">Consensus State Machine</div>
            <div class="info-row">
                <span class="info-label">Active Node ID</span>
                <span class="info-value" id="status-node-id">-</span>
            </div>
            <div class="info-row">
                <span class="info-label">Raft State</span>
                <span class="info-value" id="status-raft-state" style="text-transform: uppercase;">-</span>
            </div>
            <div class="info-row">
                <span class="info-label">Current Leader</span>
                <span class="info-value" id="status-leader">-</span>
            </div>
            <div class="info-row">
                <span class="info-label">Raft Term</span>
                <span class="info-value" id="status-term">-</span>
            </div>
            <div class="info-row">
                <span class="info-label">Keys Replicated</span>
                <span class="info-value" id="status-keys-count">-</span>
            </div>

            <div class="card-title" style="margin-top: 2rem;">Consensus Network Map</div>
            <div class="cluster-map" id="cluster-map">
                <!-- Javascript builds this map -->
            </div>
        </div>

        <!-- KV Store Interactive Card -->
        <div class="card">
            <div class="card-title">Interactive Key-Value Console</div>
            
            <div class="form-group">
                <input type="text" class="form-control" id="input-key" placeholder="Enter key (e.g. user:1)">
                <input type="text" class="form-control" id="input-value" placeholder="Enter value (e.g. Bhargav)">
                <button class="btn btn-primary" onclick="setKey()">Commit to Raft</button>
            </div>

            <div style="margin: 1.5rem 0; padding: 1rem; border-radius: 6px; background: rgba(255,255,255,0.02); border: 1px solid rgba(255,255,255,0.05); display: flex; align-items: center; justify-content: space-between; gap: 1rem;">
                <div>
                    <div style="font-weight: 600; font-size: 0.9rem;">Tunable Read Consistency</div>
                    <div style="font-size: 0.8rem; color: var(--text-muted);">Eventual consistency reads locally (low latency). Strong consistency queries the leader.</div>
                </div>
                <select id="read-consistency" class="form-control" style="width: auto; padding: 0.25rem 0.5rem; background: var(--card-bg); border-color: rgba(255,255,255,0.1); color: #fff;" onchange="fetchKeys()">
                    <option value="local">Eventual (Local - 0ms)</option>
                    <option value="strong">Strong (Leader)</option>
                </select>
            </div>

            <div style="max-height: 400px; overflow-y: auto;">
                <table class="kv-table" id="kv-table" style="display: none;">
                    <thead>
                        <tr>
                            <th>Key</th>
                            <th>Value</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody id="kv-table-body">
                        <!-- Loaded via API -->
                    </tbody>
                </table>
                <div class="empty-state" id="kv-empty">No keys committed to the database yet.</div>
            </div>
        </div>
    </div>

    <script>
        const host = window.location.host;
        const apiBase = "http://" + host;

        let activeNodeID = "";
        let clusterLeader = "";

        async function fetchStatus() {
            try {
                const response = await fetch(apiBase + "/api/status");
                const status = await response.json();

                document.getElementById("status-node-id").textContent = status.node_id;
                document.getElementById("status-raft-state").textContent = status.state;
                document.getElementById("status-leader").textContent = status.leader || "Electing...";
                document.getElementById("status-term").textContent = status.term || "1";
                document.getElementById("status-keys-count").textContent = status.num_keys;
                document.getElementById("node-label").textContent = status.node_id + " (Connected)";

                activeNodeID = status.node_id;
                clusterLeader = status.leader;

                // Adjust status color based on role
                const stateEl = document.getElementById("status-raft-state");
                if (status.state === "Leader") {
                    stateEl.style.color = "var(--accent-yellow)";
                } else {
                    stateEl.style.color = "var(--accent-green)";
                }

                buildClusterMap(status.peers);
            } catch (err) {
                console.error("Failed to fetch status:", err);
                document.getElementById("global-status").className = "status-indicator status-offline";
                document.getElementById("node-label").textContent = "Disconnected";
            }
        }

        function showToast(message, isWarning = false) {
            const banner = document.getElementById("toast-banner");
            banner.textContent = message;
            if (isWarning) {
                banner.style.background = "rgba(245, 158, 11, 0.2)";
                banner.style.borderColor = "var(--accent-yellow)";
                banner.style.color = "var(--accent-yellow)";
            } else {
                banner.style.background = "rgba(16, 185, 129, 0.2)";
                banner.style.borderColor = "var(--accent-green)";
                banner.style.color = "var(--accent-green)";
            }
            banner.style.display = "block";
            setTimeout(() => {
                banner.style.display = "none";
            }, 6000);
        }

        async function fetchKeys() {
            try {
                const consistency = document.getElementById("read-consistency").value;
                const response = await fetch(apiBase + "/api/keys?consistency=" + consistency);
                const data = await response.json();
                const table = document.getElementById("kv-table");
                const tbody = document.getElementById("kv-table-body");
                const empty = document.getElementById("kv-empty");

                tbody.innerHTML = "";

                const keys = data.keys || {};
                const keyArray = Object.keys(keys);

                if (keyArray.length === 0) {
                    table.style.display = "none";
                    empty.style.display = "block";
                } else {
                    table.style.display = "table";
                    empty.style.display = "none";

                    keyArray.sort().forEach(k => {
                        const row = document.createElement("tr");
                        row.innerHTML =
                            '<td style="font-weight: 600;">' + escapeHtml(k) + '</td>' +
                            '<td style="color: var(--text-muted); font-family: monospace;">' + escapeHtml(keys[k]) + '</td>' +
                            '<td><span class="action-link" onclick="deleteKey(\'' + escapeHtml(k) + '\')">Delete</span></td>';
                        tbody.appendChild(row);
                    });
                }
            } catch (err) {
                console.error("Failed to fetch keys:", err);
            }
        }

        async function setKey() {
            const keyEl = document.getElementById("input-key");
            const valEl = document.getElementById("input-value");
            const key = keyEl.value.trim();
            const val = valEl.value.trim();

            if (!key || !val) {
                alert("Please provide both a key and a value.");
                return;
            }

            try {
                const response = await fetch(apiBase + "/api/set?key=" + encodeURIComponent(key) + "&value=" + encodeURIComponent(val));
                const result = await response.json();
                if (result.error) {
                    if (result.leader_hint) {
                        alert("Redirect Hint: This node is not the leader. Try posting to " + result.leader_hint + " instead.");
                    } else {
                        alert("Error: " + result.error);
                    }
                } else {
                    keyEl.value = "";
                    valEl.value = "";
                    fetchKeys();
                    fetchStatus();
                    if (result.forwarded) {
                        showToast("Proxy Success: Routed set operation from Follower node through Leader (" + result.leader + ")!", false);
                    } else {
                        showToast("Consensus Success: Write committed directly on Leader!", false);
                    }
                }
            } catch (err) {
                alert("Write failed: " + err);
            }
        }

        async function deleteKey(key) {
            if (!confirm("Are you sure you want to delete \"" + key + "\"?")) return;
            try {
                const response = await fetch(apiBase + "/api/delete?key=" + encodeURIComponent(key));
                const result = await response.json();
                if (result.error) {
                    if (result.leader_hint) {
                        alert("Redirect Hint: Not the leader. Route deletions to leader " + result.leader_hint);
                    } else {
                        alert("Error: " + result.error);
                    }
                } else {
                    fetchKeys();
                    fetchStatus();
                    if (result.forwarded) {
                        showToast("Proxy Success: Routed delete operation from Follower node through Leader (" + result.leader + ")!", false);
                    } else {
                        showToast("Consensus Success: Delete committed directly on Leader!", false);
                    }
                }
            } catch (err) {
                alert("Delete failed: " + err);
            }
        }

        async function simulateCrash() {
            if (!confirm("This will terminate this node's process, causing it to fall out of consensus. Proceed?")) return;
            try {
                await fetch(apiBase + "/api/shutdown");
            } catch (e) {
                // Fetch will fail because node immediately terminates. This is expected.
            }
            alert("Crash simulated. This window will now lose connection. Open another node's dashboard to verify recovery (e.g. http://localhost:8002).");
            window.location.reload();
        }

        function buildClusterMap(peers) {
            const container = document.getElementById("cluster-map");
            container.innerHTML = "";

            // Dedup/sort all nodes
            const nodeSet = new Set();
            nodeSet.add(activeNodeID);

            peers.forEach(p => {
                const parts = p.split("=");
                if (parts.length > 0) nodeSet.add(parts[0]);
            });

            const sortedNodes = Array.from(nodeSet).sort();

            sortedNodes.forEach(node => {
                const isCurrent = node === activeNodeID;
                
                // For simplicity in UI: we check if the node address contains the node ID
                // inside our Raft leader info to see who is leader.
                const isLeader = clusterLeader.includes(node) || (clusterLeader === "" && isCurrent && document.getElementById("status-raft-state").textContent === "LEADER");

                const nodeEl = document.createElement("div");
                nodeEl.className = "node-node";
                nodeEl.onclick = () => switchNodePort(node);

                let crownHtml = "";
                if (isLeader) {
                    crownHtml = '<div class="node-crown">👑</div>';
                }

                let classes = "node-circle";
                if (isCurrent) classes += " current";
                if (isLeader) classes += " leader";

                nodeEl.innerHTML =
                    crownHtml +
                    '<div class="' + classes + '">' + node + '</div>';
                container.appendChild(nodeEl);
            });
        }

        function switchNodePort(nodeName) {
            // Mapping node1 -> 8001, node2 -> 8002, etc.
            const match = nodeName.match(/\d+/);
            if (match) {
                const num = match[0];
                const newPort = 8000 + parseInt(num);
                window.location.href = "http://" + window.location.hostname + ":" + newPort;
            }
        }

        function escapeHtml(str) {
            return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;");
        }

        // Poll API every 1.5 seconds
        fetchStatus();
        fetchKeys();
        setInterval(() => {
            fetchStatus();
            fetchKeys();
        }, 1500);
    </script>
</body>
</html>
`
