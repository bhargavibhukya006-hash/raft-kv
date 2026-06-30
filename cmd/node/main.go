// cmd/node/main.go - DistKV node entry point.
//
// This binary reads all configuration from environment variables, which is
// the 12-factor app pattern and works perfectly with Docker/Kubernetes.
// No config files needed — just set env vars and run.
//
// Required env vars:
//   NODE_ID       - Unique identifier for this node (e.g., "node1")
//   GRPC_ADDR     - gRPC listen address (e.g., ":9001")
//   RAFT_ADDR     - Raft TCP listen address (e.g., ":7001")
//   DATA_DIR      - Directory for Raft log/snapshot storage (e.g., "/data/node1")
//   BOOTSTRAP     - "true" on first cluster start, "false" otherwise
//   PEERS         - Comma-separated "nodeID=raftAddr" pairs for cluster peers
//                   Example: "node2=node2:7002,node3=node3:7003"
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	distkv_raft "github.com/distkv/raft"
	"github.com/distkv/server"
	"github.com/distkv/store"
)

func main() {
	// --- Read configuration from environment ---
	nodeID := mustEnv("NODE_ID")
	grpcAddr := getEnvOrDefault("GRPC_ADDR", ":9000")
	raftAddr := mustEnv("RAFT_ADDR")
	dataDir := getEnvOrDefault("DATA_DIR", "./data/"+nodeID)
	bootstrap := os.Getenv("BOOTSTRAP") == "true"
	peersStr := os.Getenv("PEERS") // "node2=addr2,node3=addr3"

	var peers []string
	if peersStr != "" {
		peers = strings.Split(peersStr, ",")
	}

	log.Printf("[DistKV] Starting node %s | gRPC=%s | Raft=%s | bootstrap=%v",
		nodeID, grpcAddr, raftAddr, bootstrap)

	// Step 1: Create the in-memory KV store
	kv := store.NewKVStore()

	// Step 2: Start Raft consensus
	raftCfg := distkv_raft.Config{
		NodeID:       nodeID,
		RaftBindAddr: raftAddr,
		DataDir:      dataDir,
		Peers:        peers,
		Bootstrap:    bootstrap,
	}

	raftNode, err := distkv_raft.SetupRaft(raftCfg, kv)
	if err != nil {
		log.Fatalf("[DistKV] Failed to start Raft: %v", err)
	}

	// Step 3: Create and start the gRPC server
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grpcSrv := server.NewGRPCServer(kv, raftNode, grpcAddr)

	// Step 4: Create and start the HTTP Server (Web Dashboard)
	httpAddr := getEnvOrDefault("HTTP_ADDR", ":8000")
	httpSrv := server.NewHTTPServer(kv, raftNode, grpcSrv, nodeID, httpAddr)
	go func() {
		if err := httpSrv.Start(ctx); err != nil {
			log.Printf("[DistKV] HTTP server stopped: %v", err)
		}
	}()

	// Handle shutdown signals gracefully
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("[DistKV] Received signal %v, shutting down...", sig)
		cancel()
	}()

	log.Printf("[DistKV] Node %s ready. gRPC listening on %s | HTTP listening on %s", nodeID, grpcAddr, httpAddr)
	if err := server.Serve(ctx, grpcAddr, grpcSrv); err != nil {
		log.Printf("[DistKV] gRPC server stopped: %v", err)
	}
}

func mustEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("[DistKV] Required environment variable %q is not set", key)
	}
	return val
}

func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
