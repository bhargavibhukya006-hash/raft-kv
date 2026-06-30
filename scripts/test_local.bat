@echo off
REM scripts/test_local.bat - Run DistKV 3-node cluster locally on Windows
REM
REM Usage: scripts\test_local.bat
REM This opens 3 PowerShell windows, one per node.
REM After ~5 seconds, node1 should become leader.

echo Starting DistKV 3-node local cluster...
echo Press Ctrl+C in each window to stop.
echo.

REM Clean up old data directories
if exist data rmdir /s /q data

REM Node 1 - Bootstrap node (becomes initial leader)
start "DistKV Node1 (Leader)" powershell -NoExit -Command ^
  "$env:NODE_ID='node1'; $env:GRPC_ADDR=':9001'; $env:RAFT_ADDR='127.0.0.1:7001'; ^
   $env:DATA_DIR='./data/node1'; $env:BOOTSTRAP='true'; ^
   $env:PEERS='node1=127.0.0.1:7001,node2=127.0.0.1:7002,node3=127.0.0.1:7003'; ^
   go run ./cmd/node"

timeout /t 2 /nobreak >nul

REM Node 2 - Follower
start "DistKV Node2" powershell -NoExit -Command ^
  "$env:NODE_ID='node2'; $env:GRPC_ADDR=':9002'; $env:RAFT_ADDR='127.0.0.1:7002'; ^
   $env:DATA_DIR='./data/node2'; $env:BOOTSTRAP='false'; ^
   $env:PEERS='node1=127.0.0.1:7001,node2=127.0.0.1:7002,node3=127.0.0.1:7003'; ^
   go run ./cmd/node"

timeout /t 1 /nobreak >nul

REM Node 3 - Follower
start "DistKV Node3" powershell -NoExit -Command ^
  "$env:NODE_ID='node3'; $env:GRPC_ADDR=':9003'; $env:RAFT_ADDR='127.0.0.1:7003'; ^
   $env:DATA_DIR='./data/node3'; $env:BOOTSTRAP='false'; ^
   $env:PEERS='node1=127.0.0.1:7001,node2=127.0.0.1:7002,node3=127.0.0.1:7003'; ^
   go run ./cmd/node"

echo.
echo Cluster starting...
echo Wait 10 seconds then test with:
echo   grpcurl -plaintext -import-path ./proto -proto kv.proto -d '{"key":"hello","value":"world"}' localhost:9001 kv.KV/Set
echo   grpcurl -plaintext -import-path ./proto -proto kv.proto -d '{"key":"hello"}' localhost:9001 kv.KV/Get
