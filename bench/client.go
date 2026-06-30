// bench/client.go - Load testing and benchmark client for DistKV.
//
// Usage:
//   go run ./bench/client.go -addr=:9001 -workers=50 -duration=30s -ratio=0.8
//
// This spawns concurrent goroutines sending Set and Get RPCs to one node.
// After the duration, it reports:
//   - Total requests sent
//   - Throughput (req/s)
//   - Latency percentiles: p50, p95, p99
//
// Note: In a real cluster, point this at the leader's gRPC address.
// If you get leader_hint responses, redirect automatically.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/distkv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// --- CLI flags ---
	addr := flag.String("addr", ":9001", "gRPC address of a DistKV node")
	workers := flag.Int("workers", 50, "number of concurrent goroutines")
	duration := flag.Duration("duration", 30*time.Second, "test duration")
	writeRatio := flag.Float64("ratio", 0.5, "fraction of requests that are writes (0.0-1.0)")
	keyspace := flag.Int("keyspace", 10000, "number of distinct keys to use")
	flag.Parse()

	log.Printf("Benchmark config: addr=%s workers=%d duration=%s writeRatio=%.2f keyspace=%d",
		*addr, *workers, *duration, *writeRatio, *keyspace)

	// --- Connect to gRPC ---
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("grpc connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewKVClient(conn)

	// Pre-warm: seed some keys so Gets have data to read
	log.Println("Pre-warming keyspace...")
	warmCtx, warmCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer warmCancel()
	warmCount := *keyspace
	if warmCount > 100 {
		warmCount = 100
	}
	for i := 0; i < warmCount; i++ {
		_, _ = client.Set(warmCtx, &pb.SetRequest{
			Key:   fmt.Sprintf("bench-key-%d", i),
			Value: fmt.Sprintf("value-%d", i),
		})
	}

	// --- Benchmark ---
	var (
		totalOps    int64
		totalErrors int64
	)

	// Collect raw latencies in nanoseconds
	latencies := make([]int64, 0, *workers*1000)
	var latMu sync.Mutex

	deadline := time.Now().Add(*duration)
	var wg sync.WaitGroup

	log.Printf("Starting benchmark for %s with %d workers...", *duration, *workers)

	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(workerID)))
			localLats := make([]int64, 0, 1000)

			for time.Now().Before(deadline) {
				key := fmt.Sprintf("bench-key-%d", rng.Intn(*keyspace))
				start := time.Now()
				var opErr error

				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				if rng.Float64() < *writeRatio {
					_, opErr = client.Set(ctx, &pb.SetRequest{
						Key:   key,
						Value: fmt.Sprintf("v-%d", rng.Int63()),
					})
				} else {
					_, opErr = client.Get(ctx, &pb.GetRequest{Key: key})
				}
				cancel()

				elapsed := time.Since(start).Nanoseconds()
				localLats = append(localLats, elapsed)
				atomic.AddInt64(&totalOps, 1)
				if opErr != nil {
					atomic.AddInt64(&totalErrors, 1)
				}
			}

			latMu.Lock()
			latencies = append(latencies, localLats...)
			latMu.Unlock()
		}(w)
	}

	wg.Wait()

	// --- Report results ---
	actualDuration := *duration
	throughput := float64(totalOps) / actualDuration.Seconds()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	p50 := percentile(latencies, 50)
	p95 := percentile(latencies, 95)
	p99 := percentile(latencies, 99)

	fmt.Println("\n========== DistKV Benchmark Results ==========")
	fmt.Printf("Duration:    %s\n", actualDuration)
	fmt.Printf("Workers:     %d\n", *workers)
	fmt.Printf("Total ops:   %d\n", totalOps)
	fmt.Printf("Errors:      %d (%.2f%%)\n", totalErrors, 100*float64(totalErrors)/float64(totalOps))
	fmt.Printf("Throughput:  %.1f req/s\n", throughput)
	fmt.Println()
	fmt.Printf("Latency p50: %s\n", time.Duration(p50))
	fmt.Printf("Latency p95: %s\n", time.Duration(p95))
	fmt.Printf("Latency p99: %s\n", time.Duration(p99))
	fmt.Println("================================================")
}

// percentile returns the Nth percentile from a sorted slice of nanosecond durations.
func percentile(sorted []int64, n int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * float64(n) / 100.0)
	return sorted[idx]
}
