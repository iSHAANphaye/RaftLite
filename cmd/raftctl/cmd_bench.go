package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raftlite/pkg/client"
)

func runBench(endpoints []string, token string, args []string) {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	var (
		flagClients    = fs.Int("clients", 16, "Number of concurrent client workers")
		flagDuration   = fs.Duration("duration", 10*time.Second, "Total benchmark test duration")
		flagKeySize    = fs.Int("key-size", 16, "Key size in bytes")
		flagValSize    = fs.Int("val-size", 128, "Value payload size in bytes")
		flagWriteRatio = fs.Float64("write-ratio", 1.0, "Ratio of writes vs reads [0.0 to 1.0]")
	)

	_ = fs.Parse(args)

	fmt.Printf("Starting RaftLite Benchmark: clients=%d duration=%v writeRatio=%.2f\n",
		*flagClients, *flagDuration, *flagWriteRatio)

	valPayload := strings.Repeat("x", *flagValSize)
	stopAt := time.Now().Add(*flagDuration)

	var (
		totalOps      int64
		successfulOps int64
		failedOps     int64
		bytesSent     int64
		mu            sync.Mutex
		latencies     []float64
		wg            sync.WaitGroup
	)

	latencies = make([]float64, 0, 100000)

	startTime := time.Now()

	for i := 0; i < *flagClients; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			c := client.NewClient(client.Config{
				Endpoints: endpoints,
				Token:     token,
				Timeout:   3 * time.Second,
				ClientID:  fmt.Sprintf("bench-w%d-%d", workerID, time.Now().UnixNano()),
			})
			defer c.Close()

			localLatencies := make([]float64, 0, 5000)
			opCounter := 0

			for time.Now().Before(stopAt) {
				opCounter++
				key := fmt.Sprintf("k-%04d-%08d", workerID, opCounter)
				if len(key) < *flagKeySize {
					key = key + strings.Repeat("0", *flagKeySize-len(key))
				}

				isWrite := rand.Float64() < *flagWriteRatio
				opStart := time.Now()
				var err error

				if isWrite {
					_, err = c.Put(context.Background(), key, valPayload)
					if err == nil {
						atomic.AddInt64(&bytesSent, int64(len(key)+len(valPayload)))
					}
				} else {
					_, err = c.Get(context.Background(), key, false)
					if err == nil {
						atomic.AddInt64(&bytesSent, int64(len(key)))
					}
				}

				elapsedMs := float64(time.Since(opStart).Microseconds()) / 1000.0
				localLatencies = append(localLatencies, elapsedMs)

				atomic.AddInt64(&totalOps, 1)
				if err == nil {
					atomic.AddInt64(&successfulOps, 1)
				} else {
					atomic.AddInt64(&failedOps, 1)
				}
			}

			mu.Lock()
			latencies = append(latencies, localLatencies...)
			mu.Unlock()
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(startTime)

	if len(latencies) == 0 {
		fmt.Println("No operations completed.")
		return
	}

	sort.Float64s(latencies)
	n := len(latencies)

	p50 := latencies[int(float64(n)*0.50)]
	p90 := latencies[int(float64(n)*0.90)]
	p99 := latencies[int(float64(n)*0.99)]
	maxLat := latencies[n-1]

	throughput := float64(successfulOps) / elapsed.Seconds()
	mbTransferred := float64(bytesSent) / (1024 * 1024)

	successPct := float64(successfulOps) / float64(totalOps) * 100.0
	failedPct := float64(failedOps) / float64(totalOps) * 100.0

	fmt.Println()
	fmt.Println("==================== RaftLite Benchmark Results ====================")
	fmt.Printf("Duration:             %.3fs\n", elapsed.Seconds())
	fmt.Printf("Concurrent Clients:   %d\n", *flagClients)
	fmt.Printf("Total Operations:     %d\n", totalOps)
	fmt.Printf("Successful Ops:       %d (%.2f%%)\n", successfulOps, successPct)
	fmt.Printf("Failed Ops:           %d (%.2f%%)\n", failedOps, failedPct)
	fmt.Printf("Throughput:           %.1f ops/sec\n", throughput)
	fmt.Printf("Data Transferred:     %.2f MB\n", mbTransferred)
	fmt.Println()
	fmt.Println("Latency Distribution:")
	fmt.Printf("  P50 (Median):       %.2f ms\n", p50)
	fmt.Printf("  P90:                %.2f ms\n", p90)
	fmt.Printf("  P99:                %.2f ms\n", p99)
	fmt.Printf("  Max Latency:        %.2f ms\n", maxLat)
	fmt.Println("====================================================================")
}
