package chaos

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

var (
	ErrNetworkPartition = errors.New("chaos: network partition active between nodes")
	ErrPacketDropped    = errors.New("chaos: packet dropped by chaos interceptor")
)

// ChaosController simulates network partitions, dropped packets, and latency injection.
type ChaosController struct {
	mu       sync.RWMutex
	blocked  map[string]map[string]bool
	dropRate float64
	minDelay time.Duration
	maxDelay time.Duration
}

// NewChaosController initializes an active chaos fault controller.
func NewChaosController() *ChaosController {
	return &ChaosController{
		blocked: make(map[string]map[string]bool),
	}
}

// PartitionNodes installs a bidirectional partition between groupA and groupB.
func (c *ChaosController) PartitionNodes(groupA []string, groupB []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, a := range groupA {
		if c.blocked[a] == nil {
			c.blocked[a] = make(map[string]bool)
		}
		for _, b := range groupB {
			c.blocked[a][b] = true
		}
	}

	for _, b := range groupB {
		if c.blocked[b] == nil {
			c.blocked[b] = make(map[string]bool)
		}
		for _, a := range groupA {
			c.blocked[b][a] = true
		}
	}
}

// HealAll clears all active partitions and resets packet drops.
func (c *ChaosController) HealAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blocked = make(map[string]map[string]bool)
	c.dropRate = 0.0
	c.minDelay = 0
	c.maxDelay = 0
}

// SetPacketDrop sets the simulated packet drop rate between 0.0 and 1.0.
func (c *ChaosController) SetPacketDrop(rate float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dropRate = rate
}

// SetLatency sets the min and max simulated latency delay.
func (c *ChaosController) SetLatency(min, max time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.minDelay = min
	c.maxDelay = max
}

// IsBlocked checks if communication from `from` to `to` is partitioned.
func (c *ChaosController) IsBlocked(from, to string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.blocked[from] == nil {
		return false
	}
	return c.blocked[from][to]
}

// SimulateNetwork evaluates whether a message from -> to should be delivered.
// Injects latency, returns ErrNetworkPartition if partitioned, or ErrPacketDropped.
func (c *ChaosController) SimulateNetwork(from, to string) error {
	c.mu.RLock()
	isBlocked := false
	if c.blocked[from] != nil {
		isBlocked = c.blocked[from][to]
	}
	dropRate := c.dropRate
	minDelay := c.minDelay
	maxDelay := c.maxDelay
	c.mu.RUnlock()

	if isBlocked {
		return ErrNetworkPartition
	}

	if dropRate > 0 && rand.Float64() < dropRate {
		return ErrPacketDropped
	}

	if maxDelay > 0 {
		delay := minDelay
		if maxDelay > minDelay {
			delta := maxDelay - minDelay
			delay += time.Duration(rand.Int63n(int64(delta)))
		}
		time.Sleep(delay)
	}

	return nil
}
