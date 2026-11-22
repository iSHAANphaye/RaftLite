package client

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raftlite/pkg/cluster"
	raftlitev1 "github.com/raftlite/proto/raftlite/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Client represents a resilient client for the RaftLite cluster with automatic leader failover.
type Client struct {
	mu         sync.Mutex
	endpoints  []string
	token      string
	timeout    time.Duration
	clientID   string
	seqCounter uint64

	leaderAddr string
	conns      map[string]*grpc.ClientConn
}

// Config specifies connection settings for the RaftLite client.
type Config struct {
	Endpoints []string
	Token     string
	Timeout   time.Duration
	ClientID  string
}

// NewClient initializes a new RaftLite client.
func NewClient(cfg Config) *Client {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	clientID := cfg.ClientID
	if clientID == "" {
		clientID = fmt.Sprintf("client-%d-%x", time.Now().UnixNano(), rand.Int63()&0xffff)
	}

	return &Client{
		endpoints: cfg.Endpoints,
		token:     cfg.Token,
		timeout:   timeout,
		clientID:  clientID,
		conns:     make(map[string]*grpc.ClientConn),
	}
}

func (c *Client) nextSeq() uint64 {
	return atomic.AddUint64(&c.seqCounter, 1)
}

func (c *Client) getConn(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn, exists := c.conns[addr]; exists && conn != nil {
		return conn, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("client: dial %s failed: %w", addr, err)
	}

	c.conns[addr] = conn
	return conn, nil
}

func (c *Client) withMetadata(ctx context.Context) context.Context {
	if c.token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, cluster.ClusterTokenHeader, c.token)
}

func (c *Client) executeWithRetry(ctx context.Context, op func(kvClient raftlitev1.KVServiceClient) (leaderHint string, retry bool, err error)) error {
	maxAttempts := len(c.endpoints) * 2
	if maxAttempts < 3 {
		maxAttempts = 3
	}

	c.mu.Lock()
	target := c.leaderAddr
	c.mu.Unlock()

	addrs := make([]string, 0, len(c.endpoints)+1)
	if target != "" {
		addrs = append(addrs, target)
	}
	for _, ep := range c.endpoints {
		if ep != target {
			addrs = append(addrs, ep)
		}
	}

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		addr := addrs[attempt%len(addrs)]
		conn, err := c.getConn(addr)
		if err != nil {
			lastErr = err
			continue
		}

		kvClient := raftlitev1.NewKVServiceClient(conn)
		leaderHint, shouldRetry, err := op(kvClient)
		if err == nil {
			c.mu.Lock()
			c.leaderAddr = addr
			c.mu.Unlock()
			return nil
		}

		lastErr = err
		if leaderHint != "" {
			c.mu.Lock()
			c.leaderAddr = leaderHint
			c.mu.Unlock()
			// Insert leaderHint at the next slot to query
			addrs = append([]string{leaderHint}, addrs...)
		}

		if !shouldRetry {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}

	return fmt.Errorf("client: max retry attempts exceeded: %w", lastErr)
}

// Put writes or updates a key.
func (c *Client) Put(ctx context.Context, key, val string) (*raftlitev1.ClientPutResponse, error) {
	return c.PutWithSeq(ctx, key, val, c.clientID, c.nextSeq())
}

// PutWithSeq allows specifying an explicit sequence number and client ID for deduplication tests.
func (c *Client) PutWithSeq(ctx context.Context, key, val, clientID string, seq uint64) (*raftlitev1.ClientPutResponse, error) {
	req := &raftlitev1.ClientPutRequest{
		ClientId:    clientID,
		SequenceNum: seq,
		Key:         key,
		Value:       val,
	}

	var finalResp *raftlitev1.ClientPutResponse
	err := c.executeWithRetry(ctx, func(kvClient raftlitev1.KVServiceClient) (string, bool, error) {
		callCtx, cancel := context.WithTimeout(c.withMetadata(ctx), c.timeout)
		defer cancel()

		resp, err := kvClient.ClientPut(callCtx, req)
		if err != nil {
			hint := ""
			if resp != nil {
				hint = resp.LeaderHint
			}
			return hint, true, err
		}
		if !resp.Success && resp.LeaderHint != "" {
			return resp.LeaderHint, true, fmt.Errorf("redirect to leader: %s", resp.LeaderHint)
		}
		finalResp = resp
		return "", false, nil
	})

	return finalResp, err
}

// Get retrieves a key.
func (c *Client) Get(ctx context.Context, key string, linearizable bool) (*raftlitev1.ClientGetResponse, error) {
	req := &raftlitev1.ClientGetRequest{
		Key:          key,
		Linearizable: linearizable,
	}

	var finalResp *raftlitev1.ClientGetResponse
	err := c.executeWithRetry(ctx, func(kvClient raftlitev1.KVServiceClient) (string, bool, error) {
		callCtx, cancel := context.WithTimeout(c.withMetadata(ctx), c.timeout)
		defer cancel()

		resp, err := kvClient.ClientGet(callCtx, req)
		if err != nil {
			hint := ""
			if resp != nil {
				hint = resp.LeaderHint
			}
			return hint, true, err
		}
		finalResp = resp
		return "", false, nil
	})

	return finalResp, err
}

// CAS executes an atomic Compare-And-Swap operation.
func (c *Client) CAS(ctx context.Context, key, expected, newVal string) (*raftlitev1.ClientCASResponse, error) {
	seq := c.nextSeq()
	req := &raftlitev1.ClientCASRequest{
		ClientId:      c.clientID,
		SequenceNum:   seq,
		Key:           key,
		ExpectedValue: expected,
		NewValue:      newVal,
	}

	var finalResp *raftlitev1.ClientCASResponse
	err := c.executeWithRetry(ctx, func(kvClient raftlitev1.KVServiceClient) (string, bool, error) {
		callCtx, cancel := context.WithTimeout(c.withMetadata(ctx), c.timeout)
		defer cancel()

		resp, err := kvClient.ClientCAS(callCtx, req)
		if err != nil {
			hint := ""
			if resp != nil {
				hint = resp.LeaderHint
			}
			return hint, true, err
		}
		if !resp.Success && resp.LeaderHint != "" {
			return resp.LeaderHint, true, fmt.Errorf("redirect to leader: %s", resp.LeaderHint)
		}
		finalResp = resp
		return "", false, nil
	})

	return finalResp, err
}

// Delete removes a key.
func (c *Client) Delete(ctx context.Context, key string) (*raftlitev1.ClientDeleteResponse, error) {
	seq := c.nextSeq()
	req := &raftlitev1.ClientDeleteRequest{
		ClientId:    c.clientID,
		SequenceNum: seq,
		Key:         key,
	}

	var finalResp *raftlitev1.ClientDeleteResponse
	err := c.executeWithRetry(ctx, func(kvClient raftlitev1.KVServiceClient) (string, bool, error) {
		callCtx, cancel := context.WithTimeout(c.withMetadata(ctx), c.timeout)
		defer cancel()

		resp, err := kvClient.ClientDelete(callCtx, req)
		if err != nil {
			hint := ""
			if resp != nil {
				hint = resp.LeaderHint
			}
			return hint, true, err
		}
		if !resp.Success && resp.LeaderHint != "" {
			return resp.LeaderHint, true, fmt.Errorf("redirect to leader: %s", resp.LeaderHint)
		}
		finalResp = resp
		return "", false, nil
	})

	return finalResp, err
}

// Status inspects the cluster via any available node.
func (c *Client) Status(ctx context.Context) (*raftlitev1.ClusterStatusResponse, error) {
	req := &raftlitev1.ClusterStatusRequest{}
	for _, ep := range c.endpoints {
		conn, err := c.getConn(ep)
		if err != nil {
			continue
		}
		kvClient := raftlitev1.NewKVServiceClient(conn)
		callCtx, cancel := context.WithTimeout(c.withMetadata(ctx), c.timeout)
		resp, err := kvClient.ClusterStatus(callCtx, req)
		cancel()
		if err == nil {
			return resp, nil
		}
	}
	return nil, fmt.Errorf("client: failed to query status across endpoints %s", strings.Join(c.endpoints, ","))
}

// Close closes all open connections.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.conns {
		if conn != nil {
			_ = conn.Close()
		}
	}
	c.conns = make(map[string]*grpc.ClientConn)
}
