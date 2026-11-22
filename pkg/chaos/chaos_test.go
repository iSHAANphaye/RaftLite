package chaos

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestChaos_PartitionAndHeal(t *testing.T) {
	c := NewChaosController()

	// Initially unblocked
	assert.False(t, c.IsBlocked("node-1", "node-2"))
	assert.NoError(t, c.SimulateNetwork("node-1", "node-2"))

	// Partition node-1 and node-2 against node-3
	c.PartitionNodes([]string{"node-1", "node-2"}, []string{"node-3"})

	// Within group is unblocked
	assert.False(t, c.IsBlocked("node-1", "node-2"))
	assert.NoError(t, c.SimulateNetwork("node-1", "node-2"))

	// Across group is blocked bidirectionally
	assert.True(t, c.IsBlocked("node-1", "node-3"))
	assert.True(t, c.IsBlocked("node-3", "node-1"))
	assert.True(t, c.IsBlocked("node-2", "node-3"))
	assert.True(t, c.IsBlocked("node-3", "node-2"))

	err := c.SimulateNetwork("node-1", "node-3")
	assert.ErrorIs(t, err, ErrNetworkPartition)

	// Heal
	c.HealAll()
	assert.False(t, c.IsBlocked("node-1", "node-3"))
	assert.NoError(t, c.SimulateNetwork("node-1", "node-3"))
}

func TestChaos_PacketDropRate(t *testing.T) {
	c := NewChaosController()
	c.SetPacketDrop(1.0) // 100% drop

	err := c.SimulateNetwork("a", "b")
	assert.ErrorIs(t, err, ErrPacketDropped)

	c.SetPacketDrop(0.0) // 0% drop
	err = c.SimulateNetwork("a", "b")
	assert.NoError(t, err)
}

func TestChaos_LatencyDelay(t *testing.T) {
	c := NewChaosController()
	c.SetLatency(20*time.Millisecond, 25*time.Millisecond)

	t0 := time.Now()
	err := c.SimulateNetwork("a", "b")
	require.NoError(t, err)
	elapsed := time.Since(t0)

	assert.GreaterOrEqual(t, elapsed, 18*time.Millisecond)
}

func TestChaos_UnaryInterceptor(t *testing.T) {
	c := NewChaosController()
	c.PartitionNodes([]string{"client"}, []string{"server"})

	interceptor := UnaryClientInterceptor(c, "client")

	called := false
	invoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		called = true
		return nil
	}

	// 1. Unblocked peer
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(TargetPeerHeader, "unblocked"))
	err := interceptor(ctx, "/test", nil, nil, nil, invoker)
	assert.NoError(t, err)
	assert.True(t, called)

	// 2. Blocked peer
	called = false
	ctxBlocked := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(TargetPeerHeader, "server"))
	err = interceptor(ctxBlocked, "/test", nil, nil, nil, invoker)
	assert.Error(t, err)
	assert.False(t, called)
	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.Unavailable, st.Code())
}
