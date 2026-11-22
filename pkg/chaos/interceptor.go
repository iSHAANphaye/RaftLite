package chaos

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const TargetPeerHeader = "x-raftlite-target-peer"
const SourcePeerHeader = "x-raftlite-source-peer"

// UnaryClientInterceptor returns a gRPC client interceptor that simulates chaos conditions.
func UnaryClientInterceptor(controller *ChaosController, localNodeID string) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		if controller == nil {
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		targetPeer := ""
		if cc != nil {
			targetPeer = cc.Target()
		}
		md, ok := metadata.FromOutgoingContext(ctx)
		if ok {
			if vals := md.Get(TargetPeerHeader); len(vals) > 0 {
				targetPeer = vals[0]
			}
		}

		err := controller.SimulateNetwork(localNodeID, targetPeer)
		if err != nil {
			if errorsIs(err, ErrNetworkPartition) {
				return status.Errorf(codes.Unavailable, "chaos: network partition active between %s and %s", localNodeID, targetPeer)
			}
			if errorsIs(err, ErrPacketDropped) {
				return status.Errorf(codes.DeadlineExceeded, "chaos: packet dropped by fault injector")
			}
			return status.Errorf(codes.Internal, "chaos error: %v", err)
		}

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func errorsIs(err, target error) bool {
	return err == target
}
