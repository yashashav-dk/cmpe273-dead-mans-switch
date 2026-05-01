package worker

import (
	"context"
	"time"

	deadmanv1 "github.com/yashashav/cmpe273-dead-mans-switch/gen/deadman/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Responder is the gRPC server implementation on the Worker side. It only
// implements Ping; StreamHeartbeats / Register are server-side concerns of
// the Monitor and are stubbed (Unimplemented).
type Responder struct {
	deadmanv1.UnimplementedHeartbeatServer
	workerID string
	chaos    *Chaos
}

func NewResponder(workerID string, chaos *Chaos) *Responder {
	return &Responder{workerID: workerID, chaos: chaos}
}

// Ping replies with the worker's id and current monotonic-ish timestamp,
// applying chaos: when ShouldDrop fires, return Unavailable; SampleLag inserts
// a server-side sleep before the reply.
func (r *Responder) Ping(ctx context.Context, req *deadmanv1.PingRequest) (*deadmanv1.PingReply, error) {
	if r.chaos.ShouldDrop() {
		return nil, status.Error(codes.Unavailable, "chaos: drop")
	}
	if lag := r.chaos.SampleLag(); lag > 0 {
		select {
		case <-time.After(lag):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &deadmanv1.PingReply{
		WorkerId:        r.workerID,
		SentUnixNanos:   req.SentUnixNanos,
		WorkerUnixNanos: time.Now().UnixNano(),
	}, nil
}
