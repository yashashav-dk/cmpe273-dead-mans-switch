package monitor

import (
	"context"
	"errors"
	"io"
	"time"

	deadmanv1 "github.com/yashashav/cmpe273-dead-mans-switch/gen/deadman/v1"
	"github.com/yashashav/cmpe273-dead-mans-switch/internal/detector"
	"github.com/yashashav/cmpe273-dead-mans-switch/internal/eventlog"
)

// Server is the gRPC server side of the Monitor. It implements
// StreamHeartbeats (push receiver), Register, and a stub Ping (the Monitor
// itself does not respond to pings — that's the Worker's job — but the
// generated interface requires the method).
type Server struct {
	deadmanv1.UnimplementedHeartbeatServer

	reg *Registry
	det detector.Detector
	log *eventlog.Logger
}

func NewServer(reg *Registry, det detector.Detector, log *eventlog.Logger) *Server {
	return &Server{reg: reg, det: det, log: log}
}

// Register adds the worker to the registry. Duplicate worker_ids are rejected.
func (s *Server) Register(ctx context.Context, req *deadmanv1.RegisterRequest) (*deadmanv1.RegisterReply, error) {
	ok := s.reg.Register(req.WorkerId, req.Addr)
	if !ok {
		return &deadmanv1.RegisterReply{Accepted: false, Reason: "duplicate_worker_id"}, nil
	}
	return &deadmanv1.RegisterReply{Accepted: true}, nil
}

// StreamHeartbeats reads heartbeats off the stream and feeds them to the
// registry + detector using the Monitor's monotonic clock for arrival time.
// Stream closure (clean EOF or transport error) is logged but is not fed to
// the Detector — the spec keeps the Detector mode-agnostic.
func (s *Server) StreamHeartbeats(stream deadmanv1.Heartbeat_StreamHeartbeatsServer) error {
	var workerID string
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			s.log.Event(time.Now(), eventlog.Event{Type: "stream_close", Worker: workerID, Error: "EOF"})
			return stream.SendAndClose(&deadmanv1.StreamAck{Ok: true})
		}
		if err != nil {
			s.log.Event(time.Now(), eventlog.Event{Type: "stream_close", Worker: workerID, Error: err.Error()})
			return err
		}
		arrival := time.Now()
		workerID = msg.WorkerId
		s.reg.OnHeartbeat(workerID, arrival, "push")
		s.det.Heartbeat(workerID, arrival)
	}
}

// Ping on the Monitor server is unimplemented by design: the Worker is the
// one that responds to pings. The method exists only to satisfy the generated
// interface; it returns Unimplemented.
func (s *Server) Ping(context.Context, *deadmanv1.PingRequest) (*deadmanv1.PingReply, error) {
	return nil, errPingUnsupported
}

var errPingUnsupported = errors.New("Monitor does not implement Ping; pull mode targets the Worker")
