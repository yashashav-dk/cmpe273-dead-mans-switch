package worker

import (
	"context"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	deadmanv1 "github.com/yashashav/cmpe273-dead-mans-switch/gen/deadman/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// fakeMonitor implements the Heartbeat server, counting received heartbeats.
type fakeMonitor struct {
	deadmanv1.UnimplementedHeartbeatServer
	mu       sync.Mutex
	received int32
	lastSeq  int64
}

func (f *fakeMonitor) Register(_ context.Context, _ *deadmanv1.RegisterRequest) (*deadmanv1.RegisterReply, error) {
	return &deadmanv1.RegisterReply{Accepted: true}, nil
}

func (f *fakeMonitor) StreamHeartbeats(stream deadmanv1.Heartbeat_StreamHeartbeatsServer) error {
	for {
		msg, err := stream.Recv()
		if err != nil {
			return stream.SendAndClose(&deadmanv1.StreamAck{Ok: true})
		}
		atomic.AddInt32(&f.received, 1)
		f.mu.Lock()
		f.lastSeq = msg.Seq
		f.mu.Unlock()
	}
}

func startFakeMonitor(t *testing.T) (*fakeMonitor, string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	fm := &fakeMonitor{}
	deadmanv1.RegisterHeartbeatServer(gs, fm)
	go gs.Serve(lis)
	return fm, lis.Addr().String(), func() { gs.Stop(); lis.Close() }
}

func TestPusher_SendsHeartbeatsAtInterval(t *testing.T) {
	fm, addr, cleanup := startFakeMonitor(t)
	defer cleanup()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	chaos := NewChaos(ChaosConfig{}, rand.New(rand.NewSource(1)))
	p := NewPusher(deadmanv1.NewHeartbeatClient(conn), "w1", 50*time.Millisecond, chaos)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_ = p.Run(ctx)

	got := atomic.LoadInt32(&fm.received)
	if got < 4 || got > 8 {
		t.Errorf("received = %d in 300ms @ 50ms cadence, want 4..8", got)
	}
}

func TestPusher_DropRateReducesSends(t *testing.T) {
	fm, addr, cleanup := startFakeMonitor(t)
	defer cleanup()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	chaos := NewChaos(ChaosConfig{DropRate: 1.0}, rand.New(rand.NewSource(1)))
	p := NewPusher(deadmanv1.NewHeartbeatClient(conn), "w1", 30*time.Millisecond, chaos)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = p.Run(ctx)

	if got := atomic.LoadInt32(&fm.received); got != 0 {
		t.Errorf("with DropRate=1.0 received = %d, want 0", got)
	}
}
