package monitor

import (
	"bytes"
	"context"
	"math/rand"
	"net"
	"sync/atomic"
	"testing"
	"time"

	deadmanv1 "github.com/yashashav/cmpe273-dead-mans-switch/gen/deadman/v1"
	"github.com/yashashav/cmpe273-dead-mans-switch/internal/detector"
	"github.com/yashashav/cmpe273-dead-mans-switch/internal/eventlog"
	"github.com/yashashav/cmpe273-dead-mans-switch/internal/worker"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func startPullableWorker(t *testing.T, chaos *worker.Chaos) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	deadmanv1.RegisterHeartbeatServer(gs, worker.NewResponder("w1", chaos))
	go gs.Serve(lis)
	return lis.Addr().String(), func() { gs.Stop(); lis.Close() }
}

func TestPoller_FeedsDetectorOnSuccessfulPing(t *testing.T) {
	chaos := worker.NewChaos(worker.ChaosConfig{}, rand.New(rand.NewSource(1)))
	addr, cleanup := startPullableWorker(t, chaos)
	defer cleanup()

	var buf bytes.Buffer
	logger := eventlog.NewLogger(&buf)
	reg := NewRegistry(logger)
	det := detector.NewFixedWindow(1*time.Second, 3, 10)
	reg.Register("w1", addr)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	p := NewPoller(deadmanv1.NewHeartbeatClient(conn), "w1", reg, det, logger,
		50*time.Millisecond, 200*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	p.Run(ctx)

	_, st := det.Suspicion("w1", time.Now())
	if st != detector.Alive {
		t.Errorf("detector state = %s, want ALIVE after successful pings", st)
	}
}

func TestPoller_LogsPingFailWhenWorkerDown(t *testing.T) {
	var buf bytes.Buffer
	logger := eventlog.NewLogger(&buf)
	reg := NewRegistry(logger)
	det := detector.NewFixedWindow(1*time.Second, 3, 10)
	reg.Register("w1", "127.0.0.1:1")

	conn, err := grpc.NewClient("127.0.0.1:1", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	p := NewPoller(deadmanv1.NewHeartbeatClient(conn), "w1", reg, det, logger,
		50*time.Millisecond, 100*time.Millisecond)

	failures := int32(0)
	atomic.StoreInt32(&failures, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	p.Run(ctx)

	if !bytes.Contains(buf.Bytes(), []byte(`"type":"ping_fail"`)) {
		t.Errorf("expected ping_fail event in log:\n%s", buf.String())
	}
}
