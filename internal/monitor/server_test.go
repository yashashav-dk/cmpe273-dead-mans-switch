package monitor

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	deadmanv1 "github.com/yashashav/cmpe273-dead-mans-switch/gen/deadman/v1"
	"github.com/yashashav/cmpe273-dead-mans-switch/internal/detector"
	"github.com/yashashav/cmpe273-dead-mans-switch/internal/eventlog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func newServerForTest(t *testing.T) (*Server, *Registry, detector.Detector, string, func()) {
	t.Helper()
	var buf bytes.Buffer
	logger := eventlog.NewLogger(&buf)
	reg := NewRegistry(logger)
	det := detector.NewFixedWindow(1*time.Second, 3, 10)

	s := NewServer(reg, det, logger)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	deadmanv1.RegisterHeartbeatServer(gs, s)
	go gs.Serve(lis)

	cleanup := func() { gs.Stop(); lis.Close() }
	return s, reg, det, lis.Addr().String(), cleanup
}

func dialTest(t *testing.T, addr string) (deadmanv1.HeartbeatClient, func()) {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	return deadmanv1.NewHeartbeatClient(conn), func() { conn.Close() }
}

func TestServer_RegisterAccepts(t *testing.T) {
	_, reg, _, addr, cleanup := newServerForTest(t)
	defer cleanup()
	client, dclose := dialTest(t, addr)
	defer dclose()

	reply, err := client.Register(context.Background(), &deadmanv1.RegisterRequest{
		WorkerId: "w1", Addr: "localhost:50061",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reply.Accepted {
		t.Errorf("Register not accepted: reason=%q", reply.Reason)
	}
	if got := reg.Workers(); len(got) != 1 || got[0] != "w1" {
		t.Errorf("registry workers = %v, want [w1]", got)
	}
}

func TestServer_RegisterRejectsDuplicate(t *testing.T) {
	_, _, _, addr, cleanup := newServerForTest(t)
	defer cleanup()
	client, dclose := dialTest(t, addr)
	defer dclose()

	_, err := client.Register(context.Background(), &deadmanv1.RegisterRequest{WorkerId: "w1"})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := client.Register(context.Background(), &deadmanv1.RegisterRequest{WorkerId: "w1"})
	if err != nil {
		t.Fatal(err)
	}
	if reply.Accepted {
		t.Error("duplicate Register should be rejected")
	}
	if reply.Reason != "duplicate_worker_id" {
		t.Errorf("reason = %q, want duplicate_worker_id", reply.Reason)
	}
}

func TestServer_StreamHeartbeatsFeedsDetector(t *testing.T) {
	_, reg, det, addr, cleanup := newServerForTest(t)
	defer cleanup()
	client, dclose := dialTest(t, addr)
	defer dclose()

	stream, err := client.StreamHeartbeats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := stream.Send(&deadmanv1.HeartbeatMsg{WorkerId: "w1", Seq: int64(i)}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	stream.CloseSend()
	_, _ = stream.CloseAndRecv() // ignore EOF/ack timing

	// Allow the server-side recv loop to land the heartbeats.
	time.Sleep(100 * time.Millisecond)
	if got := reg.Workers(); len(got) != 1 || got[0] != "w1" {
		t.Errorf("registry workers = %v, want [w1]", got)
	}
	_, st := det.Suspicion("w1", time.Now())
	if st != detector.Alive {
		t.Errorf("detector state = %s, want ALIVE", st)
	}
}
