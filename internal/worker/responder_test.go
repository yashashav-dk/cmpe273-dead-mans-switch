package worker

import (
	"context"
	"math/rand"
	"net"
	"testing"
	"time"

	deadmanv1 "github.com/yashashav/cmpe273-dead-mans-switch/gen/deadman/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func startResponder(t *testing.T, chaos *Chaos) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	r := NewResponder("w1", chaos)
	deadmanv1.RegisterHeartbeatServer(gs, r)
	go gs.Serve(lis)
	return lis.Addr().String(), func() { gs.Stop(); lis.Close() }
}

func TestResponder_PingRepliesWithWorkerID(t *testing.T) {
	chaos := NewChaos(ChaosConfig{}, rand.New(rand.NewSource(1)))
	addr, cleanup := startResponder(t, chaos)
	defer cleanup()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := deadmanv1.NewHeartbeatClient(conn)

	reply, err := client.Ping(context.Background(), &deadmanv1.PingRequest{SentUnixNanos: time.Now().UnixNano()})
	if err != nil {
		t.Fatal(err)
	}
	if reply.WorkerId != "w1" {
		t.Errorf("WorkerId = %q, want \"w1\"", reply.WorkerId)
	}
	if reply.WorkerUnixNanos == 0 {
		t.Error("WorkerUnixNanos should be set")
	}
}

func TestResponder_DropAlwaysFails(t *testing.T) {
	chaos := NewChaos(ChaosConfig{DropRate: 1.0}, rand.New(rand.NewSource(1)))
	addr, cleanup := startResponder(t, chaos)
	defer cleanup()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := deadmanv1.NewHeartbeatClient(conn)
	_, err = client.Ping(context.Background(), &deadmanv1.PingRequest{})
	if err == nil {
		t.Fatal("expected error with DropRate=1.0")
	}
	if got := status.Code(err); got != codes.Unavailable {
		t.Errorf("error code = %s, want Unavailable", got)
	}
}

func TestResponder_LagDelaysReply(t *testing.T) {
	chaos := NewChaos(
		ChaosConfig{LagMean: 100 * time.Millisecond, LagStddev: 0},
		rand.New(rand.NewSource(1)),
	)
	addr, cleanup := startResponder(t, chaos)
	defer cleanup()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := deadmanv1.NewHeartbeatClient(conn)
	start := time.Now()
	_, err = client.Ping(context.Background(), &deadmanv1.PingRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
		t.Errorf("Ping returned in %s, expected >=80ms due to lag", elapsed)
	}
}
