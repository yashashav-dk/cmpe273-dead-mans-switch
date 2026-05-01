// Command worker runs the Worker process: a Ping responder (always on, for
// pull mode) and a heartbeat pusher (always on, for push mode). The Monitor
// itself decides which transport pattern is used; the Worker runs both
// endpoints unconditionally so it can serve any Monitor without reconfiguration.
package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	deadmanv1 "github.com/yashashav/cmpe273-dead-mans-switch/gen/deadman/v1"
	"github.com/yashashav/cmpe273-dead-mans-switch/internal/worker"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	id := flag.String("id", "worker-1", "logical worker name")
	monitorAddr := flag.String("monitor", "localhost:50051", "primary Monitor addr (push target + Register)")
	pullMonitors := flag.String("pull-monitors", "", "comma-separated Monitor addrs that should Register and poll us")
	listen := flag.String("listen", ":50061", "gRPC bind for Ping responder")
	hbInterval := flag.Duration("hb-interval", 1*time.Second, "push cadence")
	crashAfter := flag.Duration("chaos-crash-after", 0, "exit cleanly after N (0=never)")
	killAfter := flag.Duration("chaos-kill-after", 0, "exit(1) after N (0=never)")
	lagMean := flag.Duration("chaos-lag-mean", 0, "injected latency before send / Ping reply")
	lagStddev := flag.Duration("chaos-lag-stddev", 0, "Normal stddev around lag-mean")
	dropRate := flag.Float64("chaos-drop-rate", 0, "probability of skipping a heartbeat / dropping a Ping")
	flag.Parse()

	chaos := worker.NewChaos(worker.ChaosConfig{
		LagMean:    *lagMean,
		LagStddev:  *lagStddev,
		DropRate:   *dropRate,
		KillAfter:  *killAfter,
		CrashAfter: *crashAfter,
	}, rand.New(rand.NewSource(time.Now().UnixNano())))

	// Start Ping responder (always on).
	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	deadmanv1.RegisterHeartbeatServer(gs, worker.NewResponder(*id, chaos))
	go func() {
		if err := gs.Serve(lis); err != nil {
			log.Printf("grpc serve: %v", err)
		}
	}()
	log.Printf("worker %s responder on %s", *id, *listen)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Chaos kill/crash watchdog.
	go runChaosWatchdog(ctx, chaos)

	// Connect to primary Monitor and Register + push heartbeats.
	conn, err := grpc.NewClient(*monitorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial monitor: %v", err)
	}
	defer conn.Close()
	client := deadmanv1.NewHeartbeatClient(conn)
	mustRegister(ctx, client, *id, *listen)

	// Register with additional pull-mode Monitors so they know to poll us.
	for _, addr := range splitAndTrim(*pullMonitors) {
		c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("dial pull-monitor %s: %v", addr, err)
			continue
		}
		mustRegister(ctx, deadmanv1.NewHeartbeatClient(c), *id, *listen)
		_ = c // keep open; GC takes the client when ctx ends
	}

	// Push heartbeats with reconnect backoff.
	go runPusherWithBackoff(ctx, client, *id, *hbInterval, chaos)

	<-ctx.Done()
	gs.GracefulStop()
}

func mustRegister(ctx context.Context, client deadmanv1.HeartbeatClient, id, addr string) {
	registrationCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	reply, err := client.Register(registrationCtx, &deadmanv1.RegisterRequest{WorkerId: id, Addr: localizeAddr(addr)})
	if err != nil {
		log.Printf("register: %v", err)
		return
	}
	if !reply.Accepted {
		// duplicate_worker_id is non-fatal: the monitor already knows about us
		// (e.g. same Monitor passed in both --monitor and --pull-monitors).
		log.Printf("register on this monitor not accepted: %s", reply.Reason)
	}
}

// localizeAddr converts ":50061" or "0.0.0.0:50061" to "127.0.0.1:50061" so
// the Monitor has a dialable addr for pull mode. (Workers and Monitor are
// expected to share a host in this design — multi-host is out of scope.)
func localizeAddr(listen string) string {
	if strings.HasPrefix(listen, ":") {
		return "127.0.0.1" + listen
	}
	if strings.HasPrefix(listen, "0.0.0.0:") {
		return "127.0.0.1" + listen[len("0.0.0.0"):]
	}
	return listen
}

func splitAndTrim(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func runPusherWithBackoff(ctx context.Context, client deadmanv1.HeartbeatClient, id string, interval time.Duration, chaos *worker.Chaos) {
	backoff := 100 * time.Millisecond
	const maxBackoff = 5 * time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		p := worker.NewPusher(client, id, interval, chaos)
		err := p.Run(ctx)
		if ctx.Err() != nil {
			return
		}
		log.Printf("pusher returned: %v; reconnect in %s", err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// chaosMu protects the once-off process-exit decisions from racing with each
// other across kill and crash watchdogs.
var chaosMu sync.Mutex

func runChaosWatchdog(ctx context.Context, chaos *worker.Chaos) {
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			chaosMu.Lock()
			if chaos.ShouldKill(now) {
				log.Printf("chaos: kill (exit 1)")
				os.Exit(1)
			}
			if chaos.ShouldCrash(now) {
				log.Printf("chaos: crash (exit 0)")
				os.Exit(0)
			}
			chaosMu.Unlock()
		}
	}
}
