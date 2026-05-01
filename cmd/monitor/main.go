// Command monitor runs the Monitor process: one gRPC server, optional pull
// pollers per registered worker, the periodic evaluator that feeds the
// failure detector's verdict into the registry, and an optional TUI.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	deadmanv1 "github.com/yashashav/cmpe273-dead-mans-switch/gen/deadman/v1"
	"github.com/yashashav/cmpe273-dead-mans-switch/internal/detector"
	"github.com/yashashav/cmpe273-dead-mans-switch/internal/eventlog"
	"github.com/yashashav/cmpe273-dead-mans-switch/internal/monitor"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	listen := flag.String("listen", ":50051", "gRPC bind addr")
	mode := flag.String("mode", "push", "heartbeat direction: push|pull|hybrid")
	det := flag.String("detector", "phi", "algorithm: phi|fixed")
	hbInterval := flag.Duration("hb-interval", 1*time.Second, "expected heartbeat period")
	missMul := flag.Int("miss-multiplier", 3, "fixed: T_missing = hb-interval * N")
	deadMul := flag.Int("dead-multiplier", 10, "fixed: T_dead = hb-interval * N")
	phiMissing := flag.Float64("phi-missing", 1.0, "phi threshold for MISSING")
	phiDead := flag.Float64("phi-dead", 8.0, "phi threshold for DEAD")
	phiWindow := flag.Int("phi-window", 1000, "sliding window size for Phi Accrual")
	phiMinSamples := flag.Int("phi-min-samples", 10, "bootstrap threshold")
	pollInterval := flag.Duration("poll-interval", 1*time.Second, "pull mode: how often Monitor pings")
	pollTimeout := flag.Duration("poll-timeout", 500*time.Millisecond, "pull mode: per-RPC deadline")
	evalInterval := flag.Duration("eval-interval", 200*time.Millisecond, "how often state machine re-evaluated")
	logFile := flag.String("log-file", "events.jsonl", "structured log path")
	tui := flag.Bool("tui", true, "render the live TUI; pass --tui=false for benchmarks")
	flag.Parse()

	f, err := os.Create(*logFile)
	if err != nil {
		log.Fatalf("open log file: %v", err)
	}
	defer f.Close()
	logger := eventlog.NewLogger(f)

	// Build detector.
	var d detector.Detector
	switch *det {
	case "phi":
		fb := detector.NewFixedWindow(*hbInterval, *missMul, *deadMul)
		d = detector.NewPhiAccrual(*hbInterval, *phiWindow, *phiMinSamples, *phiMissing, *phiDead, fb)
	case "fixed":
		d = detector.NewFixedWindow(*hbInterval, *missMul, *deadMul)
	default:
		log.Fatalf("unknown --detector=%q (want phi|fixed)", *det)
	}

	reg := monitor.NewRegistry(logger)
	srv := monitor.NewServer(reg, d, logger)
	eval := monitor.NewEvaluator(reg, d, *det)

	// Start gRPC server.
	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	deadmanv1.RegisterHeartbeatServer(gs, srv)
	go func() {
		log.Printf("monitor listening on %s mode=%s detector=%s", *listen, *mode, *det)
		if err := gs.Serve(lis); err != nil {
			log.Printf("grpc serve: %v", err)
		}
	}()

	// Wire up signal handling and start the evaluator.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	go eval.Run(ctx, *evalInterval)

	// Pull mode: spawn pollers as workers register.
	if *mode == "pull" || *mode == "hybrid" {
		go runPullers(ctx, reg, d, logger, *pollInterval, *pollTimeout, *mode == "hybrid")
	}

	if *tui {
		monitor.RunTUI(ctx, reg, *evalInterval, *mode, *det)
	} else {
		<-ctx.Done()
	}
	gs.GracefulStop()
}

// runPullers polls the registry for newly registered workers and starts a
// Poller for each. In hybrid mode, the Poller's events are fed to the registry
// for bandwidth accounting only (the Detector is fed by push); we mark hybrid
// here by feeding into a no-op detector instead. For simplicity in this build
// we instantiate Poller with the real detector in pull mode and a discard
// detector in hybrid mode — see spec §3 hybrid clause.
func runPullers(ctx context.Context, reg *monitor.Registry, det detector.Detector, logger *eventlog.Logger,
	interval, timeout time.Duration, hybrid bool) {

	started := make(map[string]struct{})
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			for _, w := range reg.Snapshot() {
				if _, ok := started[w.ID]; ok || w.Addr == "" {
					continue
				}
				addr := w.Addr
				if !strings.Contains(addr, ":") {
					continue
				}
				conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					log.Printf("dial %s: %v", addr, err)
					continue
				}
				client := deadmanv1.NewHeartbeatClient(conn)
				detForPoller := det
				if hybrid {
					detForPoller = discardDetector{}
				}
				p := monitor.NewPoller(client, w.ID, reg, detForPoller, logger, interval, timeout)
				go func() {
					defer conn.Close()
					p.Run(ctx)
				}()
				started[w.ID] = struct{}{}
			}
		}
	}
}

// discardDetector is fed by hybrid-mode pollers; it records nothing, so the
// detector statistics derive only from push events.
type discardDetector struct{}

func (discardDetector) Heartbeat(string, time.Time)                           {}
func (discardDetector) Suspicion(string, time.Time) (float64, detector.State) { return 0, detector.Alive }
func (discardDetector) Forget(string)                                         {}
