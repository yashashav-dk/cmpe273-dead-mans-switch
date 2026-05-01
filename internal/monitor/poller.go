package monitor

import (
	"context"
	"time"

	deadmanv1 "github.com/yashashav/cmpe273-dead-mans-switch/gen/deadman/v1"
	"github.com/yashashav/cmpe273-dead-mans-switch/internal/detector"
	"github.com/yashashav/cmpe273-dead-mans-switch/internal/eventlog"
)

// Poller is the pull-mode driver for a single worker. The Monitor spawns one
// Poller per registered worker. Successful pings produce heartbeat events
// fed to both the registry and the detector; errors are logged as ping_fail
// and intentionally not fed to the detector (the detector accrues suspicion
// from the absence of input — see spec §9 / §5).
type Poller struct {
	client       deadmanv1.HeartbeatClient
	workerID     string
	reg          *Registry
	det          detector.Detector
	log          *eventlog.Logger
	pollInterval time.Duration
	pollTimeout  time.Duration
}

func NewPoller(client deadmanv1.HeartbeatClient, workerID string, reg *Registry, det detector.Detector,
	log *eventlog.Logger, pollInterval, pollTimeout time.Duration) *Poller {
	return &Poller{
		client:       client,
		workerID:     workerID,
		reg:          reg,
		det:          det,
		log:          log,
		pollInterval: pollInterval,
		pollTimeout:  pollTimeout,
	}
}

// Run loops until ctx is cancelled, polling once per pollInterval. Each ping
// is bounded by pollTimeout.
func (p *Poller) Run(ctx context.Context) {
	t := time.NewTicker(p.pollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.pingOnce(ctx)
		}
	}
}

func (p *Poller) pingOnce(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, p.pollTimeout)
	defer cancel()
	_, err := p.client.Ping(ctx, &deadmanv1.PingRequest{SentUnixNanos: time.Now().UnixNano()})
	if err != nil {
		p.log.Event(time.Now(), eventlog.Event{
			Type:   "ping_fail",
			Worker: p.workerID,
			Error:  err.Error(),
		})
		return
	}
	arrival := time.Now()
	p.reg.OnHeartbeat(p.workerID, arrival, "pull")
	p.det.Heartbeat(p.workerID, arrival)
}
