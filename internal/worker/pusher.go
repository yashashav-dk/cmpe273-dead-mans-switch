package worker

import (
	"context"
	"time"

	deadmanv1 "github.com/yashashav/cmpe273-dead-mans-switch/gen/deadman/v1"
)

// Pusher streams heartbeats to a Monitor on a fixed cadence, applying chaos
// (lag and drop) before each send. On stream error it returns; the caller is
// responsible for retrying with backoff (cmd/worker/main.go does this).
type Pusher struct {
	client     deadmanv1.HeartbeatClient
	workerID   string
	hbInterval time.Duration
	chaos      *Chaos
}

func NewPusher(client deadmanv1.HeartbeatClient, workerID string, hbInterval time.Duration, chaos *Chaos) *Pusher {
	return &Pusher{client: client, workerID: workerID, hbInterval: hbInterval, chaos: chaos}
}

// Run opens a StreamHeartbeats stream and sends until ctx is cancelled or
// the stream errors. Returns the first error encountered (or ctx.Err()).
func (p *Pusher) Run(ctx context.Context) error {
	stream, err := p.client.StreamHeartbeats(ctx)
	if err != nil {
		return err
	}
	t := time.NewTicker(p.hbInterval)
	defer t.Stop()
	var seq int64
	for {
		select {
		case <-ctx.Done():
			_, _ = stream.CloseAndRecv()
			return ctx.Err()
		case now := <-t.C:
			if p.chaos.ShouldDrop() {
				continue
			}
			if lag := p.chaos.SampleLag(); lag > 0 {
				time.Sleep(lag)
			}
			seq++
			err := stream.Send(&deadmanv1.HeartbeatMsg{
				WorkerId:      p.workerID,
				Seq:           seq,
				SentUnixNanos: now.UnixNano(),
			})
			if err != nil {
				return err
			}
		}
	}
}
