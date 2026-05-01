// Renders one frame of the TUI with synthetic worker data, prints to stdout
// and exits. Used to produce a text snapshot for the README without running
// an interactive terminal.
package main

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/yashashav/cmpe273-dead-mans-switch/internal/detector"
	"github.com/yashashav/cmpe273-dead-mans-switch/internal/eventlog"
	"github.com/yashashav/cmpe273-dead-mans-switch/internal/monitor"
)

func main() {
	reg := monitor.NewRegistry(eventlog.NewLogger(&bytes.Buffer{}))
	now := time.Now()

	// Synthetic registry state.
	reg.Register("worker-1", "127.0.0.1:50061")
	reg.Register("worker-2", "127.0.0.1:50062")
	reg.Register("worker-3", "127.0.0.1:50063")
	reg.Register("worker-4", "127.0.0.1:50064")
	reg.Register("worker-5", "127.0.0.1:50065")

	reg.OnHeartbeat("worker-1", now.Add(-400*time.Millisecond), "push")
	reg.OnHeartbeat("worker-2", now.Add(-700*time.Millisecond), "push")
	reg.OnHeartbeat("worker-3", now.Add(-4200*time.Millisecond), "push")
	reg.OnHeartbeat("worker-4", now.Add(-18900*time.Millisecond), "push")
	reg.OnHeartbeat("worker-5", now.Add(-200*time.Millisecond), "push")

	reg.Transition("worker-1", detector.Alive, 0.02, "phi")
	reg.Transition("worker-2", detector.Alive, 0.05, "phi")
	reg.Transition("worker-3", detector.Missing, 2.31, "phi")
	reg.Transition("worker-4", detector.Dead, 1e9, "phi")
	reg.Transition("worker-5", detector.Alive, 0.01, "phi")

	// Render a single TUI frame and dump it.
	frame := monitor.RenderFrame(reg, "push", "phi", 2*time.Minute+14*time.Second)
	fmt.Fprint(os.Stdout, frame)
}
