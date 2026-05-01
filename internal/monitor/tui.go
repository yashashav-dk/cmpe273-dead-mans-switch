package monitor

import (
	"context"
	"time"
)

// RunTUI is a placeholder until Task 17. It blocks on ctx.Done() so the binary
// can be built and used in headless (--tui=false) mode.
func RunTUI(ctx context.Context, _ *Registry, _ time.Duration, _ string, _ string) {
	<-ctx.Done()
}
