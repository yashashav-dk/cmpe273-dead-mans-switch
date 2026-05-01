package worker

import (
	"math/rand"
	"testing"
	"time"
)

func TestChaos_NoLagWhenZero(t *testing.T) {
	c := NewChaos(ChaosConfig{}, rand.New(rand.NewSource(1)))
	if got := c.SampleLag(); got != 0 {
		t.Errorf("zero config lag = %s, want 0", got)
	}
}

func TestChaos_LagIsNonNegative(t *testing.T) {
	c := NewChaos(ChaosConfig{LagMean: 100 * time.Millisecond, LagStddev: 50 * time.Millisecond},
		rand.New(rand.NewSource(1)))
	for i := 0; i < 1000; i++ {
		if got := c.SampleLag(); got < 0 {
			t.Fatalf("negative lag: %s", got)
		}
	}
}

func TestChaos_DropZeroNeverDrops(t *testing.T) {
	c := NewChaos(ChaosConfig{}, rand.New(rand.NewSource(1)))
	for i := 0; i < 1000; i++ {
		if c.ShouldDrop() {
			t.Fatal("drop with rate=0")
		}
	}
}

func TestChaos_DropOneAlwaysDrops(t *testing.T) {
	c := NewChaos(ChaosConfig{DropRate: 1.0}, rand.New(rand.NewSource(1)))
	for i := 0; i < 100; i++ {
		if !c.ShouldDrop() {
			t.Fatal("no drop with rate=1")
		}
	}
}

func TestChaos_DropRateApproximate(t *testing.T) {
	c := NewChaos(ChaosConfig{DropRate: 0.3}, rand.New(rand.NewSource(42)))
	dropped := 0
	for i := 0; i < 10_000; i++ {
		if c.ShouldDrop() {
			dropped++
		}
	}
	rate := float64(dropped) / 10_000.0
	if rate < 0.27 || rate > 0.33 {
		t.Errorf("drop rate = %f, want ~0.30", rate)
	}
}

func TestChaos_KillScheduleFiresAfterDuration(t *testing.T) {
	c := NewChaos(ChaosConfig{KillAfter: 50 * time.Millisecond}, rand.New(rand.NewSource(1)))
	start := c.StartedAt()
	if c.ShouldKill(start) {
		t.Fatal("ShouldKill before deadline")
	}
	if c.ShouldKill(start.Add(40 * time.Millisecond)) {
		t.Fatal("ShouldKill before deadline")
	}
	if !c.ShouldKill(start.Add(60 * time.Millisecond)) {
		t.Fatal("ShouldKill should fire at +60ms")
	}
}

func TestChaos_KillScheduleZeroNeverFires(t *testing.T) {
	c := NewChaos(ChaosConfig{}, rand.New(rand.NewSource(1)))
	if c.ShouldKill(time.Now().Add(time.Hour)) {
		t.Error("ShouldKill should never fire when KillAfter=0")
	}
}
