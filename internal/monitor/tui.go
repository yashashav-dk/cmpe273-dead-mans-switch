package monitor

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/yashashav/cmpe273-dead-mans-switch/internal/detector"
)

// RunTUI starts the bubbletea event loop and blocks until ctx is cancelled
// or the user presses 'q'. It is safe to skip (use --tui=false) for benchmarks.
func RunTUI(ctx context.Context, reg *Registry, refresh time.Duration, mode, detName string) {
	m := tuiModel{
		reg:     reg,
		refresh: refresh,
		mode:    mode,
		det:     detName,
		started: time.Now(),
	}
	p := tea.NewProgram(m)
	go func() {
		<-ctx.Done()
		p.Quit()
	}()
	_, _ = p.Run()
}

type tuiModel struct {
	reg     *Registry
	refresh time.Duration
	mode    string
	det     string
	started time.Time
	now     time.Time
}

type tickMsg time.Time

func (m tuiModel) Init() tea.Cmd { return tickCmd(m.refresh) }

func (m tuiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		}
	case tickMsg:
		m.now = time.Time(msg)
		return m, tickCmd(m.refresh)
	}
	return m, nil
}

func (m tuiModel) View() string {
	headerStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("39"))
	titleStyle := lipgloss.NewStyle().Bold(true).Underline(true)
	aliveStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("46"))
	missingStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("226"))
	deadStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("196"))

	var b strings.Builder
	b.WriteString(titleStyle.Render("Dead Man's Switch — Monitor") + "\n")
	uptime := time.Since(m.started).Truncate(time.Second)
	b.WriteString(fmt.Sprintf("Mode: %s   Detector: %s   Workers: %d   Uptime: %s\n\n",
		m.mode, m.det, len(m.reg.Workers()), uptime))

	b.WriteString(headerStyle.Render(fmt.Sprintf("%-14s %-9s %-10s %-9s\n", "Worker", "State", "Last HB", "Suspicion")))
	for _, w := range m.reg.Snapshot() {
		var lastHB string
		if w.LastHeartbeat.IsZero() {
			lastHB = "—"
		} else {
			lastHB = fmt.Sprintf("%.1fs", time.Since(w.LastHeartbeat).Seconds())
		}
		row := fmt.Sprintf("%-14s %-9s %-10s %-9.2f", w.ID, w.State.String(), lastHB, w.LastSuspicion)
		switch w.State {
		case detector.Alive:
			row = aliveStyle.Render(row)
		case detector.Missing:
			row = missingStyle.Render(row)
		case detector.Dead:
			row = deadStyle.Render(row)
		}
		b.WriteString(row + "\n")
	}
	b.WriteString("\n[q] quit\n")
	return b.String()
}

func tickCmd(d time.Duration) tea.Cmd {
	return tea.Tick(d, func(t time.Time) tea.Msg { return tickMsg(t) })
}

// RenderFrame produces a single, non-interactive TUI frame for documentation
// snapshots and tests. It does not write to the terminal — caller decides
// where the string goes.
func RenderFrame(reg *Registry, mode, detName string, uptime time.Duration) string {
	m := tuiModel{
		reg:     reg,
		mode:    mode,
		det:     detName,
		started: time.Now().Add(-uptime),
	}
	return m.View()
}
