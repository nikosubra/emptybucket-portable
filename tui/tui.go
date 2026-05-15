// Package tui provides a Bubble Tea front-end. It presents a credentials form
// followed by a live progress view that consumes runner events.
package tui

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/nikosubra/emptybucket-portable/runner"
)

type phase int

const (
	phaseForm phase = iota
	phaseRunning
	phaseDone
)

type Model struct {
	phase       phase
	inputs      []textinput.Model
	focusIndex  int
	dryRun      bool
	insecure    bool
	engine      string
	width       int
	height      int
	statusLines []string
	stats       runner.Stats
	inv         *runner.Event
	finalRes    *runner.Result
	errMsg      string
	events      chan runner.Event
	cancel      context.CancelFunc
}

// Fields the form collects; index corresponds to inputs[].
const (
	fEndpoint = iota
	fRegion
	fBucket
	fPrefix
	fAccessKey
	fSecretKey
	fWorkers
	fBatchSize
	nFields
)

var fieldLabels = []string{"Endpoint", "Region", "Bucket", "Prefix (optional)", "Access Key", "Secret Key", "Workers", "Batch size"}

func New() Model {
	m := Model{
		phase:  phaseForm,
		inputs: make([]textinput.Model, nFields),
		engine: "sdk",
	}
	for i := range m.inputs {
		ti := textinput.New()
		ti.CharLimit = 256
		ti.Width = 50
		switch i {
		case fEndpoint:
			ti.Placeholder = "https://s3.example.com"
		case fRegion:
			ti.Placeholder = "us-east-1"
			ti.SetValue("us-east-1")
		case fBucket:
			ti.Placeholder = "my-bucket"
		case fPrefix:
			ti.Placeholder = "logs/  (leave blank to wipe whole bucket)"
		case fAccessKey:
			ti.Placeholder = "AKIA..."
		case fSecretKey:
			ti.EchoMode = textinput.EchoPassword
			ti.EchoCharacter = '•'
		case fWorkers:
			ti.SetValue("4")
		case fBatchSize:
			ti.SetValue("200")
		}
		m.inputs[i] = ti
	}
	m.inputs[0].Focus()
	return m
}

func (m Model) Init() tea.Cmd { return textinput.Blink }

type eventMsg struct{ ev runner.Event }
type finishedMsg struct{ res runner.Result }

// listenEvents subscribes the Bubble Tea program to the runner channel.
func listenEvents(events chan runner.Event) tea.Cmd {
	return func() tea.Msg {
		ev, ok := <-events
		if !ok {
			return finishedMsg{}
		}
		return eventMsg{ev: ev}
	}
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		return m, nil
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "esc":
			if m.cancel != nil {
				m.cancel()
			}
			return m, tea.Quit
		}
		if m.phase == phaseForm {
			return m.updateForm(msg)
		}
		if m.phase == phaseDone && msg.String() == "q" {
			return m, tea.Quit
		}
	case eventMsg:
		m = m.applyEvent(msg.ev)
		return m, listenEvents(m.events)
	case finishedMsg:
		m.phase = phaseDone
		return m, nil
	}
	return m, nil
}

func (m Model) updateForm(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	const totalRows = nFields + 3 // +engine, +dryRun, +insecure
	switch msg.String() {
	case "tab", "down":
		m.focusIndex = (m.focusIndex + 1) % totalRows
	case "shift+tab", "up":
		m.focusIndex = (m.focusIndex - 1 + totalRows) % totalRows
	case "enter":
		switch m.focusIndex {
		case nFields: // engine row cycles
			switch m.engine {
			case "sdk":
				m.engine = "awscli"
			case "awscli":
				m.engine = "auto"
			default:
				m.engine = "sdk"
			}
			return m, nil
		case nFields + 1: // dry-run toggles
			m.dryRun = !m.dryRun
			return m, nil
		case nFields + 2: // insecure toggles
			m.insecure = !m.insecure
			return m, nil
		case nFields - 1: // last text field submits
			return m.submit()
		default:
			m.focusIndex++
		}
	case "ctrl+s":
		return m.submit()
	case " ":
		if m.focusIndex == nFields+1 {
			m.dryRun = !m.dryRun
			return m, nil
		}
		if m.focusIndex == nFields+2 {
			m.insecure = !m.insecure
			return m, nil
		}
	}
	// Update focus + propagate key to focused input.
	for i := range m.inputs {
		if i == m.focusIndex {
			m.inputs[i].Focus()
		} else {
			m.inputs[i].Blur()
		}
	}
	var cmd tea.Cmd
	if m.focusIndex < nFields {
		m.inputs[m.focusIndex], cmd = m.inputs[m.focusIndex].Update(msg)
	}
	return m, cmd
}

func (m Model) submit() (tea.Model, tea.Cmd) {
	w, _ := strconv.Atoi(m.inputs[fWorkers].Value())
	if w <= 0 {
		w = 4
	}
	b, _ := strconv.Atoi(m.inputs[fBatchSize].Value())
	if b <= 0 {
		b = 200
	}
	req := runner.Request{
		AccessKey: m.inputs[fAccessKey].Value(),
		SecretKey: m.inputs[fSecretKey].Value(),
		Bucket:    m.inputs[fBucket].Value(),
		Prefix:    m.inputs[fPrefix].Value(),
		Endpoint:  m.inputs[fEndpoint].Value(),
		Region:    m.inputs[fRegion].Value(),
		Engine:    m.engine,
		Workers:   w,
		BatchSize: b,
		DryRun:    m.dryRun,
		Insecure:  m.insecure,
	}
	if req.Bucket == "" || req.Endpoint == "" || req.AccessKey == "" || req.SecretKey == "" {
		m.errMsg = "All fields are required"
		return m, nil
	}
	m.errMsg = ""
	m.phase = phaseRunning
	m.events = make(chan runner.Event, 256)
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	events := m.events
	go func() {
		runner.Run(ctx, req, events)
	}()
	return m, listenEvents(m.events)
}

func (m Model) applyEvent(ev runner.Event) Model {
	switch ev.Kind {
	case runner.EventStarted:
		m.appendLine("▶ " + ev.Message)
	case runner.EventInventory:
		m.inv = &ev
		if ev.Inventory != nil {
			line := fmt.Sprintf("📦 %d objects | 📁 %d folders | 💾 %s",
				ev.Inventory.TotalObjects, ev.Inventory.TopLevelFolders, runner.HumanBytes(ev.Inventory.TotalSizeBytes))
			if ev.Inventory.VersionedObjects > 0 {
				line += fmt.Sprintf(" | 🗂 %d versions | 🪦 %d markers", ev.Inventory.VersionedObjects, ev.Inventory.DeleteMarkers)
			}
			m.appendLine(line)
		}
	case runner.EventDeletion:
		if ev.Deletion != nil {
			prefix := "✓ "
			if ev.Deletion.Failed {
				prefix = "✗ "
			}
			m.appendLine(prefix + ev.Deletion.Key)
		}
	case runner.EventStats:
		if ev.Stats != nil {
			m.stats = *ev.Stats
		}
	case runner.EventFinished:
		if ev.Stats != nil {
			m.stats = *ev.Stats
			m.finalRes = &runner.Result{Deleted: int(ev.Stats.Deleted), Errors: int(ev.Stats.Errors), Duration: ev.Stats.Elapsed}
		}
		m.phase = phaseDone
		m.appendLine("✅ Finished")
	case runner.EventError:
		m.appendLine("✗ " + ev.Message)
	}
	return m
}

func (m *Model) appendLine(s string) {
	m.statusLines = append(m.statusLines, s)
	if len(m.statusLines) > 200 {
		m.statusLines = m.statusLines[len(m.statusLines)-200:]
	}
}

var (
	titleStyle  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("12"))
	labelStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	focusStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("12"))
	errStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
	muteStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	okStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("10"))
)

func (m Model) View() string {
	switch m.phase {
	case phaseForm:
		return m.viewForm()
	case phaseRunning, phaseDone:
		return m.viewProgress()
	}
	return ""
}

func (m Model) viewForm() string {
	var b strings.Builder
	b.WriteString(titleStyle.Render("emptybucket — TUI") + "\n\n")
	for i, ti := range m.inputs {
		label := fieldLabels[i]
		if i == m.focusIndex {
			label = focusStyle.Render("▸ " + label)
		} else {
			label = labelStyle.Render("  " + label)
		}
		b.WriteString(label + "\n")
		b.WriteString("  " + ti.View() + "\n\n")
	}
	// Engine row
	enginePrefix := "  "
	if m.focusIndex == nFields {
		enginePrefix = focusStyle.Render("▸ ")
	}
	b.WriteString(enginePrefix + labelStyle.Render("Engine: ") + m.engine + muteStyle.Render(" (Enter to cycle)\n\n"))
	// DryRun row
	dryPrefix := "  "
	if m.focusIndex == nFields+1 {
		dryPrefix = focusStyle.Render("▸ ")
	}
	check := "[ ]"
	if m.dryRun {
		check = "[x]"
	}
	b.WriteString(dryPrefix + labelStyle.Render("Dry run: ") + check + muteStyle.Render(" (Space to toggle)\n\n"))
	// Insecure row
	insPrefix := "  "
	if m.focusIndex == nFields+2 {
		insPrefix = focusStyle.Render("▸ ")
	}
	insCheck := "[ ]"
	if m.insecure {
		insCheck = "[x]"
	}
	b.WriteString(insPrefix + labelStyle.Render("Skip TLS verify: ") + insCheck + muteStyle.Render(" (Space to toggle — self-signed only)\n\n"))
	if m.errMsg != "" {
		b.WriteString(errStyle.Render(m.errMsg) + "\n")
	}
	b.WriteString(muteStyle.Render("Tab/Shift+Tab to navigate · Ctrl+S to start · Ctrl+C to quit"))
	return b.String()
}

func (m Model) viewProgress() string {
	var b strings.Builder
	b.WriteString(titleStyle.Render("emptybucket — running") + "\n\n")
	// Stat tiles.
	b.WriteString(fmt.Sprintf("Deleted: %s   Errors: %s   Total: %s   Rate: %.1f/s   ETA: %s\n",
		fmtInt(m.stats.Deleted), fmtInt(m.stats.Errors), fmtInt(m.stats.Total), m.stats.ObjectsPerSec, fmtDur(m.stats.ETA)))
	// Progress bar.
	width := 50
	bar := strings.Repeat("─", width)
	if m.stats.Total > 0 {
		filled := int(float64(width) * float64(m.stats.Deleted) / float64(m.stats.Total))
		if filled > width {
			filled = width
		}
		bar = strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
	}
	b.WriteString(bar + "\n\n")
	// Tail of status lines.
	start := 0
	maxLines := m.height - 10
	if maxLines < 5 {
		maxLines = 5
	}
	if len(m.statusLines) > maxLines {
		start = len(m.statusLines) - maxLines
	}
	for _, line := range m.statusLines[start:] {
		shown := line
		if m.width > 0 && len(shown) > m.width-2 {
			shown = shown[:m.width-2]
		}
		b.WriteString(shown + "\n")
	}
	if m.phase == phaseDone {
		b.WriteString("\n" + okStyle.Render("Press q to quit"))
	}
	return b.String()
}

func fmtInt(n int64) string {
	s := strconv.FormatInt(n, 10)
	// Simple thousands separator.
	if len(s) <= 3 {
		return s
	}
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	return string(out)
}

func fmtDur(d time.Duration) string {
	if d <= 0 {
		return "—"
	}
	return d.Truncate(time.Second).String()
}

// Run launches the Bubble Tea program. Returns when the user quits.
func Run() error {
	p := tea.NewProgram(New(), tea.WithAltScreen())
	_, err := p.Run()
	return err
}
