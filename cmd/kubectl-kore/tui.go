package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
)

// Okabe-Ito 色盲安全系（validator：CVD ΔE 48.9、对比度 ≥3:1 全过；亮度带越带
// 属 web 面积均衡指标，TUI 每格自带字母键作 secondary encoding，故保留原色）。
var palette = []string{"#E69F00", "#56B4E9", "#009E73", "#F0E442", "#0072B2", "#D55E00", "#CC79A7"}

var (
	stFree     = lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
	stReserved = lipgloss.NewStyle().Background(lipgloss.Color("240")).Foreground(lipgloss.Color("232"))
	stNode     = lipgloss.NewStyle().Bold(true)
	stZone     = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	stHelp     = lipgloss.NewStyle().Foreground(lipgloss.Color("241"))
)

func cellStyle(c Cell) lipgloss.Style {
	switch c.Kind {
	case CellReserved:
		return stReserved
	case CellFree:
		return stFree
	default:
		bg := palette[int(c.Key-'A')%len(palette)]
		return lipgloss.NewStyle().Background(lipgloss.Color(bg)).Foreground(lipgloss.Color("232"))
	}
}

func cellChar(c Cell) string {
	switch c.Kind {
	case CellReserved:
		return "R"
	case CellFree:
		return "·"
	default:
		return string(c.Key)
	}
}

// renderNode 渲染单节点：每 zone 一块，SMT 行对齐成列，每 8 列插空格。
func renderNode(g NodeGrid, width int) string {
	var b strings.Builder
	b.WriteString(stNode.Render(g.Node) + "\n")
	for _, z := range g.Zones {
		if len(z.Rows) == 0 {
			continue
		}
		cols := len(z.Rows[0])
		label := fmt.Sprintf("numa%d ", z.ID)
		pad := strings.Repeat(" ", len(label))
		// 按宽度分片（每格 1 字符 + 每 8 列 1 空格）
		chunk := width - len(label) - 4
		if chunk < 16 {
			chunk = 16
		}
		perLine := chunk * 8 / 9
		for start := 0; start < cols; start += perLine {
			end := start + perLine
			if end > cols {
				end = cols
			}
			for r, row := range z.Rows {
				if r == 0 && start == 0 {
					b.WriteString(stZone.Render(label))
				} else {
					b.WriteString(pad)
				}
				for i := start; i < end; i++ {
					if i > start && (i-start)%8 == 0 {
						b.WriteString(" ")
					}
					b.WriteString(cellStyle(row[i]).Render(cellChar(row[i])))
				}
				b.WriteString("\n")
			}
			if len(z.Rows) > 1 {
				b.WriteString("\n") // SMT 组间空行
			}
		}
	}
	if len(g.Legend) > 0 {
		var parts []string
		for _, e := range g.Legend {
			sw := lipgloss.NewStyle().Background(lipgloss.Color(palette[int(e.Key-'A')%len(palette)])).
				Foreground(lipgloss.Color("232")).Render(string(e.Key))
			parts = append(parts, sw+" "+e.Owner)
		}
		b.WriteString("  " + strings.Join(parts, "   ") + "\n")
	}
	return b.String()
}

func renderAll(crs []v1alpha1.KoreNodeTopology, width int) string {
	sort.Slice(crs, func(i, j int) bool { return crs[i].Name < crs[j].Name })
	var b strings.Builder
	for i := range crs {
		b.WriteString(renderNode(BuildNodeGrid(&crs[i]), width))
		b.WriteString("\n")
	}
	return b.String()
}

// --- bubbletea ---

type tickMsg time.Time
type dataMsg []v1alpha1.KoreNodeTopology
type errMsg error

type model struct {
	c       ctrlclient.Client
	crs     []v1alpha1.KoreNodeTopology
	err     error
	width   int
	height  int
	scroll  int
	content []string // 渲染后的行
}

func fetchCmd(c ctrlclient.Client) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var l v1alpha1.KoreNodeTopologyList
		if err := c.List(ctx, &l); err != nil {
			return errMsg(err)
		}
		return dataMsg(l.Items)
	}
}

func tick() tea.Cmd {
	return tea.Tick(2*time.Second, func(t time.Time) tea.Msg { return tickMsg(t) })
}

func (m model) Init() tea.Cmd { return tea.Batch(fetchCmd(m.c), tick()) }

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.rebuild()
	case tickMsg:
		return m, tea.Batch(fetchCmd(m.c), tick())
	case dataMsg:
		m.crs, m.err = msg, nil
		m.rebuild()
	case errMsg:
		m.err = msg
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "up", "k":
			if m.scroll > 0 {
				m.scroll--
			}
		case "down", "j":
			if m.scroll < len(m.content)-m.height+2 {
				m.scroll++
			}
		}
	}
	return m, nil
}

func (m *model) rebuild() {
	w := m.width
	if w <= 0 {
		w = 100
	}
	m.content = strings.Split(renderAll(m.crs, w), "\n")
}

func (m model) View() string {
	if m.err != nil {
		return "error: " + m.err.Error() + "\n"
	}
	h := m.height - 1
	if h <= 0 {
		h = 40
	}
	end := m.scroll + h
	if end > len(m.content) {
		end = len(m.content)
	}
	start := m.scroll
	if start > end {
		start = end
	}
	view := strings.Join(m.content[start:end], "\n")
	return view + "\n" + stHelp.Render("↑/↓ 滚动 · q 退出 · 2s 自动刷新")
}

func runTop(c ctrlclient.Client, once bool) error {
	if once { // 单帧快照（无 TTY 环境/截图用）
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var l v1alpha1.KoreNodeTopologyList
		if err := c.List(ctx, &l); err != nil {
			return err
		}
		fmt.Print(renderAll(l.Items, 100))
		return nil
	}
	_, err := tea.NewProgram(model{c: c}, tea.WithAltScreen()).Run()
	return err
}
