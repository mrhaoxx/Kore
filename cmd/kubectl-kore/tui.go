package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
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

const blockWidth = 46 // 单节点块内宽：numa 标签 + 32 格 + 组间空格

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

// renderNode 渲染单节点块（内宽 width）：SMT 行对齐成列，每 8 列插空格。
func renderNode(g NodeGrid, width int) string {
	var b strings.Builder
	stats := fmt.Sprintf(" %d/%dC %d/%dN", g.UsedCPUs, g.TotalCPUs, g.UsedZones, g.TotalZones)
	name := g.Node
	if lipgloss.Width(name)+len(stats) > width {
		name = name[:width-len(stats)-1] + "…"
	}
	b.WriteString(stNode.Render(name) + stHelp.Render(stats) + "\n")
	for _, z := range g.Zones {
		if len(z.Rows) == 0 {
			continue
		}
		cols := len(z.Rows[0])
		label := fmt.Sprintf("numa%d ", z.ID)
		pad := strings.Repeat(" ", len(label))
		perLine := (width - len(label)) * 8 / 9
		if perLine < 8 {
			perLine = 8
		}
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
		}
	}
	// 图例：按块宽贪心换行
	if len(g.Legend) > 0 {
		line := " "
		for _, e := range g.Legend {
			sw := lipgloss.NewStyle().Background(lipgloss.Color(palette[int(e.Key-'A')%len(palette)])).
				Foreground(lipgloss.Color("232")).Render(string(e.Key))
			entry := sw + " " + e.Owner + stHelp.Render(fmt.Sprintf(" %dC", e.CPUs))
			if lipgloss.Width(line)+lipgloss.Width(entry)+2 > width && lipgloss.Width(line) > 1 {
				b.WriteString(line + "\n")
				line = " "
			}
			line += " " + entry
		}
		if strings.TrimSpace(line) != "" {
			b.WriteString(line + "\n")
		}
	}
	return b.String()
}

// renderAll：节点块横向流式平铺（每行 width/blockWidth 列）。
func renderAll(crs []v1alpha1.KoreNodeTopology, width int) string {
	sort.Slice(crs, func(i, j int) bool { return crs[i].Name < crs[j].Name })
	perRow := width / (blockWidth + 2)
	if perRow < 1 {
		perRow = 1
	}
	blockSt := lipgloss.NewStyle().Width(blockWidth).MarginRight(2)
	var rows []string
	for start := 0; start < len(crs); start += perRow {
		end := start + perRow
		if end > len(crs) {
			end = len(crs)
		}
		var blocks []string
		for i := start; i < end; i++ {
			blocks = append(blocks, blockSt.Render(renderNode(BuildNodeGrid(&crs[i]), blockWidth)))
		}
		rows = append(rows, lipgloss.JoinHorizontal(lipgloss.Top, blocks...))
	}
	return strings.Join(rows, "\n")
}

// --- bubbletea（watch 事件驱动，非轮询）---

type snapshotMsg []v1alpha1.KoreNodeTopology
type updateMsg *v1alpha1.KoreNodeTopology
type deleteMsg string
type errMsg error

type model struct {
	items   map[string]v1alpha1.KoreNodeTopology
	err     error
	width   int
	height  int
	scroll  int
	content []string
	events  int
}

func (m model) Init() tea.Cmd { return nil }

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.rebuild()
	case snapshotMsg:
		m.items = map[string]v1alpha1.KoreNodeTopology{}
		for _, cr := range msg {
			m.items[cr.Name] = cr
		}
		m.err = nil
		m.rebuild()
	case updateMsg:
		m.items[msg.Name] = *msg
		m.events++
		m.rebuild()
	case deleteMsg:
		delete(m.items, string(msg))
		m.events++
		m.rebuild()
	case errMsg:
		m.err = msg
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "up", "k":
			m.scrollBy(-1)
		case "down", "j":
			m.scrollBy(1)
		}
	case tea.MouseMsg:
		switch msg.Button {
		case tea.MouseButtonWheelUp:
			m.scrollBy(-3)
		case tea.MouseButtonWheelDown:
			m.scrollBy(3)
		}
	}
	return m, nil
}

// scrollBy 滚动 delta 行并钳制在 [0, 最大偏移]；键盘与滚轮共用。
func (m *model) scrollBy(delta int) {
	max := len(m.content) - m.height + 2
	if max < 0 {
		max = 0
	}
	m.scroll += delta
	if m.scroll < 0 {
		m.scroll = 0
	}
	if m.scroll > max {
		m.scroll = max
	}
}

func (m *model) rebuild() {
	w := m.width
	if w <= 0 {
		w = 120
	}
	crs := make([]v1alpha1.KoreNodeTopology, 0, len(m.items))
	for _, cr := range m.items {
		crs = append(crs, cr)
	}
	m.content = strings.Split(renderAll(crs, w), "\n")
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
	return strings.Join(m.content[start:end], "\n") + "\n" +
		stHelp.Render(fmt.Sprintf("watch 事件驱动（已收 %d 事件）· ↑/↓/滚轮 滚动 · q 退出", m.events))
}

// watchLoop：list 建快照 → watch 增量推送；断流自动重连（重列以对账）。
func watchLoop(ctx context.Context, wc ctrlclient.WithWatch, p *tea.Program) {
	for ctx.Err() == nil {
		var l v1alpha1.KoreNodeTopologyList
		lctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := wc.List(lctx, &l)
		cancel()
		if err != nil {
			p.Send(errMsg(err))
			time.Sleep(2 * time.Second)
			continue
		}
		p.Send(snapshotMsg(l.Items))

		w, err := wc.Watch(ctx, &v1alpha1.KoreNodeTopologyList{},
			&ctrlclient.ListOptions{Raw: &metav1.ListOptions{ResourceVersion: l.ResourceVersion}})
		if err != nil {
			p.Send(errMsg(err))
			time.Sleep(2 * time.Second)
			continue
		}
		for ev := range w.ResultChan() {
			switch ev.Type {
			case watch.Added, watch.Modified:
				if cr, ok := ev.Object.(*v1alpha1.KoreNodeTopology); ok {
					p.Send(updateMsg(cr))
				}
			case watch.Deleted:
				if cr, ok := ev.Object.(*v1alpha1.KoreNodeTopology); ok {
					p.Send(deleteMsg(cr.Name))
				}
			case watch.Error:
				// 跳出重列（resourceVersion 过期等）
			}
		}
		w.Stop() // 通道关闭 → 外层循环重列重连
	}
}

func runTop(c ctrlclient.Client, wc ctrlclient.WithWatch, once bool) error {
	if once { // 单帧快照（无 TTY 环境/截图用）
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var l v1alpha1.KoreNodeTopologyList
		if err := c.List(ctx, &l); err != nil {
			return err
		}
		width := 120
		if v, err := strconv.Atoi(os.Getenv("COLUMNS")); err == nil && v > 40 {
			width = v
		}
		fmt.Println(renderAll(l.Items, width))
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := tea.NewProgram(model{items: map[string]v1alpha1.KoreNodeTopology{}},
		tea.WithAltScreen(), tea.WithMouseCellMotion())
	go watchLoop(ctx, wc, p)
	_, err := p.Run()
	return err
}
