package main

import (
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

// 滚轮上下滚动，并钳制在 [0, 最大偏移]，绝不越界。
func TestMouseWheelScroll(t *testing.T) {
	m := model{content: make([]string, 100), height: 12}
	maxScroll := len(m.content) - m.height + 2 // 90

	wheel := func(m model, b tea.MouseButton) model {
		next, _ := m.Update(tea.MouseMsg{Button: b, Action: tea.MouseActionPress})
		return next.(model)
	}

	m = wheel(m, tea.MouseButtonWheelDown)
	if m.scroll != 3 {
		t.Fatalf("wheel down: scroll = %d, want 3", m.scroll)
	}
	m = wheel(m, tea.MouseButtonWheelUp)
	if m.scroll != 0 {
		t.Fatalf("wheel up: scroll = %d, want 0", m.scroll)
	}
	// 顶部再上滚不越界到负
	m = wheel(m, tea.MouseButtonWheelUp)
	if m.scroll != 0 {
		t.Fatalf("wheel up at top: scroll = %d, want 0", m.scroll)
	}
	// 猛滚到底，钳在 maxScroll
	for i := 0; i < 100; i++ {
		m = wheel(m, tea.MouseButtonWheelDown)
	}
	if m.scroll != maxScroll {
		t.Fatalf("wheel down past end: scroll = %d, want %d", m.scroll, maxScroll)
	}
}

// 内容不足一屏时最大偏移钳为 0，滚轮不动。
func TestMouseWheelShortContent(t *testing.T) {
	m := model{content: make([]string, 3), height: 40}
	next, _ := m.Update(tea.MouseMsg{Button: tea.MouseButtonWheelDown, Action: tea.MouseActionPress})
	if got := next.(model).scroll; got != 0 {
		t.Fatalf("short content scroll = %d, want 0", got)
	}
}
