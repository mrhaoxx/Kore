package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/containerd/nri/pkg/api"
)

type fakeRuntimeUpdater struct {
	update func([]*api.ContainerUpdate) ([]*api.ContainerUpdate, error)
}

func (f fakeRuntimeUpdater) UpdateContainers(us []*api.ContainerUpdate) ([]*api.ContainerUpdate, error) {
	return f.update(us)
}

func TestNRIUpdaterReportsFailedContainers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	failed := &api.ContainerUpdate{}
	failed.SetContainerId("c9")
	updater := newNRIUpdater(ctx, cancel, fakeRuntimeUpdater{update: func([]*api.ContainerUpdate) ([]*api.ContainerUpdate, error) {
		return []*api.ContainerUpdate{failed}, nil
	}}, time.Second)

	err := updater(nil)
	if err == nil || !strings.Contains(err.Error(), "c9") {
		t.Fatalf("error = %v, want failed container c9", err)
	}
}

func TestNRIUpdaterCancelsAgentOnTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	unblock := make(chan struct{})
	defer close(unblock)
	updater := newNRIUpdater(ctx, cancel, fakeRuntimeUpdater{update: func([]*api.ContainerUpdate) ([]*api.ContainerUpdate, error) {
		<-unblock
		return nil, nil
	}}, 20*time.Millisecond)

	err := updater(nil)
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("error = %v, want timeout", err)
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("agent context was not cancelled after a stuck NRI update")
	}
}
