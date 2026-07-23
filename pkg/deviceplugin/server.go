// Package deviceplugin 实现 kubelet 准入门闩（spec §6 三重防线第 2 层）：
// agent 死 → 端点消失 → kubelet 拒绝启动请求 kore.zjusct.io/cpu 的 Pod。
// 设备是不透明计数 token，真正选核由 NRI 路径完成。
package deviceplugin

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"

	"github.com/zjusct/kore/pkg/request"
)

const socketName = "kore.sock"

type Server struct {
	pluginapi.UnimplementedDevicePluginServer
	count  int
	dir    string
	grpc   *grpc.Server
	stopCh chan struct{}
}

func New(count int, pluginDir string) *Server {
	return &Server{count: count, dir: pluginDir, stopCh: make(chan struct{})}
}

func (s *Server) SocketPath() string { return filepath.Join(s.dir, socketName) }

func (s *Server) Start() error {
	_ = os.Remove(s.SocketPath()) // stale socket from a previous life
	lis, err := net.Listen("unix", s.SocketPath())
	if err != nil {
		return err
	}
	s.grpc = grpc.NewServer()
	pluginapi.RegisterDevicePluginServer(s.grpc, s)
	go func() { _ = s.grpc.Serve(lis) }()
	return nil
}

func (s *Server) Stop() {
	close(s.stopCh)
	if s.grpc != nil {
		s.grpc.Stop()
	}
}

func (s *Server) Register(kubeletSocket string) error {
	conn, err := grpc.NewClient("unix://"+kubeletSocket,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = pluginapi.NewRegistrationClient(conn).Register(ctx, &pluginapi.RegisterRequest{
		Version:      pluginapi.Version,
		Endpoint:     socketName,
		ResourceName: request.ExtendedResource,
	})
	return err
}

// RunGuard keeps the registration alive across kubelet restarts. kubelet
// wipes the device-plugin dir on startup and forgets all registrations; a
// one-shot Register therefore decays into allocatable=0 and every pod
// requesting kore.zjusct.io/cpu becomes unschedulable (bit m602-604 after
// node reboots). Poll for our socket vanishing or kubelet.sock being
// recreated, then re-listen and re-register.
func (s *Server) RunGuard(ctx context.Context, kubeletSocket string) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	last, _ := os.Stat(kubeletSocket)
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-tick.C:
		}
		cur, err := os.Stat(kubeletSocket)
		if err != nil {
			continue // kubelet down/mid-restart: wait for its socket to return
		}
		_, oursErr := os.Stat(s.SocketPath())
		kubeletNew := last == nil || !os.SameFile(last, cur)
		if oursErr == nil && !kubeletNew {
			continue
		}
		log.Printf("kore deviceplugin: kubelet restart detected (our socket gone=%v, kubelet.sock new=%v); re-registering", oursErr != nil, kubeletNew)
		if s.grpc != nil {
			s.grpc.Stop()
		}
		if e := s.Start(); e != nil {
			log.Printf("kore deviceplugin: re-listen failed: %v (retrying)", e)
			last = nil
			continue
		}
		if e := s.Register(kubeletSocket); e != nil {
			log.Printf("kore deviceplugin: re-register failed: %v (retrying)", e)
			last = nil
			continue
		}
		last = cur
	}
}

func (s *Server) GetDevicePluginOptions(context.Context, *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	return &pluginapi.DevicePluginOptions{}, nil
}

func (s *Server) ListAndWatch(_ *pluginapi.Empty, stream pluginapi.DevicePlugin_ListAndWatchServer) error {
	devs := make([]*pluginapi.Device, s.count)
	for i := range devs {
		devs[i] = &pluginapi.Device{ID: fmt.Sprintf("kore-token-%d", i), Health: pluginapi.Healthy}
	}
	if err := stream.Send(&pluginapi.ListAndWatchResponse{Devices: devs}); err != nil {
		return err
	}
	select { // 设备集是静态的；阻塞直到停止
	case <-s.stopCh:
	case <-stream.Context().Done():
	}
	return nil
}

func (s *Server) Allocate(ctx context.Context, req *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	out := &pluginapi.AllocateResponse{}
	for range req.ContainerRequests {
		out.ContainerResponses = append(out.ContainerResponses, &pluginapi.ContainerAllocateResponse{})
	}
	return out, nil
}

func (s *Server) GetPreferredAllocation(context.Context, *pluginapi.PreferredAllocationRequest) (*pluginapi.PreferredAllocationResponse, error) {
	return &pluginapi.PreferredAllocationResponse{}, nil
}

func (s *Server) PreStartContainer(context.Context, *pluginapi.PreStartContainerRequest) (*pluginapi.PreStartContainerResponse, error) {
	return &pluginapi.PreStartContainerResponse{}, nil
}
