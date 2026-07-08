package deviceplugin

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"

	"github.com/zjusct/kore/pkg/request"
)

func dial(t *testing.T, socket string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("unix://"+socket,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func TestListAndWatchAndAllocate(t *testing.T) {
	dir := t.TempDir()
	s := New(6, dir)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	conn := dial(t, filepath.Join(dir, "kore.sock"))
	defer conn.Close()
	c := pluginapi.NewDevicePluginClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := c.ListAndWatch(ctx, &pluginapi.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Devices) != 6 || resp.Devices[0].Health != pluginapi.Healthy {
		t.Fatalf("devices = %+v", resp.Devices)
	}

	ar, err := c.Allocate(ctx, &pluginapi.AllocateRequest{
		ContainerRequests: []*pluginapi.ContainerAllocateRequest{{DevicesIds: []string{"kore-token-0", "kore-token-1"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ar.ContainerResponses) != 1 {
		t.Fatalf("responses = %+v", ar)
	}
}

// fakeRegistration 记录 kubelet 注册请求。
type fakeRegistration struct {
	pluginapi.UnimplementedRegistrationServer
	got chan *pluginapi.RegisterRequest
}

func (f *fakeRegistration) Register(ctx context.Context, r *pluginapi.RegisterRequest) (*pluginapi.Empty, error) {
	f.got <- r
	return &pluginapi.Empty{}, nil
}

func TestRegister(t *testing.T) {
	dir := t.TempDir()
	kubeletSock := filepath.Join(dir, "kubelet.sock")
	lis, err := net.Listen("unix", kubeletSock)
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	fr := &fakeRegistration{got: make(chan *pluginapi.RegisterRequest, 1)}
	pluginapi.RegisterRegistrationServer(gs, fr)
	go gs.Serve(lis)
	defer gs.Stop()

	s := New(4, dir)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	if err := s.Register(kubeletSock); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-fr.got:
		if r.ResourceName != request.ExtendedResource || r.Endpoint != "kore.sock" || r.Version != pluginapi.Version {
			t.Fatalf("register = %+v", r)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("kubelet did not receive registration")
	}
}
