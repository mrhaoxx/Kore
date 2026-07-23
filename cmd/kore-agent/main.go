// kore-agent：节点侧绑核执行者（NRI 插件 + device plugin 门闩 + CR 上报 + Lease）。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"net/http"

	"github.com/containerd/nri/pkg/api"
	"github.com/containerd/nri/pkg/stub"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	listerscorev1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/zjusct/kore/pkg/agent"
	"github.com/zjusct/kore/pkg/agent/config"
	"github.com/zjusct/kore/pkg/agent/lease"
	"github.com/zjusct/kore/pkg/agent/reporter"
	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
	"github.com/zjusct/kore/pkg/deviceplugin"
	"github.com/zjusct/kore/pkg/metrics"
	"github.com/zjusct/kore/pkg/nriplugin"
	"github.com/zjusct/kore/pkg/topology"
)

func main() {
	var (
		inspect    = flag.Bool("inspect", false, "发现本机拓扑并打印 status JSON 后退出")
		sysfs      = flag.String("sysfs", "/sys", "sysfs 根路径")
		reserved   = flag.String("reserved", "", "--inspect 模式的系统预留核")
		nodeName   = flag.String("node-name", os.Getenv("NODE_NAME"), "节点名")
		cfgPath    = flag.String("config", "", "agent 配置文件路径")
		namespace  = flag.String("namespace", "kore-system", "Lease 命名空间")
		kubeletDir = flag.String("kubelet-dir", "/var/lib/kubelet/device-plugins", "kubelet device plugin 目录")
	)
	flag.Parse()

	if *inspect {
		out, err := agent.Inspect(*sysfs, *reserved)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(out)
		return
	}
	if err := run(*sysfs, *nodeName, *cfgPath, *namespace, *kubeletDir); err != nil {
		log.Fatal(err)
	}
}

func run(sysfs, nodeName, cfgPath, namespace, kubeletDir string) error {
	if nodeName == "" {
		return fmt.Errorf("--node-name or $NODE_NAME required")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg, err := config.Load(cfgPath)
	if err != nil {
		return err
	}
	topo, err := topology.Discover(sysfs)
	if err != nil {
		return err
	}

	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	cs, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return err
	}
	sch := runtime.NewScheme()
	if err := scheme.AddToScheme(sch); err != nil {
		return err
	}
	if err := v1alpha1.AddToScheme(sch); err != nil {
		return err
	}
	crc, err := ctrlclient.New(restCfg, ctrlclient.Options{Scheme: sch})
	if err != nil {
		return err
	}

	// 本节点 Pod informer：NRI hook 里查 Pod spec 用（缓存优先，miss 直连）
	factory := informers.NewSharedInformerFactoryWithOptions(cs, 30*time.Second,
		informers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.FieldSelector = fields.OneTermEqualSelector("spec.nodeName", nodeName).String()
		}))
	podLister := factory.Core().V1().Pods().Lister()
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: cs.CoreV1().Events("")})
	rec := broadcaster.NewRecorder(sch, corev1.EventSource{Component: "kore-agent", Host: nodeName})

	adapters := &k8sAdapters{cs: cs, rec: rec, lister: podLister, ctx: ctx}
	rep := newAsyncReporter(ctx, reporter.New(crc, nodeName))

	plugin, err := nriplugin.New(topo, cfg, adapters, adapters, rep)
	if err != nil {
		return err
	}
	st, err := stub.New(plugin, stub.WithPluginName("kore"), stub.WithPluginIdx("10"))
	if err != nil {
		return err
	}
	plugin.SetUpdater(func(us []*api.ContainerUpdate) error {
		_, err := st.UpdateContainers(us)
		return err
	})

	reservedSet, _ := cfg.Reserved()
	dp := deviceplugin.New(topo.AllCPUs().Difference(reservedSet).Size(), kubeletDir)
	if err := dp.Start(); err != nil {
		return err
	}
	defer dp.Stop()
	if err := dp.Register(kubeletDir + "/kubelet.sock"); err != nil {
		return err
	}
	go dp.RunGuard(ctx, kubeletDir+"/kubelet.sock") // survive kubelet restarts

	renewer := lease.NewRenewer(cs, nodeName, namespace, 15)
	if err := renewer.RenewOnce(ctx); err != nil {
		return err
	}
	go renewer.Run(ctx, 5*time.Second)

	go func() { // Prometheus 指标
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		_ = (&http.Server{Addr: ":9100", Handler: mux}).ListenAndServe()
	}()

	// 共享池重围栏：在 NRI 事件流之外的独立 goroutine 做 UpdateContainers，
	// 避免在 StopPodSandbox 处理器内同步再入 containerd 导致的再入死锁（该死锁
	// 被 containerd 2s 超时打破 → 踢插件 → agent 崩溃循环 → 幽灵 → 调度死锁）。
	go plugin.RunRefencer(ctx)

	// 不在此处上报初始状态：NRI 注册时 Synchronize 必然执行并上报恢复后的
	// 权威账本；启动期的空状态报告会与之竞态，曾导致 CR 账本被清空。
	log.Printf("kore-agent up on %s: %d zones, %d cpus", nodeName, len(topo.Zones), topo.AllCPUs().Size())
	return st.Run(ctx) // 阻塞直到 NRI 连接结束/ctx 取消
}

type k8sAdapters struct {
	cs     kubernetes.Interface
	rec    record.EventRecorder
	lister listerscorev1.PodLister
	ctx    context.Context
}

func (a *k8sAdapters) GetPod(ns, name string) (*corev1.Pod, error) {
	if p, err := a.lister.Pods(ns).Get(name); err == nil {
		return p, nil
	}
	// 直连兜底超时必须远低于 containerd 的 NRI plugin_request_timeout（默认 2s）：
	// GetPod 在 CreateContainer 处理器同步路径上，若阻塞满 2s 会顶穿事件死线 →
	// containerd 踢插件。缓存已同步时几乎不会走到这里。
	ctx, cancel := context.WithTimeout(a.ctx, 500*time.Millisecond)
	defer cancel()
	return a.cs.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
}

func (a *k8sAdapters) Event(pod *corev1.Pod, et, reason, msg string) {
	a.rec.Event(pod, et, reason, msg)
}

func (a *k8sAdapters) SetPodAnnotation(pod *corev1.Pod, key, value string) {
	go func() {
		ctx, cancel := context.WithTimeout(a.ctx, 5*time.Second)
		defer cancel()
		patch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, key, value))
		_, _ = a.cs.CoreV1().Pods(pod.Namespace).Patch(ctx, pod.Name, types.MergePatchType, patch, metav1.PatchOptions{})
	}()
}

func (a *k8sAdapters) DeletePod(ns, name string) {
	go func() {
		ctx, cancel := context.WithTimeout(a.ctx, 10*time.Second)
		defer cancel()
		_ = a.cs.CoreV1().Pods(ns).Delete(ctx, name, metav1.DeleteOptions{})
	}()
}

// asyncReporter 串行上报且只保最新状态：并发 goroutine 上报会乱序，
// 旧状态可能覆盖新状态（账本竞态）。
type asyncReporter struct {
	r  *reporter.Reporter
	ch chan v1alpha1.KoreNodeTopologyStatus
}

func newAsyncReporter(ctx context.Context, r *reporter.Reporter) *asyncReporter {
	ar := &asyncReporter{r: r, ch: make(chan v1alpha1.KoreNodeTopologyStatus, 1)}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case st := <-ar.ch:
				metrics.UpdateFromStatus(st)
				c, cancel := context.WithTimeout(ctx, 10*time.Second)
				_ = ar.r.Report(c, st)
				cancel()
			}
		}
	}()
	return ar
}

func (ar *asyncReporter) Report(st v1alpha1.KoreNodeTopologyStatus) {
	for {
		select {
		case ar.ch <- st:
			return
		default: // 挤掉未发出的旧状态，只保最新
			select {
			case <-ar.ch:
			default:
			}
		}
	}
}
