// kubectl-kore：Kore 账本的只读查看器（kubectl 插件）。
// 用法：kubectl kore nodes | kubectl kore pools | kubectl kore pod <ns> <name>
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/cpuset"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
	"github.com/zjusct/kore/pkg/request"
)

var contextFlag string

func main() {
	rest := os.Args[1:]
	args := []string{os.Args[0]}
	for i := 0; i < len(rest); i++ { // 提取 --context=<name> 与 --context <name> 两种形式
		a := rest[i]
		switch {
		case strings.HasPrefix(a, "--context="):
			contextFlag = strings.TrimPrefix(a, "--context=")
		case a == "--context" && i+1 < len(rest):
			i++
			contextFlag = rest[i]
		case strings.HasPrefix(a, "--"):
			fmt.Fprintf(os.Stderr, "unknown flag %q (supported: --context)\n", a)
			os.Exit(2)
		default:
			args = append(args, a)
		}
	}
	os.Args = args
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: kubectl kore nodes|pools|pod <ns> <name>")
		os.Exit(2)
	}
	c, err := newClient()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx := context.Background()
	switch os.Args[1] {
	case "nodes":
		err = nodes(ctx, c)
	case "pools":
		err = pools(ctx, c)
	case "pod":
		if len(os.Args) != 4 {
			err = fmt.Errorf("usage: kubectl kore pod <namespace> <name>")
		} else {
			err = pod(ctx, c, os.Args[2], os.Args[3])
		}
	default:
		err = fmt.Errorf("unknown subcommand %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newClient() (ctrlclient.Client, error) {
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(),
		&clientcmd.ConfigOverrides{CurrentContext: contextFlag}).ClientConfig()
	if err != nil {
		return nil, err
	}
	sch := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(sch); err != nil {
		return nil, err
	}
	if err := v1alpha1.AddToScheme(sch); err != nil {
		return nil, err
	}
	return ctrlclient.New(cfg, ctrlclient.Options{Scheme: sch})
}

func size(cpulist string) int {
	cs, err := cpuset.Parse(cpulist)
	if err != nil {
		return 0
	}
	return cs.Size()
}

func nodes(ctx context.Context, c ctrlclient.Client) error {
	var l v1alpha1.KoreNodeTopologyList
	if err := c.List(ctx, &l); err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(w, "NODE\tZONES\tCPUS\tRESERVED\tEXCLUSIVE\tPOOLED\tSHARED\tFREE/ZONE")
	for _, cr := range l.Items {
		total, excl, pooled := 0, 0, 0
		var frees []string
		for _, z := range cr.Status.Zones {
			total += size(z.Cpus)
			frees = append(frees, fmt.Sprintf("%d:%d", z.ID, size(z.FreeCpus)))
		}
		for _, a := range cr.Status.Allocations {
			excl += size(a.Cpuset)
		}
		for _, p := range cr.Status.Pools {
			pooled += size(p.Cpuset)
		}
		reserved := size(cr.Status.ReservedSystemCpus)
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%s\n",
			cr.Name, len(cr.Status.Zones), total, reserved, excl, pooled,
			total-reserved-excl-pooled, strings.Join(frees, " "))
	}
	return w.Flush()
}

func pools(ctx context.Context, c ctrlclient.Client) error {
	var l v1alpha1.KoreNodeTopologyList
	if err := c.List(ctx, &l); err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(w, "NODE\tPOOL\tCPUS\tSIZE\tNUMA\tMEMBERS")
	for _, cr := range l.Items {
		for _, p := range cr.Status.Pools {
			fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%v\t%d\n",
				cr.Name, p.Name, p.Cpuset, size(p.Cpuset), p.NUMA, len(p.Members))
		}
	}
	return w.Flush()
}

func pod(ctx context.Context, c ctrlclient.Client, ns, name string) error {
	var p corev1.Pod
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &p); err != nil {
		return err
	}
	a := p.Annotations
	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintf(w, "Pod:\t%s/%s\n", ns, name)
	fmt.Fprintf(w, "Node:\t%s\n", p.Spec.NodeName)
	fmt.Fprintf(w, "Scheduler:\t%s\n", p.Spec.SchedulerName)
	mode := "none"
	switch {
	case a[request.AnnoPin] == "true":
		mode = "pinned"
	case a[request.AnnoPool] != "":
		mode = fmt.Sprintf("pool %q (size %s)", a[request.AnnoPool], a[request.AnnoPoolSize])
	}
	fmt.Fprintf(w, "Mode:\t%s\n", mode)
	fmt.Fprintf(w, "Reserved NUMA:\t%s\n", a[request.AnnoReservedNUMA])
	fmt.Fprintf(w, "Allocated cpus:\t%s\n", a[request.AnnoAllocated])
	if p.Spec.NodeName != "" {
		var cr v1alpha1.KoreNodeTopology
		if err := c.Get(ctx, types.NamespacedName{Name: p.Spec.NodeName}, &cr); err == nil {
			for _, al := range cr.Status.Allocations {
				if al.PodUID == string(p.UID) {
					fmt.Fprintf(w, "Ledger:\texclusive %s numa %v\n", al.Cpuset, al.NUMA)
				}
			}
			for _, pl := range cr.Status.Pools {
				for _, m := range pl.Members {
					if m == string(p.UID) {
						fmt.Fprintf(w, "Ledger:\tpool %q %s numa %v (%d members)\n",
							pl.Name, pl.Cpuset, pl.NUMA, len(pl.Members))
					}
				}
			}
		}
	}
	return w.Flush()
}
