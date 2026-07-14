// kore-kueue-admissioncheck：Kueue AdmissionCheck 控制器。Kueue 准入 pin 作业前，
// 先由本控制器按整物理核确认目标分区放得下（Ready）才放行，否则留队列（Retry）或
// 拒（Rejected），避免超额准入 → kore-agent NRI 阶段失败 → 作业白占配额堵队列。
package main

import (
	"context"
	"flag"
	"log"
	"time"

	coordv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/zjusct/kore/pkg/admissioncheck"
	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
)

func main() {
	checkName := flag.String("check-name", "kore-cores", "认领的 Kueue AdmissionCheck 名")
	resvTTL := flag.Duration("reservation-ttl", 5*time.Minute, "在途预留 TTL 兜底（pin 落账后自然退场）")
	leaseNS := flag.String("lease-namespace", "kore-system", "kore-agent Lease 所在 namespace")
	flag.Parse()

	ctrl.SetLogger(zap.New())
	sch := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(sch); err != nil {
		log.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(sch); err != nil {
		log.Fatal(err)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{Scheme: sch})
	if err != nil {
		log.Fatal(err)
	}
	c := mgr.GetClient()

	r := &admissioncheck.Reconciler{
		Client:    c,
		CheckName: *checkName,
		Cache:     admissioncheck.NewReservationCache(*resvTTL),
		LeaseFresh: func(node string) bool { // agent 失活节点不计入容量
			var l coordv1.Lease
			if err := c.Get(context.Background(),
				types.NamespacedName{Namespace: *leaseNS, Name: "kore-agent-" + node}, &l); err != nil {
				return false
			}
			if l.Spec.RenewTime == nil {
				return false
			}
			d := 15 * time.Second
			if l.Spec.LeaseDurationSeconds != nil {
				d = time.Duration(*l.Spec.LeaseDurationSeconds) * time.Second
			}
			return time.Since(l.Spec.RenewTime.Time) <= d
		},
	}
	if err := r.SetupWithManager(mgr); err != nil {
		log.Fatal(err)
	}
	log.Printf("kore-kueue-admissioncheck up: check=%q ttl=%s", *checkName, *resvTTL)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Fatal(err)
	}
}
