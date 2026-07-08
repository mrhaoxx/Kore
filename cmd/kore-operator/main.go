// kore-operator：webhook 校验/注入 + agent 健康污点控制。
package main

import (
	"log"
	"time"

	coordv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	"github.com/zjusct/kore/pkg/operator"
)

func main() {
	ctrl.SetLogger(zap.New())

	sch := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(sch); err != nil {
		log.Fatal(err)
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{Scheme: sch})
	if err != nil {
		log.Fatal(err)
	}

	mgr.GetWebhookServer().Register("/mutate-pod",
		&webhook.Admission{Handler: operator.NewMutateHandler(sch)})
	mgr.GetWebhookServer().Register("/validate-pod",
		&webhook.Admission{Handler: operator.NewValidateHandler(sch)})

	// watch 全部 Lease，Reconcile 内按 kore-agent- 前缀过滤（节点心跳 Lease 廉价跳过）
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&coordv1.Lease{}).
		Complete(&operator.TaintReconciler{Client: mgr.GetClient(), Now: time.Now}); err != nil {
		log.Fatal(err)
	}

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Fatal(err)
	}
}
