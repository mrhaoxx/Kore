// kore-scheduler：内嵌 Kore 插件的 kube-scheduler（第二调度器部署）。
package main

import (
	"os"

	"k8s.io/component-base/cli"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"

	"github.com/zjusct/kore/pkg/scheduler"
)

func main() {
	cmd := app.NewSchedulerCommand(app.WithPlugin(scheduler.Name, scheduler.New))
	os.Exit(cli.Run(cmd))
}
