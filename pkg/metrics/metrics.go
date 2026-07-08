// Package metrics 暴露 kore-agent 的 Prometheus 指标。
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"k8s.io/utils/cpuset"

	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
)

var (
	cpusExclusive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kore_cpus_exclusive", Help: "独占绑核占用的逻辑核数"})
	cpusPooled = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kore_cpus_pooled", Help: "CPU 池占用的逻辑核数"})
	cpusShared = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kore_cpus_shared", Help: "全局共享池当前核数"})
	poolSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kore_pool_size", Help: "池大小（核数）"}, []string{"pool"})
	poolMembers = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kore_pool_members", Help: "池成员 Pod 数"}, []string{"pool"})
	allocFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kore_allocation_failures_total", Help: "分配失败次数"}, []string{"kind"})
	remediations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kore_remediations_total", Help: "对账修复次数"}, []string{"mode"})
)

// AllocFailure 记录一次分配失败。kind: pin | pool。
func AllocFailure(kind string) { allocFailures.WithLabelValues(kind).Inc() }

// Remediation 记录一次"该绑未绑"对账处置。mode: strict | repair。
func Remediation(mode string) { remediations.WithLabelValues(mode).Inc() }

// UpdateFromStatus 由上报路径调用，用最新账本刷新 gauges。
func UpdateFromStatus(st v1alpha1.KoreNodeTopologyStatus) {
	total, excl, pooled := 0, 0, 0
	for _, z := range st.Zones {
		if cs, err := cpuset.Parse(z.Cpus); err == nil {
			total += cs.Size()
		}
	}
	reserved := 0
	if cs, err := cpuset.Parse(st.ReservedSystemCpus); err == nil {
		reserved = cs.Size()
	}
	for _, a := range st.Allocations {
		if cs, err := cpuset.Parse(a.Cpuset); err == nil {
			excl += cs.Size()
		}
	}
	poolSize.Reset()
	poolMembers.Reset()
	for _, p := range st.Pools {
		n := 0
		if cs, err := cpuset.Parse(p.Cpuset); err == nil {
			n = cs.Size()
		}
		pooled += n
		poolSize.WithLabelValues(p.Name).Set(float64(n))
		poolMembers.WithLabelValues(p.Name).Set(float64(len(p.Members)))
	}
	cpusExclusive.Set(float64(excl))
	cpusPooled.Set(float64(pooled))
	cpusShared.Set(float64(total - reserved - excl - pooled))
}
