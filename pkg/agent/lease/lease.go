// Package lease 维护 agent 报活 Lease（kore-scheduler Filter 与 operator 污点控制消费）。
package lease

import (
	"context"
	"time"

	coordv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Renewer struct {
	cs       kubernetes.Interface
	node, ns string
	duration int32
	now      func() time.Time
}

func NewRenewer(cs kubernetes.Interface, node, namespace string, durationSeconds int32) *Renewer {
	return &Renewer{cs: cs, node: node, ns: namespace, duration: durationSeconds, now: time.Now}
}

func (r *Renewer) name() string { return "kore-agent-" + r.node }

func (r *Renewer) RenewOnce(ctx context.Context) error {
	leases := r.cs.CoordinationV1().Leases(r.ns)
	renew := metav1.NewMicroTime(r.now())
	l, err := leases.Get(ctx, r.name(), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = leases.Create(ctx, &coordv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: r.name(), Namespace: r.ns},
			Spec: coordv1.LeaseSpec{
				HolderIdentity:       &r.node,
				LeaseDurationSeconds: &r.duration,
				RenewTime:            &renew,
			},
		}, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	l.Spec.RenewTime = &renew
	_, err = leases.Update(ctx, l, metav1.UpdateOptions{})
	return err
}

func (r *Renewer) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = r.RenewOnce(ctx) // 失败下轮重试；持续失败 → Lease 过期 → 调度侧自动排除
		}
	}
}
