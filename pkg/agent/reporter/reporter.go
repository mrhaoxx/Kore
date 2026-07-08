// Package reporter 把 agent 的分配状态写入 KoreNodeTopology CR。
package reporter

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/zjusct/kore/pkg/apis/kore/v1alpha1"
)

type Reporter struct {
	c    client.Client
	node string
}

func New(c client.Client, node string) *Reporter { return &Reporter{c: c, node: node} }

func (r *Reporter) Report(ctx context.Context, st v1alpha1.KoreNodeTopologyStatus) error {
	var cr v1alpha1.KoreNodeTopology
	err := r.c.Get(ctx, types.NamespacedName{Name: r.node}, &cr)
	if apierrors.IsNotFound(err) {
		cr = v1alpha1.KoreNodeTopology{ObjectMeta: metav1.ObjectMeta{Name: r.node}}
		if err := r.c.Create(ctx, &cr); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	cr.Status = st
	return r.c.Status().Update(ctx, &cr)
}
