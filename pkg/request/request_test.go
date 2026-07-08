package request

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func pod(annos map[string]string, mutate func(*corev1.Pod)) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default", Annotations: annos},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "app",
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8")},
				Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8")},
			},
		}}},
	}
	if mutate != nil {
		mutate(p)
	}
	return p
}

func TestParsePod(t *testing.T) {
	cases := []struct {
		name    string
		pod     *corev1.Pod
		wantNil bool
		wantErr string // 空串=期望成功；否则 error 需含该子串
		check   func(t *testing.T, r *Request)
	}{
		{name: "无注解返回 nil", pod: pod(nil, nil), wantNil: true},
		{name: "pin=false 返回 nil", pod: pod(map[string]string{AnnoPin: "false"}, nil), wantNil: true},
		{name: "pin 非法值报错", pod: pod(map[string]string{AnnoPin: "yes"}, nil), wantErr: "pin"},
		{
			name: "默认值", pod: pod(map[string]string{AnnoPin: "true"}, nil),
			check: func(t *testing.T, r *Request) {
				// placement/smt-policy 未设置时留空，由 agent 结合 ConfigMap 集群默认值解析
				if r.NUMAPolicy != NUMASingle || r.MemoryPolicy != MemStrict ||
					r.SMTPolicy != "" || r.Placement != "" {
					t.Fatalf("defaults wrong: %+v", r)
				}
				if len(r.Containers) != 1 || r.Containers[0].CPUs != 8 {
					t.Fatalf("containers: %+v", r.Containers)
				}
			},
		},
		{
			name: "显式策略", pod: pod(map[string]string{
				AnnoPin: "true", AnnoNUMAPolicy: "spread", AnnoMemoryPolicy: "preferred",
				AnnoPlacement: "scatter", AnnoSMTPolicy: "logical"}, nil),
			check: func(t *testing.T, r *Request) {
				if r.NUMAPolicy != NUMASpread || r.MemoryPolicy != MemPreferred ||
					r.Placement != PlacementScatter || r.SMTPolicy != SMTLogical {
					t.Fatalf("%+v", r)
				}
			},
		},
		{name: "非法 numa-policy", pod: pod(map[string]string{AnnoPin: "true", AnnoNUMAPolicy: "both"}, nil), wantErr: "numa-policy"},
		{
			name: "非整数 CPU 容器落共享池且报错(无可绑容器)",
			pod: pod(map[string]string{AnnoPin: "true"}, func(p *corev1.Pod) {
				p.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("500m")
				p.Spec.Containers[0].Resources.Limits[corev1.ResourceCPU] = resource.MustParse("500m")
			}),
			wantErr: "no container",
		},
		{
			name: "整数但 requests!=limits 报错",
			pod: pod(map[string]string{AnnoPin: "true"}, func(p *corev1.Pod) {
				p.Spec.Containers[0].Resources.Limits[corev1.ResourceCPU] = resource.MustParse("9")
			}),
			wantErr: "requests must equal limits",
		},
		{
			name: "sidecar 非整数被跳过",
			pod: pod(map[string]string{AnnoPin: "true"}, func(p *corev1.Pod) {
				p.Spec.Containers = append(p.Spec.Containers, corev1.Container{
					Name: "sidecar",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
						Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
					},
				})
			}),
			check: func(t *testing.T, r *Request) {
				if len(r.Containers) != 1 || r.Containers[0].Name != "app" {
					t.Fatalf("containers: %+v", r.Containers)
				}
			},
		},
		{
			name:    "显式 cpuset 需指定节点",
			pod:     pod(map[string]string{AnnoPin: "true", AnnoCPUSet: "0-7"}, nil),
			wantErr: "nodeName or nodeSelector",
		},
		{
			name: "显式 cpuset OK",
			pod: pod(map[string]string{AnnoPin: "true", AnnoCPUSet: "0-7"}, func(p *corev1.Pod) {
				p.Spec.NodeName = "m602"
			}),
			check: func(t *testing.T, r *Request) {
				if r.Explicit == nil || r.Explicit.Size() != 8 {
					t.Fatalf("explicit: %v", r.Explicit)
				}
			},
		},
		{
			name: "显式 cpuset 与 numa-policy 互斥",
			pod: pod(map[string]string{AnnoPin: "true", AnnoCPUSet: "0-7", AnnoNUMAPolicy: "spread"}, func(p *corev1.Pod) {
				p.Spec.NodeName = "m602"
			}),
			wantErr: "mutually exclusive",
		},
		{
			name: "显式 cpuset 大小必须等于 CPU 数",
			pod: pod(map[string]string{AnnoPin: "true", AnnoCPUSet: "0-3"}, func(p *corev1.Pod) {
				p.Spec.NodeName = "m602"
			}),
			wantErr: "size",
		},
		{
			name: "纯池模式合法",
			pod: pod(map[string]string{AnnoPin: "false", AnnoPool: "team-hpl", AnnoPoolSize: "64"}, func(p *corev1.Pod) {
				delete(p.Annotations, AnnoPin)
				p.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("500m")
				p.Spec.Containers[0].Resources.Limits[corev1.ResourceCPU] = resource.MustParse("500m")
			}),
			check: func(t *testing.T, r *Request) {
				if r.Pool != "team-hpl" || r.PoolSize != 64 {
					t.Fatalf("pool: %+v", r)
				}
				if len(r.Containers) != 0 {
					t.Fatalf("pool mode must not require pinned containers: %+v", r.Containers)
				}
				if r.NUMAPolicy != NUMASingle || r.MemoryPolicy != MemStrict {
					t.Fatalf("policies: %+v", r)
				}
			},
		},
		{
			name:    "池缺 size 报错",
			pod:     pod(map[string]string{AnnoPool: "demo"}, nil),
			wantErr: "pool-size",
		},
		{
			name:    "size 非正整数报错",
			pod:     pod(map[string]string{AnnoPool: "demo", AnnoPoolSize: "0"}, nil),
			wantErr: "positive integer",
		},
		{
			name:    "size 无池报错",
			pod:     pod(map[string]string{AnnoPoolSize: "8"}, nil),
			wantErr: "requires",
		},
		{
			name:    "池与 pin 互斥",
			pod:     pod(map[string]string{AnnoPin: "true", AnnoPool: "demo", AnnoPoolSize: "8"}, nil),
			wantErr: "mutually exclusive",
		},
		{
			name: "池与 cpuset 互斥",
			pod: pod(map[string]string{AnnoPool: "demo", AnnoPoolSize: "8", AnnoCPUSet: "0-7"}, func(p *corev1.Pod) {
				p.Spec.NodeName = "m602"
			}),
			wantErr: "mutually exclusive",
		},
		{
			name:    "非法池名报错",
			pod:     pod(map[string]string{AnnoPool: "Team_HPL", AnnoPoolSize: "8"}, nil),
			wantErr: "DNS label",
		},
		{
			name: "显式 cpuset 多容器不允许",
			pod: pod(map[string]string{AnnoPin: "true", AnnoCPUSet: "0-7"}, func(p *corev1.Pod) {
				p.Spec.NodeName = "m602"
				p.Spec.Containers = append(p.Spec.Containers, corev1.Container{
					Name: "app2",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
						Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
					},
				})
			}),
			wantErr: "exactly one",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := ParsePod(tc.pod)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want contains %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantNil {
				if r != nil {
					t.Fatalf("want nil, got %+v", r)
				}
				return
			}
			tc.check(t, r)
		})
	}
}
