#!/usr/bin/env bash
# Kore kind E2E：需要本机 docker daemon 运行。用法：make e2e-kind
set -euo pipefail
cd "$(dirname "$0")/../.."

CLUSTER=kore-e2e
IMG_TAG=e2e
step() { echo; echo "==> $*"; }

cleanup() { kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true; }
trap cleanup EXIT

step "1/8 创建 kind 集群（containerd 开 NRI）"
kind create cluster --name "$CLUSTER" --config test/e2e/kind-config.yaml --wait 120s

step "2/8 构建并加载镜像"
for c in agent scheduler operator; do
  docker build --target "$c" -t "kore-$c:$IMG_TAG" .
  kind load docker-image "kore-$c:$IMG_TAG" --name "$CLUSTER"
done

step "3/8 部署 CRD、namespace、RBAC、组件"
kubectl apply -f deploy/crd -f deploy/namespace.yaml
kubectl apply -f deploy/agent/rbac.yaml -f deploy/agent/configmap.yaml \
  -f deploy/scheduler/rbac.yaml -f deploy/scheduler/configmap.yaml \
  -f deploy/operator/rbac.yaml
kubectl apply -f deploy/agent/daemonset.yaml -f deploy/scheduler/deployment.yaml \
  -f deploy/operator/deployment.yaml -f deploy/operator/webhook.yaml

# kind 用本地镜像（strategic patch，避免 BSD/GNU sed 差异）
patch_image() { # $1=workload $2=container $3=image
  kubectl -n kore-system patch "$1" --type=strategic \
    -p "{\"spec\":{\"template\":{\"spec\":{\"containers\":[{\"name\":\"$2\",\"image\":\"$3\",\"imagePullPolicy\":\"Never\"}]}}}}"
}
patch_image ds/kore-agent agent "kore-agent:$IMG_TAG"
patch_image deploy/kore-scheduler scheduler "kore-scheduler:$IMG_TAG"
patch_image deploy/kore-operator operator "kore-operator:$IMG_TAG"
bash deploy/operator/gen-certs.sh

step "4/8 等待组件就绪"
kubectl -n kore-system rollout status ds/kore-agent --timeout=120s
kubectl -n kore-system rollout status deploy/kore-scheduler --timeout=120s
kubectl -n kore-system rollout status deploy/kore-operator --timeout=120s

step "5/8 提交绑核 Pod 并等待 Running"
kubectl apply -f test/e2e/testdata/pinned-pod.yaml
kubectl wait --for=condition=Ready pod/kore-e2e-pinned --timeout=120s

step "6/8 断言 cgroup 绑定与注解"
CPUS=$(kubectl exec kore-e2e-pinned -- cat /sys/fs/cgroup/cpuset.cpus.effective)
ANNO=$(kubectl get pod kore-e2e-pinned -o jsonpath='{.metadata.annotations.kore\.zjusct\.io/allocated-cpuset}')
echo "cgroup cpuset=$CPUS annotation=$ANNO"
[ -n "$CPUS" ] && [ "$CPUS" = "$ANNO" ] || { echo "FAIL: cpuset mismatch"; exit 1; }
NCPUS=$(kubectl exec kore-e2e-pinned -- sh -c 'grep -c processor /proc/cpuinfo' || true)
echo "container sees $NCPUS cpus (want 2 via cpuset)"

step "6b/8 CPU 池：两成员共享同一核心区"
for i in 1 2; do
  cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: kore-e2e-pool-$i
  annotations:
    kore.zjusct.io/pool: "demo"
    kore.zjusct.io/pool-size: "2"
spec:
  restartPolicy: Never
  containers:
  - name: app
    image: busybox:1.36
    command: ["sleep", "3600"]
    resources:
      requests: { cpu: "200m", memory: "32Mi" }
      limits: { cpu: "200m", memory: "32Mi" }
EOF
done
kubectl wait --for=condition=Ready pod/kore-e2e-pool-1 pod/kore-e2e-pool-2 --timeout=120s
POOL1=$(kubectl exec kore-e2e-pool-1 -- cat /sys/fs/cgroup/cpuset.cpus.effective)
POOL2=$(kubectl exec kore-e2e-pool-2 -- cat /sys/fs/cgroup/cpuset.cpus.effective)
echo "pool member1=$POOL1 member2=$POOL2 pinned=$CPUS"
[ -n "$POOL1" ] && [ "$POOL1" = "$POOL2" ] || { echo "FAIL: pool members must share cpuset"; exit 1; }
[ "$POOL1" != "$CPUS" ] || { echo "FAIL: pool must not overlap pinned pod"; exit 1; }
kubectl delete pod kore-e2e-pool-1 --wait=true
P2=$(kubectl exec kore-e2e-pool-2 -- cat /sys/fs/cgroup/cpuset.cpus.effective)
[ "$P2" = "$POOL1" ] || { echo "FAIL: pool must survive first member exit"; exit 1; }
kubectl delete pod kore-e2e-pool-2 --wait=true

step "7/8 三重防线：杀 agent 后新绑核 Pod 必须 Pending"
kubectl -n kore-system delete ds/kore-agent
sleep 20   # 等 Lease 过期 + 污点生效
sed 's/kore-e2e-pinned/kore-e2e-pinned2/' test/e2e/testdata/pinned-pod.yaml | kubectl apply -f -
sleep 15
PHASE=$(kubectl get pod kore-e2e-pinned2 -o jsonpath='{.status.phase}')
[ "$PHASE" = "Pending" ] || { echo "FAIL: pod phase=$PHASE, want Pending with agent down"; exit 1; }

step "8/8 恢复 agent 后 Pod 应能跑起来"
kubectl apply -f deploy/agent/daemonset.yaml
patch_image ds/kore-agent agent "kore-agent:$IMG_TAG"
kubectl -n kore-system rollout status ds/kore-agent --timeout=120s
kubectl wait --for=condition=Ready pod/kore-e2e-pinned2 --timeout=180s

echo; echo "=== KORE KIND E2E PASSED ==="
