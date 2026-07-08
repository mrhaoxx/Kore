#!/usr/bin/env bash
# 无 cert-manager 时的 webhook 证书自签脚本：
# 生成自签 CA + 服务证书 → 建 secret → 给两个 WebhookConfiguration 注入 caBundle。
set -euo pipefail

NS=kore-system
SVC=kore-webhook
DIR=$(mktemp -d)
trap 'rm -rf "$DIR"' EXIT

openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
  -keyout "$DIR/ca.key" -out "$DIR/ca.crt" -subj "/CN=kore-webhook-ca" >/dev/null 2>&1

openssl req -newkey rsa:2048 -nodes \
  -keyout "$DIR/tls.key" -out "$DIR/tls.csr" -subj "/CN=$SVC.$NS.svc" >/dev/null 2>&1

cat > "$DIR/ext.cnf" <<EOF
subjectAltName = DNS:$SVC.$NS.svc, DNS:$SVC.$NS.svc.cluster.local
EOF

openssl x509 -req -in "$DIR/tls.csr" -CA "$DIR/ca.crt" -CAkey "$DIR/ca.key" \
  -CAcreateserial -days 3650 -extfile "$DIR/ext.cnf" -out "$DIR/tls.crt" >/dev/null 2>&1

kubectl -n "$NS" create secret tls kore-webhook-certs \
  --cert="$DIR/tls.crt" --key="$DIR/tls.key" --dry-run=client -o yaml | kubectl apply -f -

CA_BUNDLE=$(base64 < "$DIR/ca.crt" | tr -d '\n')
for CFG in mutatingwebhookconfiguration/kore-mutate validatingwebhookconfiguration/kore-validate; do
  kubectl patch "$CFG" --type=json \
    -p "[{\"op\":\"add\",\"path\":\"/webhooks/0/clientConfig/caBundle\",\"value\":\"$CA_BUNDLE\"}]"
done

echo "webhook certs installed."
