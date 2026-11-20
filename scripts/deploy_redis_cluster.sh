#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-rag-search}"
RELEASE_NAME="${RELEASE_NAME:-rag-search-orchestrator-redis}"
SECRET_NAME="${SECRET_NAME:-rag-search-orchestrator-redis}"
VALUES_FILE="${VALUES_FILE:-deployments/helm/redis-cluster/values.yaml}"

for cmd in helm kubectl; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "ERROR: Required tool '$cmd' is not installed or not on PATH." >&2
    exit 1
  fi
done

if [ ! -f "$VALUES_FILE" ]; then
  echo "ERROR: Helm values file not found at '$VALUES_FILE'." >&2
  exit 1
fi

echo "Ensuring namespace '$NAMESPACE' exists..."
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

if ! kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" >/dev/null 2>&1; then
  echo "WARNING: Secret '$SECRET_NAME' not found in namespace '$NAMESPACE'. Ensure it is created before pods start." >&2
fi

echo "Adding/updating Bitnami Helm repository..."
helm repo add bitnami https://charts.bitnami.com/bitnami --force-update >/dev/null
helm repo update bitnami >/dev/null

echo "Deploying Bitnami Redis Cluster '$RELEASE_NAME' in namespace '$NAMESPACE'..."
helm upgrade --install "$RELEASE_NAME" bitnami/redis-cluster \
  --namespace "$NAMESPACE" \
  --version "11.*" \
  -f "$VALUES_FILE" \
  --wait \
  --timeout 10m

echo "Waiting for Redis Cluster pods to reach Ready state..."
kubectl wait --for=condition=Ready pod \
  -l "app.kubernetes.io/instance=${RELEASE_NAME}" \
  -n "$NAMESPACE" \
  --timeout=300s

echo "Verifying Redis Cluster slot health (16,384 slots)..."
REDIS_PASS=$(kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" -o jsonpath='{.data.REDIS_PASSWORD}' | base64 -d)
CLUSTER_INFO=$(kubectl exec -n "$NAMESPACE" "${RELEASE_NAME}-0" -- \
  env REDISCLI_AUTH="$REDIS_PASS" redis-cli cluster info)
echo "$CLUSTER_INFO"

if ! echo "$CLUSTER_INFO" | grep -q "cluster_state:ok"; then
  echo "ERROR: Redis cluster_state is not ok." >&2
  exit 1
fi
if ! echo "$CLUSTER_INFO" | grep -q "cluster_slots_assigned:16384"; then
  echo "ERROR: Not all 16,384 cluster slots are assigned." >&2
  exit 1
fi

echo "SUCCESS: Redis Cluster '$RELEASE_NAME' is healthy with 16,384 slots assigned."
