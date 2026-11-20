$ErrorActionPreference = "Stop"

$Namespace = if ($env:NAMESPACE) { $env:NAMESPACE } else { "rag-search" }
$ReleaseName = if ($env:RELEASE_NAME) { $env:RELEASE_NAME } else { "rag-search-orchestrator-redis" }
$SecretName = if ($env:SECRET_NAME) { $env:SECRET_NAME } else { "rag-search-orchestrator-redis" }
$ValuesFile = if ($env:VALUES_FILE) { $env:VALUES_FILE } else { "deployments/helm/redis-cluster/values.yaml" }

foreach ($cmd in @("helm", "kubectl")) {
    if (-not (Get-Command $cmd -ErrorAction SilentlyContinue)) {
        Write-Error "Required tool '$cmd' is not installed or not on PATH."
        exit 1
    }
}

if (-not (Test-Path $ValuesFile)) {
    Write-Error "Helm values file not found at '$ValuesFile'."
    exit 1
}

Write-Host "Ensuring namespace '$Namespace' exists..."
kubectl create namespace $Namespace --dry-run=client -o yaml | kubectl apply -f -

kubectl get secret $SecretName -n $Namespace 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Warning "Secret '$SecretName' not found in namespace '$Namespace'. Ensure it is created before pods start."
}

Write-Host "Adding/updating Bitnami Helm repository..."
helm repo add bitnami https://charts.bitnami.com/bitnami --force-update | Out-Null
helm repo update bitnami | Out-Null

Write-Host "Deploying Bitnami Redis Cluster '$ReleaseName' in namespace '$Namespace'..."
helm upgrade --install $ReleaseName bitnami/redis-cluster `
    --namespace $Namespace `
    --version "11.*" `
    -f $ValuesFile `
    --wait `
    --timeout 10m

Write-Host "Waiting for Redis Cluster pods to reach Ready state..."
kubectl wait --for=condition=Ready pod `
    -l "app.kubernetes.io/instance=$ReleaseName" `
    -n $Namespace `
    --timeout=300s

Write-Host "Verifying Redis Cluster slot health (16,384 slots)..."
$EncodedPass = kubectl get secret $SecretName -n $Namespace -o jsonpath="{.data.REDIS_PASSWORD}"
$RedisPass = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($EncodedPass))

$ClusterInfo = kubectl exec -n $Namespace "$ReleaseName-0" -- `
    env "REDISCLI_AUTH=$RedisPass" redis-cli cluster info
Write-Host $ClusterInfo

if ($ClusterInfo -notmatch "cluster_state:ok") {
    Write-Error "Redis cluster_state is not ok."
    exit 1
}
if ($ClusterInfo -notmatch "cluster_slots_assigned:16384") {
    Write-Error "Not all 16,384 cluster slots are assigned."
    exit 1
}

Write-Host "SUCCESS: Redis Cluster '$ReleaseName' is healthy with 16,384 slots assigned."
