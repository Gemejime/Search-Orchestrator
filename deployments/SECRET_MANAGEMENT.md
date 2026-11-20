# Secret Management — `rag-search-orchestrator`

This document defines how sensitive credentials are stored, injected, and rotated for `rag-search-orchestrator` across local development and Kubernetes deployments.

---

## 1. Secret vs. Non-Secret Configuration Boundary

| Category | Source of Truth | Git Status | Kubernetes Target |
| :--- | :--- | :--- | :--- |
| **Non-sensitive runtime config** (`HTTP_ADDR`, `GRPC_ADDR`, `REDIS_ADDRS`, `TRITON_*`, etc.) | `.env.prod` (synced to `deployments/helm/values.yaml` via `scripts/sync_env_to_deployments.py --sync`) | `.env.prod` is gitignored; `values.yaml` is committed | `ConfigMap` (`<release>-config`) |
| **Sensitive credentials** (`REDIS_PASSWORD`, future TLS/mTLS keys or tokens) | GitHub Environment Secrets / External Secret Store | **Never committed** to Git or `values.yaml` | `Secret` (`rag-search-orchestrator-redis`) |

`scripts/sync_env_to_deployments.py` automatically strips any variable whose name contains `PASSWORD`, `SECRET`, `TOKEN`, or `KEY` so credentials are never written into `deployments/helm/values.yaml` or rendered into a `ConfigMap`.

---

## 2. Current Secret Inventory

- **`REDIS_PASSWORD`**:
  - Consumed by `internal/config/config.go` and passed to the Redis Cluster client in `cmd/search-orchestrator/main.go`.
  - Injected into the pod via `secretKeyRef` in `deployments/helm/templates/_helpers.tpl`, referencing `redis.existingSecret` (or the chart-managed Secret when `redis.existingSecret` is empty) and `redis.secretKey` (`REDIS_PASSWORD`).

---

## 3. CI/CD Secret Injection (GitHub Actions)

Deployments are automated by [`.github/workflows/deploy.yml`](../.github/workflows/deploy.yml), which injects secrets from a protected **GitHub Environment** (`production` or `staging`) without storing them in `values.yaml` or Helm release history:

1. **Required GitHub Environment Secrets**:
   - `KUBE_CONFIG_DATA`: Base64-encoded kubeconfig for the target Kubernetes cluster.
   - `REDIS_PASSWORD`: Redis Cluster authentication password for the target environment.
2. **Injection Flow**:
   - Decodes `KUBE_CONFIG_DATA` into `${RUNNER_TEMP}/kubeconfig` (`chmod 600`) and removes it in an `if: always()` cleanup step.
   - Creates/updates the Kubernetes Secret `rag-search-orchestrator-redis` in namespace `rag-search` using `kubectl create secret generic ... --dry-run=client -o yaml | kubectl apply -f -`.
   - Runs `helm upgrade --install` with `--set redis.existingSecret=rag-search-orchestrator-redis`, preventing Helm from creating a duplicate Secret from `values.yaml` or embedding `REDIS_PASSWORD` in Helm release secrets.

> [!IMPORTANT]
> Never echo secret values in workflow steps, and never commit `.env` or `.env.prod` to Git.

---

## 4. Alternative: External Secrets Operator / Vault

If your cluster uses **External Secrets Operator (ESO)**, **HashiCorp Vault**, or **Sealed Secrets** instead of direct CI/CD `kubectl` injection:
1. Provision the Kubernetes Secret `rag-search-orchestrator-redis` containing key `REDIS_PASSWORD` in namespace `rag-search`.
2. Set `redis.existingSecret: "rag-search-orchestrator-redis"` in your environment overrides so `deployments/helm/templates/secret.yaml` is skipped.

---

## 5. Local Development

- Copy `.env.example` to `.env` (gitignored) and set `REDIS_PASSWORD` if your local Redis instance requires authentication.
- `cmd/search-orchestrator/main.go` loads `.env` at startup via `internal/config/dotenv.go`. Process-level environment variables always take precedence over `.env`.

---

## 6. Secret Rotation & Logging Hygiene

- **Rotating `REDIS_PASSWORD`**:
  1. Update `REDIS_PASSWORD` in the GitHub Environment (or External Secret Store).
  2. Re-run the `Deploy` workflow (or update the Secret and trigger `kubectl rollout restart deployment/rag-search-orchestrator -n rag-search`).
- **Logging Invariants**:
  - Per `.ai/security.md`, `REDIS_PASSWORD`, `Authorization` headers, bearer tokens, and raw `caller_acl_bitmap` bytes must never be logged.
  - `internal/config/config.go` marks `RedisPassword` with `#nosec G117` and never passes it to `slog`.
