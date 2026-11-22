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
  1. **Add the new password without invalidating the old one**: On every Redis Cluster node (primaries and replicas), add the new credential alongside the existing password using Redis ACLs so active clients and replication links continue authenticating during migration:
     - `ACL SETUSER default +@all on >"<OLD_REDIS_PASSWORD>" >"<NEW_REDIS_PASSWORD>"`
     - `CONFIG REWRITE` (if writable).
  2. **Migrate replication credentials & client Secret**:
     - On all nodes, update replication authentication: `CONFIG SET masterauth "<NEW_REDIS_PASSWORD>"` and `CONFIG REWRITE` (or update the persistent Redis Secret/ConfigMap).
     - Update `REDIS_PASSWORD` in the GitHub Environment (or External Secret Store), re-run the `Deploy` workflow (or apply the updated `rag-search-orchestrator-redis` Secret), and explicitly restart the workload so running pods pick up the new Secret even when the image tag is unchanged:
       - Standard Deployment (`canary.enabled=false`): `kubectl rollout restart deployment/rag-search-orchestrator -n rag-search`
       - Canary Rollout (`canary.enabled=true`): `kubectl argo rollouts restart rag-search-orchestrator -n rag-search`
  3. **Verify authentication & cluster health**: Before removing the old credential, verify `redis-cli` authentication with `<NEW_REDIS_PASSWORD>`, confirm `cluster_state:ok` and `cluster_slots_assigned:16384` via `redis-cli cluster info`, and verify all `rag-search-orchestrator` pods pass `/ready` checks.
  4. **Remove the old password**: Once all clients and replicas are verified on `<NEW_REDIS_PASSWORD>`, revoke the old credential on every Redis node and persist:
     - `ACL SETUSER default -"<OLD_REDIS_PASSWORD>"`
     - `CONFIG SET requirepass "<NEW_REDIS_PASSWORD>"`
     - `CONFIG REWRITE`
- **Logging Invariants**:
  - Per `.ai/security.md`, `REDIS_PASSWORD`, `Authorization` headers, bearer tokens, and raw `caller_acl_bitmap` bytes must never be logged.
  - `internal/config/config.go` marks `RedisPassword` with `#nosec G117` and never passes it to `slog`.
