# Deploy the job platform to GKE

Terraform provisions a zonal GKE cluster, VPC/subnet, an autoscaling node pool,
an Artifact Registry Docker repository, and a dedicated node service account.
Kubernetes runs two API replicas, three workers, and persistent PostgreSQL and
Redis instances. The API is a ClusterIP service accessed through port forwarding.

These files are a reproducible deployment path. They do not establish that a
particular cloud deployment has already run. A live deployment requires your
GCP project, credentials, billing, and a pushed container image.

## 1. Provision infrastructure

Install Terraform 1.6+, the Google Cloud CLI with the GKE auth plugin, Docker,
and kubectl. Authenticate with an account allowed to enable APIs and create GKE,
network, service-account, IAM, and Artifact Registry resources in your project.
Applying the plan creates billable resources.

```bash
gcloud auth login
gcloud auth application-default login
cp infra/terraform/terraform.tfvars.example infra/terraform/terraform.tfvars
# Edit project_id and admin_cidr. Keep region and zone consistent.
terraform -chdir=infra/terraform init
terraform -chdir=infra/terraform plan -out=deployment.tfplan
terraform -chdir=infra/terraform apply deployment.tfplan
terraform -chdir=infra/terraform output -raw get_credentials_command
# Run the printed gcloud command, then save the resulting context:
export JOBS_KUBE_CONTEXT="$(kubectl config current-context)"
```

The operator CIDR controls access to the Kubernetes control plane. Update it if
your public IP changes. Terraform state stays local by default; keep it out of
Git and use a secured remote backend when collaborating.

## 2. Build and push the Docker image

Run from the repository root. Set the registry region to the Terraform region.

```bash
export GCP_REGION=us-central1
gcloud auth configure-docker "$GCP_REGION-docker.pkg.dev"
export IMAGE="$(terraform -chdir=infra/terraform output -raw image_repository):$(git rev-parse --short HEAD)"
docker build --platform linux/amd64 -t "$IMAGE" .
docker push "$IMAGE"
```

Use a new tag or digest for each release. Nodes receive read access to this
repository; application pods do not need a Google service-account key.

## 3. Create credentials and deploy

Create a local, ignored credential file with a random URL-safe password:

```bash
python3 - <<'PY'
import os
import secrets
password = secrets.token_urlsafe(32)
path = "jobs.secret.env"
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, "w") as stream:
    stream.write(f"POSTGRES_PASSWORD={password}\n")
    stream.write(f"DATABASE_URL=postgresql+psycopg://jobs:{password}@postgres:5432/jobs\n")
PY
kubectl --context "$JOBS_KUBE_CONTEXT" apply -f deploy/k8s/dependencies/namespace.yaml
kubectl --context "$JOBS_KUBE_CONTEXT" -n jobs create secret generic jobs-secrets \
  --from-env-file=jobs.secret.env
./scripts/deploy-gke.sh
```

Reuse the existing Secret on later deploys. Changing `POSTGRES_PASSWORD` in a
Secret does not rotate the password of an already initialized PostgreSQL volume.

The script waits for both data stores, runs a single schema-initialization Job,
and only then starts/updates API and worker replicas. `CREATE_SCHEMA_ON_STARTUP`
is false in the cluster to avoid racing table creation across replicas. The
bootstrap Job creates missing tables; it does not migrate an existing schema.
SIGTERM stops workers from claiming new work while the current task finishes.
If a pod is killed before completion, its lease becomes eligible for recovery.

## 4. Exercise the service and recovery

```bash
kubectl --context "$JOBS_KUBE_CONTEXT" -n jobs port-forward service/jobs-api 8000:8000
```

In another terminal, submit a long task and note its returned ID:

```bash
curl -sS http://localhost:8000/v1/jobs \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: gke-recovery-1' \
  -d '{"task_type":"sleep","payload":{"seconds":20},"max_attempts":3}'
curl -sS http://localhost:8000/v1/workers
```

Find the worker whose `current_job_id` matches the job. Worker IDs start with the
pod hostname; use `kubectl -n jobs get pods -l app=jobs-worker` to find that exact
pod, then simulate a crash in this disposable test deployment:

```bash
kubectl --context "$JOBS_KUBE_CONTEXT" -n jobs delete pod ACTUAL_WORKER_POD \
  --grace-period=0 --force
curl -sS http://localhost:8000/v1/jobs/ACTUAL_JOB_ID
```

After the 30-second lease expires, a surviving worker can claim the job and
increment its attempts. After the retried task finishes, its status should be
`succeeded`. Submit the same idempotency key again to check that creation returns
the original job. The pytest suite covers the corresponding recovery cases
without requiring a live cluster.

## Reliability boundaries

- Redis leases have a fresh token per claim. An old token cannot renew, acknowledge,
  or retry a new worker's claim. Due retries and expired leases move atomically.
- PostgreSQL stores durable job state; Redis delivers work at least once. Completed
  jobs are skipped if delivered again. Task side effects must be idempotent.
- Database commits and Redis operations are separate transactions. A transactional
  outbox/reconciler is still needed to recover every enqueue or retry crash window.
- The example uses one PostgreSQL instance and one Redis instance with GKE disks.
  It is not an HA database deployment. Redis AOF uses `everysec` persistence, so
  recent queue changes can be lost on a data-store crash. Add managed databases,
  backups, authentication, and an ingress before exposing a public service.
- A successful Terraform validation or manifest render is not a cloud smoke test.
  Record project, image digest, deployment date, and observed recovery results if
  you need evidence of a specific deployment.

## Checks and cleanup

```bash
pytest
ruff check .
ruff format --check .
terraform -chdir=infra/terraform fmt -check
terraform -chdir=infra/terraform init -backend=false
terraform -chdir=infra/terraform validate
kubectl kustomize deploy/k8s/dependencies
bash -n scripts/deploy-gke.sh
```

For teardown, export any data you want to keep, then delete the `jobs` namespace
and its PVCs. Set `deletion_protection = false` in Terraform and apply that change
before `terraform destroy`. Review the destroy plan: cluster deletion and disk
cleanup remove this deployment's data. Confirm no retained disks or images remain
if you want to stop all associated charges.

References: [GKE node service accounts](https://docs.cloud.google.com/kubernetes-engine/security/configure-node-service-accounts),
[GKE stateful applications](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/stateful-apps),
and [Terraform's GKE resource](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/container_cluster).
