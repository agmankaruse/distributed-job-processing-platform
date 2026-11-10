#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${IMAGE:?Set IMAGE to a pushed Artifact Registry image tag or digest}"
: "${JOBS_KUBE_CONTEXT:?Set JOBS_KUBE_CONTEXT to the target GKE kubectl context}"
# Every operation names the supplied context so a different current context is harmless.
k() { kubectl --context "$JOBS_KUBE_CONTEXT" "$@"; }
k apply -f deploy/k8s/dependencies/namespace.yaml
k -n jobs get secret jobs-secrets >/dev/null
k apply -k deploy/k8s/dependencies
k -n jobs rollout status statefulset/postgres --timeout=300s
k -n jobs rollout status statefulset/redis --timeout=300s
# Initialize tables once, before replicas start. This is not a schema migration tool.
k -n jobs delete job jobs-schema --ignore-not-found --wait=true
kubectl set image --local -f deploy/k8s/schema-job.yaml "schema=$IMAGE" -o yaml | k apply -f -
k -n jobs wait --for=condition=complete job/jobs-schema --timeout=300s
kubectl set image --local -f deploy/k8s/workloads.yaml "*=$IMAGE" -o yaml | k apply -f -
k -n jobs rollout status deployment/jobs-api --timeout=300s
k -n jobs rollout status deployment/jobs-worker --timeout=300s
printf 'Deployment ready. Use kubectl --context %s -n jobs port-forward service/jobs-api 8000:8000\n' "$JOBS_KUBE_CONTEXT"
