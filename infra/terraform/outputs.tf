output "cluster_name" {
  value = google_container_cluster.jobs.name
}

output "image_repository" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.images.repository_id}/jobs"
}

output "get_credentials_command" {
  value = "gcloud container clusters get-credentials ${google_container_cluster.jobs.name} --zone ${var.zone} --project ${var.project_id}"
}
