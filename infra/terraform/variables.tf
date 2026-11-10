variable "project_id" {
  type        = string
  description = "Existing GCP project with billing enabled."
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "zone" {
  type    = string
  default = "us-central1-a"
}

variable "name" {
  type    = string
  default = "job-platform"
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,19}$", var.name))
    error_message = "Use 5-20 lowercase letters, digits, or hyphens, starting with a letter."
  }
}

variable "admin_cidr" {
  type        = string
  description = "Operator's public IPv4 CIDR allowed to access the control plane, usually an address/32."
  validation {
    condition     = can(cidrnetmask(var.admin_cidr)) && var.admin_cidr != "0.0.0.0/0"
    error_message = "Provide a specific IPv4 CIDR, not the entire internet."
  }
}

variable "deletion_protection" {
  type    = bool
  default = true
}
