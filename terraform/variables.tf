variable "gcp_service_account_path" {}
variable "gcp_project_id" {}
variable "gcp_region" {}

variable "instances" { default = "3" }

variable "prefix" { default = "csb" }

variable "gcp_machine_type" { default = "e2-micro" } // use e2-standard-2 // e2-micro

variable "path_private_key" { default = "C:\\Users\\Roschy\\.ssh\\ri_key" }
variable "path_public_key" { default = "C:\\Users\\Roschy\\.ssh\\ri_key.pub" }