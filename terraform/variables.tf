// Provider variables
variable "gcp_service_account_path" {}
variable "gcp_project_id" {}
variable "gcp_region" { default = "europe-west3" }
variable "gcp_availability_zones" { default = ["europe-west3-a", "europe-west3-b", "europe-west3-c"] }

// Deployment variables
variable "instances" { default = "3" }
variable "prefix" { default = "csb" }
variable "gcp_machine_type" { default = "e2-micro" } // use e2-standard-2 // e2-micro
variable "path_private_key" { default = "C:\\Users\\Roschy\\.ssh\\ri_key" }
variable "path_public_key" { default = "C:\\Users\\Roschy\\.ssh\\ri_key.pub" }

// Application variables
variable "path_to_sqlinit_file" { default = "../src/main/resources/dbinit.sql"}
variable "local_path_to_jar_file" { default = "../CloudServiceBenchmarking-1.0-SNAPSHOT.jar"}
variable "remote_path_to_jar_file" { default = "CloudServiceBenchmarking.jar"}
variable "benchmark_run_duration_in_minutes" { default = "1"}