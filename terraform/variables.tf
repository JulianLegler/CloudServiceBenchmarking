// Provider variables
variable "gcp_service_account_path" {}
variable "gcp_project_id" {}
variable "gcp_account_email" {}
variable "gcp_region" { default = "europe-west3" }
#variable "gcp_availability_zones" { default = ["europe-west3-a", "europe-west3-b", "europe-west3-c"] } # 3 suts
variable "gcp_availability_zones" { default = ["europe-west3-a", "europe-west3-b", "europe-west3-c", "europe-west3-a", "europe-west3-b", "europe-west3-c"] } # 6 suts


// Deployment variables
variable "instances" { default = "6" }
variable "prefix" { default = "csb" }
variable "gcp_machine_type_bench" { default = "e2-standard-2" } // use e2-standard-2 // e2-micro
variable "gcp_machine_type_sut" { default = "n2-standard-2" } // 3 suts = n2-standard-4 // 6 suts = n2-standard-2

variable "path_private_key" {  }
variable "path_public_key" {  }

// Application variables
variable "path_to_sqlinit_file" { default = "../src/main/resources/dbinit.sql"}
variable "local_path_to_jar_file" { default = "../build/libs/CloudServiceBenchmarking-1.0-SNAPSHOT.jar"}
variable "remote_path_to_jar_file" { default = "CloudServiceBenchmarking.jar"}
variable "benchmark_run_duration_in_minutes" { default = 30}
variable "benchmark_run_threads" { default = 4}
variable "benchmark_load_threads" { default = 5}
variable "benchmark_load_customers" { default = 100000 }
variable "benchmark_load_items" { default = 2000 }