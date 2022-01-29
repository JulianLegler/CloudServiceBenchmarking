output "gcp_private_ip_addresses_sut" {
  value = google_compute_instance.cockroach_nodes.*.network_interface.0.network_ip
}

output "public_ip_addresses_sut" {
  value = google_compute_instance.cockroach_nodes.*.network_interface.0.access_config.0.nat_ip
}

output "gcp_private_ip_addresses_bench" {
  value = google_compute_instance.benchmark_nodes.*.network_interface.0.network_ip
}

output "public_ip_addresses_bench_az" {
  value = {for k, v in google_compute_instance.benchmark_nodes: k => "${v.network_interface.0.access_config.0.nat_ip}, ${v.zone}, ${v.name}"}
}
output "public_ip_addresses_sut_az" {
  value = {for k, v in google_compute_instance.cockroach_nodes: k => "${v.network_interface.0.access_config.0.nat_ip}, ${v.zone}, ${v.name}"}
}

output "execute_strings" {
  value = {for k, v in google_compute_instance.benchmark_nodes: k => "ssh -i ${var.path_private_key} csb@${v.network_interface.0.access_config.0.nat_ip} ./runBenchmark.sh"}
}

output "extraction_strings" {
  value = {for k, v in google_compute_instance.benchmark_nodes: k => "sftp -rp -i ${var.path_private_key} csb@${v.network_interface.0.access_config.0.nat_ip}:workload/ ./workload_${v.name}/"}
}


