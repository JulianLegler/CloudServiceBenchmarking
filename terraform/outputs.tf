output "gcp_private_ip_addresses" {
  value = google_compute_instance.cockroach_nodes.*.network_interface.0.network_ip
}

output "public_ip_addresses" {
  value = google_compute_instance.cockroach_nodes.*.network_interface.0.access_config.0.nat_ip
}