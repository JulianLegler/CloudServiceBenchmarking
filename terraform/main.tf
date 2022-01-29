provider "google" {
  credentials = file(var.gcp_service_account_path)
  project     = var.gcp_project_id
  region      = var.gcp_region
}

/*
------------------------
    NETWORK
------------------------
*/

resource "google_compute_network" "main" {
  name                    = "${var.prefix}-network"
  auto_create_subnetworks = true

}


/*
-------------------------------
   VM(s)
-------------------------------
*/

resource "google_compute_instance" "cockroach_nodes" {
  count        = var.instances
  name         = "${var.prefix}-gcp-sut-vm-${count.index + 1}"
  machine_type = var.gcp_machine_type
  zone         = var.gcp_availability_zones[count.index]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
    }
  }

  network_interface {
    network = google_compute_network.main.self_link
    access_config {
      #nat_ip = google_compute_address.cockroach_nodes[count.index].address
    }
  }

  metadata = {
    ssh-keys = "csb:${file(var.path_public_key)}"
  }

  connection {
    type = "ssh"
    host = self.network_interface.0.access_config.0.nat_ip
    user = "csb"
    port = 22
    #    agent = true
    private_key = file(var.path_private_key)
  }

  provisioner "remote-exec" {
    inline = [
      #"sudo apt update && sudo apt upgrade -y",
      # follows https://www.cockroachlabs.com/docs/v21.2/install-cockroachdb-linux
      "curl https://binaries.cockroachdb.com/cockroach-v21.2.2.linux-amd64.tgz | tar -xz && sudo cp -i cockroach-v21.2.2.linux-amd64/cockroach /usr/local/bin/",
      #"mkdir -p /usr/local/lib/cockroach",
      #"cp -i cockroach-v21.2.2.linux-amd64/lib/libgeos.so /usr/local/lib/cockroach/",
      #"cp -i cockroach-v21.2.2.linux-amd64/lib/libgeos_c.so /usr/local/lib/cockroach/",
      "cockroach start --insecure --advertise-addr=${self.network_interface.0.network_ip} --join=${google_compute_instance.cockroach_nodes.0.network_interface.0.network_ip} --background"
    ]
  }
}


resource "google_compute_instance" "benchmark_nodes" {
  count        = var.instances
  name         = "${var.prefix}-gcp-bench-vm-${count.index + 1}"
  machine_type = var.gcp_machine_type
  zone         = var.gcp_availability_zones[count.index]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
    }
  }

  network_interface {
    network = google_compute_network.main.self_link
    access_config {
      #nat_ip = google_compute_address.cockroach_nodes[count.index].address
    }
  }

  metadata = {
    ssh-keys = "csb:${file(var.path_public_key)}"
  }

  connection {
    type = "ssh"
    host = self.network_interface.0.access_config.0.nat_ip
    user = "csb"
    port = 22
    #    agent = true
    private_key = file(var.path_private_key)
  }

  provisioner "file" {
    source      = var.local_path_to_jar_file
    destination = var.remote_path_to_jar_file
  }

  // Create a file that already contain the start command with all needed parameter
  provisioner "remote-exec" {
    inline = [
      "sudo apt install openjdk-17-jre -y",
      "echo 'java -jar ${var.remote_path_to_jar_file} ${google_compute_instance.cockroach_nodes[count.index].network_interface.0.access_config.0.nat_ip} ${count.index * 1000} ${var.benchmark_run_duration_in_minutes} run' > runBenchmark.sh",
      "chmod +x runBenchmark.sh",
      "mkdir workload",
      "echo '${self.name}, ${self.machine_type}, ${self.zone}, ${self.network_interface.0.access_config.0.nat_ip}, ${self.boot_disk[0].initialize_params[0].image}' > workload/machine.txt"
    ]
  }
}


/*
    Only do this on one cockroach machine to init the cluster. All machines must be aware of this machine via the join string in the startup parameter
*/
resource "null_resource" "init_cockroach" {

  triggers = {
    public_ip = google_compute_instance.cockroach_nodes.0.network_interface.0.access_config.0.nat_ip
  }

  connection {
    type = "ssh"
    host = google_compute_instance.cockroach_nodes.0.network_interface.0.access_config.0.nat_ip
    user = "csb"
    port = 22
    //agent = true
    private_key = file(var.path_private_key)
  }

  // copy our example script to the server
   provisioner "file" {
     source      = var.path_to_sqlinit_file
     destination = "dbinit.sql"
   }

  // init cockroach on one node
  provisioner "remote-exec" {
    inline = [
      "cockroach init --insecure --host=${google_compute_instance.cockroach_nodes.0.network_interface.0.access_config.0.nat_ip}:26257",
      "cat dbinit.sql | cockroach sql --url 'postgresql://root@localhost:26257?sslmode=disable'"
    ]
  }

  //provisioner "local-exec" {
    # copy the public-ip file back to CWD, which will be tested
    //command = "scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${var.ssh_user}@${aws_instance.example_public.public_ip}:/tmp/public-ip public-ip"
  //}
}

resource "local_file" "public_ip_addr" {
  content     = join(",", google_compute_instance.cockroach_nodes.*.network_interface.0.access_config.0.nat_ip)
  filename = "public_ip.csv"
}



/*
-------------------------------
   FIREWALL
-------------------------------
*/

resource "google_compute_firewall" "allow_ssh" {
  name    = "${var.prefix}-network-allow-ssh"
  network = google_compute_network.main.self_link

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
  source_ranges = ["0.0.0.0/0"]
}

resource "google_compute_firewall" "allow_internet" {
  name    = "${var.prefix}-network-allow-internet"
  network = google_compute_network.main.self_link

  allow {
    protocol = "tcp"
    ports    = ["80", "443", "8080"]
  }
  source_ranges = ["0.0.0.0/0"]
}

resource "google_compute_firewall" "allow_internal" {
  name    = "${var.prefix}-network-allow-internal"
  network = google_compute_network.main.self_link

  allow {
    protocol = "tcp"
  }
  source_ranges = ["10.0.0.0/8"]
}

resource "google_compute_firewall" "allow_icmp" {
  name    = "${var.prefix}-network-allow-icmp"
  network = google_compute_network.main.self_link

  allow {
    protocol = "icmp"
  }
  source_ranges = ["0.0.0.0/0"]
}

resource "google_compute_firewall" "allow_public_cockroach" {
  name    = "${var.prefix}-network-allow-public-cockroach"
  network = google_compute_network.main.self_link

  allow {
    protocol = "tcp"
    ports    = ["26257"]
  }
  source_ranges = ["0.0.0.0/0"]
}