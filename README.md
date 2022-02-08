# CloudServiceBenchmarking
This is a repository for the university project "Cloud Service Benchmarking" attended during the winter semester 2021/2022 at the Technical University Berlin. This project teaches practical experience and is stronlgy based on a book published by the projects lecturer Prof. David Bermbach ([Cloud Service Benchmarking (2017)](https://link.springer.com/book/10.1007/978-3-319-55483-9)). The goal is to developed a benchmarking tool for a open source project that we can deploy in a cloud enviroment and perform selfconstructed benchmarks against this system to answer some self posed questions. 

I decided to write my benchmark tool in Java and use a CockroachDB cluster as my system under test. In this case, I wanted to test the cost efficeny of scaling up vs. scaling out of the open source distributed RDBMS CockroachDB. For this I will use a 'light' version of the old tpc_w db scheme to represent a simple e-commerce book shop. (DB schema can be found at /src/main/ressources/dbinit.sql)


## How to run this?
This project can be deployed via Terraform. The configuration files can be found in the directory /terraform. You need to create a file called `/terraform/terraform.tfvars` that will look like this:

```
gcp_project_id = <<GCP Project ID>
gcp_service_account_path = <<path to servcie account credential json>>
gcp_account_email = <<gcp account email for the deployment of gcps ops agent e.g.: "XXXXXXXXXXX-compute@developer.gserviceaccount.com">> 
path_private_key = "/path/to/private/key/key"
path_public_key  = "/path/to/public/key/key.pub"
```

You may also be required to login via the gcloud cli and set this project as your default login.

The file `/terraform/variables.tf` contains some configuration parameters for the deployment and also for the benchmark run. In this file the `local_path_to_jar_file` variable should be changed to reflect the path to the pre-compiled .jar file included in the release version.

Afterwards the deployment is started like normal Terraform deployments with executing `terraform apply`. When succesfully run, Terraform will output dynamically produced commands that can be used to process further. This includes the following three command sets to connect to the just created machines and 
1. start the load phase, 
2. start the benchmark run and 
3. collecting the results via sftp.

The output section will also include a link to the web ui of the CockroachDB Cluster. The Cluster should be automatically installed, configured and initialized during the Terraform run and this web ui should be accessible imediatly to test if everything was set up correctly and monitoring the next steps.

In generel, there should be not further interefence needed to run this benchmark than the steps meantioned above. The created vms should be configured by Terraform (cockroach and benchmark vms), including firewall rules, az spanning networks etc. and the CockroachDB cluster should also be started automatically at the end of the Terraform run. The benchmark vms should contain the pre-compiled .jar file that will be automatically uploaded to the machines and dynamically configured files named `runLoad.sh` and `runBenchmark.sh` that can be simply executed to start the actual benchmark run.

## Results

The result files exceeds the normal size limitations and therefore [uploaded to our university file storage](https://tubcloud.tu-berlin.de/s/tsQqNQkgqyWppgp).

TLDR: For my choosen use case the system is not scaling at all. Reasons for this lie in serveral aspects. The most prominent one could be, that the choosen database schema turned out to be not suitable for the way CockroachDB is distributing the data internally. Therefore leading to a single hot node which daramatically impacts the performance of the entire cluster.
