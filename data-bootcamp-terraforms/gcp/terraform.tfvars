project_id = "gcp-terraform-pf"
region     = "us-central1"
location     = "us-central1-a"

#Service Account
name = "airflow-pf"

#GKE
gke_num_nodes = 2
machine_type  = "n1-standard-1"

#CloudSQL
instance_name     = "data-bootcamp-pf2"
database_version  = "POSTGRES_12"
instance_tier     = "db-f1-micro"
disk_space        = 10
database_name     = "airflow_pf"
db_username       = "de_pf"
db_password       = "1220139"

#CloudStorage

bucket = "de-bucket-terraform-pf"