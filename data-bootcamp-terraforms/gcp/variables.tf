variable "project_id" {
  description = "project id"
}
variable "location" {
  description = "location"
}

variable "region" {
  description = "region"
}

#Service Account
variable "name" {
  description = "Name for the service account"
} 

#GKE
variable "gke_num_nodes" {
  default     = 2
  description = "number of gke nodes"
}

variable "machine_type" {
  type = string
  default = "n1-standard-1"
}

# CloudSQL
variable "instance_name" {
  description = "Name for the sql instance database"
  default     = "data-bootcamp-pf2"
}

variable "database_version" {
  description = "The MySQL, PostgreSQL or SQL Server (beta) version to use. "
  default     = "POSTGRES_12"
}

variable "instance_tier" {
  description = "Sql instance tier"
  default     = "db-f1-micro"
}

variable "disk_space" {
  description = "Size of the disk in the sql instance"
  default     = 10
}

variable "database_name" {
  description = "Name for the database to be created"
  default     = "airflow_pf"
}

variable "db_username" {
  description = "Username credentials for root user"
  default     = "de_pf"
}
variable "db_password" {
  description = "Password credentials for root user"
  default     = "1220139"
}

#CloudStorage
variable "bucket" {
  description = "Name of the bucket"
} 