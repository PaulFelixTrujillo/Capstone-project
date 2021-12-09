resource "google_storage_bucket" "staging_bucket" {
  name          = "db-staging-bucket-pf"
  force_destroy = "true"
  location      = "us-central1"
  project       = "gcp-terraform-pf"
  storage_class = "standard"
}

resource "google_storage_bucket_iam_member" "bucket_creator" {
  bucket        = "${google_storage_bucket.staging_bucket.name}"
  role          = "roles/storage.objectAdmin"
  member        = "serviceAccount:airflow-pf@gcp-terraform-pf.iam.gserviceaccount.com"
}