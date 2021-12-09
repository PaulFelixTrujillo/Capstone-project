resource "google_storage_bucket" "raw_bucket" {
  name          = "db-raw-bucket-pf"
  force_destroy = "true"
  location      = "us-central1"
  project       = "gcp-terraform-pf"
  storage_class = "standard"
}

resource "google_storage_bucket_iam_member" "bucket_creator" {
  bucket        = "${google_storage_bucket.raw_bucket.name}"
  role          = "roles/storage.objectAdmin"
  member        = "serviceAccount:airflow-pf@gcp-terraform-pf.iam.gserviceaccount.com"
}