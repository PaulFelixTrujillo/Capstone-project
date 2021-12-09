resource "google_service_account" "airflow-pf" {
  account_id   = "airflow-pf"
  display_name = "airflow-pf"
}

resource "google_project_iam_member" "service_account_bucket" {
  project       = "gcp-terraform-pf"
  role          = "roles/storage.objectAdmin"
  member        = "serviceAccount:${google_service_account.airflow-pf.email}"
}