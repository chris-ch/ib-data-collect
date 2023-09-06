/**
* Copyright 2021 Google LLC
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

data "google_project" "project" {
  project_id = var.GOOGLE_PROJECT_ID
}


module "project-services" {
  source = "terraform-google-modules/project-factory/google//modules/project_services"
  version = "13.0.0"
  disable_services_on_destroy = false

  project_id = var.GOOGLE_PROJECT_ID
  enable_apis = var.enable_apis

  activate_apis = [
    "compute.googleapis.com",
    "cloudapis.googleapis.com",
    "cloudbuild.googleapis.com",
    "run.googleapis.com",
  ]
}

resource "random_id" "default" {
  byte_length = 8
}

resource "google_storage_bucket" "default" {
  name = "${random_id.default.hex}-gcf-source"
  # Every bucket name must be globally unique
  location = "US"
  uniform_bucket_level_access = true
  project = var.GOOGLE_PROJECT_ID
}

data "archive_file" "server_files" {
  type = "zip"
  output_path = "/tmp/function-source.zip"
  source_dir = "app/"
}

resource "google_storage_bucket_object" "object" {
  name = "function-source.zip"
  bucket = google_storage_bucket.default.name
  source = data.archive_file.server_files.output_path
}

resource "google_cloudfunctions2_function" "http_services" {
  name = "${var.deployment_name}-http-services"
  provider = google-beta
  description = "HTTP services"
  location = var.GOOGLE_REGION
  project = var.GOOGLE_PROJECT_ID

  build_config {
    runtime = "python311"
    entry_point = "main"
    source {
      storage_source {
        bucket = google_storage_bucket.default.name
        object = google_storage_bucket_object.object.name
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory = "256M"
    timeout_seconds = 60
    environment_variables = {
      GOOGLE_PROJECT_ID = var.GOOGLE_PROJECT_ID
      GOOGLE_MESSAGING_SENDER_ID = var.GOOGLE_MESSAGING_SENDER_ID
      GOOGLE_API_KEY = var.GOOGLE_API_KEY
    }
  }
}

resource "google_cloud_run_service_iam_member" "http-services" {
  location = google_cloudfunctions2_function.http_services.location
  project = google_cloudfunctions2_function.http_services.project
  service = google_cloudfunctions2_function.http_services.name
  role = "roles/run.invoker"
  member = "allUsers"
}
