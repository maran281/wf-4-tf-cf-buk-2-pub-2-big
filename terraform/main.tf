provider "google" {
  project = "plated-hash-405319"
  region = "us-east1"
}

#resource which stores the TF state file in the bucket
terraform {
  backend "gcs" {
    bucket = "tf_statebucket_4_wf_4_tf_buk_2_pub_big"
    prefix = "value"
  }
}

#Creating a zip file from the code files
data "archive_file" "cfFiles" {

  type = "zip"
  output_path = "../code/main.zip"

  source {
    content = "../code/main.py"
    filename = "main.py"
  }

  source {
    content = "../code/requirements.txt"
    filename = "requirements.txt"
  }
}

#Bucket which stores the souce code for the cloud function
resource "google_storage_bucket" "cf_sc_4_wf_4_tf_buk_2_pub_big" {
  name = "bucket_sourcecode_4_wf_4_tf_buk_2_pub_big"
  location = "us-east1"
}

#resource which places the source code object for the cloud function from local to the bucket
resource "google_storage_bucket_object" "cf_sourcecodeobject_4_wf_4_tf_buk_2_pub_big" {
    name = "main.zip"
    bucket = google_storage_bucket.cf_sc_4_wf_4_tf_buk_2_pub_big.name 
    source = "../code/main.zip"
}

#Bucket which acts as a source. Cloud function from where CF picks the file and process the data
resource "google_storage_bucket" "cf_sourcefilebucket_4_wf_4_tf_buk_2_pub_big" {
  name = "bucket_sourcefile_4_wf_4_tf_buk_2_pub_big"
  location = "us-east1"
}

#Bucket which acts as a target for CF. After processing the data CF places a file in this bucket
resource "google_storage_bucket" "cf_targetfilebucket_4_wf_4_tf_buk_2_pub_big" {
  name = "bucket_targetfile_4_wf_4_tf_buk_2_pub_big"
  location = "us-east1"
}

#Pubsub topic where CF will push the messages
resource "google_pubsub_topic" "cf_pubsub_4_wf_4_tf_buk_2_pub_big" {
  name = "pubsub_4_wf_4_tf_buk_2_pub_big"
}

#pull subscription
resource "google_pubsub_subscription" "pubsub_subscription_4_wf_4_tf_buk_2_pub_big" {
  name = "pubsub_subscription_4_wf_4_tf_buk_2_pub_big"
  topic = google_pubsub_topic.cf_pubsub_4_wf_4_tf_buk_2_pub_big.name
}

#Bigquery dataset where CF will publish the data
resource "google_bigquery_dataset" "bq_dataset_4_wf_4_tf_buk_2_pub_big" {
    dataset_id = "bq_dataset_4_wf_4_tf_buk_2_pub_big_id"
    project = "plated-hash-405319"
    location = "us-east1"
    default_table_expiration_ms = 3600000

    labels = {
      environment="development_env"
    }
}

resource "google_cloudfunctions_function" "cf_4_wf_4_tf_buk_2_pub_big_1" {
  name = "cf_4_wf_4_tf_buk_2_pub_big"
  runtime = "python310"
  source_archive_bucket = google_storage_bucket.cf_sc_4_wf_4_tf_buk_2_pub_big.name
  source_archive_object = google_storage_bucket_object.cf_sourcecodeobject_4_wf_4_tf_buk_2_pub_big.name
  entry_point = "publish_message"

  event_trigger {
    event_type = "google.storage.object.finalize"
    resource = google_storage_bucket.cf_sourcefilebucket_4_wf_4_tf_buk_2_pub_big.name
  }

  service_account_email = "my-wf-4-tf-cf-buk-2-pub-2-big@plated-hash-405319.iam.gserviceaccount.com"
}

resource "google_cloudfunctions_function_iam_member" "invoker" {
  cloud_function = google_cloudfunctions_function.cf_4_wf_4_tf_buk_2_pub_big_1.name
  member = "allUsers"
  role = "roles/cloudfunctions.invoker"
  depends_on = [ google_cloudfunctions_function.cf_4_wf_4_tf_buk_2_pub_big_1 ]
  
}
