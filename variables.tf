variable "credentials" {
  description = "My Credentials"
  default     = "./mycreds.json"
}

variable "project" {
  description = "Project"
  default     = "dltdemo"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "us-west1"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default     = "yf"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "dataenggzoomcamp_proj_yf"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}
