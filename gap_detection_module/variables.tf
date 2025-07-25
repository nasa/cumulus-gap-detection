variable "DEPLOY_NAME" {
  type        = string
  description = "Deployment name used as a prefix for gap detection resources"
}

variable "engine_version" {
  description = "Postgres engine version for cluster"
  default     = "15.4"
  type        = string
}

variable "backup_window" {
  description = "Preferred database backup window (UTC)"
  type        = string
  default     = "07:00-09:00"
}

variable "apply_immediately" {
  description = "If true, RDS will apply updates to cluster immediately, instead of in the maintenance window"
  type        = bool
  default     = false
}

variable "deletion_protection" {
  description = "Flag to prevent terraform from making changes that delete the database in CI"
  type        = bool
  default     = true
}



variable "snapshot_identifier" {
  description = "Snapshot identifer to create/restore database from a snapshot"
  default     = null
}

variable "db_admin_username" {
  description = "Username for RDS database administrator authentication"
  type        = string
  default     = "postgres"
}

variable "sqs_trigger_process_gaps_batch_size" {
  description = "Number of messages in a batch sent from the SQS queue to the processGaps lambda function"
  type        = string
  default     = 10000
}

variable "sqs_trigger_max_batch_window" {
  description = "The maximum amount of time to gather records before invoking the function, in seconds"
  type        = string
  default     = 10
}

variable "security_group_ids" {
  type        = list(string)
  description = "Security Group IDs for Lambdas"
}

variable "security_group_id" {
  type = string

}

variable "subnet_ids" {
  type        = list(string)
  description = "Subnets to assign to the lambdas"
}
variable "lambda_processing_role_name" {
  type        = string
  description = "Cumulus module output for lambda processing role name e.g cumulus.outputs.lambda_processing_role_name"
}

variable "lambda_processing_role_arn" {
  type        = string
  description = "Cumulus module output for lambda processing role arn e.g cumulus.outputs.lambda_processing_role_arn"
}

variable "vpc_id" {
  type        = string
  description = "Associates security group names with a specific VPC id"
}

variable "report_granules_topic_arn" {
  type = string
}

variable "state_machine_name_lst" {
  description = "List of state machine names used to construct prefixes for ingest filter policy"
  type        = list(string)
}

