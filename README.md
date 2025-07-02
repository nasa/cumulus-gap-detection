This module allows for the installation of the GES DISC cumulus gap detection module. Upon deployment the following documentation located here https://wiki.earthdata.nasa.gov/display/GESDISCUMU/How+to+run+Gap+Detection provides an indepth breakdown of the API endpoints used to configure which collections to monitor gaps for, manage and update known gaps, and retrieve gap reports.

## Installation

The primary way to install this module is to reference a stable github release. Please note that the source code and layers are packaged alongside the module it self in the release artifact. Below is an example module configuration. 


```hcl
module "gesdisc-cumulus-gap-detection" {
   source      = "https://github.com/nasa/official_public_repo/releases/download/v1.0.0/gesdisc_cumulus_gap_detection_06-03.zip"
   DEPLOY_NAME = var.DEPLOY_NAME # Deployment name used as a prefix for gap detection resources
   vpc_id = data.aws_vpc.application_vpcs.id # Associates security group names with a specific VPC id
   report_granules_topic_arn = data.terraform_remote_state.cumulus.outputs.report_granules_sns_topic_arn # Cumulus module output for report granules arn
   lambda_processing_role_name = data.aws_iam_role.lambda_iam_role.id # Cumulus module output for lambda processing role
   lambda_processing_role_arn = data.terraform_remote_state.cumulus.outputs.lambda_processing_role_arn # Cumulus module output for lambda processing role arn
   security_group_ids = [data.aws_security_group.security_group.id] # Desired aws security group ids for lambdas
   security_group_id =  data.aws_security_group.security_group.id # Security group id
   subnet_ids = data.aws_subnets.subnet_ids.ids # Desired subnet ids for lambdas 
   component_metadata_state_machine_name = data.aws_sfn_state_machine.component_metadata_state_machine.name # Component metadata state machine name
   component_cmr_state_machine_name      = data.aws_sfn_state_machine.component_cmr_state_machine.name      # Component CMR state machine name

   
   // Optional parameters
   db_admin_username = "postgres" # Defaults to postgres
   engine_version = var.engine_version # Defaults to 15.4
   backup_window =var.backup_window # Defaults to "07:00-09:00"
   apply_immediately = var.apply_immediately # Defaults to false
   deletion_protection = var.deletion_protection # Defaults to true
   snapshot_identifier = var.snapshot_identifier # Defaults to null but can be used to create the cluster from a snapshot. Please use this cautiously. 
   sqs_trigger_process_gaps_batch_size = var.sqs_trigger_process_gaps_batch_size # Defaults to 10000
   sqs_trigger_max_batch_window = var.sqs_trigger_max_batch_window # Defaults to 10 
}
```

## Post deployment procedures

Upon successful deployment you will need to manually create the gap table. This only needs to be done on the very first deployment.

```
aws lambda invoke \
  --function-name arn:aws:lambda:YOUR_REGION:YOUR_ACCOUNT_NUMBER:function:DEPLOY_NAME-gapCreateTable \
  --payload '{}' \
  output.json
```

This will create tables and indexes required before invoking API endpoints.

## Additional Notes

Calling the gapConfig endpoint against large collections greater than 1 million granules may result in API GATEWAY timeouts. If this collection exceeds the threshhold then please follow instructions under gap_detection_operations. 