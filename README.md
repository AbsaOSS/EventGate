# EventGate
Python lambda for sending well-defined messages to confluent kafka
assumes AWS Deployment with API Gateway exposure of endpoint

<!-- toc -->
- [Lambda itself](#lambda-itself)
- [API](#api)
- [Config](#config)
- [Terraform Deplyoment](#terraform-deplyoment)
- [Scripts](#scripts)
<!-- tocstop -->

## Lambda itself
Hearth of the solution lives in the Src folder

## API
POST 🔒 method is guarded by JWT token in standard header "bearer"

| Method  | Endpoint              | Info                                                                         |
|---------|-----------------------|------------------------------------------------------------------------------|
| GET     | `/api`                | OpenAPI 3 definition                                                         |
| GET     | `/token`              | forwards (HTTP303) caller to where to obtain JWT token for posting to topic |
| GET     | `/topics`             | lists available topics                                                       |
| GET     | `/topics/{topicName}` | schema for given topic                                                       |
| POST 🔒  | `/topics/{topicName}` | posts payload (after authorization and schema validation) into kafka topic   |
| POST    | `terminate`           | kills lambda - useful for when forcing config reload is desired              |


## Config
There are 3 configs for this solution (in conf folder)

 - config.json
 -   this one needs to live in the conf folder
 -   defines where are other resources/configs
 -   for SASL_SSL also points to required secrets
 - access.json
 -   this one could be local or in AWS S3
 -   defines who has access to post to individual topics
 - topics.json
 -   this one could be local or in AWS S3
 -   defines schema of the topics, as well as enumerates those

## Terraform Deplyoment

Whole solution expects to be deployed as lambda in AWS,
there are prepared terraform scripts to make initial deplyoment, and can be found in "terraform" folder

### Zip lambda

Designated for use without authentication towards kafka

 - create **zip** archive `scripts/prepare.deplyoment.sh`
 - upload **zip** to **S3**
 - provide terraform variables with tfvars
 -   `aws_region`
 -   `vpc_id`
 -   `vpc_endpoint`
 -   `resource prefix`
 -     all terraform resources would be prefixed my this
 -   `lambda_role_arn `
 -     the role for the lambda
 -     should be able to make HTTP calls to wherever kafka server lives
 -   `lambda_vpc_subnet_ids`
 -   `lambda_package_type`
 -     `Zip`
 -   `lambda_src_s3_bucket `
 -     the bucket where **zip** is already uploaded
 -   `lambda_src_s3_key`
 -     name of already uploaded **zip**
 -   `lambda_src_ecr_image`
 -     ignored
 - `terraform apply`
 
### Containerized lambda

Designated for use with kerberizes SASL_SSL authentication towards kafka, as it requires custom librdkafka compilation

 - build docker (**[follow comments at the top of Dockerfile](./Dockerfile)**)
 - upload docker **image** to **ECR**
 - provide terraform variables with tfvars
 -   `aws_region`
 -   `vpc_id`
 -   `vpc_endpoint`
 -   `resource prefix`
 -     all terraform resources would be prefixed my this
 -   `lambda_role_arn `
 -     the role for the lambda
 -     should be able to make HTTP calls to wherever kafka server lives
 -   `lambda_vpc_subnet_ids`
 -   `lambda_package_type`
 -     `Image`
 -   `lambda_src_s3_bucket `
 -     ignored
 -   `lambda_src_s3_key`
 -     ignored
 -   `lambda_src_ecr_image`
 -     already uploaded **image** in **ECR**
 - `terraform apply`

## Scripts
Useful scripts for dev and Deployment

### Notebook
Jupyter notebook, with one cell for lambda initialization and one cell per method, for testing purposes
Obviously using it requires correct configs to be in place (PUBLIC key is being loaded during initilization)

### Preapare Deployment
Shell script for fetching python requirements and ziping it together with sources and config into lambda archive
it needs to be uploaded to s3 bucket first before running the terraform.
