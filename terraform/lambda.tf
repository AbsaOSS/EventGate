resource "aws_security_group" "event_gate_sg" {
  name = "${var.resource_prefix}event-gate-sg"
  description = "SG for Event Gate"
  vpc_id = var.vpc_id
  tags = {"BuiltBy" = "Terraform"}
}

resource "aws_vpc_security_group_egress_rule" "allow_all_traffic_ipv4" {
  security_group_id = aws_security_group.event_gate_sg.id
  cidr_ipv4 = "0.0.0.0/0"
  ip_protocol = "-1"
}

data "aws_s3_object" "event_gate_lambda_zip" {
  count  = var.lambda_package_type == "Zip" ? 1 : 0
  bucket = var.lambda_src_s3_bucket
  key    = var.lambda_src_s3_key
}

resource "aws_lambda_function" "event_gate_lambda" {
  function_name = "${var.resource_prefix}event-gate-lambda"
  role = var.lambda_role_arn
  architectures = ["x86_64"]
  timeout = 60
  runtime = "python3.12"
  package_type = var.lambda_package_type
  
  s3_bucket = var.lambda_package_type == "Zip" ? var.lambda_src_s3_bucket : null
  s3_key = var.lambda_package_type == "Zip" ? var.lambda_src_s3_key : null
  handler = var.lambda_package_type == "Zip" ? "event_gate_lambda.lambda_handler" : null
  source_code_hash = var.lambda_package_type == "Zip" ? data.aws_s3_object.event_gate_lambda_zip[0].etag : null
  
  image_uri = var.lambda_package_type == "Image" ? var.lambda_src_ecr_image : null
  
  vpc_config {
    subnet_ids = var.lambda_vpc_subnet_ids
    security_group_ids = [aws_security_group.event_gate_sg.id]
  }
  tags = {"BuiltBy" = "Terraform"}
  
  environment {
   variables = {
    LOG_LEVEL = "INFO"
   }
  }
}
