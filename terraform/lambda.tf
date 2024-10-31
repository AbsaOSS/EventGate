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

resource "aws_lambda_function" "event_gate_lambda" {
  s3_bucket = var.lambda_source_bucket
  s3_key = "lambda_function.zip"
  function_name = "${var.resource_prefix}event-gate-lambda"
  role = var.lambda_role_arn
  handler = "event_gate_lambda.lambda_handler"
  runtime = "python3.12"
  vpc_config {
    subnet_ids = var.lambda_vpc_subnet_ids
    security_group_ids = [aws_security_group.event_gate_sg.id]
  }
  tags = {"BuiltBy" = "Terraform"}
}
