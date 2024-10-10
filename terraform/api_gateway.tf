resource "aws_api_gateway_rest_api" "event_gate_api" {
  name = "${var.resource_prefix}event-gate-api"
  description = "API for EventGate"
  tags = {"BuiltBy" = "Terraform"}
  endpoint_configuration {
    types = ["PRIVATE"]
    vpc_endpoint_ids = [var.vpc_endpoint]
  }
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = "execute-api:Invoke",
        Resource = "*",
        Principal = "*"
		Condition = {
          StringEquals = {
            "aws:sourceVpce" = var.vpc_endpoint
          }
        }
      }
    ]
  })
}

resource "aws_api_gateway_resource" "event_gate_api_api" {
  rest_api_id = aws_api_gateway_rest_api.event_gate_api.id
  parent_id = aws_api_gateway_rest_api.event_gate_api.root_resource_id
  path_part = "api"
}

resource "aws_api_gateway_method" "event_gate_api_api_get" {
  rest_api_id = aws_api_gateway_rest_api.event_gate_api.id
  resource_id = aws_api_gateway_resource.event_gate_api_api.id
  authorization = "NONE"
  http_method  = "GET"
}

resource "aws_api_gateway_integration" "event_gate_api_api_get_integration" {
  rest_api_id = aws_api_gateway_rest_api.event_gate_api.id
  resource_id = aws_api_gateway_resource.event_gate_api_api.id
  http_method = aws_api_gateway_method.event_gate_api_api_get.http_method
  integration_http_method = "POST"
  type = "AWS_PROXY"
  uri = aws_lambda_function.event_gate_lambda.invoke_arn
}

resource "aws_api_gateway_resource" "event_gate_api_token" {
  rest_api_id = aws_api_gateway_rest_api.event_gate_api.id
  parent_id = aws_api_gateway_rest_api.event_gate_api.root_resource_id
  path_part = "token"
}

resource "aws_api_gateway_method" "event_gate_api_token_get" {
  rest_api_id = aws_api_gateway_rest_api.event_gate_api.id
  resource_id = aws_api_gateway_resource.event_gate_api_token.id
  authorization = "NONE"
  http_method  = "GET"
}

resource "aws_api_gateway_integration" "event_gate_api_token_get_integration" {
  rest_api_id = aws_api_gateway_rest_api.event_gate_api.id
  resource_id = aws_api_gateway_resource.event_gate_api_token.id
  http_method = aws_api_gateway_method.event_gate_api_token_get.http_method
  integration_http_method = "POST"
  type = "AWS_PROXY"
  uri = aws_lambda_function.event_gate_lambda.invoke_arn
}

resource "aws_api_gateway_resource" "event_gate_api_topics" {
  rest_api_id = aws_api_gateway_rest_api.event_gate_api.id
  parent_id = aws_api_gateway_rest_api.event_gate_api.root_resource_id
  path_part = "topics"
}

resource "aws_api_gateway_method" "event_gate_api_topics_get" {
  rest_api_id = aws_api_gateway_rest_api.event_gate_api.id
  resource_id = aws_api_gateway_resource.event_gate_api_topics.id
  authorization = "NONE"
  http_method = "GET"
}

resource "aws_api_gateway_integration" "event_gate_api_topics_get_integration" {
  rest_api_id = aws_api_gateway_rest_api.event_gate_api.id
  resource_id = aws_api_gateway_resource.event_gate_api_topics.id
  http_method = aws_api_gateway_method.event_gate_api_topics_get.http_method
  integration_http_method = "POST"
  type = "AWS_PROXY"
  uri = aws_lambda_function.event_gate_lambda.invoke_arn
}

resource "aws_api_gateway_resource" "event_gate_api_topic_name" {
  rest_api_id = aws_api_gateway_rest_api.event_gate_api.id
  parent_id = aws_api_gateway_resource.event_gate_api_topics.id
  path_part = "{topic_name}"
}

resource "aws_api_gateway_method" "event_gate_api_topic_name_get" {
  rest_api_id = aws_api_gateway_rest_api.event_gate_api.id
  resource_id = aws_api_gateway_resource.event_gate_api_topic_name.id
  authorization = "NONE"
  http_method = "GET"
  request_parameters = {
    "method.request.path.topic_name" = true
  }
}

resource "aws_api_gateway_integration" "event_gate_api_topic_name_get_integration" {
  rest_api_id = aws_api_gateway_rest_api.event_gate_api.id
  resource_id = aws_api_gateway_resource.event_gate_api_topic_name.id
  http_method = aws_api_gateway_method.event_gate_api_topic_name_get.http_method
  integration_http_method = "POST"
  type = "AWS_PROXY"
  uri = aws_lambda_function.event_gate_lambda.invoke_arn
}

resource "aws_api_gateway_method" "event_gate_api_topic_name_post" {
  rest_api_id = aws_api_gateway_rest_api.event_gate_api.id
  resource_id = aws_api_gateway_resource.event_gate_api_topic_name.id
  authorization = "NONE"
  http_method = "POST"
  request_parameters = {
    "method.request.path.topic_name" = true
  }
}

resource "aws_api_gateway_integration" "event_gate_api_topic_name_post_integration" {
  rest_api_id = aws_api_gateway_rest_api.event_gate_api.id
  resource_id = aws_api_gateway_resource.event_gate_api_topic_name.id
  http_method = aws_api_gateway_method.event_gate_api_topic_name_post.http_method
  integration_http_method = "POST"
  type = "AWS_PROXY"
  uri = aws_lambda_function.event_gate_lambda.invoke_arn
}

resource "aws_api_gateway_resource" "event_gate_api_terminate" {
  rest_api_id = aws_api_gateway_rest_api.event_gate_api.id
  parent_id = aws_api_gateway_rest_api.event_gate_api.root_resource_id
  path_part = "terminate"
}

resource "aws_api_gateway_method" "event_gate_api_terminate_post" {
  rest_api_id = aws_api_gateway_rest_api.event_gate_api.id
  resource_id = aws_api_gateway_resource.event_gate_api_terminate.id
  authorization = "NONE"
  http_method  = "POST"
}

resource "aws_api_gateway_integration" "event_gate_api_terminate_post_integration" {
  rest_api_id = aws_api_gateway_rest_api.event_gate_api.id
  resource_id = aws_api_gateway_resource.event_gate_api_terminate.id
  http_method = aws_api_gateway_method.event_gate_api_terminate_post.http_method
  integration_http_method = "POST"
  type = "AWS_PROXY"
  uri = aws_lambda_function.event_gate_lambda.invoke_arn
}

resource "aws_lambda_permission" "event_gate_api_lambda_permissions" {
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.event_gate_lambda.function_name
  principal = "apigateway.amazonaws.com"
  source_arn = "${aws_api_gateway_rest_api.event_gate_api.execution_arn}/*"
}

resource "aws_api_gateway_deployment" "event_gate_api_deployment" {
  rest_api_id = aws_api_gateway_rest_api.event_gate_api.id
  triggers = {
    redeployment = sha1(jsonencode([
      aws_api_gateway_integration.event_gate_api_token_get_integration,
	  aws_api_gateway_integration.event_gate_api_topics_get_integration,
	  aws_api_gateway_integration.event_gate_api_topic_name_get_integration,
	  aws_api_gateway_integration.event_gate_api_topic_name_post_integration,
	  aws_api_gateway_integration.event_gate_api_terminate_post_integration
    ]))
  }
  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_api_gateway_stage" "event_gate_api_stage" {
  rest_api_id = aws_api_gateway_rest_api.event_gate_api.id
  deployment_id = aws_api_gateway_deployment.event_gate_api_deployment.id
  stage_name = "DEV"
  tags = {"BuiltBy" = "Terraform"}
}
