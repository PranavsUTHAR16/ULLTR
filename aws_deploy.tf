# ==============================================================================
# ULLTR System - AWS Terraform Deployment (Mumbai ap-south-1)
# ==============================================================================
# This Terraform configuration provisions:
# 1. An Elastic IP (EIP) for a static Upstox whitelisted broker access.
# 2. A Security Group with minimal ports (SSH only).
# 3. An EC2 Instance of type m7i-flex.large using Ubuntu 22.04 LTS.
# ==============================================================================

terraform {
  required_version = ">= 1.0.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }
}

provider "aws" {
  region = "ap-south-1" # Mumbai Region
}

# 1. VPC Data Source (Default VPC)
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# 2. Dynamic AMI Lookup (Ubuntu 22.04 LTS)
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# 3. Security Group
resource "aws_security_group" "ulltr_sg" {
  name        = "ulltr-system-security-group"
  description = "Security Group for ULLTR Option Ingestor & Reconciler VM"
  vpc_id      = data.aws_vpc.default.id

  # SSH Inbound (SSH port 22)
  ingress {
    description = "SSH Inbound"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # All Outbound (required for Upstox feed websockets, Redis connections, NTP sync)
  egress {
    description = "All Outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "ULLTR-System-SG"
  }
}

# 4. EC2 Instance (m7i-flex.large)
resource "aws_instance" "ulltr_vm" {
  ami           = data.aws_ami.ubuntu.id
  instance_type = "m7i-flex.large" # 2 vCPU, 8 GB RAM, Sapphire Rapids
  key_name      = "openalgo-aws-key"

  # Root disk size and type (GP3 recommended for database/log operations)
  root_block_device {
    volume_size           = 25 # 25 GB GP3 SSD
    volume_type           = "gp3"
    iops                  = 3000
    throughput            = 125
    delete_on_termination = true
  }

  # Associate Security Group
  vpc_security_group_ids = [aws_security_group.ulltr_sg.id]

  # User data to bootstrap timezone and prep directories
  user_data = <<-EOF
              #!/bin/bash
              sudo timedatectl set-timezone Asia/Kolkata
              sudo apt update && sudo apt upgrade -y
              mkdir -p /Users/prana/Desktop/open_source/web
              mkdir -p /Users/prana/Desktop/black_box/Rho_Phi_Nifty
              mkdir -p /Users/prana/Desktop/black_box/bord
              EOF

  tags = {
    Name = "ULLTR-System-VM"
  }

  # Prevent accidental termination
  disable_api_termination = false
}

# 5. Elastic IP Allocation
resource "aws_eip" "ulltr_eip" {
  domain = "vpc"

  tags = {
    Name = "ULLTR-Static-EIP"
  }
}

# 6. EIP Association with EC2
resource "aws_eip_association" "eip_assoc" {
  instance_id   = aws_instance.ulltr_vm.id
  allocation_id = aws_eip.ulltr_eip.id
}

# ==============================================================================
# Outputs
# ==============================================================================
output "instance_id" {
  description = "The ID of the EC2 instance"
  value       = aws_instance.ulltr_vm.id
}

output "elastic_ip" {
  description = "The reserved static Elastic IP for Whitelisting"
  value       = aws_eip.ulltr_eip.public_ip
}

output "ssh_instruction" {
  description = "SSH Command"
  value       = "ssh -i /path/to/your-key.pem ubuntu@${aws_eip.ulltr_eip.public_ip}"
}

# ==============================================================================
# EC2 Automatic Start/Stop Scheduler (EventBridge & Lambda)
# ==============================================================================

# 1. Archive the Lambda script dynamically
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/aws_scheduler_lambda.py"
  output_path = "${path.module}/aws_scheduler_lambda.zip"
}

# 2. IAM Role for Lambda Function
resource "aws_iam_role" "lambda_scheduler_role" {
  name = "ulltr-lambda-scheduler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

# 3. IAM Policy for Lambda (EC2 start/stop and CloudWatch logging)
resource "aws_iam_policy" "lambda_scheduler_policy" {
  name        = "ulltr-lambda-scheduler-policy"
  description = "Permissions to start/stop ULLTR EC2 instance and write logs"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "ec2:StartInstances",
          "ec2:StopInstances",
          "ec2:DescribeInstances"
        ]
        Resource = "*"
      }
    ]
  })
}

# 4. Attach Policy to Role
resource "aws_iam_role_policy_attachment" "lambda_scheduler_attach" {
  role       = aws_iam_role.lambda_scheduler_role.name
  policy_arn = aws_iam_policy.lambda_scheduler_policy.arn
}

# 5. Lambda Function
resource "aws_lambda_function" "scheduler_lambda" {
  filename         = data.archive_file.lambda_zip.output_path
  function_name    = "ulltr-ec2-scheduler"
  role             = aws_iam_role.lambda_scheduler_role.arn
  handler          = "aws_scheduler_lambda.lambda_handler"
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  runtime          = "python3.10"
  timeout          = 30

  environment {
    variables = {
      INSTANCE_ID = aws_instance.ulltr_vm.id
    }
  }
}

# 6. EventBridge Rule for Start Schedule (MON-FRI at 03:00 UTC / 08:30 IST)
resource "aws_cloudwatch_event_rule" "start_rule" {
  name                = "ulltr-vm-start-rule"
  description         = "Starts the ULLTR EC2 instance at 08:30 AM IST (03:00 AM UTC) Mon-Fri"
  schedule_expression = "cron(0 3 ? * MON-FRI *)"
}

resource "aws_cloudwatch_event_target" "start_target" {
  rule      = aws_cloudwatch_event_rule.start_rule.name
  target_id = "start-ulltr-vm"
  arn       = aws_lambda_function.scheduler_lambda.arn
  input     = jsonencode({ action = "START" })
}

resource "aws_lambda_permission" "allow_eventbridge_start" {
  statement_id  = "AllowExecutionFromEventBridgeStart"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.scheduler_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.start_rule.arn
}

# 7. EventBridge Rule for Stop Schedule (MON-FRI at 11:00 UTC / 16:30 IST)
resource "aws_cloudwatch_event_rule" "stop_rule" {
  name                = "ulltr-vm-stop-rule"
  description         = "Stops the ULLTR EC2 instance at 04:30 PM IST (11:00 AM UTC) Mon-Fri"
  schedule_expression = "cron(0 11 ? * MON-FRI *)"
}

resource "aws_cloudwatch_event_target" "stop_target" {
  rule      = aws_cloudwatch_event_rule.stop_rule.name
  target_id = "stop-ulltr-vm"
  arn       = aws_lambda_function.scheduler_lambda.arn
  input     = jsonencode({ action = "STOP" })
}

resource "aws_lambda_permission" "allow_eventbridge_stop" {
  statement_id  = "AllowExecutionFromEventBridgeStop"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.scheduler_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.stop_rule.arn
}
